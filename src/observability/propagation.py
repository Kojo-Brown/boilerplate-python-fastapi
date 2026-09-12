"""W3C context propagation, and the carrier HTTP does not give us for free.

The inbound and outbound HTTP halves are handled by the instrumentation
libraries: the ASGI instrumentation extracts a `traceparent` from the request
headers, and the httpx instrumentation injects one into every outbound call.
Both go through the *global* propagator, which is why `configure_propagation`
is the first thing `configure_observability` does — an SDK left at its default
still propagates W3C trace context, but not `baggage`, and the difference only
shows up as missing correlation in a backend weeks later.

What HTTP does not cover is everything this service publishes: a Kafka record,
an outbox row, a Redis Streams message. Those carry headers of their own, as
`(name, bytes)` pairs rather than a string mapping, and a context that is not
injected at the publish is a trace that ends at the producer and a second,
unrelated trace that starts at the consumer — the exact seam where "the request
was fast but the effect took a minute" lives.

Three details in the pair-shaped carrier are easy to get wrong:

**Duplicate names are legal.** Kafka headers are an ordered sequence, not a
mapping, and a proxy appending its own `traceparent` to one a producer already
set really happens. `MessageHeaderGetter.get` returns *every* match, in order,
because that is the `Getter` contract and because deciding what a duplicate
means is the propagator's job rather than the carrier's — which matters for
staying consistent with the HTTP side, where the same propagator reads the same
duplicates through the SDK's own header getter. As of SDK 1.44 that decision is
"the first one wins"; a `dict(headers)` carrier would have quietly made it "the
last one wins" and hidden the disagreement from any propagator that starts
enforcing the W3C rule that more than one is invalid.

**Injecting twice must replace, not append.** A record that is republished —
the retry ladder in `src/dlq` does exactly this — would otherwise accumulate a
`traceparent` per hop, and a consumer would join whichever one its propagator
happened to pick. `inject` drops the propagation fields it is about to write
before writing them, and leaves every other header where it was.

**A header value is bytes and need not be text.** A non-UTF-8 value is skipped
rather than replaced with U+FFFD: a mangled `traceparent` is not a context, and
decoding it leniently would hand the propagator something that parses to
nonsense instead of to nothing.
"""

from __future__ import annotations

from typing import Final

from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.context import Context
from opentelemetry.propagate import get_global_textmap, set_global_textmap
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.propagators.textmap import Getter, Setter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

#: Message headers as every broker in this codebase spells them: an ordered
#: sequence of pairs with byte values. Structurally identical to
#: `src.kafka.base.Headers`, and defined here rather than imported so that this
#: package does not depend on the messaging one — propagation is also what
#: `src/redis_streams` and `src/outbox` would use.
MessageHeaders = tuple[tuple[str, bytes], ...]

#: The header names W3C propagation owns. Taken from the propagators rather
#: than written out, so that a future field (the specification has added one
#: before) is dropped by `inject`'s replace step without this constant moving.
PROPAGATION_FIELDS: Final[frozenset[str]] = frozenset(
    TraceContextTextMapPropagator().fields | W3CBaggagePropagator().fields
)


class MessageHeaderGetter(Getter[MessageHeaders]):
    """Reads propagation fields out of `(name, bytes)` pairs.

    Header names are compared case-insensitively. Kafka does not define a case
    for them and nothing normalises them on the way in, so a producer in
    another language that wrote `TraceParent` is a context this service can
    still read.
    """

    def get(self, carrier: MessageHeaders, key: str) -> list[str] | None:
        wanted = key.lower()
        values: list[str] = []
        for name, value in carrier:
            if name.lower() != wanted:
                continue
            try:
                values.append(value.decode("utf-8"))
            except UnicodeDecodeError:
                # Not text, so not a context — and the whole key is abandoned
                # rather than this one value skipped, because the alternative
                # is answering with the *other* `traceparent` and silently
                # joining a trace the mangled header may have disagreed with.
                # An empty list reads as "present and unusable", which sends
                # the propagator down the same path as "absent".
                return []
        return values or None

    def keys(self, carrier: MessageHeaders) -> list[str]:
        return [name for name, _ in carrier]


class MessageHeaderSetter(Setter[list[tuple[str, bytes]]]):
    """Writes propagation fields into a mutable list of pairs."""

    def set(self, carrier: list[tuple[str, bytes]], key: str, value: str) -> None:
        carrier.append((key, value.encode("utf-8")))


message_header_getter: Final[MessageHeaderGetter] = MessageHeaderGetter()
message_header_setter: Final[MessageHeaderSetter] = MessageHeaderSetter()


def configure_propagation() -> None:
    """Install the W3C propagators globally: trace context, then baggage.

    Composite rather than either alone. `traceparent` carries the identity of
    the trace and the sampling decision; `baggage` carries whatever the caller
    attached to the request (a tenant, a feature-flag cohort) and is the only
    way for that to survive a hop. Idempotent, and safe to call before the
    providers exist — a propagator extracts a context whether or not anything
    is recording.
    """
    set_global_textmap(
        CompositePropagator([TraceContextTextMapPropagator(), W3CBaggagePropagator()])
    )


def inject_trace_context(
    headers: MessageHeaders = (), context: Context | None = None
) -> MessageHeaders:
    """`headers` with this process's current trace context stamped onto them.

    Returns a new tuple; the input is never mutated, because the caller's
    headers are usually a frozen `Headers` on a message envelope that may be
    published more than once.

    With nothing recording — the SDK switched off, or outside any span — the
    propagator writes nothing and the headers come back as they went in, minus
    any stale propagation fields. That removal is the point: republishing a
    record without a live context must not leave the *previous* context on it,
    or the consumer joins a trace that ended hours ago.
    """
    carrier: list[tuple[str, bytes]] = [
        (name, value)
        for name, value in headers
        if name.lower() not in PROPAGATION_FIELDS
    ]
    get_global_textmap().inject(carrier, context=context, setter=message_header_setter)
    return tuple(carrier)


def extract_trace_context(
    headers: MessageHeaders, context: Context | None = None
) -> Context:
    """The context carried by `headers`, for a consumer to run a span under.

    An absent, duplicated or malformed `traceparent` yields a context with no
    span in it, which is what starts a fresh trace — never an error, because a
    consumer must keep consuming records that a broken producer wrote.
    """
    return get_global_textmap().extract(
        headers, context=context, getter=message_header_getter
    )


__all__ = [
    "PROPAGATION_FIELDS",
    "MessageHeaderGetter",
    "MessageHeaderSetter",
    "MessageHeaders",
    "configure_propagation",
    "extract_trace_context",
    "inject_trace_context",
    "message_header_getter",
    "message_header_setter",
]
