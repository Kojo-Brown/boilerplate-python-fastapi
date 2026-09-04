"""The messaging contracts: what a record looks like going out and coming back.

Nothing here imports `aiokafka`. That is what lets the consumer *policy* — the
per-partition isolation, the commit points, the backoff, the shutdown — be
tested deterministically against an in-process broker, while the parts that are
genuinely Kafka (a real group rebalance, a real `__consumer_offsets` write) are
tested against a real broker in CI.

Two ports, and they are not symmetrical:

`MessagePublisher` is one method wide plus a lifecycle. A producer needs
somewhere to *put a record*; it does not need to know about partitions,
offsets or brokers, so those appear only in what `publish` hands back.

`MessageSource` is wider, and every extra method on it is there because Kafka's
consumer is not a queue client. `poll` returns a *batch* rather than a message,
because the fetcher hands back what arrived per partition and one-at-a-time
would either throw the rest away or hide a buffer. `commit` takes a mapping
rather than a message, because an offset is a per-partition watermark and not
an acknowledgement of one record. `seek` exists because a handler that failed
has to be able to make the broker send that record again — the position has
already moved past it in memory, and without a seek the record is only
redelivered after a restart or a rebalance.

## Offsets are watermarks, not acknowledgements

This is the single fact that shapes everything in this package. A queue lets
you ack message 5 and leave 4 outstanding. Kafka has one number per partition,
meaning "the next record this group will read", so committing 6 says 5 *and* 4
are done. There is no way to say otherwise.

Two consequences follow, and both are silent when you get them wrong:

- A committed offset is `record.offset + 1`. Committing `record.offset` itself
  replays that record after every restart, forever, and looks perfect in a test
  that never restarts a consumer.
- Per-*message* failure isolation — the shape `OutboxRelay` uses, where one bad
  event is retried and the rest of the batch proceeds — cannot be transplanted
  here. Skipping record 4 and committing through 5 does not retry 4; it drops
  it. So `ConsumerRunner` stops a partition at its first failure and keeps
  going on the others, which is per-partition isolation, the finest grain the
  storage model actually offers.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable

from src.exceptions import AppException

#: Kafka record headers, as they exist on the wire: an ordered sequence of
#: pairs, not a mapping. Duplicate names are legal and are used in practice —
#: a proxy appending its own trace header to one a producer already set — so
#: collapsing them into a dict would silently drop data that arrived.
Headers = tuple[tuple[str, bytes], ...]


class MessagingError(AppException):
    """Base for the failures this package produces.

    503 rather than 500: every subclass means a dependency of this service is
    unhappy, not that the request was wrong. A handler that publishes as part
    of serving a request should let this reach the client as "try again", which
    is what `AllExceptionsFilter`-style rendering does with the status code.
    """

    status_code = 503
    error_code = "MESSAGING_ERROR"

    def __init__(self, message: str, details: object = None) -> None:
        super().__init__(message, details)


class PublishError(MessagingError):
    """A record could not be handed to the broker.

    Raised for a broker that is unreachable, a topic that cannot be resolved,
    and a send that timed out. What it deliberately does *not* mean is "the
    record was not written": a timeout is an unknown, not a negative, and the
    idempotent producer is what keeps a retry of that unknown from becoming a
    duplicate.
    """

    error_code = "MESSAGE_PUBLISH_FAILED"


class ConsumerError(MessagingError):
    """The consumer could not be started, polled or committed."""

    error_code = "MESSAGE_CONSUME_FAILED"


class MessageNotSerializableError(MessagingError):
    """A payload cannot be encoded, so the caller that produced it fails.

    A 500 rather than a 503, because unlike its siblings this one is about the
    value in hand rather than the state of the broker: retrying sends the same
    unencodable object again.
    """

    status_code = 500
    error_code = "MESSAGE_NOT_SERIALIZABLE"


class MessageNotDecodableError(MessagingError):
    """A consumed record's bytes are not what this consumer expected.

    Deliberately not fatal to the loop: it is a poison record, and what happens
    to one is the runner's policy rather than the codec's.
    """

    status_code = 500
    error_code = "MESSAGE_NOT_DECODABLE"


class RetryAfter(Exception):
    """Raised by a handler that cannot process a record *yet*.

    Not a `MessagingError` and deliberately not named `...Error`: nothing has
    gone wrong. The record is fine, the handler is fine, and the only thing
    missing is time — which is why `ConsumerRunner` treats it as neither a
    success nor a failure. The partition stops at the record and is seeked back
    to it, exactly as a failure would be, but no failure is counted, no
    exponential backoff is started, and the wait is `delay` rather than a
    guess.

    That distinction matters for two reasons. `kafka.partition_stalled` is a
    warning about a partition in trouble, and a retry-tier consumer waiting
    fifteen minutes for a record that is not due is not in trouble — logging it
    as such is how a real stall gets lost among the healthy ones. And the
    runner's own backoff is full-jittered and capped at `retry_max_delay`, so a
    handler that knows the record is due in nine hundred seconds would
    otherwise be polled at a uniformly random interval up to sixty, several
    hundred times, to be told no.

    The retry ladder in `src/dlq` is the reason this exists, but it is not
    specific to it: a handler backing off a downstream that answered `429` with
    a `Retry-After` header has exactly the same thing to say.
    """

    def __init__(self, delay: float, *, reason: str | None = None) -> None:
        if delay < 0:
            raise ValueError("delay cannot be negative.")
        self.delay = delay
        self.reason = reason
        super().__init__(
            f"Not ready for {delay:.3f}s" + (f": {reason}" if reason else ".")
        )


class LifecycleError(MessagingError):
    """A publisher or source was used before `start()` or after `stop()`.

    A hard error rather than an implicit start. An implicit start would build a
    client — sockets, a background sender, a group membership — from inside
    whatever coroutine happened to publish first, so the connection's lifetime
    would be tied to a request rather than to the process, and a shutdown would
    have nothing to close.
    """

    status_code = 500
    error_code = "MESSAGING_LIFECYCLE"


@dataclass(frozen=True, slots=True, order=True)
class Partition:
    """One topic-partition. The unit everything about ordering is stated in.

    `order=True` because assignments and commit maps are logged and asserted,
    and a stable sort makes both readable. Equality and hashing come free with
    the frozen dataclass, which is what lets this be a dict key — the shape
    every commit map in this package has.
    """

    topic: str
    number: int

    def __str__(self) -> str:
        return f"{self.topic}-{self.number}"


@dataclass(frozen=True, slots=True)
class PublishedMessage:
    """What the broker acknowledged: where the record landed.

    Handed back rather than the driver's own metadata object so that callers,
    fakes and logs share one shape, and so that nothing outside this package
    holds a value whose type depends on which client library is installed.
    """

    partition: Partition
    offset: int
    timestamp: datetime

    @property
    def topic(self) -> str:
        return self.partition.topic


@dataclass(frozen=True, slots=True)
class ConsumedMessage:
    """One record, detached from the consumer that fetched it.

    `value` is `bytes | None` because a null value is not an empty one: on a
    compacted topic it is a tombstone, the record that tells the log to forget
    the key. Dropping the distinction would make `b""` and "delete this key"
    the same message.
    """

    partition: Partition
    offset: int
    key: str | None
    value: bytes | None
    headers: Headers
    timestamp: datetime

    @property
    def topic(self) -> str:
        return self.partition.topic

    @property
    def is_tombstone(self) -> bool:
        """A null value on a keyed record: "forget this key" on a compacted topic."""
        return self.value is None

    @property
    def next_offset(self) -> int:
        """The offset to commit once this record is done.

        Named rather than written as `offset + 1` at each call site, because
        the `+ 1` is the whole difference between a consumer that resumes and
        one that replays its last record after every restart.
        """
        return self.offset + 1

    def header(self, name: str) -> bytes | None:
        """The first value for `name`, or `None`.

        First rather than last: when a name repeats, the earlier one is the one
        the producer set and the later ones were appended in transit.
        """
        for key, value in self.headers:
            if key == name:
                return value
        return None

    def all_headers(self, name: str) -> tuple[bytes, ...]:
        """Every value for `name`, in wire order."""
        return tuple(value for key, value in self.headers if key == name)


def normalize_headers(headers: Mapping[str, bytes] | Headers | None) -> Headers:
    """Accept the convenient form, store the faithful one.

    A mapping is what calling code usually has, and it cannot express a
    repeated name; the wire form can. Converting here means `publish` takes
    either without every implementation deciding for itself.
    """
    if headers is None:
        return ()
    items = headers.items() if isinstance(headers, Mapping) else headers
    normalized: list[tuple[str, bytes]] = []
    for name, value in items:
        if not isinstance(value, bytes):  # pragma: no cover - defensive
            raise TypeError(
                f"Header {name!r} must be bytes, got {type(value).__name__}."
            )
        normalized.append((name, value))
    return tuple(normalized)


def validate_record(topic: str, key: str | None, value: bytes | None) -> None:
    """The three refusals every publisher makes, in one place.

    An empty topic is a typo that Kafka answers with an obscure metadata error.
    A null value without a key is the sharper one: it looks like "send nothing"
    and *means* "delete a key" the moment the topic is compacted, and there is
    no key for it to delete, so the record is either dropped by compaction or
    kept forever depending on the topic's configuration. Refusing it here
    fails at the call site instead.
    """
    if not topic:
        raise ValueError("topic must not be empty.")
    if value is None and key is None:
        raise ValueError(
            "A null value is a tombstone and needs a key. "
            "Pass an empty bytes value to send an empty record."
        )


def utc_now() -> datetime:
    """Timezone-aware now, for record timestamps a fake produces itself."""
    return datetime.now(UTC)


@runtime_checkable
class MessagePublisher(Protocol):
    """Where records go out.

    `start`/`stop` rather than a context manager because the lifetime is the
    process's, not a block's: the application starts one publisher in its
    lifespan and every handler shares it. A per-call client would pay a
    metadata fetch and a TCP handshake per record and would batch nothing.

    `start` and `stop` are both idempotent, and callers rely on it: the consume
    loop in `runner.py` calls `start` on every pass so that a broker which is
    unreachable at start-up becomes a retry rather than a background task that
    died before consuming anything.
    """

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def publish(
        self,
        topic: str,
        *,
        value: bytes | None,
        key: str | None = None,
        headers: Mapping[str, bytes] | Headers | None = None,
    ) -> PublishedMessage:
        """Send one record and wait for the broker to acknowledge it.

        Waiting is the default here rather than fire-and-forget, because a
        future nobody awaits is a record nobody knows was lost. Callers that
        genuinely want to overlap sends can gather several of these.
        """
        ...


@runtime_checkable
class MessageSource(Protocol):
    """Where records come in, with the offsets left to the caller.

    Deliberately not an async iterator. An iterator hides the batch boundary,
    and the batch boundary is where the commit goes: a consumer that commits
    per record pays a round trip per record, and one that commits on a timer
    has no idea which records the timer covered.
    """

    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def poll(
        self, *, max_records: int, timeout: float
    ) -> Sequence[ConsumedMessage]:
        """Fetch up to `max_records`, waiting at most `timeout` seconds.

        Returns an empty sequence when nothing arrived — an idle topic is the
        normal case, not an error. Records are grouped by partition and in
        offset order within each, which is the only ordering Kafka promises and
        the only one the runner relies on.
        """
        ...

    async def commit(self, offsets: Mapping[Partition, int]) -> None:
        """Store `offsets` for this consumer's group.

        Each value is the offset the group should read *next*, so it is one
        past the last record processed — see `ConsumedMessage.next_offset`.
        """
        ...

    def seek(self, partition: Partition, offset: int) -> None:
        """Move this consumer's read position, without committing anything.

        Synchronous because it is bookkeeping in the client: nothing is sent
        until the next fetch. Used to re-read a record whose handler failed.
        """
        ...

    def assignment(self) -> frozenset[Partition]:
        """The partitions this member currently owns.

        Empty before the first poll: assignment is the outcome of joining the
        group, and joining happens on the first fetch.
        """
        ...


__all__ = [
    "ConsumedMessage",
    "ConsumerError",
    "Headers",
    "LifecycleError",
    "MessageNotDecodableError",
    "MessageNotSerializableError",
    "MessagePublisher",
    "MessageSource",
    "MessagingError",
    "Partition",
    "PublishError",
    "PublishedMessage",
    "RetryAfter",
    "normalize_headers",
    "utc_now",
    "validate_record",
]
