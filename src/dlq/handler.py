"""The two wrappers that turn a plain handler into one the ladder can drive.

Wrappers rather than a subclass of `ConsumerRunner`, because what changes is
what one record *means*, not how the loop is scheduled. `MessageHandler` is
already the seam — `Callable[[ConsumedMessage], Awaitable[None]]` — so a
handler that knows nothing about retries can be composed into one that
participates in a ladder, and both the wrapped and the unwrapped form run under
the same runner.

## What `with_dead_letter` changes about the runner's behaviour

The runner's rule is that a handler which returns has succeeded, and a
succeeded record's offset is committed. `with_dead_letter` returns after
routing a failure, so the origin partition commits past the record and keeps
going. That is the whole of the head-of-line fix, and it is one line of control
flow rather than a change to the loop.

It is also a real trade, and stating it plainly is more useful than a paragraph
on how the retries work:

**Ordering per key is given up for records that fail.** Records 5 and 6 share a
key. 5 fails and is routed to `orders.retry.1`; 6 is handled immediately; 5 is
handled five seconds later, after 6. If the handler is "apply this update to
this row", that is last-write-wins with the wrong last write. A ladder is the
right answer when records are independent of one another — an email to send, a
webhook to deliver, a document to index — and the wrong one when a key's
records are a sequence of edits. For those the honest behaviour is the stall,
and the way to keep it is to not wrap the handler.

## What `with_due_time` changes about a retry-tier consumer

Nothing, until a record is not due yet, at which point it raises `RetryAfter`
and the runner stalls the partition without counting a failure. Stalling is
correct and complete here rather than a compromise, and the reason is a
property the ladder was built to have: within one tier the delay is fixed and
never jittered, so due time rises with offset and nothing behind a record that
is not due is due either. See `src/dlq/base.py`.

## Order of composition, and the two exceptions that decide it

`with_dead_letter(with_due_time(handler), router)` — the gate *inside*. Two
exceptions have to come out of the gate and go to different places, and only
this order sends each of them somewhere sensible.

`RetryAfter` must reach the runner untouched. `with_dead_letter` re-raises it
explicitly rather than catching it with everything else, because a record that
was merely early would otherwise be moved one rung down the ladder every time a
consumer looked at it — and would reach the dead-letter topic after four
glances without a handler ever having run.

`MalformedEnvelopeError`, raised by the gate when it cannot read the due time,
must reach the router. With the gate outside it would escape to the runner
instead and stall the partition on a record no amount of retrying can repair.
Inside, it is caught by `with_dead_letter` like any other failure, and the
router recognises it and dead-letters the record — which is the correct end for
a record whose own metadata is unreadable.

`retry_tier_handler` composes them so that no call site has to know any of this.
"""

from __future__ import annotations

import structlog

from src.dlq.envelope import read
from src.dlq.router import Clock, DeadLetterRouter
from src.kafka.base import ConsumedMessage, RetryAfter, utc_now
from src.kafka.runner import MessageHandler

logger = structlog.get_logger(__name__)


def with_dead_letter(
    handler: MessageHandler, router: DeadLetterRouter
) -> MessageHandler:
    """`handler`, with failures routed onto the ladder instead of stalling.

    The wrapped handler returns normally once a failure has been routed, which
    is what lets the runner commit past the record. It raises only when the
    record could not be routed — a broker that will not take the publish — and
    that is deliberate: an unroutable record must stall its partition, because
    the alternative is committing past a record that now exists nowhere.
    """

    async def handle(message: ConsumedMessage) -> None:
        try:
            await handler(message)
        except RetryAfter:
            # Not a failure, and not this wrapper's to interpret. See the
            # module docstring: routing it would move an early record one rung
            # down the ladder per glance.
            raise
        except Exception as exc:
            outcome = await router.route(message, exc)
            logger.info(
                "dlq.routed",
                from_topic=message.topic,
                from_partition=str(message.partition),
                from_offset=message.offset,
                to_topic=outcome.topic,
                attempts=outcome.attempts,
                dead_lettered=outcome.dead_lettered,
            )

    return handle


def with_due_time(
    handler: MessageHandler, *, clock: Clock = utc_now, grace: float = 0.0
) -> MessageHandler:
    """`handler`, refusing records before their `x-dlq-not-before`.

    Args:
        handler: What to run once the record is due.
        clock: Source of `now`. Injectable so a test does not wait.
        grace: Seconds of slack allowed on the due time. Clock skew between the
            process that stamped `not_before` and the one reading it is real —
            they are different machines — and without slack a record stamped by
            a host running a second fast is deferred for a second that has, by
            every other clock, already passed. Zero by default because being
            early costs one extra pass round the loop and being late costs the
            record's latency; set it when the tier delays are short enough for
            skew to be a meaningful fraction of one.

    A record with no envelope passes straight through. That is not a permissive
    fallback but the only correct reading: such a record was produced onto the
    tier topic by something other than this router — an operator replaying by
    hand, most likely — and inventing a due time for it would hold it for a
    delay nobody asked for.

    A record whose envelope cannot be parsed raises `MalformedEnvelopeError`,
    which `with_dead_letter` turns into a dead letter. See the module docstring
    for why that requires this wrapper to be the inner one.
    """

    async def handle(message: ConsumedMessage) -> None:
        envelope = read(message)
        if envelope is not None:
            wait = envelope.wait_for(clock())
            if wait > grace:
                raise RetryAfter(wait, reason="not due yet")
        await handler(message)

    return handle


def retry_tier_handler(
    handler: MessageHandler,
    router: DeadLetterRouter,
    *,
    clock: Clock = utc_now,
    grace: float = 0.0,
) -> MessageHandler:
    """The handler a retry-tier consumer runs: wait until due, then as usual.

    Composed in the one order that sends `RetryAfter` and
    `MalformedEnvelopeError` where each of them belongs — see the module
    docstring — so that the two wrappers are never assembled by hand.
    """
    return with_dead_letter(with_due_time(handler, clock=clock, grace=grace), router)


__all__ = ["retry_tier_handler", "with_dead_letter", "with_due_time"]
