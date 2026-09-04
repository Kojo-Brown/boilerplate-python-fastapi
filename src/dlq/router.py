"""Where a failed record goes, and the one thing that must never happen to it.

`DeadLetterRouter.route` is called with a record and the exception its handler
raised, and it publishes that record onto the next rung of the ladder or onto
the dead-letter topic. It is the only part of this package that talks to a
broker.

## Losing the record is the failure mode, not a slow retry

Every decision below falls out of one rule: **a record is never dropped.** The
routing publish is what makes it safe for the consumer to commit past a failed
record, so if that publish does not land, the commit must not happen either.
`route` therefore lets `PublishError` propagate, and `with_dead_letter` in
`handler.py` lets it reach the runner, which stalls the partition and retries —
back to the head-of-line blocking this package exists to end, and correct,
because the alternative during a broker outage is a topic quietly emptying
itself into nowhere.

It is worth being concrete about what "never dropped" does and does not cover.
The publish is acknowledged by the broker before `route` returns, so a record
that reaches a retry topic is durable. A crash *between* the publish and the
consumer's commit replays the origin record, which produces a duplicate in the
retry topic rather than a loss — at-least-once, the same guarantee the runner
already gives, arrived at the same way. Handlers were already required to be
idempotent; the ladder does not add a requirement, it adds occasions.

## Some failures are not worth waiting for

A record whose bytes will not decode fails identically in fifteen minutes. Nine
hundred seconds of ladder buys nothing and costs the latency of every record
behind it in the retry topics, so `non_retryable` sends those straight to the
dead-letter topic. The default is `MessageNotDecodableError` and nothing else:
widening it is a decision about the application's own exceptions, and guessing
on its behalf — treating every `ValueError` as permanent, say — would
dead-letter a validation failure caused by a dependency that was briefly
returning nonsense.

## An unreadable envelope is itself a dead letter

`MalformedEnvelopeError` from `envelope.read` is caught here rather than raised
at the caller. The record cannot be placed on the ladder, because where it goes
next is a function of an attempt count that cannot be read, and reading that
count as zero would send it round the ladder again on every pass forever. So it
goes to the dead-letter topic, where the destination is derived from the topic
name — `RetryPolicy.origin_of` — rather than from the headers that are the
problem.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Final

import structlog

from src.dlq.base import MalformedEnvelopeError, RetryLadder, RetryPolicy, RetryTier
from src.dlq.envelope import (
    DeadLetterEnvelope,
    advance,
    read,
    read_replays,
    stamp,
    truncate_error,
)
from src.kafka.base import (
    ConsumedMessage,
    Headers,
    MessageNotDecodableError,
    MessagePublisher,
    PublishedMessage,
    utc_now,
)

logger = structlog.get_logger(__name__)

#: Returns the current UTC time. Injectable so a test can decide that fifteen
#: minutes have passed without spending them.
Clock = Callable[[], datetime]

#: Marks a record that arrived with no key *and* no value and was given one, so
#: that `replay` can refuse to put it back rather than silently inventing a key
#: on the origin topic. See `_republish_key`.
HEADER_SYNTHETIC_KEY: Final[str] = "x-dlq-synthetic-key"


@dataclass(frozen=True, slots=True)
class RouteOutcome:
    """What happened to one failed record.

    Returned rather than logged-and-forgotten so a caller — a test, or an
    application that wants a metric per outcome — can act on the decision
    instead of parsing it back out of a log line.
    """

    topic: str
    attempts: int
    #: The tier's fixed wait, or zero for a record that was dead-lettered.
    delay: float
    dead_lettered: bool
    published: PublishedMessage
    envelope: DeadLetterEnvelope

    @property
    def retried(self) -> bool:
        return not self.dead_lettered


class DeadLetterRouter:
    """Publishes failed records onto the ladder derived from `policy`."""

    def __init__(
        self,
        *,
        publisher: MessagePublisher,
        policy: RetryPolicy | None = None,
        clock: Clock = utc_now,
        non_retryable: tuple[type[BaseException], ...] = (MessageNotDecodableError,),
    ) -> None:
        """
        Args:
            publisher: Where records are republished. Must be started; the
                router does not start it, because a publisher's lifetime is the
                process's and belongs to whatever built it.
            policy: Tier count, delays and topic naming.
            clock: Source of `now`, for `not_before` and `first_failed_at`.
            non_retryable: Exceptions that skip the ladder entirely. Matched
                with `isinstance`, so a base class covers its subclasses.
        """
        self._publisher = publisher
        self._policy = policy if policy is not None else RetryPolicy()
        self._clock = clock
        self._non_retryable = non_retryable

    @property
    def policy(self) -> RetryPolicy:
        return self._policy

    def ladder_for(self, message: ConsumedMessage) -> RetryLadder:
        """The ladder a record belongs to, whichever rung it arrived on.

        Uses the origin topic from the record's headers when they can be read,
        and falls back to inverting the topic name when they cannot. Both are
        needed: the header is authoritative — a deployment can change a suffix,
        and records already in flight keep the old names — and the name is what
        is left when the header is the thing that is broken.
        """
        try:
            previous = read(message)
        except MalformedEnvelopeError:
            previous = None
        return self._ladder(previous, message)

    def _ladder(
        self, previous: DeadLetterEnvelope | None, message: ConsumedMessage
    ) -> RetryLadder:
        origin = (
            previous.origin_topic
            if previous is not None
            else self._policy.origin_of(message.topic) or message.topic
        )
        return self._policy.ladder_for(origin)

    async def route(self, message: ConsumedMessage, exc: BaseException) -> RouteOutcome:
        """Publish `message` onto its next destination and say where it went.

        Raises whatever the publish raises. That is the contract the caller
        depends on to know when the record is safe to commit past.
        """
        now = self._clock()

        try:
            previous = read(message)
            # Read inside the same guard: a replayed record carries the lap
            # count and nothing else, so this is the only place it can be
            # picked up, and a corrupt one is as unreadable as any other header.
            replays = read_replays(message)
        except MalformedEnvelopeError as malformed:
            return await self._dead_letter_unreadable(message, now, malformed)

        ladder = self._ladder(previous, message)
        attempts = (previous.attempts if previous is not None else 0) + 1
        permanent = isinstance(exc, self._non_retryable)
        tier = None if permanent else ladder.destination(attempts)
        delay = tier.delay if tier is not None else 0.0

        envelope = advance(
            previous,
            message,
            now=now,
            delay=delay,
            error=truncate_error(f"{type(exc).__name__}: {exc}"),
            replays=replays,
            # The ladder's origin, not the topic in hand. A record reaching its
            # first failure *on a tier* — hand-published there, or left by an
            # older deployment — would otherwise be stamped with the tier's own
            # name as its origin, and its second failure would try to build a
            # ladder on `orders.events.retry.1`. That is refused, the
            # `ValueError` escapes `route`, and the record stalls its partition
            # forever: a loop introduced by the code meant to end one.
            origin_topic=ladder.origin_topic,
        )
        destination = tier.topic if tier is not None else ladder.dead_letter_topic
        published = await self._publish(message, destination, envelope)

        self._log(
            message=message,
            destination=destination,
            envelope=envelope,
            ladder=ladder,
            tier=tier,
            attempts=attempts,
            now=now,
            reason="non_retryable" if permanent else "ladder_exhausted",
        )
        return RouteOutcome(
            topic=destination,
            attempts=attempts,
            delay=delay,
            dead_lettered=tier is None,
            published=published,
            envelope=envelope,
        )

    def _log(
        self,
        *,
        message: ConsumedMessage,
        destination: str,
        envelope: DeadLetterEnvelope,
        ladder: RetryLadder,
        tier: RetryTier | None,
        attempts: int,
        now: datetime,
        reason: str,
    ) -> None:
        common = {
            "topic": destination,
            "from_topic": message.topic,
            "from_partition": str(message.partition),
            "from_offset": message.offset,
            "key": message.key,
            **envelope.log_fields(),
        }
        if tier is None:
            logger.warning(
                "dlq.dead_lettered",
                reason=reason,
                max_attempts=ladder.max_attempts,
                age_seconds=round(envelope.age(now), 3),
                **common,
            )
        else:
            logger.warning(
                "dlq.retry_scheduled",
                tier=tier.index,
                delay=tier.delay,
                not_before=envelope.not_before.isoformat(),
                remaining_attempts=ladder.max_attempts - attempts,
                **common,
            )

    async def _dead_letter_unreadable(
        self,
        message: ConsumedMessage,
        now: datetime,
        malformed: MalformedEnvelopeError,
    ) -> RouteOutcome:
        """Send a record whose own envelope cannot be parsed to the DLT.

        The envelope written on the way is a fresh one describing this record's
        position in the topic it arrived on — which is not the origin topic if
        it was already partway down the ladder. That is a deliberate loss of
        provenance in exchange for a record that can be read at all: the
        alternative is copying fields out of the structure that has just been
        established to be untrustworthy.
        """
        ladder = self._ladder(None, message)
        envelope = advance(
            None,
            message,
            now=now,
            delay=0.0,
            error=f"{type(malformed).__name__}: {malformed}",
        )
        published = await self._publish(message, ladder.dead_letter_topic, envelope)
        self._log(
            message=message,
            destination=ladder.dead_letter_topic,
            envelope=envelope,
            ladder=ladder,
            tier=None,
            attempts=envelope.attempts,
            now=now,
            reason="malformed_envelope",
        )
        return RouteOutcome(
            topic=ladder.dead_letter_topic,
            attempts=envelope.attempts,
            delay=0.0,
            dead_lettered=True,
            published=published,
            envelope=envelope,
        )

    async def _publish(
        self, message: ConsumedMessage, topic: str, envelope: DeadLetterEnvelope
    ) -> PublishedMessage:
        """Republish the record's key, value and headers onto `topic`.

        The key is kept, and that is worth stating rather than assuming. It
        keeps a hot key's failures on one partition of the tier topic, so the
        records of one entity are still retried in the order they failed; it
        also means an entity generating most of the failures generates most of
        one tier partition, which is the honest shape of that problem rather
        than one spread evenly and hidden.
        """
        key, headers = self._republish_key(message, envelope)
        return await self._publisher.publish(
            topic, value=message.value, key=key, headers=headers
        )

    def _republish_key(
        self, message: ConsumedMessage, envelope: DeadLetterEnvelope
    ) -> tuple[str | None, Headers]:
        """The key to republish under, and the headers to go with it.

        Almost always the record's own key. The exception is a record with no
        key *and* no value, which `validate_record` refuses to produce: it
        reads as a tombstone for no key, and republishing it verbatim would
        raise `ValueError` out of `route`, stall the partition on a record that
        can never be routed anywhere, and turn a poison record into an outage.

        Such a record cannot arrive from this codebase's own publisher, only
        from a foreign producer, and it carries no application data at all — no
        key, no value, only headers and a position. So it is given a key
        derived from where it was, which loses nothing (a null key carries no
        information and round-robins) and is stable across redeliveries. The
        record is flagged, and `replay` refuses to put a flagged record back:
        restoring the original null key is exactly what the publisher will not
        do, and quietly replaying a record with a key it never had would send
        it to a partition it never belonged to.
        """
        headers = stamp(message.headers, envelope)
        if message.key is not None or message.value is not None:
            return message.key, headers
        synthetic = (
            f"{envelope.origin_topic}:{envelope.origin_partition}"
            f":{envelope.origin_offset}"
        )
        logger.warning(
            "dlq.synthetic_key_assigned",
            from_topic=message.topic,
            from_partition=str(message.partition),
            from_offset=message.offset,
            key=synthetic,
            detail=(
                "The record had neither a key nor a value, which cannot be "
                "republished as-is. It is preserved under a key derived from "
                "its position and will not be replayed automatically."
            ),
        )
        return synthetic, (*headers, (HEADER_SYNTHETIC_KEY, b"1"))


__all__ = ["HEADER_SYNTHETIC_KEY", "Clock", "DeadLetterRouter", "RouteOutcome"]
