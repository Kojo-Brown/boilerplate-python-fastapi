"""Putting a dead letter back, which is the only reason to keep one.

A dead-letter topic that is never drained is a landfill with retention on it:
the records expire, and the incident they were evidence of is closed by the
log rolling over. So the package ships the way back as code rather than as a
paragraph suggesting somebody write a script under pressure.

## Replay is a deliberate act, not a running consumer

`replay_handler` can be driven by a `ConsumerRunner`, and the way to use it is
to drain the topic once — `consume_once` until it comes back empty — after the
bug is fixed, not to leave it consuming. A permanent replay consumer is a loop:
origin, ladder, dead letter, origin, and round again for as long as whatever
broke the record stays broken. Each lap costs the full ladder in latency and
four topics' worth of writes, and the group's lag stays at zero throughout, so
the one signal an operator would look at says everything is fine.

`max_replays` is the guard for when it is left running anyway, because it will
be. A record that has been round `max_replays` times is refused rather than
republished, and stays in the dead-letter topic where somebody has to look at
it.

## What is restored, and what cannot be

The value, the key, and every header the application set are restored exactly.
The `x-dlq-*` headers are not: `attempts` goes back to zero, so a replayed
record gets the whole ladder again rather than being dead-lettered on its first
failure. `x-dlq-replays` is the one that survives and grows, because it is the
only evidence that this is the third time this record has been round — without
it, each replay's failure looks like a first failure and the log says so.

One record cannot be restored: the keyless, valueless record the router had to
invent a key for. Its original null key is not something the publisher will
write, so replaying it would put a record on a partition it never came from.
`ReplayNotPossibleError`, and the operator decides.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

import structlog

from src.dlq.base import MalformedEnvelopeError, ReplayNotPossibleError, RetryPolicy
from src.dlq.envelope import HEADER_REPLAYS, DeadLetterEnvelope, read, strip
from src.dlq.router import HEADER_SYNTHETIC_KEY, Clock
from src.kafka.base import (
    ConsumedMessage,
    MessagePublisher,
    PublishedMessage,
    utc_now,
)
from src.kafka.runner import MessageHandler

logger = structlog.get_logger(__name__)

#: How many times one record may be put back before a person has to decide.
#: Three rather than one: the common case for a replay is that the outage it
#: died in is over, and the second-commonest is that it was not quite over.
DEFAULT_MAX_REPLAYS: int = 3


@dataclass(frozen=True, slots=True)
class ReplayOutcome:
    """Where a dead letter was put back, and how many times that has now been."""

    topic: str
    replays: int
    published: PublishedMessage
    envelope: DeadLetterEnvelope


class DeadLetterReplayer:
    """Republishes dead letters onto the origin topic they came from."""

    def __init__(
        self,
        *,
        publisher: MessagePublisher,
        policy: RetryPolicy | None = None,
        max_replays: int = DEFAULT_MAX_REPLAYS,
        clock: Clock = utc_now,
    ) -> None:
        if max_replays < 0:
            raise ValueError("max_replays cannot be negative.")
        self._publisher = publisher
        self._policy = policy if policy is not None else RetryPolicy()
        self._max_replays = max_replays
        self._clock = clock

    @property
    def max_replays(self) -> int:
        return self._max_replays

    async def replay(self, message: ConsumedMessage) -> ReplayOutcome:
        """Put one dead letter back on its origin topic.

        Raises `ReplayNotPossibleError` for a record that cannot be restored
        faithfully — no envelope, an unreadable one, a synthesised key, or one
        that has already been round `max_replays` times. Raises whatever the
        publish raises, so a failure to republish leaves the record in the
        dead-letter topic rather than losing it.
        """
        try:
            envelope = read(message)
        except MalformedEnvelopeError as exc:
            raise ReplayNotPossibleError(
                "The record's dead-letter envelope cannot be read, so there is "
                "no origin topic to replay it to.",
                {"topic": message.topic, "offset": message.offset},
            ) from exc

        if envelope is None:
            raise ReplayNotPossibleError(
                "The record carries no dead-letter envelope, so nothing records "
                "which topic it was consumed from.",
                {"topic": message.topic, "offset": message.offset},
            )
        if message.header(HEADER_SYNTHETIC_KEY) is not None:
            raise ReplayNotPossibleError(
                "The record arrived with neither a key nor a value and was "
                "given a synthetic key. Replaying it would place it on a "
                "partition it never came from.",
                {"topic": message.topic, "offset": message.offset},
            )
        if self._policy.is_ladder_topic(envelope.origin_topic):
            # A header, and therefore something a foreign producer or a corrupt
            # record can say. Believing it would republish the record into a
            # retry tier — where it is due immediately, has no attempt count,
            # and starts a fresh ladder under a name that already ends in one.
            raise ReplayNotPossibleError(
                f"The record's origin topic {envelope.origin_topic!r} is itself "
                "a retry or dead-letter topic, which is not somewhere a record "
                "can be replayed to.",
                {"topic": message.topic, "offset": message.offset},
            )
        if envelope.replays >= self._max_replays:
            raise ReplayNotPossibleError(
                f"The record has already been replayed {envelope.replays} times "
                f"(limit {self._max_replays}). Whatever is failing is not "
                "transient; fix it before putting the record back again.",
                {
                    "topic": message.topic,
                    "offset": message.offset,
                    "origin_topic": envelope.origin_topic,
                },
            )

        now = self._clock()
        restored = replace(envelope, replays=envelope.replays + 1)
        published = await self._publisher.publish(
            envelope.origin_topic,
            value=message.value,
            key=message.key,
            # Stripped, not stamped: the record goes back looking like the one
            # the application published, so its next failure starts at tier 1.
            # The replay count is the single exception and rides along as its
            # own header, because a record on its third lap must not read as a
            # first failure.
            headers=(
                *strip(message.headers),
                *_replay_headers(restored),
            ),
        )
        logger.info(
            "dlq.replayed",
            topic=envelope.origin_topic,
            from_topic=message.topic,
            from_partition=str(message.partition),
            from_offset=message.offset,
            key=message.key,
            dead_for_seconds=round(envelope.age(now), 3),
            # `restored`, not `envelope`: the lap count in the log line is the
            # one the record is going back with, which is the number an
            # operator watching a drain is counting.
            **restored.log_fields(),
        )
        return ReplayOutcome(
            topic=envelope.origin_topic,
            replays=restored.replays,
            published=published,
            envelope=restored,
        )


def _replay_headers(envelope: DeadLetterEnvelope) -> tuple[tuple[str, bytes], ...]:
    """The one `x-dlq-*` header a replayed record keeps.

    Only `x-dlq-replays`. Writing the whole envelope back would set an attempt
    count on a record that is starting again, and `envelope.read` treats
    `x-dlq-attempts` as the marker of a record that has failed — so the record
    would be routed to tier 2 on its first failure, skipping the short rung
    that is the one most likely to fix it.
    """
    return ((HEADER_REPLAYS, str(envelope.replays).encode("ascii")),)


def replay_handler(replayer: DeadLetterReplayer) -> MessageHandler:
    """A `MessageHandler` that replays every record handed to it.

    Meant for a one-shot drain — `consume_once` until the result is empty —
    rather than a consumer left running. See the module docstring.

    A record the replayer refuses is *not* re-raised: raising would stall the
    dead-letter partition on a record that will be refused identically forever,
    and the whole point of draining is to get through the ones that can be
    saved. The refusal is logged, the record's offset is committed, and the
    record itself stays in the topic until its retention expires or somebody
    consumes it deliberately.
    """

    async def handle(message: ConsumedMessage) -> None:
        try:
            await replayer.replay(message)
        except ReplayNotPossibleError as exc:
            logger.warning(
                "dlq.replay_refused",
                topic=message.topic,
                partition=str(message.partition),
                offset=message.offset,
                key=message.key,
                reason=str(exc),
            )

    return handle


__all__ = [
    "DEFAULT_MAX_REPLAYS",
    "DeadLetterReplayer",
    "ReplayOutcome",
    "replay_handler",
]
