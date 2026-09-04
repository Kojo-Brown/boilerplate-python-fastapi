"""The ladder: which topic a record goes to next, and how long it waits there.

Everything in this module is a value. No broker, no clock, no I/O — so the
arithmetic that decides a record's fate is testable by calling a function, and
`router.py` is left with nothing but "publish it there".

## A delay is a topic, not a sleep

The obvious implementation of "retry this in 30 seconds" inside a consumer is
`await asyncio.sleep(30)`. It is wrong twice over, and both failures are the
kind that look fine in development.

It stalls the partition. Records behind the sleeping one are not being handled
either, so a 1% failure rate at a 30-second retry is a partition that spends
most of its time asleep — which is the head-of-line blocking a dead-letter
queue exists to end, reintroduced in the thing meant to fix it.

Worse, it gets the member evicted. A consumer must return to `poll` within
`max.poll.interval.ms` (five minutes by default), and the broker's answer to a
member that does not is to declare it dead and hand its partitions — including
the records it is still holding — to somebody else. So a sleep longer than that
interval does not delay a record; it re-delivers the whole assignment
elsewhere, repeatedly, forever.

So a tier is a *topic*. A record that fails is published to `orders.retry.1`
and the origin partition advances immediately. A separate consumer reads that
topic and is the one that waits.

## Why the tier delay is fixed, and never jittered

Every other backoff in this codebase uses `backoff_delay` with full jitter, for
a good reason: retries of a shared failure that are not spread out arrive as
one spike. Here jitter would be a bug, and this is the one place in the
repository where that call goes the other way.

A tier topic is consumed in offset order, and the consumer stalls at the first
record that is not due yet — which is only correct if *nothing behind it is due
earlier*. With one fixed delay per tier, due time is `produced_at + delay`, so
due time rises with offset and stalling at the head is complete: when the head
becomes due, everything behind it is due or will be, in order. Add jitter and
that invariant is gone — a record drawn 40 seconds at offset 0 makes a record
drawn 10 seconds at offset 1 wait the full 40, and it was due 30 seconds ago.

The spreading jitter would have bought is already there for free: records enter
a tier at the moment they fail, which is spread out by whatever spread the
failures had.

## The ladder is named, not stored

`RetryPolicy` derives every topic name from the origin topic, so a record in
`orders.retry.2` needs no registry to be understood, and a second service
consuming the same origin topic derives the same names. The cost is that the
naming convention is now an interface: renaming a suffix orphans whatever is
already sitting in the old topics, which is why both suffixes are settings that
a deployment sets once rather than constants it is free to edit.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from src.exceptions import AppException

#: Ceiling on the exponent so that a large `tiers` cannot turn into an
#: `OverflowError` inside `multiplier ** index` before `max_delay` clamps it.
#: 2**32 seconds is a century and a half; no tier that was going to work is
#: anywhere near it.
MAX_TIER_EXPONENT: Final[int] = 32


class DeadLetterError(AppException):
    """Base for the failures this package produces.

    500 rather than the 503 `MessagingError` uses: every subclass here means
    the metadata on a record, or a limit this service set, is what stopped the
    routing — not that the broker is unwell. A retry sends the same record with
    the same headers and gets the same answer.
    """

    status_code = 500
    error_code = "DEAD_LETTER_ERROR"

    def __init__(self, message: str, details: object = None) -> None:
        super().__init__(message, details)


class MalformedEnvelopeError(DeadLetterError):
    """A record's `x-dlq-*` headers cannot be read.

    Deliberately not fatal to the consumer: `DeadLetterRouter` catches it and
    sends the record straight to a dead-letter topic. A record whose retry
    metadata is unreadable cannot be retried *correctly* — an unparseable
    attempt count read as zero would send it around the ladder again on every
    pass, forever — and a record that can never be interpreted is exactly what
    a dead-letter topic is for.
    """

    error_code = "DEAD_LETTER_ENVELOPE_MALFORMED"


class ReplayNotPossibleError(DeadLetterError):
    """A dead letter cannot be put back where it came from.

    Either it carries no envelope — so nothing records which topic it was
    consumed from — or it has already been replayed `max_replays` times.
    """

    error_code = "DEAD_LETTER_REPLAY_REFUSED"


@dataclass(frozen=True, slots=True)
class RetryTier:
    """One rung: a topic, and the fixed wait every record in it serves."""

    #: 1-based, so `attempts` and `index` are the same number and no call site
    #: has to remember which of the two is off by one.
    index: int
    topic: str
    delay: float


@dataclass(frozen=True, slots=True)
class RetryLadder:
    """The tiers for one origin topic, plus where the ladder ends."""

    origin_topic: str
    tiers: tuple[RetryTier, ...]
    dead_letter_topic: str

    @property
    def max_attempts(self) -> int:
        """Total handler attempts before a record is dead-lettered.

        One on the origin topic plus one per tier. A record is dead-lettered on
        the failure *after* this many, so `max_attempts=4` means the DLT holds
        records that four separate consumers could not handle.
        """
        return len(self.tiers) + 1

    @property
    def topics(self) -> tuple[str, ...]:
        """Every topic in the ladder, origin first and dead letter last."""
        return (
            self.origin_topic,
            *(tier.topic for tier in self.tiers),
            self.dead_letter_topic,
        )

    @property
    def retry_topics(self) -> tuple[str, ...]:
        """Just the tiers — what a retry consumer subscribes to."""
        return tuple(tier.topic for tier in self.tiers)

    @property
    def total_delay(self) -> float:
        """How long a record can be retried for before it is dead-lettered.

        Worth reading before choosing the tier count: it is the age of the
        oldest thing that can still arrive at the handler, and a handler whose
        work is only valid for five minutes should not be behind an hour-long
        ladder.
        """
        return sum(tier.delay for tier in self.tiers)

    def destination(self, attempts: int) -> RetryTier | None:
        """The tier for a record that has now failed `attempts` times.

        `None` means the ladder is exhausted and the record is dead-lettered.
        `attempts` is 1-based and counts the failure that just happened, so the
        first failure goes to tier 1.
        """
        if attempts < 1:
            raise ValueError("attempts is 1-based and counts the failure just seen.")
        if attempts > len(self.tiers):
            return None
        return self.tiers[attempts - 1]

    def tier_for_topic(self, topic: str) -> RetryTier | None:
        """The tier a record arrived on, or `None` for origin/dead-letter."""
        for tier in self.tiers:
            if tier.topic == topic:
                return tier
        return None


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """The knobs, and the naming convention that turns them into topics.

    One policy per application rather than one ladder per topic: the tier
    delays are an operational choice about how long a failure may be retried
    for, and it does not usually differ by topic. `ladder_for` derives the
    names, so a consumer that meets a record from a topic it has never heard of
    can still work out where it goes next.
    """

    #: What tier 1 waits. Short enough that a transient failure — a rolling
    #: restart downstream, a lock contended for a moment — is served by it,
    #: because everything the first tier fixes never reaches the second.
    base_delay: float = 5.0
    #: Growth per tier. Five rather than two: with a small tier count, doubling
    #: barely separates the rungs (5s / 10s / 20s is 35 seconds of total
    #: patience), and the point of a ladder is that the last rung is long
    #: enough to outlast a real outage.
    multiplier: float = 5.0
    tiers: int = 3
    #: Clamp on one tier's wait. Also the ceiling on how stale a record handed
    #: to a handler can be, which is the number to check against whatever the
    #: handler's work assumes about freshness.
    max_delay: float = 900.0
    retry_suffix: str = ".retry"
    dead_letter_suffix: str = ".dlt"

    def __post_init__(self) -> None:
        if self.base_delay <= 0:
            raise ValueError("base_delay must be positive.")
        if self.multiplier < 1:
            raise ValueError(
                "multiplier below 1 would make each tier wait less than the last."
            )
        if self.tiers < 0:
            raise ValueError("tiers cannot be negative.")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay cannot be below base_delay.")
        if not self.retry_suffix:
            raise ValueError("retry_suffix must not be empty.")
        if not self.dead_letter_suffix:
            raise ValueError("dead_letter_suffix must not be empty.")
        if self.retry_suffix == self.dead_letter_suffix:
            raise ValueError(
                "retry_suffix and dead_letter_suffix must differ, or a retry "
                "tier and the dead-letter topic would be the same topic."
            )

    def tier_delay(self, index: int) -> float:
        """The fixed wait for tier `index` (1-based)."""
        if index < 1:
            raise ValueError("Tier indexes are 1-based.")
        exponent = min(index - 1, MAX_TIER_EXPONENT)
        return min(self.max_delay, self.base_delay * (self.multiplier**exponent))

    def retry_topic(self, origin_topic: str, index: int) -> str:
        return f"{origin_topic}{self.retry_suffix}.{index}"

    def dead_letter_topic(self, origin_topic: str) -> str:
        return f"{origin_topic}{self.dead_letter_suffix}"

    def ladder_for(self, origin_topic: str) -> RetryLadder:
        """Build the ladder for `origin_topic`.

        Refuses a topic that is already part of a ladder. Building
        `orders.retry.1`'s ladder would produce `orders.retry.1.retry.1`, and
        the record that caused it would climb a ladder of its own for as long
        as anyone kept consuming — the sort of loop whose only symptom is a
        topic count that grows.
        """
        if not origin_topic:
            raise ValueError("origin_topic must not be empty.")
        if self.is_ladder_topic(origin_topic):
            raise ValueError(
                f"{origin_topic!r} is already a retry or dead-letter topic. "
                "Build the ladder from the origin topic recorded in the "
                "record's x-dlq-origin-topic header."
            )
        return RetryLadder(
            origin_topic=origin_topic,
            tiers=tuple(
                RetryTier(
                    index=index,
                    topic=self.retry_topic(origin_topic, index),
                    delay=self.tier_delay(index),
                )
                for index in range(1, self.tiers + 1)
            ),
            dead_letter_topic=self.dead_letter_topic(origin_topic),
        )

    def is_ladder_topic(self, topic: str) -> bool:
        """Whether `topic` is one this policy would have generated."""
        return self.origin_of(topic) is not None

    def origin_of(self, topic: str) -> str | None:
        """The origin topic `topic` belongs to, or `None` if it is one itself.

        The inverse of the naming convention, and the reason the router can
        still find a dead-letter topic for a record whose headers are
        unreadable: the topic it arrived on says where it came from even when
        nothing else on it does.
        """
        if topic.endswith(self.dead_letter_suffix):
            return topic[: -len(self.dead_letter_suffix)] or None
        marker = f"{self.retry_suffix}."
        head, separator, tail = topic.rpartition(marker)
        if separator and head and tail.isdigit():
            return head
        return None


__all__ = [
    "MAX_TIER_EXPONENT",
    "DeadLetterError",
    "MalformedEnvelopeError",
    "ReplayNotPossibleError",
    "RetryLadder",
    "RetryPolicy",
    "RetryTier",
]
