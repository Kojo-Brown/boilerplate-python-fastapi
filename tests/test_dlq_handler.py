"""The wrappers, and the two exceptions whose destination the order decides.

`TestCompositionOrder` is the one to read: `RetryAfter` has to reach the runner
and `MalformedEnvelopeError` has to reach the router, and there is exactly one
nesting that sends each of them somewhere sensible.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from src.dlq.base import MalformedEnvelopeError, RetryPolicy
from src.dlq.envelope import HEADER_ATTEMPTS, DeadLetterEnvelope
from src.dlq.handler import retry_tier_handler, with_dead_letter, with_due_time
from src.dlq.router import DeadLetterRouter
from src.kafka.base import (
    ConsumedMessage,
    Headers,
    Partition,
    PublishError,
    RetryAfter,
    utc_now,
)
from tests.test_dlq_router import RecordingPublisher

NOW = datetime(2026, 9, 4, 12, 0, 0, tzinfo=UTC)
ORIGIN = Partition(topic="orders.events", number=0)


def a_message(
    *,
    partition: Partition = ORIGIN,
    offset: int = 0,
    headers: Headers = (),
) -> ConsumedMessage:
    return ConsumedMessage(
        partition=partition,
        offset=offset,
        key="k",
        value=b"payload",
        headers=headers,
        timestamp=utc_now(),
    )


def an_envelope(*, not_before: datetime, attempts: int = 1) -> DeadLetterEnvelope:
    return DeadLetterEnvelope(
        origin_topic="orders.events",
        origin_partition=0,
        origin_offset=0,
        attempts=attempts,
        first_failed_at=NOW,
        not_before=not_before,
        error="RuntimeError: nope",
    )


def a_router(publisher: RecordingPublisher) -> DeadLetterRouter:
    return DeadLetterRouter(
        publisher=publisher,
        policy=RetryPolicy(base_delay=5.0, multiplier=5.0, tiers=3, max_delay=900),
        clock=lambda: NOW,
    )


class RecordingHandler:
    """Counts what it was given, and fails whichever offsets it is told to."""

    def __init__(self, *, fail: Exception | None = None) -> None:
        self.seen: list[ConsumedMessage] = []
        self.fail = fail

    async def __call__(self, message: ConsumedMessage) -> None:
        self.seen.append(message)
        if self.fail is not None:
            raise self.fail


class TestWithDeadLetter:
    async def test_a_success_passes_straight_through(self) -> None:
        publisher = RecordingPublisher()
        handler = RecordingHandler()

        await with_dead_letter(handler, a_router(publisher))(a_message())

        assert len(handler.seen) == 1
        assert publisher.published == []

    async def test_a_failure_is_routed_and_the_wrapper_returns(self) -> None:
        """Returning is the whole head-of-line fix: the runner commits past it."""
        publisher = RecordingPublisher()
        handler = RecordingHandler(fail=RuntimeError("nope"))

        await with_dead_letter(handler, a_router(publisher))(a_message())

        assert publisher.topics == ["orders.events.retry.1"]

    async def test_an_unroutable_record_raises_so_the_partition_stalls(self) -> None:
        """A commit past a record that now exists nowhere is the one
        unacceptable outcome, so a broker that refuses the publish has to
        reach the runner rather than being logged and swallowed."""
        publisher = RecordingPublisher()
        publisher.error = PublishError("broker unreachable")
        handler = RecordingHandler(fail=RuntimeError("nope"))

        with pytest.raises(PublishError):
            await with_dead_letter(handler, a_router(publisher))(a_message())

    async def test_it_does_not_route_a_cancelled_handler(self) -> None:
        """A cancelled shutdown is not a failed record.

        `except Exception` does not catch `CancelledError` — it derives from
        `BaseException` — so this holds by construction, and is asserted
        because the day somebody widens that clause is the day a rolling
        restart starts dead-lettering whatever was in flight.
        """
        publisher = RecordingPublisher()

        async def cancelled(message: ConsumedMessage) -> None:
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await with_dead_letter(cancelled, a_router(publisher))(a_message())

        assert publisher.published == []


class TestWithDueTime:
    async def test_a_record_with_no_envelope_passes_through(self) -> None:
        """It was published by something other than the router — an operator
        replaying by hand, most likely — and inventing a due time for it would
        hold it for a delay nobody asked for."""
        handler = RecordingHandler()

        await with_due_time(handler, clock=lambda: NOW)(a_message())

        assert len(handler.seen) == 1

    async def test_a_due_record_is_handled(self) -> None:
        handler = RecordingHandler()
        headers = an_envelope(not_before=NOW).to_headers()

        await with_due_time(handler, clock=lambda: NOW)(a_message(headers=headers))

        assert len(handler.seen) == 1

    async def test_an_early_record_is_deferred_for_exactly_its_remaining_wait(
        self,
    ) -> None:
        handler = RecordingHandler()
        headers = an_envelope(not_before=NOW + timedelta(seconds=125)).to_headers()

        with pytest.raises(RetryAfter) as caught:
            await with_due_time(handler, clock=lambda: NOW)(a_message(headers=headers))

        assert caught.value.delay == 125.0
        assert handler.seen == []

    async def test_grace_absorbs_clock_skew(self) -> None:
        """The stamping host and the reading host are different machines."""
        handler = RecordingHandler()
        headers = an_envelope(not_before=NOW + timedelta(seconds=0.4)).to_headers()

        await with_due_time(handler, clock=lambda: NOW, grace=1.0)(
            a_message(headers=headers)
        )

        assert len(handler.seen) == 1

    async def test_without_grace_a_record_a_fraction_early_is_deferred(self) -> None:
        handler = RecordingHandler()
        headers = an_envelope(not_before=NOW + timedelta(seconds=0.4)).to_headers()

        with pytest.raises(RetryAfter):
            await with_due_time(handler, clock=lambda: NOW)(a_message(headers=headers))

    async def test_an_unreadable_envelope_raises_rather_than_running_the_handler(
        self,
    ) -> None:
        handler = RecordingHandler()

        with pytest.raises(MalformedEnvelopeError):
            await with_due_time(handler, clock=lambda: NOW)(
                a_message(headers=((HEADER_ATTEMPTS, b"broken"),))
            )

        assert handler.seen == []


class TestCompositionOrder:
    """Two exceptions leave the gate, and they must arrive in different places."""

    async def test_an_early_record_is_not_pushed_down_the_ladder(self) -> None:
        """The gate inside, `RetryAfter` re-raised.

        With the wrappers the other way round the dead-letter wrapper's
        `except Exception` would catch it, and a record that was merely early
        would be moved one rung down the ladder every time a consumer looked at
        it — reaching the dead-letter topic after four glances without a
        handler ever having run.
        """
        publisher = RecordingPublisher()
        handler = RecordingHandler()
        headers = an_envelope(not_before=NOW + timedelta(seconds=125)).to_headers()

        with pytest.raises(RetryAfter):
            await retry_tier_handler(handler, a_router(publisher), clock=lambda: NOW)(
                a_message(headers=headers)
            )

        assert publisher.published == []

    async def test_an_unreadable_envelope_becomes_a_dead_letter(self) -> None:
        """The gate inside again, this time so its failure *is* caught.

        With the gate outside, `MalformedEnvelopeError` would escape to the
        runner and stall the partition on a record no amount of retrying can
        repair.
        """
        publisher = RecordingPublisher()
        handler = RecordingHandler()

        await retry_tier_handler(handler, a_router(publisher), clock=lambda: NOW)(
            a_message(headers=((HEADER_ATTEMPTS, b"broken"),))
        )

        assert publisher.topics == ["orders.events.dlt"]
        assert handler.seen == []

    async def test_a_due_record_that_fails_climbs_the_ladder(self) -> None:
        publisher = RecordingPublisher()
        handler = RecordingHandler(fail=RuntimeError("still broken"))
        headers = an_envelope(not_before=NOW, attempts=1).to_headers()
        tier = Partition(topic="orders.events.retry.1", number=0)

        await retry_tier_handler(handler, a_router(publisher), clock=lambda: NOW)(
            a_message(partition=tier, headers=headers)
        )

        assert publisher.topics == ["orders.events.retry.2"]
        assert len(handler.seen) == 1

    async def test_a_due_record_that_succeeds_publishes_nothing(self) -> None:
        publisher = RecordingPublisher()
        handler = RecordingHandler()
        headers = an_envelope(not_before=NOW).to_headers()

        await retry_tier_handler(handler, a_router(publisher), clock=lambda: NOW)(
            a_message(headers=headers)
        )

        assert publisher.published == []
        assert len(handler.seen) == 1
