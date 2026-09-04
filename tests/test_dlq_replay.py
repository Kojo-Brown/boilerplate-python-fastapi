"""Putting a dead letter back, and the four records that cannot be put back.

The refusals are the interesting half. A replayer that puts everything back is
a loop — origin, ladder, dead letter, origin — whose lag reads as zero
throughout, so the one signal an operator would look at says the system is
healthy while it burns a full ladder of latency per lap.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from src.dlq.base import ReplayNotPossibleError, RetryPolicy
from src.dlq.envelope import (
    HEADER_ATTEMPTS,
    HEADER_REPLAYS,
    DeadLetterEnvelope,
    read,
    read_replays,
)
from src.dlq.replay import DeadLetterReplayer, replay_handler
from src.dlq.router import HEADER_SYNTHETIC_KEY
from src.kafka.base import (
    ConsumedMessage,
    Headers,
    Partition,
    PublishError,
    utc_now,
)
from tests.test_dlq_router import RecordingPublisher

NOW = datetime(2026, 9, 4, 12, 0, 0, tzinfo=UTC)
DLT = Partition(topic="orders.events.dlt", number=0)


def an_envelope(**overrides: object) -> DeadLetterEnvelope:
    fields: dict[str, object] = {
        "origin_topic": "orders.events",
        "origin_partition": 1,
        "origin_offset": 42,
        "attempts": 4,
        "first_failed_at": NOW - timedelta(minutes=30),
        "not_before": NOW - timedelta(minutes=30),
        "error": "RuntimeError: downstream refused",
        "replays": 0,
    }
    fields.update(overrides)
    return DeadLetterEnvelope(**fields)  # type: ignore[arg-type]


def a_dead_letter(
    *,
    envelope: DeadLetterEnvelope | None = None,
    key: str | None = "customer-7",
    value: bytes | None = b'{"id":1}',
    extra: Headers = (),
) -> ConsumedMessage:
    headers = (envelope or an_envelope()).to_headers() if envelope is not False else ()
    return ConsumedMessage(
        partition=DLT,
        offset=3,
        key=key,
        value=value,
        headers=(*headers, *extra),
        timestamp=utc_now(),
    )


def a_replayer(
    publisher: RecordingPublisher, *, max_replays: int = 3
) -> DeadLetterReplayer:
    return DeadLetterReplayer(
        publisher=publisher,
        policy=RetryPolicy(tiers=3),
        max_replays=max_replays,
        clock=lambda: NOW,
    )


class TestPuttingARecordBack:
    async def test_it_goes_to_the_origin_topic_from_its_headers(self) -> None:
        publisher = RecordingPublisher()

        outcome = await a_replayer(publisher).replay(a_dead_letter())

        assert outcome.topic == "orders.events"
        assert publisher.topics == ["orders.events"]

    async def test_the_key_and_the_value_are_restored_exactly(self) -> None:
        publisher = RecordingPublisher()

        await a_replayer(publisher).replay(a_dead_letter())

        _, key, value, _ = publisher.published[-1]
        assert (key, value) == ("customer-7", b'{"id":1}')

    async def test_the_applications_headers_are_restored(self) -> None:
        publisher = RecordingPublisher()

        await a_replayer(publisher).replay(
            a_dead_letter(extra=(("traceparent", b"00-abc"),))
        )

        assert publisher.last_headers()["traceparent"] == b"00-abc"

    async def test_the_attempt_count_is_reset_so_the_whole_ladder_is_available(
        self,
    ) -> None:
        """A replayed record whose attempt count survived would be routed to
        tier 4 on its first failure — skipping the short rung that is the one
        most likely to fix it, and dead-lettering immediately if the ladder had
        already run out."""
        publisher = RecordingPublisher()

        await a_replayer(publisher).replay(a_dead_letter(envelope=an_envelope()))

        assert HEADER_ATTEMPTS not in publisher.last_headers()
        replayed = ConsumedMessage(
            partition=Partition(topic="orders.events", number=0),
            offset=0,
            key="customer-7",
            value=b"{}",
            headers=publisher.published[-1][3],
            timestamp=utc_now(),
        )
        assert read(replayed) is None

    async def test_the_lap_count_survives_and_grows(self) -> None:
        """The one `x-dlq-*` header a replayed record keeps.

        Without it every replay's failure looks like a first failure, and a
        record on its third lap is indistinguishable from a new one.
        """
        publisher = RecordingPublisher()

        outcome = await a_replayer(publisher).replay(
            a_dead_letter(envelope=an_envelope(replays=1))
        )

        assert outcome.replays == 2
        assert publisher.last_headers()[HEADER_REPLAYS] == b"2"

    async def test_the_lap_count_is_readable_without_an_envelope(self) -> None:
        publisher = RecordingPublisher()

        await a_replayer(publisher).replay(a_dead_letter())

        replayed = ConsumedMessage(
            partition=Partition(topic="orders.events", number=0),
            offset=0,
            key="k",
            value=b"{}",
            headers=publisher.published[-1][3],
            timestamp=utc_now(),
        )
        assert read_replays(replayed) == 1


class TestWhatCannotBePutBack:
    async def test_a_record_with_no_envelope_is_refused(self) -> None:
        """Nothing records which topic it was consumed from."""
        publisher = RecordingPublisher()

        with pytest.raises(ReplayNotPossibleError, match="no dead-letter envelope"):
            await a_replayer(publisher).replay(a_dead_letter(envelope=False))  # type: ignore[arg-type]

        assert publisher.published == []

    async def test_a_record_with_an_unreadable_envelope_is_refused(self) -> None:
        publisher = RecordingPublisher()
        broken = ConsumedMessage(
            partition=DLT,
            offset=3,
            key="k",
            value=b"{}",
            headers=((HEADER_ATTEMPTS, b"broken"),),
            timestamp=utc_now(),
        )

        with pytest.raises(ReplayNotPossibleError, match="cannot be read"):
            await a_replayer(publisher).replay(broken)

    async def test_a_synthetic_key_is_refused(self) -> None:
        """Its original null key is not something the publisher will write, so
        replaying it would place the record on a partition it never came from."""
        publisher = RecordingPublisher()

        with pytest.raises(ReplayNotPossibleError, match="synthetic key"):
            await a_replayer(publisher).replay(
                a_dead_letter(extra=((HEADER_SYNTHETIC_KEY, b"1"),))
            )

    async def test_a_record_past_the_replay_limit_is_refused(self) -> None:
        publisher = RecordingPublisher()

        with pytest.raises(ReplayNotPossibleError, match="already been replayed"):
            await a_replayer(publisher, max_replays=3).replay(
                a_dead_letter(envelope=an_envelope(replays=3))
            )

    async def test_an_origin_topic_that_is_itself_a_ladder_topic_is_refused(
        self,
    ) -> None:
        """The origin topic is a header, and therefore something a corrupt or
        foreign record can say. Believing it would republish into a retry tier,
        where the record is due immediately and has no attempt count."""
        publisher = RecordingPublisher()

        with pytest.raises(ReplayNotPossibleError, match="itself"):
            await a_replayer(publisher).replay(
                a_dead_letter(
                    envelope=an_envelope(origin_topic="orders.events.retry.1")
                )
            )

    async def test_max_replays_of_zero_refuses_everything(self) -> None:
        publisher = RecordingPublisher()

        with pytest.raises(ReplayNotPossibleError):
            await a_replayer(publisher, max_replays=0).replay(a_dead_letter())

    def test_a_negative_replay_limit_is_refused_at_construction(self) -> None:
        with pytest.raises(ValueError, match="cannot be negative"):
            DeadLetterReplayer(publisher=RecordingPublisher(), max_replays=-1)


class TestTheRecordIsNeverLost:
    async def test_a_publish_failure_reaches_the_caller(self) -> None:
        """The record stays in the dead-letter topic rather than vanishing."""
        publisher = RecordingPublisher()
        publisher.error = PublishError("broker unreachable")

        with pytest.raises(PublishError):
            await a_replayer(publisher).replay(a_dead_letter())


class TestTheDrainHandler:
    async def test_it_replays_what_it_can(self) -> None:
        publisher = RecordingPublisher()

        await replay_handler(a_replayer(publisher))(a_dead_letter())

        assert publisher.topics == ["orders.events"]

    async def test_a_refusal_does_not_stall_the_partition(self) -> None:
        """Raising would stop the drain on a record that will be refused
        identically forever, and the point of draining is to get through the
        ones that can be saved."""
        publisher = RecordingPublisher()

        await replay_handler(a_replayer(publisher))(a_dead_letter(envelope=False))  # type: ignore[arg-type]

        assert publisher.published == []

    async def test_a_publish_failure_still_stalls_the_partition(self) -> None:
        """A refusal is a decision; a broker outage is not, and committing past
        the record would be losing it."""
        publisher = RecordingPublisher()
        publisher.error = PublishError("broker unreachable")

        with pytest.raises(PublishError):
            await replay_handler(a_replayer(publisher))(a_dead_letter())
