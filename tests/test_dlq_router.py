"""Where a failed record goes, over a publisher that does exactly what it is told.

A recording publisher rather than the in-memory broker, for the same reason
`test_kafka_runner.py` uses a fake source: what is asserted here is *which*
topic was chosen and *what headers* went with it, and reading those back out of
a broker would be inferring the decision from its consequences.

The two cases worth reading first are `TestTheRecordIsNeverDropped` — where a
publish that fails must not let the caller commit past the record — and
`TestAnUnreadableEnvelope`, where the record's own metadata is the broken part.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta

import pytest

from src.dlq.base import RetryPolicy
from src.dlq.envelope import (
    HEADER_ATTEMPTS,
    HEADER_NOT_BEFORE,
    HEADER_ORIGIN_OFFSET,
    HEADER_ORIGIN_TOPIC,
    HEADER_REPLAYS,
    DeadLetterEnvelope,
    read,
)
from src.dlq.router import HEADER_SYNTHETIC_KEY, DeadLetterRouter
from src.kafka.base import (
    ConsumedMessage,
    Headers,
    MessageNotDecodableError,
    Partition,
    PublishedMessage,
    PublishError,
    normalize_headers,
    utc_now,
)

NOW = datetime(2026, 9, 4, 12, 0, 0, tzinfo=UTC)
ORIGIN = Partition(topic="orders.events", number=1)


class RecordingPublisher:
    """A `MessagePublisher` that keeps every record and can refuse on demand."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str | None, bytes | None, Headers]] = []
        self.error: Exception | None = None
        self.starts = 0
        self.stops = 0

    async def start(self) -> None:
        self.starts += 1

    async def stop(self) -> None:
        self.stops += 1

    async def publish(
        self,
        topic: str,
        *,
        value: bytes | None,
        key: str | None = None,
        headers: Mapping[str, bytes] | Headers | None = None,
    ) -> PublishedMessage:
        if self.error is not None:
            raise self.error
        normalized = normalize_headers(headers)
        self.published.append((topic, key, value, normalized))
        return PublishedMessage(
            partition=Partition(topic=topic, number=0),
            offset=len(self.published) - 1,
            timestamp=utc_now(),
        )

    @property
    def topics(self) -> list[str]:
        return [topic for topic, _, _, _ in self.published]

    def last_headers(self) -> dict[str, bytes]:
        return dict(self.published[-1][3])


def a_message(
    *,
    partition: Partition = ORIGIN,
    offset: int = 42,
    key: str | None = "k",
    value: bytes | None = b"payload",
    headers: Headers = (),
) -> ConsumedMessage:
    return ConsumedMessage(
        partition=partition,
        offset=offset,
        key=key,
        value=value,
        headers=headers,
        timestamp=utc_now(),
    )


def a_router(
    publisher: RecordingPublisher,
    *,
    tiers: int = 3,
    now: datetime = NOW,
    non_retryable: tuple[type[BaseException], ...] = (MessageNotDecodableError,),
) -> DeadLetterRouter:
    return DeadLetterRouter(
        publisher=publisher,
        policy=RetryPolicy(base_delay=5.0, multiplier=5.0, tiers=tiers, max_delay=900),
        clock=lambda: now,
        non_retryable=non_retryable,
    )


async def route_through_the_ladder(
    publisher: RecordingPublisher, router: DeadLetterRouter, *, hops: int
) -> list[str]:
    """Fail one record `hops` times, feeding each republished record back in."""
    message = a_message()
    topics: list[str] = []
    for _ in range(hops):
        outcome = await router.route(message, RuntimeError("nope"))
        topics.append(outcome.topic)
        topic, key, value, headers = publisher.published[-1]
        message = a_message(
            partition=Partition(topic=topic, number=0),
            offset=0,
            key=key,
            value=value,
            headers=headers,
        )
    return topics


class TestTheFirstFailure:
    async def test_it_goes_to_the_first_tier(self) -> None:
        publisher = RecordingPublisher()

        outcome = await a_router(publisher).route(a_message(), RuntimeError("nope"))

        assert outcome.topic == "orders.events.retry.1"
        assert outcome.attempts == 1
        assert outcome.delay == 5.0
        assert outcome.retried

    async def test_it_keeps_the_key_and_the_value_untouched(self) -> None:
        """The payload is the application's; a dead letter has to be replayable."""
        publisher = RecordingPublisher()

        await a_router(publisher).route(
            a_message(key="customer-7", value=b'{"id":1}'), RuntimeError("nope")
        )

        _, key, value, _ = publisher.published[-1]
        assert key == "customer-7"
        assert value == b'{"id":1}'

    async def test_it_keeps_the_applications_headers(self) -> None:
        publisher = RecordingPublisher()

        await a_router(publisher).route(
            a_message(headers=(("traceparent", b"00-abc"),)), RuntimeError("nope")
        )

        assert publisher.last_headers()["traceparent"] == b"00-abc"

    async def test_it_stamps_where_the_record_came_from(self) -> None:
        publisher = RecordingPublisher()

        await a_router(publisher).route(a_message(offset=42), RuntimeError("nope"))

        headers = publisher.last_headers()
        assert headers[HEADER_ORIGIN_TOPIC] == b"orders.events"
        assert headers[HEADER_ORIGIN_OFFSET] == b"42"
        assert headers[HEADER_ATTEMPTS] == b"1"

    async def test_the_due_time_is_now_plus_the_tier_delay(self) -> None:
        publisher = RecordingPublisher()

        await a_router(publisher).route(a_message(), RuntimeError("nope"))

        due = publisher.last_headers()[HEADER_NOT_BEFORE].decode()
        assert datetime.fromisoformat(due) == NOW + timedelta(seconds=5)

    async def test_the_error_is_recorded_on_the_record(self) -> None:
        publisher = RecordingPublisher()

        outcome = await a_router(publisher).route(
            a_message(), RuntimeError("downstream refused")
        )

        assert outcome.envelope.error == "RuntimeError: downstream refused"


class TestClimbingTheLadder:
    async def test_each_failure_moves_one_rung_down(self) -> None:
        publisher = RecordingPublisher()

        topics = await route_through_the_ladder(publisher, a_router(publisher), hops=4)

        assert topics == [
            "orders.events.retry.1",
            "orders.events.retry.2",
            "orders.events.retry.3",
            "orders.events.dlt",
        ]

    async def test_the_delay_grows_with_the_tier(self) -> None:
        publisher = RecordingPublisher()
        router = a_router(publisher)
        message = a_message()
        delays: list[float] = []

        for _ in range(4):
            outcome = await router.route(message, RuntimeError("nope"))
            delays.append(outcome.delay)
            topic, key, value, headers = publisher.published[-1]
            message = a_message(
                partition=Partition(topic=topic, number=0), key=key, headers=headers
            )

        assert delays == [5.0, 25.0, 125.0, 0.0]

    async def test_the_attempt_count_is_written_once_per_hop(self) -> None:
        """Appending would leave the first value winning forever — see envelope.py."""
        publisher = RecordingPublisher()

        await route_through_the_ladder(publisher, a_router(publisher), hops=3)

        names = [name for name, _ in publisher.published[-1][3]]
        assert names.count(HEADER_ATTEMPTS) == 1
        assert dict(publisher.published[-1][3])[HEADER_ATTEMPTS] == b"3"

    async def test_provenance_survives_every_hop(self) -> None:
        publisher = RecordingPublisher()

        await route_through_the_ladder(publisher, a_router(publisher), hops=4)

        headers = publisher.last_headers()
        assert headers[HEADER_ORIGIN_TOPIC] == b"orders.events"
        assert headers[HEADER_ORIGIN_OFFSET] == b"42"

    async def test_a_ladder_with_no_tiers_dead_letters_immediately(self) -> None:
        publisher = RecordingPublisher()

        outcome = await a_router(publisher, tiers=0).route(
            a_message(), RuntimeError("nope")
        )

        assert outcome.topic == "orders.events.dlt"
        assert outcome.dead_lettered


class TestSkippingTheLadder:
    async def test_a_non_retryable_failure_goes_straight_to_the_dead_letter_topic(
        self,
    ) -> None:
        """Bytes that will not decode now will not decode in fifteen minutes."""
        publisher = RecordingPublisher()

        outcome = await a_router(publisher).route(
            a_message(), MessageNotDecodableError("not json")
        )

        assert outcome.topic == "orders.events.dlt"
        assert outcome.dead_lettered
        assert outcome.delay == 0.0

    async def test_a_subclass_of_a_non_retryable_type_is_also_permanent(self) -> None:
        class NotJson(MessageNotDecodableError):
            pass

        publisher = RecordingPublisher()

        outcome = await a_router(publisher).route(a_message(), NotJson("nope"))

        assert outcome.dead_lettered

    async def test_the_set_is_configurable(self) -> None:
        publisher = RecordingPublisher()

        outcome = await a_router(publisher, non_retryable=(KeyError,)).route(
            a_message(), KeyError("missing")
        )

        assert outcome.dead_lettered

    async def test_a_decode_error_is_the_only_default(self) -> None:
        """Treating every ValueError as permanent would dead-letter a
        validation failure caused by a dependency briefly returning nonsense."""
        publisher = RecordingPublisher()

        outcome = await a_router(publisher).route(a_message(), ValueError("odd"))

        assert outcome.retried


class TestTheRecordIsNeverDropped:
    async def test_a_publish_failure_reaches_the_caller(self) -> None:
        """The caller commits past a record only because routing succeeded.

        Swallowing this would commit past a record that now exists nowhere,
        which is the one failure mode this package must not have.
        """
        publisher = RecordingPublisher()
        publisher.error = PublishError("broker unreachable")

        with pytest.raises(PublishError):
            await a_router(publisher).route(a_message(), RuntimeError("nope"))

    async def test_nothing_is_published_when_the_publish_fails(self) -> None:
        publisher = RecordingPublisher()
        publisher.error = PublishError("broker unreachable")

        with pytest.raises(PublishError):
            await a_router(publisher).route(a_message(), RuntimeError("nope"))

        assert publisher.published == []


class TestAnUnreadableEnvelope:
    """A record whose own metadata is broken cannot be retried correctly."""

    async def test_it_is_dead_lettered_rather_than_stalling_the_partition(
        self,
    ) -> None:
        publisher = RecordingPublisher()

        outcome = await a_router(publisher).route(
            a_message(headers=((HEADER_ATTEMPTS, b"not-a-number"),)),
            RuntimeError("nope"),
        )

        assert outcome.topic == "orders.events.dlt"
        assert outcome.dead_lettered

    async def test_the_destination_comes_from_the_topic_name(self) -> None:
        """The headers are the broken part, so the name is what is left."""
        publisher = RecordingPublisher()

        outcome = await a_router(publisher).route(
            a_message(
                partition=Partition(topic="orders.events.retry.2", number=0),
                headers=((HEADER_ATTEMPTS, b"???"),),
            ),
            RuntimeError("nope"),
        )

        assert outcome.topic == "orders.events.dlt"

    async def test_the_reason_is_recorded_on_the_record(self) -> None:
        publisher = RecordingPublisher()

        outcome = await a_router(publisher).route(
            a_message(headers=((HEADER_ATTEMPTS, b"???"),)), RuntimeError("nope")
        )

        assert "MalformedEnvelopeError" in outcome.envelope.error

    async def test_a_readable_record_on_a_tier_uses_its_header_not_its_topic(
        self,
    ) -> None:
        """The header is authoritative: a deployment can change a suffix, and
        records already in flight keep the names they were published under."""
        publisher = RecordingPublisher()
        envelope = DeadLetterEnvelope(
            origin_topic="orders.events",
            origin_partition=1,
            origin_offset=42,
            attempts=1,
            first_failed_at=NOW,
            not_before=NOW,
            error="RuntimeError: nope",
        )
        arrived_on = a_message(
            partition=Partition(topic="orders.events.oldsuffix.1", number=0),
            headers=envelope.to_headers(),
        )

        outcome = await a_router(publisher).route(arrived_on, RuntimeError("nope"))

        assert outcome.topic == "orders.events.retry.2"


class TestAKeylessValuelessRecord:
    """A record with no key and no value cannot be republished verbatim.

    `validate_record` refuses it — it reads as a tombstone for no key — so
    routing it as-is would raise `ValueError` out of `route`, stall the
    partition on a record that can never be routed anywhere, and turn one
    poison record into an outage.
    """

    async def test_it_is_routed_under_a_synthetic_key_rather_than_stalling(
        self,
    ) -> None:
        publisher = RecordingPublisher()

        outcome = await a_router(publisher).route(
            a_message(key=None, value=None), RuntimeError("nope")
        )

        assert outcome.topic == "orders.events.retry.1"
        _, key, value, _ = publisher.published[-1]
        assert key == "orders.events:1:42"
        assert value is None

    async def test_the_synthetic_key_is_flagged_for_the_replayer(self) -> None:
        publisher = RecordingPublisher()

        await a_router(publisher).route(
            a_message(key=None, value=None), RuntimeError("nope")
        )

        assert publisher.last_headers()[HEADER_SYNTHETIC_KEY] == b"1"

    async def test_a_keyless_record_with_a_value_keeps_its_null_key(self) -> None:
        """A null key is legal with a value, and round-robins. Inventing one
        would move the record to a partition it never belonged to."""
        publisher = RecordingPublisher()

        await a_router(publisher).route(
            a_message(key=None, value=b"payload"), RuntimeError("nope")
        )

        _, key, _, headers = publisher.published[-1]
        assert key is None
        assert HEADER_SYNTHETIC_KEY not in dict(headers)


class TestTombstones:
    async def test_a_keyed_tombstone_stays_a_tombstone(self) -> None:
        """A null value on a keyed record means "forget this key", and a
        dead letter that arrived as one has to still be one on the way back."""
        publisher = RecordingPublisher()

        await a_router(publisher).route(
            a_message(key="k", value=None), RuntimeError("nope")
        )

        _, key, value, _ = publisher.published[-1]
        assert (key, value) == ("k", None)


class TestTheLadderHelper:
    def test_ladder_for_reads_the_header_when_it_can(self) -> None:
        envelope = DeadLetterEnvelope(
            origin_topic="orders.events",
            origin_partition=0,
            origin_offset=1,
            attempts=2,
            first_failed_at=NOW,
            not_before=NOW,
            error="",
        )
        message = a_message(
            partition=Partition(topic="orders.events.retry.2", number=0),
            headers=envelope.to_headers(),
        )

        ladder = a_router(RecordingPublisher()).ladder_for(message)

        assert ladder.origin_topic == "orders.events"

    def test_ladder_for_falls_back_to_the_topic_name(self) -> None:
        message = a_message(
            partition=Partition(topic="orders.events.retry.2", number=0),
            headers=((HEADER_ATTEMPTS, b"broken"),),
        )

        ladder = a_router(RecordingPublisher()).ladder_for(message)

        assert ladder.origin_topic == "orders.events"


class TestReplayCounts:
    async def test_a_replayed_records_lap_count_survives_the_ladder(self) -> None:
        """Or a record on its third lap reads as a first failure every time."""
        publisher = RecordingPublisher()

        await a_router(publisher).route(
            a_message(headers=((HEADER_REPLAYS, b"2"),)), RuntimeError("nope")
        )

        assert publisher.last_headers()[HEADER_REPLAYS] == b"2"
        envelope = read(
            ConsumedMessage(
                partition=Partition(topic="orders.events.retry.1", number=0),
                offset=0,
                key="k",
                value=b"payload",
                headers=publisher.published[-1][3],
                timestamp=utc_now(),
            )
        )
        assert envelope is not None
        assert envelope.replays == 2


class TestAFirstFailureOnATierTopic:
    """A record can reach its *first* failure somewhere other than the origin.

    An operator hand-publishing into a tier, or a record left by an older
    deployment, arrives with no envelope on `orders.events.retry.1`. Stamping
    the topic in hand as its origin makes it unroutable on the *second*
    failure: the next hop tries to build a ladder on `orders.events.retry.1`,
    which is refused, and the `ValueError` escapes `route` and stalls the
    partition forever — a loop introduced by the code meant to end one.
    """

    async def test_the_origin_stamped_is_the_ladders_not_the_topic_in_hand(
        self,
    ) -> None:
        publisher = RecordingPublisher()
        tier = Partition(topic="orders.events.retry.1", number=0)

        await a_router(publisher).route(a_message(partition=tier), RuntimeError("nope"))

        assert publisher.last_headers()[HEADER_ORIGIN_TOPIC] == b"orders.events"

    async def test_it_keeps_climbing_instead_of_stalling(self) -> None:
        publisher = RecordingPublisher()
        router = a_router(publisher)
        message = a_message(
            partition=Partition(topic="orders.events.retry.1", number=0)
        )
        topics: list[str] = []

        for _ in range(4):
            outcome = await router.route(message, RuntimeError("nope"))
            topics.append(outcome.topic)
            topic, key, value, headers = publisher.published[-1]
            message = a_message(
                partition=Partition(topic=topic, number=0),
                key=key,
                value=value,
                headers=headers,
            )

        assert topics == [
            "orders.events.retry.1",
            "orders.events.retry.2",
            "orders.events.retry.3",
            "orders.events.dlt",
        ]
