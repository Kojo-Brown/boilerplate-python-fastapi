"""The ladder driven through a broker: origin topic in, dead-letter topic out.

The unit tests decide *what* the router publishes. This decides whether the
whole arrangement — two runners, two consumer groups, four topics, a clock the
test moves by hand — actually gets a record from `orders.events` to
`orders.events.dlt` and lets the partition behind it carry on.

The in-memory broker rather than a real one, deliberately: what is under test
is the composition, and the in-memory broker models the parts it depends on
(partitions, committed offsets, positions that are not offsets). The transports
themselves already agree — `test_kafka_contract.py` proves that against a real
cluster — and nothing here reaches for a behaviour the model does not have.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from src.dlq.base import RetryPolicy
from src.dlq.envelope import read
from src.dlq.handler import retry_tier_handler, with_dead_letter
from src.dlq.replay import DeadLetterReplayer, replay_handler
from src.dlq.router import DeadLetterRouter
from src.kafka.base import ConsumedMessage, MessageNotDecodableError, Partition
from src.kafka.memory import InMemoryBroker
from src.kafka.runner import ConsumerConfig, ConsumerRunner

ORIGIN = "orders.events"
POLICY = RetryPolicy(base_delay=5.0, multiplier=5.0, tiers=2, max_delay=900)
LADDER = POLICY.ladder_for(ORIGIN)


class MovableClock:
    """A clock the test winds forward, so a 25-second tier costs nothing."""

    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


class Handler:
    """Fails every record whose key is in `failing`, and counts every call."""

    def __init__(self, failing: set[str], *, error: Exception | None = None) -> None:
        self.failing = failing
        self.error = error
        self.calls: list[tuple[str, str | None]] = []

    async def __call__(self, message: ConsumedMessage) -> None:
        self.calls.append((message.topic, message.key))
        if message.key in self.failing:
            raise self.error or RuntimeError(f"cannot handle {message.key}")

    def attempts_for(self, key: str) -> int:
        return sum(1 for _, seen in self.calls if seen == key)


@pytest.fixture
def broker() -> InMemoryBroker:
    broker = InMemoryBroker(default_partitions=1)
    for topic in LADDER.topics:
        broker.create_topic(topic, partitions=1)
    return broker


class Ladder:
    """One origin runner, one retry runner, and the broker underneath them."""

    def __init__(
        self, broker: InMemoryBroker, handler: Handler, clock: MovableClock
    ) -> None:
        self.broker = broker
        self.handler = handler
        self.clock = clock
        self.publisher = broker.publisher()
        self.router = DeadLetterRouter(
            publisher=self.publisher, policy=POLICY, clock=clock
        )
        config = ConsumerConfig(poll_timeout=0.01, jitter=False)
        # The sources are held rather than left inside the runners: these tests
        # drive `consume_once` directly, which assumes a started source — `run`
        # is what would otherwise have started it.
        self.origin_source = broker.source(topics=[ORIGIN], group_id="app")
        self.retry_source = broker.source(
            topics=LADDER.retry_topics, group_id="app.retry"
        )
        self.origin = ConsumerRunner(
            source=self.origin_source,
            handler=with_dead_letter(handler, self.router),
            name="origin",
            config=config,
        )
        self.retries = ConsumerRunner(
            source=self.retry_source,
            handler=retry_tier_handler(handler, self.router, clock=clock),
            name="retry",
            config=config,
        )

    async def start(self) -> None:
        await self.publisher.start()
        await self.origin_source.start()
        await self.retry_source.start()

    async def produce(self, key: str, value: bytes = b"{}") -> None:
        await self.publisher.publish(ORIGIN, value=value, key=key)

    async def drain(self) -> None:
        """Poll both consumers until neither has anything left to do."""
        for _ in range(20):
            origin = await self.origin.consume_once()
            retries = await self.retries.consume_once()
            if origin.empty and retries.empty:
                return

    def records_in(self, topic: str) -> list[ConsumedMessage]:
        partition = Partition(topic=topic, number=0)
        return [
            record.consumed(partition)
            for record in self.broker.topic(topic).read(partition, 0, 100)
        ]


@pytest.fixture
async def ladder(broker: InMemoryBroker) -> Ladder:
    built = Ladder(
        broker, Handler({"bad"}), MovableClock(datetime(2026, 9, 4, tzinfo=UTC))
    )
    await built.start()
    return built


class TestAPoisonRecordNoLongerBlocksItsPartition:
    async def test_the_records_behind_it_are_handled(self, ladder: Ladder) -> None:
        """The whole point. Without the wrapper, `good` is never seen at all:
        the partition stops at `bad` and stays there."""
        await ladder.produce("bad")
        await ladder.produce("good")

        await ladder.drain()

        assert ("orders.events", "good") in ladder.handler.calls

    async def test_the_origin_partition_commits_past_the_failure(
        self, ladder: Ladder
    ) -> None:
        await ladder.produce("bad")
        await ladder.produce("good")

        await ladder.drain()

        assert ladder.broker.committed("app", Partition(topic=ORIGIN, number=0)) == 2

    async def test_the_failed_record_is_in_the_first_tier(self, ladder: Ladder) -> None:
        await ladder.produce("bad")

        await ladder.drain()

        tier = ladder.records_in("orders.events.retry.1")
        assert [record.key for record in tier] == ["bad"]


class TestTheLadderIsWalkedOverTime:
    async def test_a_record_is_not_retried_before_it_is_due(
        self, ladder: Ladder
    ) -> None:
        await ladder.produce("bad")
        await ladder.drain()

        assert ladder.handler.attempts_for("bad") == 1
        assert ladder.records_in("orders.events.retry.2") == []

    async def test_advancing_the_clock_lets_the_first_tier_run(
        self, ladder: Ladder
    ) -> None:
        await ladder.produce("bad")
        await ladder.drain()

        ladder.clock.advance(5)
        await ladder.drain()

        assert ladder.handler.attempts_for("bad") == 2
        assert [r.key for r in ladder.records_in("orders.events.retry.2")] == ["bad"]

    async def test_the_record_reaches_the_dead_letter_topic_after_the_last_tier(
        self, ladder: Ladder
    ) -> None:
        await ladder.produce("bad")
        await ladder.drain()
        for delay in (5, 25):
            ladder.clock.advance(delay)
            await ladder.drain()

        dead = ladder.records_in("orders.events.dlt")
        assert [record.key for record in dead] == ["bad"]
        assert ladder.handler.attempts_for("bad") == LADDER.max_attempts == 3

    async def test_the_dead_letter_carries_its_whole_history(
        self, ladder: Ladder
    ) -> None:
        await ladder.produce("bad")
        await ladder.drain()
        for delay in (5, 25):
            ladder.clock.advance(delay)
            await ladder.drain()

        envelope = read(ladder.records_in("orders.events.dlt")[0])
        assert envelope is not None
        assert envelope.origin_topic == ORIGIN
        assert envelope.origin_offset == 0
        assert envelope.attempts == 3
        assert envelope.error == "RuntimeError: cannot handle bad"
        assert envelope.age(ladder.clock.now) == 30.0

    async def test_a_record_that_starts_working_never_reaches_the_dead_letter_topic(
        self, ladder: Ladder
    ) -> None:
        """The ordinary happy path of a ladder: the outage ends mid-climb."""
        await ladder.produce("bad")
        await ladder.drain()

        ladder.handler.failing.clear()
        ladder.clock.advance(5)
        await ladder.drain()
        ladder.clock.advance(25)
        await ladder.drain()

        assert ladder.records_in("orders.events.dlt") == []
        assert ladder.handler.attempts_for("bad") == 2


class TestSkippingTheLadder:
    async def test_an_undecodable_record_goes_straight_to_the_dead_letter_topic(
        self, broker: InMemoryBroker
    ) -> None:
        clock = MovableClock(datetime(2026, 9, 4, tzinfo=UTC))
        handler = Handler({"bad"}, error=MessageNotDecodableError("not json"))
        ladder = Ladder(broker, handler, clock)
        await ladder.start()
        await ladder.produce("bad")

        await ladder.drain()

        assert [r.key for r in ladder.records_in("orders.events.dlt")] == ["bad"]
        assert ladder.records_in("orders.events.retry.1") == []


class TestDrainingTheDeadLetterTopic:
    async def test_a_replayed_record_goes_back_and_is_handled(
        self, ladder: Ladder
    ) -> None:
        await ladder.produce("bad")
        await ladder.drain()
        for delay in (5, 25):
            ladder.clock.advance(delay)
            await ladder.drain()
        assert len(ladder.records_in("orders.events.dlt")) == 1

        ladder.handler.failing.clear()
        dlt_source = ladder.broker.source(
            topics=[LADDER.dead_letter_topic], group_id="drain"
        )
        drain = ConsumerRunner(
            source=dlt_source,
            handler=replay_handler(
                DeadLetterReplayer(
                    publisher=ladder.publisher, policy=POLICY, clock=ladder.clock
                )
            ),
            name="drain",
            config=ConsumerConfig(poll_timeout=0.01, jitter=False),
        )
        await dlt_source.start()
        await drain.consume_once()
        await ladder.drain()

        assert ladder.handler.calls[-1] == (ORIGIN, "bad")

    async def test_the_replayed_record_gets_the_whole_ladder_again(
        self, ladder: Ladder
    ) -> None:
        """Its attempt count was reset, so a fresh failure starts at tier 1
        rather than being dead-lettered immediately."""
        await ladder.produce("bad")
        await ladder.drain()
        for delay in (5, 25):
            ladder.clock.advance(delay)
            await ladder.drain()

        replayer = DeadLetterReplayer(
            publisher=ladder.publisher, policy=POLICY, clock=ladder.clock
        )
        await replayer.replay(ladder.records_in("orders.events.dlt")[0])
        await ladder.drain()

        tier = ladder.records_in("orders.events.retry.1")
        assert [record.key for record in tier] == ["bad", "bad"]
        envelope = read(tier[-1])
        assert envelope is not None
        assert envelope.attempts == 1
        assert envelope.replays == 1
