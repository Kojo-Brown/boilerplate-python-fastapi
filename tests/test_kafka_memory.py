"""The in-process broker itself.

The contract suite already asserts the promises this shares with Kafka. What is
here is the modelling: that assignment moves when members come and go, that a
position is not a committed offset, and that the refusals a real broker makes —
committing a partition you no longer own — are made here too. A fake that
accepts what the real thing rejects is a fake that lets a bug through.
"""

from __future__ import annotations

import asyncio

import pytest

from src.kafka.base import ConsumerError, LifecycleError, Partition
from src.kafka.memory import DEFAULT_PARTITIONS, InMemoryBroker


@pytest.fixture
def broker() -> InMemoryBroker:
    return InMemoryBroker()


class TestTopics:
    def test_a_topic_is_created_on_demand_with_the_default_partitions(
        self, broker: InMemoryBroker
    ) -> None:
        assert len(broker.partitions_for(["events"])) == DEFAULT_PARTITIONS

    def test_declaring_a_topic_twice_is_refused(self, broker: InMemoryBroker) -> None:
        broker.create_topic("events", partitions=1)
        with pytest.raises(ValueError, match="already exists"):
            broker.create_topic("events", partitions=3)

    def test_a_topic_needs_a_partition(self, broker: InMemoryBroker) -> None:
        with pytest.raises(ValueError, match="at least one partition"):
            broker.create_topic("events", partitions=0)

    async def test_key_placement_is_stable_across_processes(
        self, broker: InMemoryBroker
    ) -> None:
        """`hash()` is salted per process, so a test asserting "one key, one
        partition" over it would pass or fail by luck."""
        broker.create_topic("events", partitions=8)
        publisher = broker.publisher()
        await publisher.start()

        first = await publisher.publish("events", value=b"1", key="stable-key")

        # A literal rather than a second publish: comparing two calls in one
        # process would pass under `hash()` too, which is the thing being ruled
        # out. FNV-1a of "stable-key" mod 8 is 0 in every process there is.
        assert first.partition == Partition("events", 0)

    async def test_an_unkeyed_record_round_robins(self, broker: InMemoryBroker) -> None:
        broker.create_topic("events", partitions=2)
        publisher = broker.publisher()
        await publisher.start()

        landed = [
            (await publisher.publish("events", value=b"x", key=None)).partition.number
            for _ in range(4)
        ]

        assert landed == [0, 1, 0, 1]


class TestLifecycle:
    async def test_publishing_before_start_is_refused(
        self, broker: InMemoryBroker
    ) -> None:
        """The same refusal the Kafka publisher makes, so a memory-backed test
        catches a missing `start()` rather than the deployment doing it."""
        publisher = broker.publisher()
        assert not publisher.started
        with pytest.raises(LifecycleError):
            await publisher.publish("events", value=b"x", key="k")
        await publisher.start()
        assert publisher.started

    async def test_polling_before_start_is_refused(
        self, broker: InMemoryBroker
    ) -> None:
        source = broker.source(topics=["events"], group_id="g")
        with pytest.raises(LifecycleError):
            await source.poll(max_records=1, timeout=0.01)

    async def test_committing_before_start_is_refused(
        self, broker: InMemoryBroker
    ) -> None:
        source = broker.source(topics=["events"], group_id="g")
        with pytest.raises(LifecycleError):
            await source.commit({Partition("events", 0): 1})

    async def test_start_and_stop_are_idempotent(self, broker: InMemoryBroker) -> None:
        """The runner calls `start` on every pass, so this is load-bearing."""
        source = broker.source(topics=["events"], group_id="g")
        assert not source.started
        assert source.group_id == "g"
        assert source.topics == ("events",)
        await source.start()
        assert source.started
        await source.start()
        assert source.assignment()
        await source.stop()
        await source.stop()
        assert source.assignment() == frozenset()

    def test_a_source_needs_topics_and_a_group(self, broker: InMemoryBroker) -> None:
        with pytest.raises(ValueError, match="topic"):
            broker.source(topics=[], group_id="g")
        with pytest.raises(ValueError, match="group_id"):
            broker.source(topics=["events"], group_id="")

    def test_an_unknown_offset_reset_is_refused(self, broker: InMemoryBroker) -> None:
        with pytest.raises(ValueError, match="auto_offset_reset"):
            broker.source(topics=["e"], group_id="g", auto_offset_reset="whenever")


class TestAssignment:
    async def test_one_member_holds_every_partition(
        self, broker: InMemoryBroker
    ) -> None:
        broker.create_topic("events", partitions=4)
        source = broker.source(topics=["events"], group_id="g")
        await source.start()

        assert len(source.assignment()) == 4

    async def test_a_second_member_takes_half(self, broker: InMemoryBroker) -> None:
        broker.create_topic("events", partitions=4)
        first = broker.source(topics=["events"], group_id="g")
        second = broker.source(topics=["events"], group_id="g")
        await first.start()
        await second.start()

        assert len(first.assignment()) == 2
        assert len(second.assignment()) == 2
        assert not (first.assignment() & second.assignment())

    async def test_a_departing_member_hands_its_partitions_back(
        self, broker: InMemoryBroker
    ) -> None:
        broker.create_topic("events", partitions=4)
        first = broker.source(topics=["events"], group_id="g")
        second = broker.source(topics=["events"], group_id="g")
        await first.start()
        await second.start()

        await second.stop()

        assert len(first.assignment()) == 4

    async def test_a_rebalance_resumes_from_the_committed_offset(
        self, broker: InMemoryBroker
    ) -> None:
        """What a real rebalance does: a partition takes its *committed*
        position with it and nothing else, which is why uncommitted work is
        repeated by whoever receives it."""
        broker.create_topic("events", partitions=1)
        publisher = broker.publisher()
        await publisher.start()
        for index in range(3):
            await publisher.publish("events", value=str(index).encode(), key="k")

        first = broker.source(topics=["events"], group_id="g")
        await first.start()
        received = await first.poll(max_records=3, timeout=0.1)
        await first.commit({received[0].partition: received[0].next_offset})
        await first.stop()

        second = broker.source(topics=["events"], group_id="g")
        await second.start()
        resumed = await second.poll(max_records=3, timeout=0.1)

        assert [m.value for m in resumed] == [b"1", b"2"]


class TestPositionsAndCommits:
    async def test_polling_moves_the_position_and_leaves_the_commit_alone(
        self, broker: InMemoryBroker
    ) -> None:
        """The difference is the entire reason an uncommitted batch comes back."""
        broker.create_topic("events", partitions=1)
        partition = Partition("events", 0)
        publisher = broker.publisher()
        await publisher.start()
        await publisher.publish("events", value=b"x", key="k")

        source = broker.source(topics=["events"], group_id="g")
        await source.start()
        await source.poll(max_records=1, timeout=0.1)

        assert source.position(partition) == 1
        assert broker.committed("g", partition) is None

    async def test_committing_a_partition_you_do_not_own_is_refused(
        self, broker: InMemoryBroker
    ) -> None:
        """What a real broker answers with CommitFailedError."""
        broker.create_topic("events", partitions=1)
        source = broker.source(topics=["events"], group_id="g")
        await source.start()

        with pytest.raises(ConsumerError, match="not assigned"):
            await source.commit({Partition("other", 0): 1})

    async def test_an_empty_commit_is_a_no_op(self, broker: InMemoryBroker) -> None:
        source = broker.source(topics=["events"], group_id="g")
        await source.start()
        await source.commit({})

    async def test_a_negative_offset_is_refused(self, broker: InMemoryBroker) -> None:
        broker.create_topic("events", partitions=1)
        source = broker.source(topics=["events"], group_id="g")
        await source.start()

        with pytest.raises(ValueError):
            await source.commit({Partition("events", 0): -1})
        with pytest.raises(ValueError):
            source.seek(Partition("events", 0), -1)

    async def test_seeking_a_partition_you_do_not_own_is_refused(
        self, broker: InMemoryBroker
    ) -> None:
        source = broker.source(topics=["events"], group_id="g")
        await source.start()

        with pytest.raises(ConsumerError, match="not assigned"):
            source.seek(Partition("elsewhere", 3), 0)

    async def test_latest_skips_what_was_published_before_joining(
        self, broker: InMemoryBroker
    ) -> None:
        broker.create_topic("events", partitions=1)
        publisher = broker.publisher()
        await publisher.start()
        await publisher.publish("events", value=b"old", key="k")

        source = broker.source(
            topics=["events"], group_id="g", auto_offset_reset="latest"
        )
        await source.start()
        await publisher.publish("events", value=b"new", key="k")

        received = await source.poll(max_records=10, timeout=0.1)
        assert [m.value for m in received] == [b"new"]


class TestPolling:
    async def test_an_idle_poll_returns_empty_after_its_timeout(
        self, broker: InMemoryBroker
    ) -> None:
        source = broker.source(topics=["events"], group_id="g")
        await source.start()

        assert await source.poll(max_records=10, timeout=0.05) == []

    async def test_a_waiting_poll_is_woken_by_a_publish(
        self, broker: InMemoryBroker
    ) -> None:
        """Without the wake-up every consumer would be a polling loop, and the
        loop's poll timeout would be its delivery latency."""
        broker.create_topic("events", partitions=1)
        publisher = broker.publisher()
        await publisher.start()
        source = broker.source(topics=["events"], group_id="g")
        await source.start()

        async def publish_soon() -> None:
            await asyncio.sleep(0.01)
            await publisher.publish("events", value=b"late", key="k")

        publishing = asyncio.create_task(publish_soon())
        received = await source.poll(max_records=10, timeout=5.0)
        await publishing

        assert [m.value for m in received] == [b"late"]

    async def test_max_records_bounds_the_batch(self, broker: InMemoryBroker) -> None:
        broker.create_topic("events", partitions=1)
        publisher = broker.publisher()
        await publisher.start()
        for index in range(5):
            await publisher.publish("events", value=str(index).encode(), key="k")

        source = broker.source(topics=["events"], group_id="g")
        await source.start()

        assert len(await source.poll(max_records=2, timeout=0.1)) == 2

    async def test_a_bounded_batch_stops_before_the_next_partition(
        self, broker: InMemoryBroker
    ) -> None:
        """`max_records` bounds the batch across partitions, not per partition:
        a limit applied per partition would be a limit multiplied by the
        assignment, which is not what a caller sizing a batch means."""
        broker.create_topic("events", partitions=2)
        publisher = broker.publisher()
        await publisher.start()
        for _ in range(4):
            await publisher.publish("events", value=b"x", key=None)

        source = broker.source(topics=["events"], group_id="g")
        await source.start()
        first = await source.poll(max_records=1, timeout=0.1)
        second = await source.poll(max_records=3, timeout=0.1)

        assert len(first) == 1
        assert len(second) == 3

    async def test_a_batch_of_none_is_refused(self, broker: InMemoryBroker) -> None:
        source = broker.source(topics=["events"], group_id="g")
        await source.start()

        with pytest.raises(ValueError, match="max_records"):
            await source.poll(max_records=0, timeout=0.1)

    async def test_records_are_drained_partition_by_partition(
        self, broker: InMemoryBroker
    ) -> None:
        broker.create_topic("events", partitions=2)
        publisher = broker.publisher()
        await publisher.start()
        for index in range(6):
            await publisher.publish("events", value=str(index).encode(), key=None)

        source = broker.source(topics=["events"], group_id="g")
        await source.start()
        received = await source.poll(max_records=10, timeout=0.1)

        numbers = [m.partition.number for m in received]
        assert numbers == sorted(numbers)
        offsets_by_partition: dict[int, list[int]] = {}
        for message in received:
            offsets_by_partition.setdefault(message.partition.number, []).append(
                message.offset
            )
        for offsets in offsets_by_partition.values():
            assert offsets == sorted(offsets)

    async def test_the_end_offset_is_the_length_of_the_log(
        self, broker: InMemoryBroker
    ) -> None:
        broker.create_topic("events", partitions=1)
        publisher = broker.publisher()
        await publisher.start()
        await publisher.publish("events", value=b"x", key="k")

        assert broker.end_offset(Partition("events", 0)) == 1
