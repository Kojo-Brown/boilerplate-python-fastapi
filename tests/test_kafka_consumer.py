"""The aiokafka consumer wrapper: what it configures and how it translates.

A double for `AIOKafkaConsumer`, for the reason the producer's suite uses one:
the assertions here are about what is *asked of the driver* — auto-commit off,
the offsets sent as a mapping of `TopicPartition`, the batch flattened in
partition order — and against a real broker those are visible only in their
consequences, hours later, as a record that was skipped.
"""

from __future__ import annotations

from typing import Any

import pytest
from aiokafka import TopicPartition
from aiokafka.errors import CommitFailedError
from aiokafka.errors import KafkaError as DriverKafkaError

from src.kafka import consumer as consumer_module
from src.kafka.base import ConsumerError, LifecycleError, Partition
from src.kafka.consumer import ConsumerConnectionConfig, KafkaMessageSource


class FakeRecord:
    def __init__(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        key: bytes | None = b"k",
        value: bytes | None = b"v",
        headers: list[tuple[str, bytes]] | None = None,
        timestamp: int = 1700000000000,
    ) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.key = key
        self.value = value
        self.headers = headers
        self.timestamp = timestamp


class FakeDriverConsumer:
    instances: list[FakeDriverConsumer] = []

    def __init__(self, *topics: str, **kwargs: Any) -> None:
        self.topics = topics
        self.kwargs = kwargs
        self.batches: dict[Any, list[FakeRecord]] = {}
        self.commits: list[dict[Any, int]] = []
        self.seeks: list[tuple[Any, int]] = []
        self.started = False
        self.stopped = False
        self.assigned: set[Any] = set()
        self.start_error: BaseException | None = None
        self.poll_error: BaseException | None = None
        self.commit_error: BaseException | None = None
        self.seek_error: BaseException | None = None
        FakeDriverConsumer.instances.append(self)

    async def start(self) -> None:
        if self.start_error is not None:
            raise self.start_error
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def getmany(
        self, *, timeout_ms: int, max_records: int
    ) -> dict[Any, list[FakeRecord]]:
        if self.poll_error is not None:
            raise self.poll_error
        self.last_poll = (timeout_ms, max_records)
        return self.batches

    async def commit(self, offsets: dict[Any, int]) -> None:
        if self.commit_error is not None:
            raise self.commit_error
        self.commits.append(offsets)

    def seek(self, topic_partition: Any, offset: int) -> None:
        if self.seek_error is not None:
            raise self.seek_error
        self.seeks.append((topic_partition, offset))

    def assignment(self) -> set[Any]:
        return self.assigned


@pytest.fixture(autouse=True)
def driver(monkeypatch: pytest.MonkeyPatch) -> type[FakeDriverConsumer]:
    FakeDriverConsumer.instances.clear()
    monkeypatch.setattr(consumer_module, "AIOKafkaConsumer", FakeDriverConsumer)
    return FakeDriverConsumer


async def a_source(
    config: ConsumerConnectionConfig | None = None,
    topics: tuple[str, ...] = ("orders",),
) -> KafkaMessageSource:
    built = KafkaMessageSource(
        bootstrap_servers="broker:9092",
        topics=topics,
        config=config or ConsumerConnectionConfig(group_id="g"),
    )
    await built.start()
    return built


class TestConfiguration:
    async def test_auto_commit_is_off(self) -> None:
        """The whole delivery guarantee rests on it: auto-commit stores offsets
        for records the fetcher handed over, whether or not they were handled."""
        await a_source()

        assert FakeDriverConsumer.instances[0].kwargs["enable_auto_commit"] is False

    async def test_a_new_group_reads_from_the_beginning(self) -> None:
        """The driver's default is `latest`, which silently ignores everything
        produced before the consumer started."""
        await a_source()

        assert FakeDriverConsumer.instances[0].kwargs["auto_offset_reset"] == "earliest"

    async def test_the_group_is_passed_through(self) -> None:
        source = await a_source(ConsumerConnectionConfig(group_id="audit"))

        assert FakeDriverConsumer.instances[0].kwargs["group_id"] == "audit"
        assert source.group_id == "audit"
        assert source.topics == ("orders",)
        assert source.started

    def test_a_consumer_without_a_group_is_refused(self) -> None:
        """No group is nowhere to commit — a failure that would otherwise
        arrive after the first batch had been handled."""
        with pytest.raises(ValueError, match="group_id"):
            ConsumerConnectionConfig(group_id="")

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"auto_offset_reset": "whenever"},
            {"max_poll_records": 0},
            {"session_timeout_ms": 1000, "heartbeat_interval_ms": 3000},
        ],
    )
    def test_a_nonsensical_configuration_is_refused(
        self, kwargs: dict[str, object]
    ) -> None:
        with pytest.raises(ValueError):
            ConsumerConnectionConfig(group_id="g", **kwargs)  # type: ignore[arg-type]

    def test_a_source_needs_a_topic(self) -> None:
        with pytest.raises(ValueError, match="topic"):
            KafkaMessageSource(
                bootstrap_servers="b",
                topics=[],
                config=ConsumerConnectionConfig(group_id="g"),
            )


class TestLifecycle:
    async def test_start_is_idempotent(self) -> None:
        source = await a_source()
        await source.start()

        assert len(FakeDriverConsumer.instances) == 1

    async def test_stop_leaves_the_group_once(self) -> None:
        source = await a_source()

        await source.stop()
        await source.stop()

        assert FakeDriverConsumer.instances[0].stopped
        assert not source.started

    async def test_polling_before_start_is_refused(self) -> None:
        source = KafkaMessageSource(
            bootstrap_servers="b",
            topics=["orders"],
            config=ConsumerConnectionConfig(group_id="g"),
        )

        with pytest.raises(LifecycleError):
            await source.poll(max_records=1, timeout=1.0)

    async def test_a_failure_to_join_becomes_a_domain_error(self) -> None:
        def build(*topics: str, **kwargs: Any) -> FakeDriverConsumer:
            fake = FakeDriverConsumer(*topics, **kwargs)
            fake.start_error = DriverKafkaError("no brokers available")
            return fake

        source = KafkaMessageSource(
            bootstrap_servers="b",
            topics=["orders"],
            config=ConsumerConnectionConfig(group_id="g"),
        )
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(consumer_module, "AIOKafkaConsumer", build)
            with pytest.raises(ConsumerError, match="failed to start"):
                await source.start()

    async def test_an_unstarted_source_holds_no_assignment(self) -> None:
        source = KafkaMessageSource(
            bootstrap_servers="b",
            topics=["orders"],
            config=ConsumerConnectionConfig(group_id="g"),
        )

        assert source.assignment() == frozenset()


class TestPolling:
    async def test_a_batch_is_flattened_in_partition_then_offset_order(self) -> None:
        source = await a_source()
        driver = FakeDriverConsumer.instances[0]
        driver.batches = {
            TopicPartition("orders", 1): [
                FakeRecord(topic="orders", partition=1, offset=5),
                FakeRecord(topic="orders", partition=1, offset=6),
            ],
            TopicPartition("orders", 0): [
                FakeRecord(topic="orders", partition=0, offset=0)
            ],
        }

        received = await source.poll(max_records=10, timeout=1.0)

        assert [(m.partition.number, m.offset) for m in received] == [
            (0, 0),
            (1, 5),
            (1, 6),
        ]

    async def test_the_timeout_reaches_the_driver_in_milliseconds(self) -> None:
        source = await a_source()

        await source.poll(max_records=7, timeout=1.5)

        assert FakeDriverConsumer.instances[0].last_poll == (1500, 7)

    async def test_a_record_is_translated_field_by_field(self) -> None:
        source = await a_source()
        FakeDriverConsumer.instances[0].batches = {
            TopicPartition("orders", 0): [
                FakeRecord(
                    topic="orders",
                    partition=0,
                    offset=4,
                    key=b"order-9",
                    value=b'{"x":1}',
                    headers=[("trace", b"abc")],
                )
            ]
        }

        [received] = await source.poll(max_records=10, timeout=1.0)

        assert received.partition == Partition("orders", 0)
        assert received.offset == 4
        assert received.key == "order-9"
        assert received.value == b'{"x":1}'
        assert received.header("trace") == b"abc"
        assert received.timestamp.tzinfo is not None

    async def test_an_undecodable_key_does_not_stall_the_partition(self) -> None:
        """A key this service did not produce is not a reason to stop; the
        bytes of the value are untouched for a handler that cares."""
        source = await a_source()
        FakeDriverConsumer.instances[0].batches = {
            TopicPartition("orders", 0): [
                FakeRecord(topic="orders", partition=0, offset=0, key=b"\xff\xfe")
            ]
        }

        [received] = await source.poll(max_records=10, timeout=1.0)

        assert received.key is not None
        assert received.value == b"v"

    async def test_a_tombstone_arrives_as_a_null_value(self) -> None:
        source = await a_source()
        FakeDriverConsumer.instances[0].batches = {
            TopicPartition("orders", 0): [
                FakeRecord(topic="orders", partition=0, offset=0, value=None)
            ]
        }

        [received] = await source.poll(max_records=10, timeout=1.0)

        assert received.is_tombstone

    async def test_a_fetch_failure_becomes_a_domain_error(self) -> None:
        source = await a_source()
        FakeDriverConsumer.instances[0].poll_error = DriverKafkaError("gone")

        with pytest.raises(ConsumerError, match="Fetching records failed"):
            await source.poll(max_records=1, timeout=1.0)


class TestCommitting:
    async def test_offsets_are_sent_as_topic_partitions(self) -> None:
        source = await a_source()

        await source.commit({Partition("orders", 2): 11})

        assert FakeDriverConsumer.instances[0].commits == [
            {TopicPartition("orders", 2): 11}
        ]

    async def test_an_empty_commit_never_reaches_the_driver(self) -> None:
        """`commit()` with no argument means "everything fetched", which is the
        at-most-once behaviour auto-commit was turned off to avoid — and it is
        what a batch whose every handler failed would otherwise send."""
        source = await a_source()

        await source.commit({})

        assert FakeDriverConsumer.instances[0].commits == []

    async def test_a_rebalance_during_the_batch_becomes_a_domain_error(self) -> None:
        source = await a_source()
        FakeDriverConsumer.instances[0].commit_error = CommitFailedError()

        with pytest.raises(ConsumerError, match="Committing offsets failed"):
            await source.commit({Partition("orders", 0): 1})


class TestSeeking:
    async def test_it_moves_the_position_through_the_driver(self) -> None:
        source = await a_source()

        source.seek(Partition("orders", 1), 42)

        assert FakeDriverConsumer.instances[0].seeks == [
            (TopicPartition("orders", 1), 42)
        ]

    async def test_seeking_an_unassigned_partition_becomes_a_domain_error(self) -> None:
        source = await a_source()
        FakeDriverConsumer.instances[0].seek_error = DriverKafkaError("not assigned")

        with pytest.raises(ConsumerError, match="Cannot seek"):
            source.seek(Partition("orders", 1), 42)

    async def test_the_assignment_is_reported_in_this_packages_types(self) -> None:
        source = await a_source()
        FakeDriverConsumer.instances[0].assigned = {TopicPartition("orders", 0)}

        assert source.assignment() == frozenset({Partition("orders", 0)})
