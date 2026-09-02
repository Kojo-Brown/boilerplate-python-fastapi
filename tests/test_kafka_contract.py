"""One suite, run against the in-memory broker and against a real Kafka.

The point of a contract suite is that the runner's correctness rests on
promises the *protocols* make — records keyed alike stay ordered, offsets live
on the group rather than the consumer, an uncommitted batch comes back — and a
promise tested against one implementation is a promise about that
implementation. The in-memory broker exists so these can be asserted in
milliseconds; the Kafka leg exists so that what is asserted is true of Kafka.

The Kafka leg is skipped when nothing is listening on
`KAFKA_BOOTSTRAP_SERVERS`, and CI always has a broker (see the `kafka` service
in ci.yml), so every one of these runs against the real thing on every pull
request. A skip locally is the price of not requiring a cluster to run the
suite; a skip in CI would be the failure mode that lets this rot, which is why
the service is not optional there.

Topics are created explicitly with two partitions rather than left to
auto-creation. A cluster's `num.partitions` defaults to 1, and half of what is
tested here — assignment across members, keys sticking to partitions — is
invisible on a single-partition topic and would pass without measuring
anything.
"""

from __future__ import annotations

import asyncio
import os
import socket
import uuid
from collections.abc import AsyncGenerator, Sequence

import pytest

from src.kafka.base import ConsumedMessage, MessagePublisher, MessageSource, Partition
from src.kafka.consumer import ConsumerConnectionConfig, KafkaMessageSource
from src.kafka.memory import InMemoryBroker
from src.kafka.producer import KafkaMessagePublisher, ProducerConfig

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def kafka_reachable(servers: str = KAFKA_BOOTSTRAP_SERVERS) -> bool:
    """Cheap liveness probe used to skip, not to assert.

    A TCP connect proves a listener rather than a broker, which is deliberate:
    if something else is on the port these tests fail loudly instead of
    skipping quietly, and a silent skip is what lets a backend rot.
    """
    host, _, port = servers.partition(":")
    try:
        with socket.create_connection(
            (host or "localhost", int(port or 9092)), timeout=1
        ):
            return True
    except OSError:
        return False


KAFKA_SKIP_REASON = f"no Kafka listening on {KAFKA_BOOTSTRAP_SERVERS}"

#: Generous, because the first poll of a real consumer includes joining the
#: group and fetching metadata. Nothing here waits this long when the broker is
#: healthy — `drain` returns as soon as it has what it was told to expect.
DRAIN_TIMEOUT = 30.0


class Cluster:
    """What a backend has to offer these tests: topics, publishers, sources."""

    def __init__(self, backend: str) -> None:
        self.backend = backend
        self._broker = InMemoryBroker() if backend == "memory" else None
        self._clients: list[MessagePublisher | MessageSource] = []

    async def topic(self, *, partitions: int = 2) -> str:
        name = f"test.{uuid.uuid4().hex[:12]}"
        if self._broker is not None:
            self._broker.create_topic(name, partitions=partitions)
            return name

        from aiokafka.admin import AIOKafkaAdminClient, NewTopic

        admin = AIOKafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        await admin.start()
        try:
            await admin.create_topics(
                [NewTopic(name=name, num_partitions=partitions, replication_factor=1)]
            )
        finally:
            await admin.close()
        return name

    async def publisher(self) -> MessagePublisher:
        built: MessagePublisher
        if self._broker is not None:
            built = self._broker.publisher()
        else:
            built = KafkaMessagePublisher(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                config=ProducerConfig(client_id="contract-test"),
            )
        await built.start()
        self._clients.append(built)
        return built

    async def source(
        self,
        topics: Sequence[str],
        *,
        group_id: str,
        auto_offset_reset: str = "earliest",
    ) -> MessageSource:
        built: MessageSource
        if self._broker is not None:
            built = self._broker.source(
                topics=topics, group_id=group_id, auto_offset_reset=auto_offset_reset
            )
        else:
            built = KafkaMessageSource(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                topics=topics,
                config=ConsumerConnectionConfig(
                    group_id=group_id,
                    client_id="contract-test",
                    auto_offset_reset=auto_offset_reset,
                    # Well under the default so a member that this test stops
                    # does not keep its partitions for ten seconds afterwards.
                    session_timeout_ms=6000,
                    heartbeat_interval_ms=1000,
                ),
            )
        await built.start()
        self._clients.append(built)
        return built

    async def aclose(self) -> None:
        for client in reversed(self._clients):
            await client.stop()
        self._clients.clear()


@pytest.fixture(params=["memory", "kafka"])
async def cluster(request: pytest.FixtureRequest) -> AsyncGenerator[Cluster]:
    if request.param == "kafka" and not kafka_reachable():
        pytest.skip(KAFKA_SKIP_REASON)
    built = Cluster(request.param)
    yield built
    await built.aclose()


async def drain(
    source: MessageSource,
    *,
    expected: int,
    timeout: float = DRAIN_TIMEOUT,
    max_records: int = 100,
) -> list[ConsumedMessage]:
    """Poll until `expected` records have arrived, or give up.

    Returns what it has either way: a test asserting "and nothing more" passes
    a larger `expected` and checks the length, which is the only way to be sure
    a record did *not* arrive without waiting for the full timeout on the happy
    path.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    collected: list[ConsumedMessage] = []
    while len(collected) < expected and loop.time() < deadline:
        batch = await source.poll(max_records=max_records, timeout=1.0)
        collected.extend(batch)
    return collected


class TestPublishAndConsume:
    async def test_a_record_survives_the_round_trip_intact(
        self, cluster: Cluster
    ) -> None:
        topic = await cluster.topic()
        publisher = await cluster.publisher()
        source = await cluster.source([topic], group_id=f"g-{uuid.uuid4().hex[:8]}")

        published = await publisher.publish(
            topic,
            value=b'{"id":1}',
            key="user-1",
            headers={"content-type": b"application/json"},
        )

        [received] = await drain(source, expected=1)
        assert received.topic == topic
        assert received.key == "user-1"
        assert received.value == b'{"id":1}'
        assert received.header("content-type") == b"application/json"
        assert received.partition == published.partition
        assert received.offset == published.offset

    async def test_one_key_always_lands_on_one_partition(
        self, cluster: Cluster
    ) -> None:
        """The only ordering Kafka offers, and what a key is chosen for."""
        topic = await cluster.topic(partitions=4)
        publisher = await cluster.publisher()

        landed = {
            (
                await publisher.publish(topic, value=str(i).encode(), key="order-42")
            ).partition
            for i in range(8)
        }

        assert len(landed) == 1

    async def test_a_null_value_is_a_tombstone_rather_than_an_empty_record(
        self, cluster: Cluster
    ) -> None:
        topic = await cluster.topic(partitions=1)
        publisher = await cluster.publisher()
        source = await cluster.source([topic], group_id=f"g-{uuid.uuid4().hex[:8]}")

        await publisher.publish(topic, value=None, key="gone")
        await publisher.publish(topic, value=b"", key="empty")

        tombstone, empty = await drain(source, expected=2)
        assert tombstone.is_tombstone
        assert tombstone.value is None
        assert not empty.is_tombstone
        assert empty.value == b""

    async def test_records_arrive_in_offset_order_within_a_partition(
        self, cluster: Cluster
    ) -> None:
        topic = await cluster.topic(partitions=1)
        publisher = await cluster.publisher()
        source = await cluster.source([topic], group_id=f"g-{uuid.uuid4().hex[:8]}")

        for index in range(10):
            await publisher.publish(topic, value=str(index).encode(), key="k")

        received = await drain(source, expected=10)
        assert [m.value for m in received] == [str(i).encode() for i in range(10)]
        assert [m.offset for m in received] == sorted(m.offset for m in received)


class TestManualCommits:
    """What committing by hand buys, stated as the two halves of one promise."""

    async def test_a_committed_offset_is_where_the_group_resumes(
        self, cluster: Cluster
    ) -> None:
        topic = await cluster.topic(partitions=1)
        group = f"g-{uuid.uuid4().hex[:8]}"
        publisher = await cluster.publisher()
        for index in range(4):
            await publisher.publish(topic, value=str(index).encode(), key="k")

        first = await cluster.source([topic], group_id=group)
        received = await drain(first, expected=4)
        assert len(received) == 4
        # Through the second record only: the commit is a watermark, so this
        # says records 0 and 1 are done and says nothing about 2 and 3.
        await first.commit({received[1].partition: received[1].next_offset})
        await first.stop()

        second = await cluster.source([topic], group_id=group)
        resumed = await drain(second, expected=2)
        assert [m.value for m in resumed] == [b"2", b"3"]

    async def test_an_uncommitted_batch_is_redelivered(self, cluster: Cluster) -> None:
        """The other half: at-least-once is what "no commit" means."""
        topic = await cluster.topic(partitions=1)
        group = f"g-{uuid.uuid4().hex[:8]}"
        publisher = await cluster.publisher()
        await publisher.publish(topic, value=b"once", key="k")

        first = await cluster.source([topic], group_id=group)
        assert len(await drain(first, expected=1)) == 1
        await first.stop()

        second = await cluster.source([topic], group_id=group)
        again = await drain(second, expected=1)
        assert [m.value for m in again] == [b"once"]

    async def test_seek_re_reads_without_touching_the_commit(
        self, cluster: Cluster
    ) -> None:
        """What the runner does to a partition whose handler failed."""
        topic = await cluster.topic(partitions=1)
        group = f"g-{uuid.uuid4().hex[:8]}"
        publisher = await cluster.publisher()
        for index in range(3):
            await publisher.publish(topic, value=str(index).encode(), key="k")

        source = await cluster.source([topic], group_id=group)
        received = await drain(source, expected=3)
        assert len(received) == 3

        source.seek(received[1].partition, received[1].offset)
        replayed = await drain(source, expected=2)
        assert [m.value for m in replayed] == [b"1", b"2"]


class TestConsumerGroups:
    async def test_two_members_of_one_group_split_the_partitions(
        self, cluster: Cluster
    ) -> None:
        topic = await cluster.topic(partitions=2)
        group = f"g-{uuid.uuid4().hex[:8]}"
        publisher = await cluster.publisher()
        for index in range(20):
            await publisher.publish(topic, value=str(index).encode(), key=f"k{index}")

        first = await cluster.source([topic], group_id=group)
        second = await cluster.source([topic], group_id=group)

        # Neither member can be assumed to hold a particular partition — the
        # assignment strategy is the broker's business — so what is asserted is
        # the property that matters: between them they see every record, and
        # neither sees one the other did.
        collected: list[ConsumedMessage] = []
        loop = asyncio.get_running_loop()
        deadline = loop.time() + DRAIN_TIMEOUT
        while len(collected) < 20 and loop.time() < deadline:
            for member in (first, second):
                collected.extend(await member.poll(max_records=100, timeout=1.0))

        assert len(collected) == 20
        assert {m.value for m in collected} == {str(i).encode() for i in range(20)}
        held: set[Partition] = set()
        for member in (first, second):
            assignment = member.assignment()
            assert not (assignment & held), "a partition was assigned twice"
            held |= assignment
        assert len(held) == 2

    async def test_two_groups_each_receive_everything(self, cluster: Cluster) -> None:
        """Offsets belong to the group, which is what makes fan-out possible."""
        topic = await cluster.topic(partitions=1)
        publisher = await cluster.publisher()
        for index in range(5):
            await publisher.publish(topic, value=str(index).encode(), key="k")

        analytics = await cluster.source([topic], group_id=f"a-{uuid.uuid4().hex[:8]}")
        audit = await cluster.source([topic], group_id=f"b-{uuid.uuid4().hex[:8]}")

        assert len(await drain(analytics, expected=5)) == 5
        assert len(await drain(audit, expected=5)) == 5

    async def test_a_commit_by_one_group_does_not_move_another(
        self, cluster: Cluster
    ) -> None:
        topic = await cluster.topic(partitions=1)
        publisher = await cluster.publisher()
        for index in range(3):
            await publisher.publish(topic, value=str(index).encode(), key="k")

        fast_group = f"a-{uuid.uuid4().hex[:8]}"
        slow_group = f"b-{uuid.uuid4().hex[:8]}"
        fast = await cluster.source([topic], group_id=fast_group)
        received = await drain(fast, expected=3)
        await fast.commit({received[-1].partition: received[-1].next_offset})
        await fast.stop()

        slow = await cluster.source([topic], group_id=slow_group)
        assert len(await drain(slow, expected=3)) == 3
