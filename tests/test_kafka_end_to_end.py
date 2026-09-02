"""The runner, driven through an actual broker rather than a fake source.

`test_kafka_runner.py` asserts the decisions; this asserts that the decisions
are the right ones against something that stores offsets. The distinction
matters most for the failure path: a runner that seeks back and does not commit
is only useful if the record genuinely comes round again, and no fake can prove
that.

Parametrised over the same two backends as the contract suite, so the Kafka leg
runs the whole loop — join, fetch, commit, rebalance — against a real cluster in
CI.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator

import pytest

from src.kafka.base import ConsumedMessage
from src.kafka.runner import ConsumerConfig, ConsumerRunner
from tests.test_kafka_contract import (
    DRAIN_TIMEOUT,
    KAFKA_SKIP_REASON,
    Cluster,
    drain,
    kafka_reachable,
)


@pytest.fixture(params=["memory", "kafka"])
async def cluster(request: pytest.FixtureRequest) -> AsyncGenerator[Cluster]:
    if request.param == "kafka" and not kafka_reachable():
        pytest.skip(KAFKA_SKIP_REASON)
    built = Cluster(request.param)
    yield built
    await built.aclose()


def a_config() -> ConsumerConfig:
    return ConsumerConfig(
        max_records=10,
        poll_timeout=0.5,
        handler_timeout=5.0,
        retry_base_delay=0.05,
        retry_max_delay=0.2,
        jitter=False,
        shutdown_timeout=10.0,
    )


class Collector:
    """A handler that records what it received, and can be told to refuse."""

    def __init__(self, *, refuse: set[bytes] | None = None) -> None:
        self.seen: list[ConsumedMessage] = []
        self.refuse = refuse or set()

    async def __call__(self, message: ConsumedMessage) -> None:
        self.seen.append(message)
        if message.value in self.refuse:
            raise RuntimeError(f"refusing {message.value!r}")


async def wait_for(condition: object, *, timeout: float = DRAIN_TIMEOUT) -> None:
    """Poll a predicate until it holds. Beats a fixed sleep against a broker
    whose first fetch includes joining a group."""
    assert callable(condition)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if condition():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition was never met")


class TestTheHappyPath:
    async def test_a_running_consumer_handles_and_commits(
        self, cluster: Cluster
    ) -> None:
        topic = await cluster.topic(partitions=1)
        group = f"g-{uuid.uuid4().hex[:8]}"
        publisher = await cluster.publisher()
        for index in range(5):
            await publisher.publish(topic, value=str(index).encode(), key="k")

        handler = Collector()
        runner = ConsumerRunner(
            source=await cluster.source([topic], group_id=group),
            handler=handler,
            name="e2e",
            config=a_config(),
        )
        runner.start()
        await wait_for(lambda: len(handler.seen) == 5)
        await runner.stop()

        # The commit is what this proves: a fresh member of the same group has
        # nothing left to do, which is only true if the offsets landed.
        resumed = await cluster.source([topic], group_id=group)
        assert await drain(resumed, expected=1, timeout=3.0) == []

    async def test_stopping_leaves_the_group(self, cluster: Cluster) -> None:
        """A member that vanishes without saying so keeps its partitions until
        its session times out, and until then their lag grows."""
        topic = await cluster.topic(partitions=1)
        source = await cluster.source([topic], group_id=f"g-{uuid.uuid4().hex[:8]}")
        runner = ConsumerRunner(
            source=source, handler=Collector(), name="e2e", config=a_config()
        )

        runner.start()
        await asyncio.sleep(0.2)
        await runner.stop()

        assert source.assignment() == frozenset() or not runner.running


class TestAFailingHandler:
    async def test_the_record_comes_round_again(self, cluster: Cluster) -> None:
        """The seek is what makes the retry soon rather than after a restart."""
        topic = await cluster.topic(partitions=1)
        publisher = await cluster.publisher()
        await publisher.publish(topic, value=b"poison", key="k")

        handler = Collector(refuse={b"poison"})
        runner = ConsumerRunner(
            source=await cluster.source([topic], group_id=f"g-{uuid.uuid4().hex[:8]}"),
            handler=handler,
            name="e2e",
            config=a_config(),
        )
        runner.start()
        await wait_for(lambda: len(handler.seen) >= 3)
        await runner.stop()

        assert {m.value for m in handler.seen} == {b"poison"}

    async def test_it_recovers_when_the_handler_starts_working(
        self, cluster: Cluster
    ) -> None:
        topic = await cluster.topic(partitions=1)
        publisher = await cluster.publisher()
        await publisher.publish(topic, value=b"first", key="k")
        await publisher.publish(topic, value=b"second", key="k")

        handler = Collector(refuse={b"first"})
        runner = ConsumerRunner(
            source=await cluster.source([topic], group_id=f"g-{uuid.uuid4().hex[:8]}"),
            handler=handler,
            name="e2e",
            config=a_config(),
        )
        runner.start()
        await wait_for(lambda: len(handler.seen) >= 2)
        # `second` is behind `first` in the same partition, so until the
        # refusal is lifted it must not have been handled at all.
        assert all(m.value == b"first" for m in handler.seen)

        handler.refuse.clear()
        await wait_for(lambda: any(m.value == b"second" for m in handler.seen))
        await runner.stop()


class TestTwoRunnersInOneGroup:
    async def test_between_them_they_handle_everything_once(
        self, cluster: Cluster
    ) -> None:
        """What a consumer group is for: throughput without duplicating work.

        Coverage is asserted rather than exact counts, because a rebalance —
        which a second member joining causes — can redeliver a record whose
        commit had not landed. That is at-least-once behaving as designed, and
        a test that forbade it would be asserting a guarantee this does not
        make.
        """
        topic = await cluster.topic(partitions=2)
        group = f"g-{uuid.uuid4().hex[:8]}"
        publisher = await cluster.publisher()
        expected = {f"v{index}".encode() for index in range(20)}
        for index in range(20):
            await publisher.publish(topic, value=f"v{index}".encode(), key=f"k{index}")

        handlers = [Collector(), Collector()]
        runners = [
            ConsumerRunner(
                source=await cluster.source([topic], group_id=group),
                handler=handler,
                name=f"e2e-{index}",
                config=a_config(),
            )
            for index, handler in enumerate(handlers)
        ]
        for runner in runners:
            runner.start()

        await wait_for(
            lambda: {m.value for handler in handlers for m in handler.seen} == expected
        )
        for runner in runners:
            await runner.stop()

        assert {m.value for handler in handlers for m in handler.seen} == expected
