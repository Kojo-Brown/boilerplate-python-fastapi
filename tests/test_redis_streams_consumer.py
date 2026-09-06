"""The consume policy: what a cycle does, and in what order.

Everything here runs against `InMemoryStreamServer` with a pinned clock, which
is why a test about a message that has been idle for a minute takes no time at
all. That the model tells the truth about idleness, ownership and delivery
counts is `tests/test_redis_streams_contract.py`'s job, and it proves it
against a real server; this file is about the decisions layered on top —
claiming before reading, the delivery cap, and what is left unacknowledged.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Sequence

import pytest

from src.redis_streams.base import (
    ClaimedMessage,
    Fields,
    PendingEntry,
    StreamConsumerGroup,
    StreamEntryId,
    StreamMessage,
    StreamPublishError,
    StreamUnavailableError,
)
from src.redis_streams.consumer import (
    DEAD_LETTER_PREFIX,
    ConsumeResult,
    StreamConsumerConfig,
    StreamConsumerRunner,
    pending_summary,
)
from src.redis_streams.memory import InMemoryStreamGroup, InMemoryStreamServer

STREAM = "orders"
GROUP = "orders-workers"


class FakeClock:
    """Monotonic seconds under the test's control.

    Idle time is the only input the claiming policy has, and spending it in
    real seconds would make every test here a multi-second one.
    """

    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class RecordingSleeper:
    """`asyncio.sleep` that records the delay instead of spending it."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


class Handler:
    """A handler that remembers what it was given, and can be told to fail."""

    def __init__(
        self,
        *,
        fail_on: frozenset[bytes] = frozenset(),
        hang_on: frozenset[bytes] = frozenset(),
    ) -> None:
        self.seen: list[StreamMessage] = []
        self._fail_on = fail_on
        self._hang_on = hang_on

    async def __call__(self, message: StreamMessage) -> None:
        self.seen.append(message)
        payload = message.field("n")
        if payload in self._hang_on:
            await asyncio.sleep(3600)
        if payload in self._fail_on:
            raise RuntimeError(f"handler refused {payload!r}")

    @property
    def payloads(self) -> list[bytes | None]:
        return [message.field("n") for message in self.seen]


class BrokenPublisher:
    """Delegates everything, and refuses to publish.

    Wrapping the real in-process group rather than faking the whole protocol:
    the point of the test is what the *runner* does when a dead-letter publish
    fails, and everything else about the group has to keep behaving.
    """

    def __init__(self, inner: InMemoryStreamGroup) -> None:
        self._inner = inner

    @property
    def stream(self) -> str:
        return self._inner.stream

    @property
    def group(self) -> str:
        return self._inner.group

    @property
    def consumer(self) -> str:
        return self._inner.consumer

    async def start(self) -> None:
        await self._inner.start()

    async def stop(self) -> None:
        await self._inner.stop()

    async def publish(
        self, fields: Fields, *, stream: str | None = None
    ) -> StreamEntryId:
        raise StreamPublishError("Redis is not answering.")

    async def read(self, *, count: int, block: float) -> Sequence[StreamMessage]:
        return await self._inner.read(count=count, block=block)

    async def stalled(
        self, *, min_idle: float, count: int, start: StreamEntryId | None = None
    ) -> Sequence[PendingEntry]:
        return await self._inner.stalled(min_idle=min_idle, count=count, start=start)

    async def claim(
        self, entries: Sequence[PendingEntry], *, min_idle: float
    ) -> ClaimedMessage:
        return await self._inner.claim(entries, min_idle=min_idle)

    async def ack(self, ids: Sequence[StreamEntryId]) -> int:
        return await self._inner.ack(ids)

    async def pending_count(self, *, consumer: str | None = None) -> int:
        return await self._inner.pending_count(consumer=consumer)


class BrokenAck(BrokenPublisher):
    """Publishes fine; cannot acknowledge."""

    async def publish(
        self, fields: Fields, *, stream: str | None = None
    ) -> StreamEntryId:
        return await self._inner.publish(fields, stream=stream)

    async def ack(self, ids: Sequence[StreamEntryId]) -> int:
        raise StreamUnavailableError("Redis is not answering.")


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def server(clock: FakeClock) -> InMemoryStreamServer:
    return InMemoryStreamServer(clock=clock)


@pytest.fixture
def config() -> StreamConsumerConfig:
    """Small numbers, and a `min_idle` a test can step past in one `advance`."""
    return StreamConsumerConfig(
        batch_size=10,
        block_timeout=0.05,
        min_idle=30.0,
        claim_batch=5,
        max_deliveries=3,
        handler_timeout=0.05,
        jitter=False,
    )


async def started_group(
    server: InMemoryStreamServer, consumer: str = "worker-1"
) -> InMemoryStreamGroup:
    group = server.group(stream=STREAM, group=GROUP, consumer=consumer)
    await group.start()
    return group


def runner_for(
    group: StreamConsumerGroup,
    handler: Handler,
    config: StreamConsumerConfig,
    *,
    sleep: RecordingSleeper | None = None,
) -> StreamConsumerRunner:
    return StreamConsumerRunner(
        group=group,
        handler=handler,
        name="orders",
        config=config,
        sleep=sleep if sleep is not None else RecordingSleeper(),
        rng=random.Random(1),
    )


class TestTheHappyPath:
    async def test_it_reads_handles_and_acknowledges(
        self, server: InMemoryStreamServer, config: StreamConsumerConfig
    ) -> None:
        group = await started_group(server)
        for index in range(3):
            await group.publish({"n": str(index).encode()})
        handler = Handler()

        result = await runner_for(group, handler, config).consume_once()

        assert handler.payloads == [b"0", b"1", b"2"]
        assert result == ConsumeResult(read=3, delivered=3)
        assert await group.pending_count() == 0

    async def test_an_empty_cycle_is_empty(
        self, server: InMemoryStreamServer, config: StreamConsumerConfig
    ) -> None:
        group = await started_group(server)

        result = await runner_for(group, Handler(), config).consume_once()

        assert result.empty
        assert result.handled == 0


class TestFailureIsolation:
    async def test_a_failed_message_stays_pending_and_the_rest_do_not(
        self, server: InMemoryStreamServer, config: StreamConsumerConfig
    ) -> None:
        """The headline difference from `src/kafka`: a PEL acknowledges
        messages, so message 1 failing says nothing about message 2. There is
        no partition to stall and nothing behind it to hold up."""
        group = await started_group(server)
        for index in range(3):
            await group.publish({"n": str(index).encode()})
        handler = Handler(fail_on=frozenset({b"1"}))

        result = await runner_for(group, handler, config).consume_once()

        assert result == ConsumeResult(read=3, delivered=2, failed=1)
        assert handler.payloads == [b"0", b"1", b"2"]
        assert await group.pending_count() == 1

    async def test_a_handler_that_hangs_is_timed_out_and_the_message_kept(
        self, server: InMemoryStreamServer, config: StreamConsumerConfig
    ) -> None:
        group = await started_group(server)
        await group.publish({"n": b"slow"})
        handler = Handler(hang_on=frozenset({b"slow"}))

        result = await runner_for(group, handler, config).consume_once()

        assert result == ConsumeResult(read=1, failed=1)
        assert await group.pending_count() == 1

    async def test_a_failed_acknowledgement_is_not_fatal(
        self, server: InMemoryStreamServer, config: StreamConsumerConfig
    ) -> None:
        """The messages stay pending and are claimed back later, which is the
        at-least-once guarantee doing its job rather than a case to handle."""
        inner = await started_group(server)
        await inner.publish({"n": b"0"})
        group = BrokenAck(inner)
        handler = Handler()

        result = await runner_for(group, handler, config).consume_once()

        assert result == ConsumeResult(read=1, delivered=1)
        assert await inner.pending_count() == 1


class TestClaiming:
    async def test_it_claims_what_another_consumer_abandoned(
        self,
        server: InMemoryStreamServer,
        clock: FakeClock,
        config: StreamConsumerConfig,
    ) -> None:
        dead = await started_group(server, "worker-gone")
        await dead.publish({"n": b"orphan"})
        await dead.read(count=10, block=0)
        clock.advance(config.min_idle * 2)

        alive = await started_group(server, "worker-2")
        handler = Handler()
        result = await runner_for(alive, handler, config).consume_once()

        assert handler.payloads == [b"orphan"]
        assert result == ConsumeResult(claimed=1, delivered=1)
        assert handler.seen[0].claimed is True
        assert handler.seen[0].delivery_count == 2
        assert await alive.pending_count() == 0

    async def test_a_message_still_being_worked_on_is_left_alone(
        self,
        server: InMemoryStreamServer,
        clock: FakeClock,
        config: StreamConsumerConfig,
    ) -> None:
        busy = await started_group(server, "worker-busy")
        await busy.publish({"n": b"in-flight"})
        await busy.read(count=10, block=0)
        clock.advance(config.min_idle / 2)

        alive = await started_group(server, "worker-2")
        handler = Handler()
        result = await runner_for(alive, handler, config).consume_once()

        assert handler.payloads == []
        assert result.claimed == 0
        assert await busy.pending_count(consumer="worker-busy") == 1

    async def test_claiming_comes_before_reading(
        self,
        server: InMemoryStreamServer,
        clock: FakeClock,
        config: StreamConsumerConfig,
    ) -> None:
        """Order, not preference: if new messages came first, a busy stream
        would starve the stalled ones forever and every dashboard would stay
        green, because a pending entry is not lag."""
        dead = await started_group(server, "worker-gone")
        await dead.publish({"n": b"orphan"})
        await dead.read(count=10, block=0)
        clock.advance(config.min_idle * 2)
        await dead.publish({"n": b"fresh"})

        alive = await started_group(server, "worker-2")
        handler = Handler()
        result = await runner_for(alive, handler, config).consume_once()

        assert handler.payloads == [b"orphan", b"fresh"]
        assert result == ConsumeResult(read=1, claimed=1, delivered=2)

    async def test_a_claim_takes_from_the_cycle_s_budget(
        self,
        server: InMemoryStreamServer,
        clock: FakeClock,
        config: StreamConsumerConfig,
    ) -> None:
        """`batch_size` bounds the whole cycle, so a backlog of stalled
        messages cannot make one cycle unboundedly large."""
        dead = await started_group(server, "worker-gone")
        for index in range(5):
            await dead.publish({"n": f"old-{index}".encode()})
        await dead.read(count=10, block=0)
        clock.advance(config.min_idle * 2)
        for index in range(20):
            await dead.publish({"n": f"new-{index}".encode()})

        alive = await started_group(server, "worker-2")
        handler = Handler()
        small = StreamConsumerConfig(
            batch_size=8,
            block_timeout=config.block_timeout,
            min_idle=config.min_idle,
            claim_batch=5,
            max_deliveries=config.max_deliveries,
            handler_timeout=config.handler_timeout,
        )
        result = await runner_for(alive, handler, small).consume_once()

        assert result.claimed == 5
        assert result.read == 3
        assert len(handler.seen) == 8

    async def test_a_cycle_that_claimed_does_not_then_park_on_an_empty_stream(
        self,
        server: InMemoryStreamServer,
        clock: FakeClock,
        config: StreamConsumerConfig,
    ) -> None:
        """More stalled work is probably waiting, and blocking for the full
        timeout would delay the next claim scan for nothing."""
        dead = await started_group(server, "worker-gone")
        await dead.publish({"n": b"orphan"})
        await dead.read(count=10, block=0)
        clock.advance(config.min_idle * 2)

        patient = StreamConsumerConfig(
            batch_size=config.batch_size,
            block_timeout=5.0,
            min_idle=config.min_idle,
            claim_batch=config.claim_batch,
            max_deliveries=config.max_deliveries,
            handler_timeout=config.handler_timeout,
        )
        alive = await started_group(server, "worker-2")
        started = time.perf_counter()
        result = await runner_for(alive, Handler(), patient).consume_once()
        elapsed = time.perf_counter() - started

        assert result.claimed == 1
        assert elapsed < 1.0

    async def test_an_entry_that_vanished_is_counted_not_raised(
        self,
        server: InMemoryStreamServer,
        clock: FakeClock,
        config: StreamConsumerConfig,
    ) -> None:
        dead = await started_group(server, "worker-gone")
        entry_id = await dead.publish({"n": b"trimmed-away"})
        await dead.read(count=10, block=0)
        server.delete(STREAM, entry_id)
        clock.advance(config.min_idle * 2)

        alive = await started_group(server, "worker-2")
        result = await runner_for(alive, Handler(), config).consume_once()

        assert result == ConsumeResult(vanished=1)
        assert await alive.pending_count() == 0


class TestTheDeliveryCap:
    async def test_a_message_over_the_cap_is_dead_lettered_and_acknowledged(
        self,
        server: InMemoryStreamServer,
        clock: FakeClock,
        config: StreamConsumerConfig,
    ) -> None:
        group = await started_group(server)
        await group.publish({"n": b"poison"})
        handler = Handler(fail_on=frozenset({b"poison"}))
        runner = runner_for(group, handler, config)

        # Delivery 1 is the read; two claims take it to 3, which is the cap.
        for _ in range(config.max_deliveries):
            await runner.consume_once()
            clock.advance(config.min_idle * 2)
        assert len(handler.seen) == config.max_deliveries

        result = await runner.consume_once()

        assert result == ConsumeResult(claimed=1, dead_lettered=1)
        # The handler was not asked a fourth time.
        assert len(handler.seen) == config.max_deliveries
        assert await group.pending_count() == 0

    async def test_the_dead_letter_carries_where_it_came_from(
        self,
        server: InMemoryStreamServer,
        clock: FakeClock,
        config: StreamConsumerConfig,
    ) -> None:
        group = await started_group(server)
        origin_id = await group.publish({"n": b"poison"})
        runner = runner_for(group, Handler(fail_on=frozenset({b"poison"})), config)
        for _ in range(config.max_deliveries + 1):
            await runner.consume_once()
            clock.advance(config.min_idle * 2)

        assert runner.dead_letter_stream == f"{STREAM}.dead"
        ((_dead_id, fields),) = server.entries(runner.dead_letter_stream)

        assert fields["n"] == b"poison"
        assert fields[f"{DEAD_LETTER_PREFIX}stream"] == STREAM.encode()
        assert fields[f"{DEAD_LETTER_PREFIX}id"] == str(origin_id).encode()
        assert fields[f"{DEAD_LETTER_PREFIX}group"] == GROUP.encode()
        assert fields[f"{DEAD_LETTER_PREFIX}consumer"] == b"worker-1"
        assert (
            fields[f"{DEAD_LETTER_PREFIX}deliveries"]
            == str(config.max_deliveries + 1).encode()
        )

    async def test_a_dead_letter_that_cannot_be_published_is_not_acknowledged(
        self,
        server: InMemoryStreamServer,
        clock: FakeClock,
        config: StreamConsumerConfig,
    ) -> None:
        """Publish first, acknowledge second. The other order empties the
        stream into nowhere during a Redis incident."""
        inner = await started_group(server)
        await inner.publish({"n": b"poison"})
        handler = Handler(fail_on=frozenset({b"poison"}))
        working = runner_for(inner, handler, config)
        for _ in range(config.max_deliveries):
            await working.consume_once()
            clock.advance(config.min_idle * 2)

        broken = runner_for(BrokenPublisher(inner), handler, config)
        result = await broken.consume_once()

        assert result == ConsumeResult(claimed=1, failed=1)
        assert await inner.pending_count() == 1


class TestTheLoop:
    async def test_start_consumes_until_stopped(
        self, server: InMemoryStreamServer, config: StreamConsumerConfig
    ) -> None:
        group = await started_group(server)
        await group.publish({"n": b"0"})
        handler = Handler()
        runner = runner_for(group, handler, config)

        runner.start()
        runner.start()  # idempotent while running
        assert runner.running
        for _ in range(50):
            if handler.seen:
                break
            await asyncio.sleep(0.01)
        await runner.stop()

        assert handler.payloads == [b"0"]
        assert runner.running is False
        assert group.started is False

    async def test_stopping_a_runner_that_never_started_is_harmless(
        self, server: InMemoryStreamServer, config: StreamConsumerConfig
    ) -> None:
        runner = runner_for(await started_group(server), Handler(), config)

        await runner.stop()

    async def test_a_failing_cycle_backs_off_and_carries_on(
        self, server: InMemoryStreamServer, config: StreamConsumerConfig
    ) -> None:
        """A server that is unreachable at start-up must become a retry rather
        than a consumer that quietly never consumes."""
        failures = 0

        class Unreachable(BrokenPublisher):
            async def read(
                self, *, count: int, block: float
            ) -> Sequence[StreamMessage]:
                nonlocal failures
                failures += 1
                if failures <= 3:
                    raise StreamUnavailableError("Redis is not answering.")
                return await super().read(count=count, block=block)

        group = Unreachable(await started_group(server))
        sleeper = RecordingSleeper()
        runner = runner_for(group, Handler(), config, sleep=sleeper)

        runner.start()
        for _ in range(100):
            if len(sleeper.delays) >= 3:
                break
            await asyncio.sleep(0.01)
        await runner.stop()

        assert sleeper.delays[:3] == [1.0, 2.0, 4.0]

    async def test_the_group_is_left_even_when_the_loop_is_cancelled(
        self, server: InMemoryStreamServer, config: StreamConsumerConfig
    ) -> None:
        group = await started_group(server)
        runner = runner_for(group, Handler(hang_on=frozenset({b"0"})), config)
        await group.publish({"n": b"0"})

        runner.start()
        await asyncio.sleep(0.02)
        await runner.stop()

        assert group.started is False


class TestConfiguration:
    @pytest.mark.parametrize(
        ("field", "value", "message"),
        [
            ("batch_size", 0, "batch_size must be at least 1"),
            ("block_timeout", 0.0, "block_timeout must be positive"),
            ("min_idle", 0.0, "min_idle must be positive"),
            ("claim_batch", 0, "claim_batch must be at least 1"),
            ("max_deliveries", 0, "max_deliveries must be at least 1"),
            ("handler_timeout", 0.0, "handler_timeout must be positive"),
            ("retry_base_delay", 0.0, "retry_base_delay must be positive"),
            ("retry_max_delay", 0.5, "retry_max_delay cannot be below"),
            ("shutdown_timeout", 0.0, "shutdown_timeout must be positive"),
            ("dead_letter_suffix", "", "dead_letter_suffix must not be empty"),
        ],
    )
    def test_a_nonsensical_setting_is_refused_at_construction(
        self, field: str, value: object, message: str
    ) -> None:
        with pytest.raises(ValueError, match=message):
            StreamConsumerConfig(**{field: value})  # type: ignore[arg-type]

    def test_the_dead_letter_stream_is_derived_from_the_suffix(
        self, server: InMemoryStreamServer
    ) -> None:
        runner = StreamConsumerRunner(
            group=server.group(stream=STREAM, group=GROUP, consumer="c"),
            handler=Handler(),
            config=StreamConsumerConfig(dead_letter_suffix=".rejected"),
        )

        assert runner.dead_letter_stream == "orders.rejected"

    def test_the_defaults_keep_min_idle_clear_of_the_handler_timeout(self) -> None:
        """Not a style preference: a `min_idle` at or below the handler timeout
        turns a slow handler into concurrent duplicate processing."""
        config = StreamConsumerConfig()

        assert config.min_idle >= config.handler_timeout * 2

    def test_the_group_is_reachable_from_the_runner(
        self, server: InMemoryStreamServer, config: StreamConsumerConfig
    ) -> None:
        group = server.group(stream=STREAM, group=GROUP, consumer="c")
        runner = StreamConsumerRunner(group=group, handler=Handler(), config=config)

        assert runner.group is group
        assert runner.name == "default"
        assert runner.config is config


class TestPendingSummary:
    def test_an_empty_scan_summarises_to_nothing_pending(self) -> None:
        assert pending_summary([]) == {"pending": 0}

    def test_it_reports_the_worst_of_what_it_was_given(self) -> None:
        entries = [
            PendingEntry(
                id=StreamEntryId(1, 0), consumer="a", idle=12.3456, delivery_count=1
            ),
            PendingEntry(
                id=StreamEntryId(2, 0), consumer="b", idle=4.0, delivery_count=7
            ),
        ]

        assert pending_summary(entries) == {
            "pending": 2,
            "oldest_idle": 12.346,
            "max_deliveries": 7,
            "consumers": ["a", "b"],
        }
