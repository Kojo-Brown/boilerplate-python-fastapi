"""The consume loop's policy, over a source that does exactly what it is told.

The contract suite proves the transports agree; this proves the loop does the
right thing with them. A fake source rather than the in-memory broker, because
what is asserted here is *which* offsets were committed and *when* a seek
happened, and reading those back out of a broker would be inferring the
decision from its consequences.

The sharp cases are in `TestAFailingPartition`: they are the ones where the
obvious implementation — the per-event isolation `OutboxRelay` uses — silently
drops records, because a Kafka offset is a watermark and skipping one record
commits past it.
"""

from __future__ import annotations

import asyncio
import random
from collections import deque
from collections.abc import Mapping, Sequence

import pytest

from src.kafka.base import (
    ConsumedMessage,
    ConsumerError,
    MessageSource,
    Partition,
    utc_now,
)
from src.kafka.runner import ConsumerConfig, ConsumerRunner

P0 = Partition(topic="t", number=0)
P1 = Partition(topic="t", number=1)


def a_message(
    partition: Partition = P0, offset: int = 0, *, key: str | None = "k"
) -> ConsumedMessage:
    return ConsumedMessage(
        partition=partition,
        offset=offset,
        key=key,
        value=f"{partition.number}:{offset}".encode(),
        headers=(),
        timestamp=utc_now(),
    )


class FakeSource:
    """A `MessageSource` whose every answer the test decides."""

    def __init__(self, batches: Sequence[Sequence[ConsumedMessage]] = ()) -> None:
        self.batches: deque[Sequence[ConsumedMessage]] = deque(batches)
        self.commits: list[dict[Partition, int]] = []
        self.seeks: list[tuple[Partition, int]] = []
        self.starts = 0
        self.stops = 0
        self.polls = 0
        self.poll_error: Exception | None = None
        self.commit_error: Exception | None = None
        self.seek_error: Exception | None = None

    async def start(self) -> None:
        self.starts += 1

    async def stop(self) -> None:
        self.stops += 1

    async def poll(
        self, *, max_records: int, timeout: float
    ) -> Sequence[ConsumedMessage]:
        self.polls += 1
        if self.poll_error is not None:
            raise self.poll_error
        if not self.batches:
            # A real fetch blocks for its timeout when the topic is idle, and a
            # loop test that spins instead would be a busy loop pretending to
            # be a consumer.
            await asyncio.sleep(0.01)
            return []
        return self.batches.popleft()

    async def commit(self, offsets: Mapping[Partition, int]) -> None:
        if self.commit_error is not None:
            raise self.commit_error
        self.commits.append(dict(offsets))

    def seek(self, partition: Partition, offset: int) -> None:
        if self.seek_error is not None:
            raise self.seek_error
        self.seeks.append((partition, offset))

    def assignment(self) -> frozenset[Partition]:
        return frozenset({P0, P1})


class RecordingHandler:
    """Records what it was given, and fails whichever offsets it was told to."""

    def __init__(self, *, fail_on: set[tuple[Partition, int]] | None = None) -> None:
        self.seen: list[ConsumedMessage] = []
        self.fail_on = fail_on or set()

    async def __call__(self, message: ConsumedMessage) -> None:
        self.seen.append(message)
        if (message.partition, message.offset) in self.fail_on:
            raise RuntimeError(f"handler refused {message.partition}@{message.offset}")


def a_runner(
    source: FakeSource,
    handler: RecordingHandler,
    *,
    config: ConsumerConfig | None = None,
    sleeps: list[float] | None = None,
) -> ConsumerRunner:
    async def sleep(delay: float) -> None:
        if sleeps is not None:
            sleeps.append(delay)
        await asyncio.sleep(0)

    return ConsumerRunner(
        source=source,
        handler=handler,
        name="test",
        config=config or ConsumerConfig(jitter=False, poll_timeout=0.01),
        sleep=sleep,
        rng=random.Random(1234),
    )


class TestTheFakeIsASource:
    def test_the_fake_satisfies_the_protocol(self) -> None:
        """Otherwise every test below is about a shape nothing else has."""
        assert isinstance(FakeSource(), MessageSource)


class TestACleanBatch:
    async def test_it_commits_one_offset_past_the_last_record(self) -> None:
        source = FakeSource([[a_message(P0, 0), a_message(P0, 1), a_message(P0, 2)]])
        handler = RecordingHandler()

        result = await a_runner(source, handler).consume_once()

        assert result.delivered == 3
        assert result.failed == 0
        # 3, not 2: committing the last record's own offset would replay it
        # after every restart.
        assert source.commits == [{P0: 3}]

    async def test_each_partition_gets_its_own_watermark(self) -> None:
        source = FakeSource(
            [[a_message(P0, 0), a_message(P0, 1), a_message(P1, 7), a_message(P1, 8)]]
        )

        await a_runner(source, RecordingHandler()).consume_once()

        assert source.commits == [{P0: 2, P1: 9}]

    async def test_an_empty_poll_commits_nothing(self) -> None:
        """A commit with no offsets is `commit()` with none — which in aiokafka
        means "everything fetched", the at-most-once behaviour this package
        turned auto-commit off to avoid."""
        source = FakeSource()
        handler = RecordingHandler()

        result = await a_runner(source, handler).consume_once()

        assert result.empty
        assert source.commits == []
        assert handler.seen == []

    async def test_records_reach_the_handler_in_offset_order(self) -> None:
        source = FakeSource([[a_message(P0, o) for o in range(5)]])
        handler = RecordingHandler()

        await a_runner(source, handler).consume_once()

        assert [m.offset for m in handler.seen] == [0, 1, 2, 3, 4]


class TestAFailingPartition:
    async def test_the_partition_stops_at_the_failing_record(self) -> None:
        """Record 2 fails, so record 3 is not handled — it is behind it."""
        source = FakeSource([[a_message(P0, o) for o in range(4)]])
        handler = RecordingHandler(fail_on={(P0, 2)})

        result = await a_runner(source, handler).consume_once()

        assert [m.offset for m in handler.seen] == [0, 1, 2]
        assert result.delivered == 2
        assert result.failed == 1

    async def test_it_commits_through_the_record_before_the_failure(self) -> None:
        """Work already done is never repeated; the failure is not committed past."""
        source = FakeSource([[a_message(P0, o) for o in range(4)]])

        await a_runner(source, RecordingHandler(fail_on={(P0, 2)})).consume_once()

        assert source.commits == [{P0: 2}]

    async def test_it_seeks_back_so_the_record_returns_on_the_next_poll(self) -> None:
        """Without this the position has already moved past it in the client,
        and the record only returns after a restart or a rebalance."""
        source = FakeSource([[a_message(P0, o) for o in range(4)]])

        await a_runner(source, RecordingHandler(fail_on={(P0, 2)})).consume_once()

        assert source.seeks == [(P0, 2)]

    async def test_a_failure_on_the_first_record_commits_nothing(self) -> None:
        source = FakeSource([[a_message(P0, 0), a_message(P0, 1)]])

        await a_runner(source, RecordingHandler(fail_on={(P0, 0)})).consume_once()

        assert source.commits == []
        assert source.seeks == [(P0, 0)]

    async def test_the_other_partitions_are_unaffected(self) -> None:
        source = FakeSource(
            [[a_message(P0, 0), a_message(P0, 1), a_message(P1, 5), a_message(P1, 6)]]
        )
        handler = RecordingHandler(fail_on={(P0, 0)})

        result = await a_runner(source, handler).consume_once()

        assert source.commits == [{P1: 7}]
        assert [(m.partition, m.offset) for m in handler.seen] == [
            (P0, 0),
            (P1, 5),
            (P1, 6),
        ]
        assert result.delivered == 2
        assert result.failed == 1

    async def test_a_handler_that_overruns_its_timeout_fails_the_record(self) -> None:
        """Left unbounded it would hold the batch past `max_poll_interval_ms`,
        and the whole assignment would be handed to another member mid-batch."""

        async def slow(message: ConsumedMessage) -> None:
            await asyncio.sleep(1)

        source = FakeSource([[a_message(P0, 0)]])
        runner = ConsumerRunner(
            source=source,
            handler=slow,
            config=ConsumerConfig(handler_timeout=0.01, jitter=False),
        )

        result = await runner.consume_once()

        assert result.failed == 1
        assert source.commits == []
        assert source.seeks == [(P0, 0)]

    async def test_a_seek_that_fails_is_survivable(self) -> None:
        """`seek` fails when a rebalance took the partition. Its new owner
        reads from the last committed offset, so nothing is lost."""
        source = FakeSource([[a_message(P0, 0)]])
        source.seek_error = ConsumerError("not assigned")

        result = await a_runner(
            source, RecordingHandler(fail_on={(P0, 0)})
        ).consume_once()

        assert result.failed == 1

    async def test_a_commit_that_fails_is_survivable(self) -> None:
        """A commit fails when this member has left the group. The records are
        someone else's now; retrying would land after their own commit."""
        source = FakeSource([[a_message(P0, 0)]])
        source.commit_error = ConsumerError("rebalanced")

        result = await a_runner(source, RecordingHandler()).consume_once()

        assert result.delivered == 1


class TestBackoff:
    async def test_consecutive_failures_on_one_partition_back_off(self) -> None:
        source = FakeSource(
            [[a_message(P0, 0)], [a_message(P0, 0)], [a_message(P0, 0)]]
        )
        runner = a_runner(source, RecordingHandler(fail_on={(P0, 0)}))

        delays = [(await runner.consume_once()).retry_delay for _ in range(3)]

        assert delays == [1.0, 2.0, 4.0]

    async def test_a_success_clears_the_partition(self) -> None:
        source = FakeSource(
            [[a_message(P0, 0)], [a_message(P0, 0)], [a_message(P0, 0)]]
        )
        handler = RecordingHandler(fail_on={(P0, 0)})
        runner = a_runner(source, handler)

        first = await runner.consume_once()
        assert runner.failing_partitions == frozenset({P0})

        handler.fail_on.clear()
        await runner.consume_once()
        assert runner.failing_partitions == frozenset()

        handler.fail_on.add((P0, 0))
        third = await runner.consume_once()
        # Back to the base delay rather than continuing to double, because the
        # count is about a partition that is currently stuck, not a tally.
        assert first.retry_delay == third.retry_delay == 1.0

    async def test_a_healthy_batch_asks_for_no_delay(self) -> None:
        """The poll itself blocks, so a working consumer never sleeps."""
        source = FakeSource([[a_message(P0, 0)]])

        result = await a_runner(source, RecordingHandler()).consume_once()

        assert result.retry_delay == 0.0

    async def test_one_stalled_partition_paces_the_loop_by_its_own_backoff(
        self,
    ) -> None:
        source = FakeSource([[a_message(P0, 0), a_message(P1, 0)]])

        result = await a_runner(
            source, RecordingHandler(fail_on={(P1, 0)})
        ).consume_once()

        assert result.retry_delay == 1.0
        assert source.commits == [{P0: 1}]


class TestTheLoop:
    async def test_it_starts_the_source_and_stops_it_on_the_way_out(self) -> None:
        source = FakeSource([[a_message(P0, 0)]])
        runner = a_runner(source, RecordingHandler())

        runner.start()
        await asyncio.sleep(0.05)
        assert runner.running
        await runner.stop()

        assert source.starts >= 1
        # Through `finalize`, which is why it happens at all: the await is on
        # an already-cancelled task, and a bare one would be cut.
        assert source.stops == 1
        assert not runner.running

    async def test_a_poll_failure_backs_off_and_carries_on(self) -> None:
        source = FakeSource()
        source.poll_error = ConsumerError("broker gone")
        sleeps: list[float] = []
        runner = a_runner(source, RecordingHandler(), sleeps=sleeps)

        runner.start()
        await asyncio.sleep(0.05)
        source.poll_error = None
        await runner.stop()

        assert sleeps[:3] == [1.0, 2.0, 4.0]
        assert source.polls > 1

    async def test_an_unreachable_broker_at_start_up_is_retried(self) -> None:
        """A `start()` outside the loop would raise into a task nobody awaits,
        leaving a consumer that never consumes and never says so."""
        failures = 2

        class FlakySource(FakeSource):
            async def start(self) -> None:
                nonlocal failures
                self.starts += 1
                if failures:
                    failures -= 1
                    raise ConsumerError("connection refused")

        source = FlakySource([[a_message(P0, 0)]])
        handler = RecordingHandler()
        runner = a_runner(source, handler)

        runner.start()
        for _ in range(50):
            await asyncio.sleep(0.01)
            if handler.seen:
                break
        await runner.stop()

        assert failures == 0
        assert [m.offset for m in handler.seen] == [0]

    async def test_start_is_idempotent_while_running(self) -> None:
        runner = a_runner(FakeSource(), RecordingHandler())

        runner.start()
        first = runner._task
        runner.start()

        assert runner._task is first
        await runner.stop()

    async def test_stop_is_idempotent_and_safe_before_start(self) -> None:
        runner = a_runner(FakeSource(), RecordingHandler())

        await runner.stop()
        runner.start()
        await runner.stop()
        await runner.stop()

        assert not runner.running


class TestCancellation:
    async def test_a_cancelled_batch_commits_nothing(self) -> None:
        """The records are redelivered, which is what at-least-once means. The
        alternative — committing on the way out — would be a commit for a
        handler that never ran."""
        started = asyncio.Event()

        async def blocks(message: ConsumedMessage) -> None:
            started.set()
            await asyncio.sleep(10)

        source = FakeSource([[a_message(P0, 0), a_message(P0, 1)]])
        runner = ConsumerRunner(
            source=source, handler=blocks, config=ConsumerConfig(jitter=False)
        )

        task = asyncio.create_task(runner.consume_once())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert source.commits == []

    async def test_the_cancellation_is_not_recorded_as_a_handler_failure(self) -> None:
        """Nothing is wrong with the record, so nothing should be seeked back."""
        started = asyncio.Event()

        async def blocks(message: ConsumedMessage) -> None:
            started.set()
            await asyncio.sleep(10)

        source = FakeSource([[a_message(P0, 0)]])
        runner = ConsumerRunner(
            source=source, handler=blocks, config=ConsumerConfig(jitter=False)
        )

        task = asyncio.create_task(runner.consume_once())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert source.seeks == []
        assert runner.failing_partitions == frozenset()


class TestConfiguration:
    @pytest.mark.parametrize(
        "kwargs",
        [
            {"max_records": 0},
            {"poll_timeout": 0.0},
            {"handler_timeout": -1.0},
            {"retry_base_delay": 0.0},
            {"retry_base_delay": 10.0, "retry_max_delay": 1.0},
            {"shutdown_timeout": 0.0},
        ],
    )
    def test_a_nonsensical_policy_is_refused_at_construction(
        self, kwargs: dict[str, float | int]
    ) -> None:
        with pytest.raises(ValueError):
            ConsumerConfig(**kwargs)  # type: ignore[arg-type]

    def test_the_runner_names_its_task_after_itself(self) -> None:
        runner = ConsumerRunner(
            source=FakeSource(), handler=RecordingHandler(), name="audit"
        )
        assert runner.name == "audit"
