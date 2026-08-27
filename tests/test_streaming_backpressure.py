"""`with_readahead`: overlap producing and sending, and bound the overlap.

`TestTheProblemBeingSolved` asserts on `asyncio.Queue` itself rather than
describing it, in the style of `tests/test_parallel_io.py` and
`tests/test_structured_scope.py`: if an unbounded queue ever stops letting a
producer run away, the justification for this module fails the build instead of
quietly becoming folklore.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import aclosing

import pytest

from src.streaming.backpressure import DEFAULT_READAHEAD, with_readahead
from src.structured.errors import DeadlineExceeded

TOTAL = 200


async def _settle() -> None:
    """Give every runnable task a chance to reach its next await."""
    for _ in range(20):
        await asyncio.sleep(0)


class TestTheProblemBeingSolved:
    """Why a queue is needed, and why it has to have a maximum size."""

    async def test_an_unbounded_queue_lets_the_producer_read_the_whole_table(
        self,
    ) -> None:
        """The naive fix: a task, a queue, and the export in memory."""
        produced = 0
        queue: asyncio.Queue[int] = asyncio.Queue()

        async def produce() -> None:
            nonlocal produced
            for i in range(TOTAL):
                await queue.put(i)
                produced += 1

        task = asyncio.create_task(produce())
        try:
            await queue.get()
            await _settle()

            assert produced == TOTAL, (
                "an unbounded queue no longer lets the producer finish while "
                "the consumer has read one item — with_readahead's ceiling may "
                "no longer be what bounds memory"
            )
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def test_a_maxsize_is_what_stops_it(self) -> None:
        produced = 0
        queue: asyncio.Queue[int] = asyncio.Queue(maxsize=2)

        async def produce() -> None:
            nonlocal produced
            for i in range(TOTAL):
                await queue.put(i)
                produced += 1

        task = asyncio.create_task(produce())
        try:
            await queue.get()
            await _settle()

            # Two in the queue, one handed over, one blocked in `put`.
            assert produced <= 4
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task


class TestWhatItYields:
    async def test_every_item_in_order(self) -> None:
        async def source() -> AsyncIterator[int]:
            for i in range(10):
                yield i

        got = [item async for item in with_readahead(source, name="t")]

        assert got == list(range(10))

    async def test_an_empty_source_yields_nothing(self) -> None:
        async def source() -> AsyncIterator[int]:
            return
            yield 0  # pragma: no cover - unreachable, makes this a generator

        assert [item async for item in with_readahead(source, name="t")] == []

    async def test_readahead_below_one_is_refused(self) -> None:
        async def source() -> AsyncIterator[int]:
            yield 1  # pragma: no cover - never reached

        with pytest.raises(ValueError, match="at least 1"):
            async for _ in with_readahead(source, readahead=0, name="t"):
                pass  # pragma: no cover - the first __anext__ raises


class TestBackpressure:
    async def test_the_producer_stays_within_the_readahead_of_the_consumer(
        self,
    ) -> None:
        produced: list[int] = []

        async def source() -> AsyncIterator[int]:
            for i in range(TOTAL):
                produced.append(i)
                yield i

        consumed = 0
        async with aclosing(with_readahead(source, readahead=2, name="t")) as stream:
            async for _ in stream:
                consumed += 1
                await _settle()
                # Two queued, one in the consumer's hand, one blocked in `put`.
                assert len(produced) <= consumed + 3
                if consumed == 5:
                    break

        assert consumed == 5
        assert len(produced) < TOTAL

    async def test_readahead_of_one_still_overlaps(self) -> None:
        """The smallest setting is not "no queue": one chunk is prepared ahead."""
        produced: list[int] = []

        async def source() -> AsyncIterator[int]:
            for i in range(5):
                produced.append(i)
                yield i

        async with aclosing(with_readahead(source, readahead=1, name="t")) as stream:
            first = await anext(stream)
            await _settle()

        assert first == 0
        assert len(produced) >= 2

    def test_the_default_readahead_overlaps_without_buffering_much(self) -> None:
        assert DEFAULT_READAHEAD == 2


class TestTheProducerIsOwned:
    async def test_it_runs_as_a_named_child_of_a_scope(self) -> None:
        seen: set[str] = set()

        async def source() -> AsyncIterator[int]:
            seen.update(task.get_name() for task in asyncio.all_tasks())
            yield 1

        async for _ in with_readahead(source, name="users-export"):
            pass

        assert "readahead:users-export:produce" in seen

    async def test_it_is_gone_once_the_stream_ends(self) -> None:
        async def source() -> AsyncIterator[int]:
            yield 1

        async for _ in with_readahead(source, name="users-export"):
            pass

        assert not [
            task
            for task in asyncio.all_tasks()
            if task.get_name().startswith("readahead:users-export")
        ]

    async def test_closing_the_stream_early_finalizes_the_source(self) -> None:
        """A client that hangs up must release the cursor, not wait for the GC."""
        closed = asyncio.Event()

        async def source() -> AsyncIterator[int]:
            try:
                i = 0
                while True:
                    yield i
                    i += 1
            finally:
                closed.set()

        async with aclosing(with_readahead(source, readahead=2, name="t")) as stream:
            async for _ in stream:
                break

        assert closed.is_set()
        assert not [
            task for task in asyncio.all_tasks() if task.get_name().startswith("read")
        ]


class TestFailure:
    async def test_the_error_arrives_behind_the_items_that_preceded_it(self) -> None:
        async def source() -> AsyncIterator[int]:
            yield 1
            yield 2
            raise RuntimeError("boom")

        got: list[int] = []
        with pytest.raises(RuntimeError, match="boom"):
            async with aclosing(
                with_readahead(source, readahead=2, name="t")
            ) as stream:
                async for item in stream:
                    got.append(item)

        assert got == [1, 2]

    async def test_a_slow_producer_runs_out_of_budget(self) -> None:
        async def source() -> AsyncIterator[int]:
            yield 1
            await asyncio.sleep(10)
            yield 2  # pragma: no cover - the budget expires first

        got: list[int] = []
        with pytest.raises(DeadlineExceeded) as caught:
            async with aclosing(
                with_readahead(source, readahead=2, name="slow", budget=0.05)
            ) as stream:
                async for item in stream:
                    got.append(item)

        assert got == [1]
        assert caught.value.scope == "slow"

    async def test_the_budget_covers_time_spent_waiting_on_the_consumer(self) -> None:
        """The clock runs while the producer is blocked in `put`, by design.

        What has to be bounded is how long the cursor is held, and a client
        that stopped reading holds it exactly as effectively as a slow query.
        """

        async def source() -> AsyncIterator[int]:
            for i in range(TOTAL):
                yield i

        with pytest.raises(DeadlineExceeded):
            async with aclosing(
                with_readahead(source, readahead=1, name="slow", budget=0.05)
            ) as stream:
                async for _ in stream:
                    await asyncio.sleep(0.05)

    async def test_a_budget_that_is_never_reached_does_not_fire(self) -> None:
        async def source() -> AsyncIterator[int]:
            for i in range(5):
                yield i

        got = [item async for item in with_readahead(source, name="t", budget=30.0)]

        assert got == list(range(5))
