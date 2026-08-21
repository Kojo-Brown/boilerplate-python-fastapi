"""`gather_bounded` and friends, including the `asyncio.gather` bug they fix.

Two of these tests assert on plain `asyncio.gather` rather than on this module's
code. They are here on purpose: the whole justification for `gather_bounded` is
that `gather` is unbounded and leaks siblings on failure, and a README sentence
claiming that is worth much less than a test that fails if it ever stops being
true.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from functools import partial

import pytest

from src.parallel.io import (
    WhenOneFails,
    gather_bounded,
    map_bounded,
    partition_results,
)


class ConcurrencyProbe:
    """Records how many of its calls were ever in flight at the same time."""

    def __init__(self) -> None:
        self.in_flight = 0
        self.peak = 0
        self.started: list[int] = []
        self.completed: list[int] = []
        self.cancelled: list[int] = []

    async def __call__(self, item: int, *, delay: float = 0.02) -> int:
        self.in_flight += 1
        self.peak = max(self.peak, self.in_flight)
        self.started.append(item)
        try:
            await asyncio.sleep(delay)
            self.completed.append(item)
            return item * 10
        except asyncio.CancelledError:
            self.cancelled.append(item)
            raise
        finally:
            self.in_flight -= 1

    async def failing_on(self, item: int, *, fail: int, delay: float = 0.02) -> int:
        if item == fail:
            await asyncio.sleep(delay / 2)
            raise ValueError(f"item {item} failed")
        return await self(item, delay=delay)


class TestTheProblemBeingSolved:
    async def test_plain_gather_leaves_siblings_running_after_a_failure(self) -> None:
        # This is the leak `WhenOneFails.CANCEL_REST` exists to close. `gather`
        # propagates the first exception and does *not* cancel the others: the
        # handler returns 502 while four upstream calls are still in flight,
        # holding connections and writing their results nowhere.
        probe = ConcurrencyProbe()

        async def failing() -> int:
            await asyncio.sleep(0.01)
            raise ValueError("first")

        survivor = asyncio.ensure_future(probe(1, delay=0.2))
        with pytest.raises(ValueError):
            await asyncio.gather(failing(), survivor)

        assert survivor.done() is False
        assert probe.in_flight == 1

        # Left running by gather. Cleaned up here so the test does not leak it
        # into the rest of the suite — which is exactly the chore a handler
        # would have to do, and does not.
        survivor.cancel()
        await asyncio.gather(survivor, return_exceptions=True)

    async def test_plain_gather_is_unbounded(self) -> None:
        probe = ConcurrencyProbe()
        await asyncio.gather(*(probe(item) for item in range(20)))
        assert probe.peak == 20


class TestBounding:
    async def test_never_exceeds_the_limit(self) -> None:
        probe = ConcurrencyProbe()
        await gather_bounded([partial(probe, item) for item in range(20)], limit=4)
        assert probe.peak == 4

    async def test_a_limit_above_the_batch_size_runs_everything_at_once(
        self,
    ) -> None:
        probe = ConcurrencyProbe()
        await gather_bounded([partial(probe, item) for item in range(3)], limit=10)
        assert probe.peak == 3

    async def test_a_limit_of_one_serialises(self) -> None:
        probe = ConcurrencyProbe()
        await gather_bounded([partial(probe, item) for item in range(5)], limit=1)
        assert probe.peak == 1
        assert probe.completed == [0, 1, 2, 3, 4]

    async def test_a_shared_semaphore_bounds_across_concurrent_batches(self) -> None:
        # The reason `get_outbound_semaphore` exists. Two batches of limit=3
        # each would be six sockets; one shared semaphore of 3 is three, which
        # is the number an engineer reading the config expects to hold.
        probe = ConcurrencyProbe()
        shared = asyncio.Semaphore(3)

        await asyncio.gather(
            gather_bounded(
                [partial(probe, item) for item in range(6)], semaphore=shared
            ),
            gather_bounded(
                [partial(probe, item) for item in range(6, 12)], semaphore=shared
            ),
        )
        assert probe.peak == 3

    async def test_factories_are_not_called_before_their_slot_is_free(self) -> None:
        # The memory argument for taking factories rather than coroutines: with
        # a limit of 2, only two awaitables have been constructed at any point,
        # not all fifty.
        constructed = 0
        probe = ConcurrencyProbe()

        def factory(item: int) -> Awaitable[int]:
            nonlocal constructed
            constructed += 1
            return probe(item, delay=0.05)

        task: asyncio.Task[list[int]] = asyncio.create_task(
            gather_bounded([partial(factory, item) for item in range(50)], limit=2)
        )
        await asyncio.sleep(0.02)
        assert constructed <= 2
        await task
        assert constructed == 50


class TestOrdering:
    async def test_results_are_in_input_order_not_completion_order(self) -> None:
        async def variable(item: int) -> int:
            # Later items finish first, so completion order is the reverse.
            await asyncio.sleep((10 - item) * 0.01)
            return item

        results = await gather_bounded(
            [partial(variable, item) for item in range(10)], limit=10
        )
        assert results == list(range(10))

    async def test_an_empty_batch_returns_an_empty_list(self) -> None:
        assert await gather_bounded([], limit=4) == []


class TestFailureHandling:
    async def test_cancel_rest_raises_the_original_exception(self) -> None:
        probe = ConcurrencyProbe()
        with pytest.raises(ValueError, match="item 2 failed"):
            await gather_bounded(
                [partial(probe.failing_on, item, fail=2) for item in range(6)],
                limit=6,
            )

    async def test_cancel_rest_leaves_nothing_running(self) -> None:
        # The difference from plain gather, asserted directly: by the time the
        # exception reaches the caller, every sibling has been cancelled *and*
        # awaited, so its `finally` has run and its connection is back.
        probe = ConcurrencyProbe()
        with pytest.raises(ValueError):
            await gather_bounded(
                [
                    partial(probe.failing_on, item, fail=2, delay=0.2)
                    for item in range(6)
                ],
                limit=6,
            )

        assert probe.in_flight == 0
        assert sorted(probe.cancelled) == [0, 1, 3, 4, 5]

    async def test_cancel_rest_reports_the_first_failure_when_several_fail(
        self,
    ) -> None:
        async def fail_after(item: int, delay: float) -> int:
            await asyncio.sleep(delay)
            raise ValueError(f"item {item}")

        with pytest.raises(ValueError, match="item 0"):
            await gather_bounded(
                [partial(fail_after, 0, 0.01), partial(fail_after, 1, 0.2)],
                limit=2,
            )

    async def test_run_all_returns_exceptions_in_place(self) -> None:
        probe = ConcurrencyProbe()
        results = await gather_bounded(
            [partial(probe.failing_on, item, fail=2) for item in range(4)],
            limit=4,
            when_one_fails=WhenOneFails.RUN_ALL,
        )

        assert results[0] == 0
        assert results[1] == 10
        assert isinstance(results[2], ValueError)
        assert results[3] == 30

    async def test_run_all_lets_every_other_item_finish(self) -> None:
        probe = ConcurrencyProbe()
        await gather_bounded(
            [partial(probe.failing_on, item, fail=2) for item in range(6)],
            limit=6,
            when_one_fails=WhenOneFails.RUN_ALL,
        )
        assert sorted(probe.completed) == [0, 1, 3, 4, 5]
        assert probe.cancelled == []


class TestCancellation:
    async def test_outside_cancellation_drains_every_item(self) -> None:
        # A client disconnect cancels the handler. Nothing may outlive it —
        # returning before the children unwind would leave their `finally`
        # blocks unrun and their connections checked out.
        probe = ConcurrencyProbe()
        task = asyncio.create_task(
            gather_bounded(
                [partial(probe, item, delay=5.0) for item in range(4)], limit=4
            )
        )
        await asyncio.sleep(0.05)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert probe.in_flight == 0
        assert sorted(probe.cancelled) == [0, 1, 2, 3]

    async def test_a_timeout_around_the_batch_drains_it(self) -> None:
        probe = ConcurrencyProbe()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(
                gather_bounded(
                    [partial(probe, item, delay=5.0) for item in range(3)], limit=3
                ),
                timeout=0.1,
            )
        assert probe.in_flight == 0
        assert sorted(probe.cancelled) == [0, 1, 2]

    async def test_queued_items_are_never_started(self) -> None:
        probe = ConcurrencyProbe()
        task = asyncio.create_task(
            gather_bounded(
                [partial(probe, item, delay=5.0) for item in range(10)], limit=2
            )
        )
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert len(probe.started) == 2


class TestArgumentValidation:
    async def test_requires_a_limit_or_a_semaphore(self) -> None:
        with pytest.raises(ValueError, match="Pass either"):
            await gather_bounded([], limit=None)

    async def test_refuses_both_a_limit_and_a_semaphore(self) -> None:
        with pytest.raises(ValueError, match="not both"):
            await gather_bounded([], limit=2, semaphore=asyncio.Semaphore(2))

    async def test_the_limit_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="at least 1"):
            await gather_bounded([], limit=0)


class TestMapBounded:
    async def test_applies_the_function_over_the_items_in_order(self) -> None:
        async def triple(item: int) -> int:
            await asyncio.sleep((5 - item) * 0.01)
            return item * 3

        assert await map_bounded(triple, range(5), limit=2) == [0, 3, 6, 9, 12]

    async def test_binds_each_item_rather_than_capturing_the_loop_variable(
        self,
    ) -> None:
        # The late-binding trap `_bind` exists for: a naive
        # `[lambda: fn(item) for item in items]` would run `fn` on the last
        # item five times over.
        seen: list[int] = []

        async def record(item: int) -> int:
            seen.append(item)
            return item

        await map_bounded(record, range(5), limit=1)
        assert seen == [0, 1, 2, 3, 4]

    async def test_honours_the_limit(self) -> None:
        probe = ConcurrencyProbe()
        await map_bounded(probe, range(12), limit=3)
        assert probe.peak == 3

    async def test_passes_the_failure_policy_through(self) -> None:
        async def fail_on_two(item: int) -> int:
            if item == 2:
                raise ValueError("two")
            return item

        results = await map_bounded(
            fail_on_two, range(4), limit=4, when_one_fails=WhenOneFails.RUN_ALL
        )
        assert isinstance(results[2], ValueError)

    async def test_accepts_a_shared_semaphore(self) -> None:
        probe = ConcurrencyProbe()
        await map_bounded(probe, range(6), semaphore=asyncio.Semaphore(2))
        assert probe.peak == 2


class TestPartitionResults:
    def test_splits_successes_from_failures_keeping_indices(self) -> None:
        error = ValueError("nope")
        successes, failures = partition_results(["a", error, "c"])
        assert successes == [(0, "a"), (2, "c")]
        assert failures == [(1, error)]

    def test_indices_survive_a_leading_failure(self) -> None:
        # The reason indices are returned at all: filtering the list would keep
        # "c" but lose that it came from input 2, and the caller needs that to
        # report which items failed.
        successes, _ = partition_results([ValueError("x"), "b", "c"])
        assert successes == [(1, "b"), (2, "c")]

    def test_an_all_success_list_has_no_failures(self) -> None:
        successes, failures = partition_results([1, 2, 3])
        assert successes == [(0, 1), (1, 2), (2, 3)]
        assert failures == []

    def test_treats_base_exceptions_as_failures_too(self) -> None:
        # `RUN_ALL` uses `gather(return_exceptions=True)`, which returns
        # `BaseException` subclasses as well — a `CancelledError` among them.
        cancelled = asyncio.CancelledError()
        _, failures = partition_results([cancelled])
        assert failures == [(0, cancelled)]
