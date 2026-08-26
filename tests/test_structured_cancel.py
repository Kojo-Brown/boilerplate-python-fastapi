"""`protect` and `finalize`: cleanup that finishes after the caller is cancelled.

`TestTheProblemBeingSolved` demonstrates the two failures these exist for
against bare `asyncio` — cleanup cut in half by a second cancellation, and
`asyncio.shield` turning that into an unowned task rather than fixing it.
They are the justification, so they are tests rather than prose.

Everything here drives cancellation the way it actually arrives: an outer task
running the code under test, cancelled from the test's own task. Calling
`raise CancelledError` by hand would exercise the `except` clauses without
exercising the task-state bookkeeping — `uncancel`, `cancelling` — which is
where the interesting behaviour is.
"""

from __future__ import annotations

import asyncio
from functools import partial

import pytest

from src.structured.cancel import finalize, protect
from src.structured.errors import DeadlineExceeded

TICK = 0.05


class Cleanup:
    """A cleanup that takes two ticks and records whether it got to the end."""

    def __init__(self, *, duration: float = TICK * 2) -> None:
        self.duration = duration
        self.started = False
        self.completed = False

    async def run(self) -> str:
        self.started = True
        await asyncio.sleep(self.duration)
        self.completed = True
        return "released"


async def _cancel_after(task: asyncio.Task[object], delay: float, times: int) -> None:
    """Cancel `task` `times` times, `delay` apart. One cancel is not enough.

    The bug being tested needs two: the first is delivered and caught by the
    code under test, and it takes a second — a shutdown, a `TaskGroup` abort —
    to cut the cleanup that first one started.
    """
    for _ in range(times):
        await asyncio.sleep(delay)
        task.cancel()


class TestTheProblemBeingSolved:
    async def test_an_unprotected_cleanup_is_cut_by_a_second_cancellation(
        self,
    ) -> None:
        cleanup = Cleanup()

        async def handler() -> None:
            try:
                await asyncio.sleep(TICK * 100)
            except asyncio.CancelledError:
                await cleanup.run()
                raise

        task = asyncio.ensure_future(handler())
        await _cancel_after(task, TICK, times=2)
        with pytest.raises(asyncio.CancelledError):
            await task

        assert cleanup.started is True
        assert cleanup.completed is False

    async def test_shield_returns_early_and_leaves_the_work_unowned(self) -> None:
        cleanup = Cleanup()
        escaped: list[asyncio.Task[str]] = []

        async def handler() -> None:
            try:
                await asyncio.sleep(TICK * 100)
            except asyncio.CancelledError:
                inner = asyncio.ensure_future(cleanup.run())
                escaped.append(inner)
                await asyncio.shield(inner)  # pragma: no cover - always raises
                raise

        task = asyncio.ensure_future(handler())
        await _cancel_after(task, TICK, times=2)
        with pytest.raises(asyncio.CancelledError):
            await task

        # The handler is gone and the cleanup is still going: shielded from the
        # cancellation, and owned by nobody.
        assert task.done()
        assert cleanup.completed is False
        assert escaped[0].done() is False
        await escaped[0]


class TestProtect:
    async def test_returns_the_result_when_nothing_is_cancelled(self) -> None:
        cleanup = Cleanup()
        assert await protect(cleanup.run, name="release") == "released"
        assert cleanup.completed is True

    async def test_the_work_finishes_despite_repeated_cancellation(self) -> None:
        cleanup = Cleanup()

        async def handler() -> None:
            try:
                await asyncio.sleep(TICK * 100)
            except asyncio.CancelledError:
                await protect(cleanup.run, name="release")
                raise  # pragma: no cover - protect re-raises first

        task = asyncio.ensure_future(handler())
        await _cancel_after(task, TICK, times=4)
        with pytest.raises(asyncio.CancelledError):
            await task

        assert cleanup.completed is True

    async def test_the_cancellation_is_re_raised_not_swallowed(self) -> None:
        cleanup = Cleanup()
        reached_the_end = False

        async def handler() -> None:
            nonlocal reached_the_end
            await protect(cleanup.run, name="release")
            reached_the_end = True  # pragma: no cover - must not be reached

        task = asyncio.ensure_future(handler())
        await _cancel_after(task, TICK, times=1)
        with pytest.raises(asyncio.CancelledError):
            await task

        assert cleanup.completed is True
        assert reached_the_end is False

    async def test_the_work_own_exception_propagates(self) -> None:
        async def fails() -> None:
            raise ValueError("release failed")

        with pytest.raises(ValueError, match="release failed"):
            await protect(fails, name="release")

    async def test_a_cancellation_outranks_the_work_own_exception(self) -> None:
        async def fails() -> None:
            await asyncio.sleep(TICK * 2)
            raise ValueError("release failed")

        async def handler() -> None:
            await protect(fails, name="release")

        task = asyncio.ensure_future(handler())
        await _cancel_after(task, TICK, times=1)
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_the_timeout_cancels_and_drains_the_work(self) -> None:
        cleanup = Cleanup(duration=TICK * 100)

        with pytest.raises(DeadlineExceeded) as caught:
            await protect(cleanup.run, name="release", timeout=TICK)

        assert caught.value.scope == "release"
        assert cleanup.started is True
        assert cleanup.completed is False
        # Drained, not abandoned: nothing is still running behind the error.
        assert not [
            task
            for task in asyncio.all_tasks()
            if task.get_name() == "protected:release"
        ]

    async def test_the_factory_is_called_once(self) -> None:
        calls = 0

        async def cleanup() -> int:
            nonlocal calls
            calls += 1
            return calls

        assert await protect(cleanup, name="release") == 1
        assert calls == 1

    async def test_the_child_is_named_for_the_scope(self) -> None:
        async def observe() -> str:
            await asyncio.sleep(0)
            current = asyncio.current_task()
            assert current is not None
            return current.get_name()

        assert await protect(observe, name="release") == "protected:release"

    async def test_an_enclosing_timeout_still_converts_its_own_expiry(self) -> None:
        """The `uncancel()` bookkeeping, asserted through its consequence.

        Leaving the cancelling count high makes `asyncio.timeout` stop
        recognising its own expiry, and a `CancelledError` escapes where a
        `TimeoutError` was promised.
        """
        cleanup = Cleanup(duration=TICK)

        async def handler() -> None:
            async with asyncio.timeout(TICK * 4):
                await protect(cleanup.run, name="release")
                await asyncio.sleep(TICK * 100)

        with pytest.raises(TimeoutError):
            await handler()

        assert cleanup.completed is True


class TestFinalize:
    async def test_returns_the_result_when_nothing_goes_wrong(self) -> None:
        cleanup = Cleanup()
        assert await finalize(cleanup.run, name="release") == "released"

    async def test_does_not_replace_the_exception_being_unwound(self) -> None:
        """The reason this exists rather than only `protect`."""
        cleanup = Cleanup()

        async def caller() -> None:
            try:
                raise ValueError("the original failure")
            except ValueError:
                await finalize(cleanup.run, name="release")
                raise

        with pytest.raises(ValueError, match="the original failure"):
            await caller()

        assert cleanup.completed is True

    async def test_an_absorbed_cancellation_is_re_armed_not_dropped(self) -> None:
        """Deferred by the length of the cleanup, never discarded."""
        cleanup = Cleanup()
        after_cleanup = False

        async def handler() -> None:
            nonlocal after_cleanup
            try:
                raise ValueError("the original failure")
            except ValueError:
                await finalize(cleanup.run, name="release")
                after_cleanup = True
                await asyncio.sleep(0)  # pragma: no cover - cancelled here

        task = asyncio.ensure_future(handler())
        await _cancel_after(task, TICK, times=2)
        with pytest.raises(asyncio.CancelledError):
            await task

        assert cleanup.completed is True
        assert after_cleanup is True
        assert task.cancelled()

    async def test_a_failing_cleanup_returns_none_instead_of_raising(self) -> None:
        async def fails() -> str:
            raise ValueError("release failed")

        assert await finalize(fails, name="release") is None

    async def test_a_cleanup_that_cancels_itself_returns_none(self) -> None:
        async def self_cancelling() -> str:
            current = asyncio.current_task()
            assert current is not None
            current.cancel()
            await asyncio.sleep(0)
            return "unreachable"  # pragma: no cover - cancelled above

        assert await finalize(self_cancelling, name="release") is None

    async def test_the_timeout_returns_none_and_drains(self) -> None:
        cleanup = Cleanup(duration=TICK * 100)

        assert await finalize(cleanup.run, name="release", timeout=TICK) is None

        assert cleanup.started is True
        assert cleanup.completed is False
        assert not [
            task
            for task in asyncio.all_tasks()
            if task.get_name() == "finalize:release"
        ]

    async def test_bound_arguments_are_supported(self) -> None:
        async def release(key: str) -> str:
            return key

        assert await finalize(partial(release, "abc"), name="release") == "abc"
