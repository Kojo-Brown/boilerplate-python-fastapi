"""`TaskScope`: ownership, the two exit rules, and the failures they surface.

`TestTheProblemBeingSolved` asserts on bare `asyncio` for the reason
`tests/test_parallel_io.py` does — the case for this module is that
`create_task` loses exceptions and `TaskGroup` cannot exit around a daemon, and
those claims are worth more as tests than as prose.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from functools import partial
from typing import Any

import pytest

from src.structured.errors import TaskScopeClosedError
from src.structured.scope import TaskScope, WhenScopeExits

TICK = 0.05


async def _forever() -> None:
    """A daemon: it ends when it is cancelled and not before."""
    while True:
        await asyncio.sleep(TICK)


class TestTheProblemBeingSolved:
    async def test_a_bare_create_task_loses_the_exception(self) -> None:
        """Nothing raises here. That is the point."""
        recorded: list[BaseException] = []
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(lambda _loop, ctx: recorded.append(ctx["exception"]))

        async def fails() -> None:
            raise RuntimeError("nobody hears this")

        task = asyncio.create_task(fails())
        await asyncio.sleep(TICK)

        assert task.done()
        # The caller was never told, and the loop's handler only hears about it
        # when the task is collected — which is why this asserts on `recorded`
        # being empty rather than on it containing the error.
        assert recorded == []
        loop.set_exception_handler(None)

    async def test_a_plain_task_group_cannot_exit_around_a_daemon(self) -> None:
        """`TaskGroup.__aexit__` waits, so a `while True:` child hangs it."""

        async def run_group() -> None:
            async with asyncio.TaskGroup() as group:
                group.create_task(_forever())

        with pytest.raises(TimeoutError):
            async with asyncio.timeout(TICK * 4):
                await run_group()


class TestWaitOnExit:
    async def test_children_have_finished_when_the_block_ends(self) -> None:
        finished: list[str] = []

        async def work(label: str) -> None:
            await asyncio.sleep(TICK)
            finished.append(label)

        async with TaskScope("fanout") as scope:
            scope.start_soon(partial(work, "a"), name="a")
            scope.start_soon(partial(work, "b"), name="b")
            assert finished == []

        assert sorted(finished) == ["a", "b"]

    async def test_a_failing_child_arrives_as_an_exception_group(self) -> None:
        async def fails() -> None:
            raise RuntimeError("child failed")

        with pytest.raises(BaseExceptionGroup) as caught:
            async with TaskScope("fanout") as scope:
                scope.start_soon(fails, name="bad")

        assert [type(exc) for exc in caught.value.exceptions] == [RuntimeError]

    async def test_a_failing_child_cancels_its_siblings(self) -> None:
        """Inherited from `TaskGroup`, and the reason it is used underneath."""
        cancelled = asyncio.Event()

        async def fails() -> None:
            await asyncio.sleep(0)
            raise RuntimeError("child failed")

        async def sibling() -> None:
            try:
                await asyncio.sleep(TICK * 100)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        with pytest.raises(BaseExceptionGroup):
            async with TaskScope("fanout") as scope:
                scope.start_soon(sibling, name="sibling")
                scope.start_soon(fails, name="bad")

        assert cancelled.is_set()


class TestCancelOnExit:
    async def test_a_daemon_is_cancelled_and_the_block_exits(self) -> None:
        loop = asyncio.get_running_loop()
        started = loop.time()

        async with TaskScope("app", on_exit=WhenScopeExits.CANCEL) as scope:
            child = scope.start_soon(_forever, name="relay")
            await asyncio.sleep(0)

        assert child.cancelled()
        assert loop.time() - started < TICK * 20

    async def test_the_cancellation_is_awaited_so_cleanup_runs(self) -> None:
        """`cancel()` only schedules the error; the `finally` needs a resume."""
        cleaned = asyncio.Event()

        async def daemon() -> None:
            try:
                while True:
                    await asyncio.sleep(TICK)
            finally:
                await asyncio.sleep(0)
                cleaned.set()

        async with TaskScope("app", on_exit=WhenScopeExits.CANCEL) as scope:
            scope.start_soon(daemon, name="relay")
            await asyncio.sleep(0)

        assert cleaned.is_set()

    async def test_a_clean_shutdown_is_not_an_exception_group(self) -> None:
        """Cancelled children contribute no error, which is what makes this
        usable as a lifespan: eight daemons must not become eight failures."""
        async with TaskScope("app", on_exit=WhenScopeExits.CANCEL) as scope:
            for index in range(8):
                scope.start_soon(_forever, name=f"daemon-{index}")
            await asyncio.sleep(0)

    async def test_a_child_that_crashed_first_still_surfaces(self) -> None:
        """The asymmetry: cancelled is silent, crashed is not."""

        async def crashes() -> None:
            await asyncio.sleep(0)
            raise RuntimeError("the relay died")

        with pytest.raises(BaseExceptionGroup) as caught:
            async with TaskScope("app", on_exit=WhenScopeExits.CANCEL) as scope:
                scope.start_soon(crashes, name="relay")
                await asyncio.sleep(TICK)

        assert [type(exc) for exc in caught.value.exceptions] == [RuntimeError]

    async def test_a_child_that_finished_on_its_own_is_left_alone(self) -> None:
        async def quick() -> str:
            return "done"

        async with TaskScope("app", on_exit=WhenScopeExits.CANCEL) as scope:
            child = scope.start_soon(quick, name="quick")
            await asyncio.sleep(TICK)

        assert child.result() == "done"
        assert not child.cancelled()


class TestLifecycle:
    async def test_start_soon_before_entering_is_refused(self) -> None:
        scope = TaskScope("app")
        with pytest.raises(TaskScopeClosedError, match="is not open"):
            scope.start_soon(_forever, name="relay")

    async def test_start_soon_after_exiting_is_refused(self) -> None:
        async with TaskScope("app") as scope:
            pass
        with pytest.raises(TaskScopeClosedError, match="app"):
            scope.start_soon(_forever, name="relay")

    async def test_entering_twice_is_refused(self) -> None:
        scope = TaskScope("app")
        async with scope:
            pass
        with pytest.raises(TaskScopeClosedError, match="cannot be entered twice"):
            async with scope:  # pragma: no cover - the body never runs
                pass

    async def test_reentering_while_open_is_refused(self) -> None:
        scope = TaskScope("app")
        async with scope:
            with pytest.raises(TaskScopeClosedError, match="cannot be entered twice"):
                async with scope:  # pragma: no cover - the body never runs
                    pass

    async def test_open_reports_whether_start_soon_would_be_accepted(self) -> None:
        scope = TaskScope("app")
        assert scope.open is False
        async with scope:
            assert scope.open is True
        assert scope.open is False

    async def test_children_are_named_for_the_scope(self) -> None:
        async with TaskScope("app", on_exit=WhenScopeExits.CANCEL) as scope:
            scope.start_soon(_forever, name="relay")
            names = {task.get_name() for task in asyncio.all_tasks()}
            assert "app:relay" in names

    async def test_children_survives_the_block_as_a_record(self) -> None:
        async with TaskScope("app", on_exit=WhenScopeExits.CANCEL) as scope:
            scope.start_soon(_forever, name="relay")
            scope.start_soon(_forever, name="renewer")
            await asyncio.sleep(0)

        assert [task.get_name() for task in scope.children] == [
            "app:relay",
            "app:renewer",
        ]
        assert all(task.done() for task in scope.children)

    async def test_name_and_exit_rule_are_readable(self) -> None:
        scope = TaskScope("app", on_exit=WhenScopeExits.CANCEL)
        assert scope.name == "app"
        assert scope.on_exit is WhenScopeExits.CANCEL

    async def test_a_body_exception_still_stops_the_children(self) -> None:
        with pytest.raises(ValueError, match="boom"):
            async with TaskScope("app", on_exit=WhenScopeExits.CANCEL) as scope:
                scope.start_soon(_forever, name="relay")
                await asyncio.sleep(0)
                raise ValueError("boom")

        assert all(task.done() for task in scope.children)

    async def test_a_plain_task_group_would_have_wrapped_that(self) -> None:
        """The behaviour the unwrapping rule exists to undo."""
        with pytest.raises(BaseExceptionGroup):
            async with asyncio.TaskGroup() as group:
                group.create_task(asyncio.sleep(0))
                raise ValueError("boom")

    async def test_a_body_exception_beside_a_child_failure_stays_grouped(
        self,
    ) -> None:
        async def crashes() -> None:
            await asyncio.sleep(0)
            raise RuntimeError("the relay died")

        with pytest.raises(BaseExceptionGroup) as caught:
            async with TaskScope("app", on_exit=WhenScopeExits.CANCEL) as scope:
                scope.start_soon(crashes, name="relay")
                await asyncio.sleep(TICK)
                raise ValueError("boom")  # pragma: no cover - child wins first

        assert {type(exc) for exc in caught.value.exceptions} == {RuntimeError}

    async def test_a_factory_is_only_called_when_the_child_starts(self) -> None:
        """Factories rather than coroutines: nothing is constructed early."""
        calls = 0

        def factory() -> Coroutine[Any, Any, None]:
            nonlocal calls
            calls += 1
            return asyncio.sleep(0)

        scope = TaskScope("app")
        with pytest.raises(TaskScopeClosedError):
            scope.start_soon(factory, name="never")

        assert calls == 0
