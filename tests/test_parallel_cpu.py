"""`CpuPool` against real worker processes.

Nothing here mocks the executor. The behaviours worth testing — that a deadline
actually interrupts a busy worker, that a killed child breaks the pool, that a
lambda cannot cross the boundary — are all properties of `multiprocessing` and
`signal`, so a fake would only assert that the fake behaves as assumed.

The cost is real: every pool here spawns interpreters, so worker counts are kept
to one or two and spins are measured in tenths of a second.
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from collections.abc import AsyncIterator

import pytest

from src.parallel.cpu import (
    CpuPool,
    _call_with_deadline,
    _raise_worker_deadline,
    _worker_initializer,
    default_workers,
    ensure_offloadable,
    supported_start_methods,
)
from src.parallel.errors import (
    CpuPoolOverloadedError,
    CpuPoolUnavailableError,
    CpuTaskTimeoutError,
    NotOffloadableError,
    WorkerDeadline,
)
from tests import cpu_workloads as work


@pytest.fixture
async def pool() -> AsyncIterator[CpuPool]:
    """A started single-worker pool, torn down even if a test raises."""
    instance = CpuPool(max_workers=1, queue_depth_per_worker=4)
    instance.start()
    try:
        yield instance
    finally:
        await instance.shutdown(wait=False)


class TestRunning:
    async def test_returns_the_workload_result(self, pool: CpuPool) -> None:
        assert await pool.run(work.double, 21) == 42

    async def test_passes_positional_default_and_keyword_only_arguments(
        self, pool: CpuPool
    ) -> None:
        assert await pool.run(work.add, 1, 2, offset=3) == 6
        assert await pool.run(work.add, 1) == 1

    async def test_the_work_really_leaves_this_process(self, pool: CpuPool) -> None:
        # The claim the whole module rests on. A thread pool would pass every
        # other test in this class and fail this one.
        assert await pool.run(work.current_pid) != os.getpid()

    async def test_workload_exceptions_propagate_unchanged(self, pool: CpuPool) -> None:
        with pytest.raises(ValueError, match="workload failed"):
            await pool.run(work.raise_value_error)

    async def test_the_worker_traceback_is_attached_as_the_cause(
        self, pool: CpuPool
    ) -> None:
        # Without this the exception arrives with a traceback that stops at the
        # process boundary, and the line that actually raised is invisible.
        with pytest.raises(ValueError) as caught:
            await pool.run(work.raise_value_error, "boom")

        cause = caught.value.__cause__
        assert cause is not None
        assert "cpu_workloads.py" in str(cause)
        assert "raise_value_error" in str(cause)

    async def test_an_unpicklable_result_surfaces_rather_than_hanging(
        self, pool: CpuPool
    ) -> None:
        # The worker cannot send it back, so the failure happens on the far side
        # of the queue. It must still land on the future rather than leaving the
        # caller awaiting forever.
        with pytest.raises(Exception):  # noqa: B017 - pickle raises several types
            await pool.run(work.return_unpicklable)

    async def test_calls_run_concurrently_across_workers(self) -> None:
        instance = CpuPool(max_workers=2)
        instance.start()
        try:
            # Distinct PIDs rather than a wall-clock comparison: both calls
            # occupy a worker for the same window, so they can only report the
            # same PID if the pool serialised them. A timing assertion would
            # say the same thing far less reliably on a loaded CI runner.
            first, second = await asyncio.gather(
                instance.run(work.spin_and_pid, 0.4),
                instance.run(work.spin_and_pid, 0.4),
            )
            assert first != second
            assert {first, second} != {os.getpid()}
        finally:
            await instance.shutdown(wait=False)

    async def test_the_initializer_makes_workers_ignore_sigint(
        self, pool: CpuPool
    ) -> None:
        assert await pool.run(work.sigint_is_ignored) is True


class TestDeadlines:
    @pytest.mark.skipif(
        not hasattr(signal, "setitimer"), reason="POSIX-only deadline enforcement"
    )
    async def test_a_deadline_interrupts_a_busy_worker(self, pool: CpuPool) -> None:
        loop = asyncio.get_running_loop()
        started = loop.time()
        with pytest.raises(CpuTaskTimeoutError) as caught:
            await pool.with_timeout(0.5).run(work.spin, 30.0)
        elapsed = loop.time() - started

        # The assertion that matters is the upper bound: without in-worker
        # enforcement this call takes 30 seconds, not one.
        assert elapsed < 10.0
        assert caught.value.details == {
            "timeout_seconds": 0.5,
            "enforced_by": "worker",
        }
        assert caught.value.status_code == 504

    @pytest.mark.skipif(
        not hasattr(signal, "setitimer"), reason="POSIX-only deadline enforcement"
    )
    async def test_defensive_error_handling_cannot_swallow_a_deadline(
        self, pool: CpuPool
    ) -> None:
        # `WorkerDeadline` derives from BaseException for exactly this. If it
        # derived from Exception, the workload's `except Exception` would catch
        # it and this would return "swallowed" — a timeout that does nothing.
        with pytest.raises(CpuTaskTimeoutError):
            await pool.with_timeout(0.5).run(work.spin_swallowing_exceptions, 30.0)

    @pytest.mark.skipif(
        not hasattr(signal, "setitimer"), reason="POSIX-only deadline enforcement"
    )
    async def test_the_slot_comes_back_after_a_deadline(self, pool: CpuPool) -> None:
        # The reason the deadline is enforced in the worker at all. A pool that
        # reports a timeout while the worker keeps the slot runs out of capacity
        # without ever saying so.
        with pytest.raises(CpuTaskTimeoutError):
            await pool.with_timeout(0.4).run(work.spin, 30.0)

        assert await pool.run(work.double, 2) == 4

    @pytest.mark.skipif(
        not hasattr(signal, "setitimer"), reason="POSIX-only deadline enforcement"
    )
    async def test_a_deadline_leaves_no_timer_armed_for_the_next_call(
        self, pool: CpuPool
    ) -> None:
        # Workers are reused. A live ITIMER_REAL left behind would fire during
        # somebody else's work, and the resulting timeout would be attributed to
        # whichever request came next.
        with pytest.raises(CpuTaskTimeoutError):
            await pool.with_timeout(0.4).run(work.spin, 30.0)

        assert await pool.run(work.alarm_is_disarmed) is True

    async def test_a_call_that_finishes_inside_its_deadline_is_unaffected(
        self, pool: CpuPool
    ) -> None:
        assert await pool.with_timeout(30.0).run(work.double, 4) == 8
        assert await pool.run(work.alarm_is_disarmed) is True

    async def test_no_timeout_means_no_deadline(self, pool: CpuPool) -> None:
        assert await pool.run(work.spin, 0.2) == "finished"

    async def test_deadline_enforced_reports_the_platform_regime(
        self, pool: CpuPool
    ) -> None:
        assert pool.deadline_enforced is hasattr(signal, "setitimer")
        assert pool.deadline_enforced is (sys.platform != "win32")

    def test_with_timeout_returns_a_view_over_the_same_pool(
        self, pool: CpuPool
    ) -> None:
        # A view, not a pool: it must not own processes or slots of its own, or
        # the admission control the real pool does would be bypassed.
        view = pool.with_timeout(1.5)
        assert view.timeout == 1.5
        assert view._pool is pool

    def test_with_timeout_rejects_a_non_positive_deadline(self, pool: CpuPool) -> None:
        # `setitimer(ITIMER_REAL, 0)` *disarms* the timer, so a zero deadline
        # would silently mean "no deadline" — the opposite of what it reads as.
        with pytest.raises(ValueError, match="positive"):
            pool.with_timeout(0)
        with pytest.raises(ValueError, match="positive"):
            pool.with_timeout(-1.0)

    async def test_a_view_is_reusable_across_calls(self, pool: CpuPool) -> None:
        view = pool.with_timeout(30.0)
        assert await view.run(work.double, 1) == 2
        assert await view.run(work.double, 2) == 4

    async def test_a_view_goes_through_the_pools_admission_control(self) -> None:
        instance = CpuPool(max_workers=1, queue_depth_per_worker=0)
        instance.start()
        try:
            running = asyncio.create_task(
                instance.with_timeout(30.0).run(work.spin, 1.0)
            )
            await asyncio.sleep(0.2)
            with pytest.raises(CpuPoolOverloadedError):
                await instance.with_timeout(30.0).run(work.double, 1)
            await running
        finally:
            await instance.shutdown(wait=False)


class TestAdmissionControl:
    async def test_refuses_work_past_capacity_rather_than_queueing_it(self) -> None:
        # `ProcessPoolExecutor`'s queue is unbounded, so without this the excess
        # accumulates as pickled payloads in this process's heap while every one
        # of those requests waits on a socket that has likely already given up.
        instance = CpuPool(max_workers=1, queue_depth_per_worker=1)
        instance.start()
        try:
            assert instance.capacity == 2
            running = [
                asyncio.create_task(instance.run(work.spin, 1.5)) for _ in range(2)
            ]
            await asyncio.sleep(0.2)

            with pytest.raises(CpuPoolOverloadedError) as caught:
                await instance.run(work.double, 1)

            assert caught.value.status_code == 503
            assert caught.value.headers == {"Retry-After": "1"}
            assert caught.value.details == {"capacity": 2, "max_workers": 1}

            await asyncio.gather(*running)
        finally:
            await instance.shutdown(wait=False)

    async def test_capacity_is_restored_as_calls_finish(self) -> None:
        instance = CpuPool(max_workers=1, queue_depth_per_worker=0)
        instance.start()
        try:
            assert instance.capacity == 1
            first = asyncio.create_task(instance.run(work.spin, 0.8))
            await asyncio.sleep(0.2)
            with pytest.raises(CpuPoolOverloadedError):
                await instance.run(work.double, 1)

            await first
            assert await instance.run(work.double, 1) == 2
        finally:
            await instance.shutdown(wait=False)

    async def test_capacity_is_workers_times_one_plus_queue_depth(self) -> None:
        assert CpuPool(max_workers=3, queue_depth_per_worker=4).capacity == 15
        assert CpuPool(max_workers=2, queue_depth_per_worker=0).capacity == 2


class TestBrokenPool:
    async def test_a_killed_worker_is_reported_and_the_pool_recovers(self) -> None:
        # `BrokenProcessPool` is terminal for a `ProcessPoolExecutor`: every
        # later submission fails with it forever. Without the replacement, one
        # OOM kill takes the endpoint down until the pod restarts.
        instance = CpuPool(max_workers=1)
        instance.start()
        try:
            with pytest.raises(CpuPoolUnavailableError) as caught:
                await instance.run(work.kill_own_process)
            assert caught.value.status_code == 503

            assert await instance.run(work.double, 5) == 10
        finally:
            await instance.shutdown(wait=False)

    async def test_the_break_does_not_leak_the_slot(self) -> None:
        instance = CpuPool(max_workers=1, queue_depth_per_worker=0)
        instance.start()
        try:
            with pytest.raises(CpuPoolUnavailableError):
                await instance.run(work.kill_own_process)
            # Capacity is 1; if the broken call had kept its slot this would be
            # a 503 rather than a result.
            assert await instance.run(work.double, 3) == 6
        finally:
            await instance.shutdown(wait=False)


class TestLifecycle:
    async def test_run_before_start_is_a_503_not_an_attribute_error(self) -> None:
        instance = CpuPool(max_workers=1)
        with pytest.raises(CpuPoolUnavailableError, match="not running"):
            await instance.run(work.double, 1)

    async def test_start_is_idempotent(self) -> None:
        instance = CpuPool(max_workers=1)
        instance.start()
        first = instance._executor
        instance.start()
        try:
            assert instance._executor is first
        finally:
            await instance.shutdown(wait=False)

    async def test_shutdown_is_idempotent_and_leaves_the_pool_not_running(
        self,
    ) -> None:
        instance = CpuPool(max_workers=1)
        instance.start()
        assert instance.running is True
        await instance.shutdown(wait=False)
        assert instance.running is False
        await instance.shutdown(wait=False)
        assert instance.running is False

    async def test_run_after_shutdown_is_a_503(self) -> None:
        instance = CpuPool(max_workers=1)
        instance.start()
        await instance.shutdown(wait=False)
        with pytest.raises(CpuPoolUnavailableError):
            await instance.run(work.double, 1)

    async def test_shutdown_waiting_lets_in_flight_work_finish(self) -> None:
        instance = CpuPool(max_workers=1)
        instance.start()
        pending = asyncio.create_task(instance.run(work.spin, 0.5))
        await asyncio.sleep(0.1)
        await instance.shutdown(wait=True)
        assert await pending == "finished"


class TestConfiguration:
    def test_fork_is_refused(self) -> None:
        # Not a warning. A forked child of an async server inherits the parent's
        # open Postgres and Redis sockets, and two writers on one connection
        # interleave protocol frames rather than failing cleanly.
        with pytest.raises(ValueError, match="fork"):
            CpuPool(start_method="fork")

    def test_max_workers_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="at least 1"):
            CpuPool(max_workers=0)

    def test_queue_depth_must_not_be_negative(self) -> None:
        with pytest.raises(ValueError, match="not be negative"):
            CpuPool(queue_depth_per_worker=-1)

    def test_max_workers_defaults_to_the_derived_count(self) -> None:
        assert CpuPool().max_workers == default_workers()

    def test_default_workers_leaves_the_loop_a_core_and_never_returns_zero(
        self,
    ) -> None:
        count = default_workers()
        assert count >= 1
        assert count <= (os.cpu_count() or 1)

    def test_supported_start_methods_never_offers_fork(self) -> None:
        methods = supported_start_methods()
        assert "fork" not in methods
        assert "spawn" in methods


class TestEnsureOffloadable:
    def test_accepts_a_module_level_function(self) -> None:
        ensure_offloadable(work.double)

    def test_rejects_a_lambda(self) -> None:
        with pytest.raises(NotOffloadableError, match="qualified name"):
            ensure_offloadable(lambda: None)

    def test_rejects_a_closure(self) -> None:
        def inner() -> int:  # pragma: no cover - never called
            return 1

        with pytest.raises(NotOffloadableError):
            ensure_offloadable(inner)

    async def test_run_rejects_a_lambda_at_the_call_site(self, pool: CpuPool) -> None:
        # Without the eager check this surfaces as an `AttributeError` raised on
        # the executor's queue-management thread, with a traceback through
        # `concurrent.futures.process` and no mention of this line.
        with pytest.raises(NotOffloadableError):
            await pool.run(lambda: None)

    def test_the_message_names_the_callable(self) -> None:
        with pytest.raises(NotOffloadableError, match="test_the_message_names"):

            def local() -> None:  # pragma: no cover - never called
                return None

            local.__qualname__ = "test_the_message_names_the_callable.<locals>.local"
            ensure_offloadable(local)


class TestWorkerSideHelpers:
    """The functions that run *in the child*, exercised here in the parent.

    They are covered indirectly by every test above, but only inside a
    subprocess — where `coverage` cannot see them and where a wrong assertion
    would be reported as a pickled exception rather than a failure. Calling
    them directly is both measurable and much sharper about what broke.
    """

    def test_no_timeout_calls_straight_through(self) -> None:
        assert _call_with_deadline(work.add, (1, 2), {"offset": 3}, None) == 6

    @pytest.mark.skipif(
        not hasattr(signal, "setitimer"), reason="POSIX-only deadline enforcement"
    )
    def test_a_deadline_that_is_not_reached_leaves_no_trace(self) -> None:
        before = signal.getsignal(signal.SIGALRM)
        assert _call_with_deadline(work.double, (21,), {}, 30.0) == 42

        # Both restored, not merely cleared. Workers are reused, so a live timer
        # or a lingering handler would fire during the *next* call and report a
        # timeout against whichever request came after this one.
        assert signal.getsignal(signal.SIGALRM) is before
        assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)

    @pytest.mark.skipif(
        not hasattr(signal, "setitimer"), reason="POSIX-only deadline enforcement"
    )
    def test_an_overrun_raises_worker_deadline_and_still_restores(self) -> None:
        before = signal.getsignal(signal.SIGALRM)
        with pytest.raises(WorkerDeadline):
            _call_with_deadline(work.spin, (30.0,), {}, 0.3)

        # The restoration is in a `finally`, so it has to survive the raise.
        assert signal.getsignal(signal.SIGALRM) is before
        assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)

    def test_worker_deadline_is_not_an_exception(self) -> None:
        # The property the whole deadline design leans on. If this ever became
        # an `Exception`, `spin_swallowing_exceptions` would absorb it and every
        # timeout in this module would quietly become advisory.
        assert issubclass(WorkerDeadline, BaseException)
        assert not issubclass(WorkerDeadline, Exception)

    @pytest.mark.skipif(
        not hasattr(signal, "setitimer"), reason="POSIX-only deadline enforcement"
    )
    def test_the_alarm_handler_raises(self) -> None:
        with pytest.raises(WorkerDeadline, match="deadline exceeded"):
            _raise_worker_deadline(signal.SIGALRM, None)

    def test_the_initializer_ignores_sigint(self) -> None:
        before = signal.getsignal(signal.SIGINT)
        try:
            _worker_initializer()
            assert signal.getsignal(signal.SIGINT) is signal.SIG_IGN
        finally:
            signal.signal(signal.SIGINT, before)


class UninterruptiblePool(CpuPool):
    """A pool that reports the platform cannot interrupt its workers.

    Windows is permanently in this regime, and a POSIX box lands in it whenever
    a C extension holds the GIL past the deadline. Subclassed rather than
    monkeypatched because the worker has to be uninterruptible *as well* — see
    `spin_with_alarm_blocked` — and a patched module constant in the parent
    would not reach the child.
    """

    @property
    def deadline_enforced(self) -> bool:
        return False


class TestUninterruptibleWorkerFallback:
    async def test_the_parent_side_wait_bounds_the_request(self) -> None:
        instance = UninterruptiblePool(max_workers=1)
        instance.start()
        try:
            loop = asyncio.get_running_loop()
            started = loop.time()
            with pytest.raises(CpuTaskTimeoutError) as caught:
                await instance.with_timeout(0.4).run(work.spin_with_alarm_blocked, 8.0)
            elapsed = loop.time() - started

            # The request is bounded even though the worker is not: 0.4s, not 8.
            assert elapsed < 5.0
            assert caught.value.details == {
                "timeout_seconds": 0.4,
                "enforced_by": "wait",
            }
            assert caught.value.status_code == 504
        finally:
            await instance.shutdown(wait=False)

    async def test_the_slot_stays_held_while_the_worker_runs_on(self) -> None:
        # The honest accounting, asserted rather than only documented. The
        # worker is still busy, so its slot is not free — releasing it when the
        # *wait* ended would let the pool admit more work than it can run.
        instance = UninterruptiblePool(max_workers=1, queue_depth_per_worker=0)
        instance.start()
        try:
            with pytest.raises(CpuTaskTimeoutError):
                await instance.with_timeout(0.3).run(work.spin_with_alarm_blocked, 3.0)

            with pytest.raises(CpuPoolOverloadedError):
                await instance.run(work.double, 1)
        finally:
            await instance.shutdown(wait=False)

    async def test_the_slot_returns_once_the_worker_finishes(self) -> None:
        instance = UninterruptiblePool(max_workers=1, queue_depth_per_worker=0)
        instance.start()
        try:
            with pytest.raises(CpuTaskTimeoutError):
                await instance.with_timeout(0.3).run(work.spin_with_alarm_blocked, 1.0)

            # The done callback fires when the work really ends, so capacity
            # recovers on its own rather than being lost for good.
            for _ in range(100):
                await asyncio.sleep(0.1)
                if not instance._slots.locked():
                    break
            assert await instance.run(work.double, 4) == 8
        finally:
            await instance.shutdown(wait=False)


class TestSubmitRaces:
    async def test_a_shutdown_executor_underneath_the_pool_is_a_503(self) -> None:
        # `submit` raises `RuntimeError` after the executor is shut down. The
        # pool reaches this when the lifespan tears it down while a request is
        # still resolving its dependencies.
        instance = CpuPool(max_workers=1)
        instance.start()
        try:
            assert instance._executor is not None
            instance._executor.shutdown(wait=False)

            with pytest.raises(CpuPoolUnavailableError, match="shutting down"):
                await instance.run(work.double, 1)

            # The refused call must not have kept its slot.
            assert instance._slots.locked() is False
        finally:
            await instance.shutdown(wait=False)

    async def test_submitting_to_an_already_broken_executor_is_a_503(self) -> None:
        instance = CpuPool(max_workers=1)
        instance.start()
        try:
            with pytest.raises(CpuPoolUnavailableError):
                await instance.run(work.kill_own_process)
            first_replacement = instance._executor

            # The replacement is a working executor, so the pool recovers rather
            # than answering 503 forever.
            assert await instance.run(work.double, 1) == 2
            assert instance._executor is first_replacement
        finally:
            await instance.shutdown(wait=False)

    def test_replacing_an_executor_that_is_no_longer_current_is_a_no_op(self) -> None:
        # Several requests can fail on the same break. Without the identity
        # guard, each would install another executor and orphan the last.
        instance = CpuPool(max_workers=1)
        instance.start()
        try:
            current = instance._executor
            assert current is not None
            stale = instance._new_executor()
            try:
                instance._replace_broken_executor(stale)
                assert instance._executor is current
            finally:
                stale.shutdown(wait=False)
        finally:
            instance._executor = current

    async def test_a_pool_broken_before_submit_is_replaced_too(self) -> None:
        # The other half of the break: a pool that broke while this request was
        # elsewhere fails at `submit` rather than at `await`. Both paths have to
        # replace the executor, or the second request onwards is 503 forever.
        #
        # `_broken` is set directly because racing a real break to land between
        # two specific statements is not something a test can do reliably; what
        # is being tested is this class's reaction, and that is the state
        # `ProcessPoolExecutor.submit` reacts to.
        instance = CpuPool(max_workers=1)
        instance.start()
        try:
            broken = instance._executor
            assert broken is not None
            # typeshed types `_broken` as a bool; CPython stores the reason
            # string it will raise, and `submit` only tests it for truthiness.
            broken._broken = "simulated worker death"  # type: ignore[assignment]

            with pytest.raises(CpuPoolUnavailableError, match="being replaced"):
                await instance.run(work.double, 1)

            assert instance._executor is not broken
            assert instance._slots.locked() is False
            assert await instance.run(work.double, 21) == 42
        finally:
            await instance.shutdown(wait=False)


class TestDefaultWorkers:
    def test_falls_back_to_cpu_count_when_affinity_reports_nothing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # `sched_getaffinity` returning an empty set is not hypothetical on
        # exotic schedulers, and `max(1, 0 - 1)` would otherwise be a pool of
        # one built from a number that meant "unknown", not "one".
        monkeypatch.delattr(os, "process_cpu_count", raising=False)
        monkeypatch.setattr(os, "sched_getaffinity", lambda _pid: set())
        monkeypatch.setattr(os, "cpu_count", lambda: 8)
        assert default_workers() == 7

    def test_falls_back_to_one_when_nothing_can_say(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delattr(os, "process_cpu_count", raising=False)
        monkeypatch.setattr(os, "sched_getaffinity", lambda _pid: set())
        monkeypatch.setattr(os, "cpu_count", lambda: None)
        assert default_workers() == 1

    def test_uses_the_affinity_mask_over_the_machines_core_count(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The container case: a 64-core node, a 4-CPU limit. Sizing from
        # `cpu_count` would spawn 63 interpreters to share four cores' quota.
        monkeypatch.delattr(os, "process_cpu_count", raising=False)
        monkeypatch.setattr(os, "sched_getaffinity", lambda _pid: {0, 1, 2, 3})
        monkeypatch.setattr(os, "cpu_count", lambda: 64)
        assert default_workers() == 3
