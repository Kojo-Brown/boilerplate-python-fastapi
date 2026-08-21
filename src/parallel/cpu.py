"""Running CPU-bound work without stopping the event loop.

## The problem this solves

An `async def` route handler runs on the event loop thread. While it holds that
thread, *nothing else in the process progresses* — not the other requests
mid-flight, not the health check the orchestrator is about to give up on, not
the keep-alives. A route that spends 400ms hashing an image is not a route that
is slow; it is a route that makes every concurrent request slow, and the effect
is invisible under single-request testing and obvious under load.

`await` does not help, because there is nothing to wait for: the work is
compute. A thread does not help either, and this is the part that surprises
people — CPython's global interpreter lock means a thread doing pure-Python
compute takes the GIL away from the event loop thread in the same way, just
with extra context switching. `run_in_threadpool` is the right answer for
*blocking IO* (a synchronous database driver, `open()`, `subprocess.wait`) and
the wrong answer for compute.

So the work has to leave the process. That is all `CpuPool` is: a
`ProcessPoolExecutor` with the sharp edges of using one from an async server
turned into explicit, documented behaviour.

## When not to use it

The round trip costs a pickle of the arguments, a pipe write, a pickle of the
result and a pipe read — order of a hundred microseconds plus the size of the
payload. Offloading work that takes less time than that makes the endpoint
slower and adds a failure mode. The rule of thumb: if it does not block the
loop for a visible fraction of a millisecond, leave it inline. If it runs for
*seconds*, this is still the wrong home for it — that is a Celery task
(`src/tasks/`), because a request the client is holding open is not where
multi-second work belongs, and a pool slot occupied for seconds is a slot the
next request cannot have.

The band this is for is in between: tens of milliseconds to a couple of
seconds. Image resizing, PDF rendering, CSV parsing, signature verification,
compression. Password hashing is the notable exception — argon2 releases the
GIL, so it belongs in a thread, not here.

## Why `spawn`, not `fork`

The start method is not a tuning knob here; `fork` is a correctness bug in this
process.

A forked child is a memory copy of the parent, and the parent is an async web
server. It inherits the SQLAlchemy connection pool's *open sockets*, the Redis
client's, and the event loop's epoll set — file descriptors whose other end
belongs to the parent. Two processes then read and write the same TCP stream,
and a Postgres connection with two writers does not fail cleanly; it interleaves
protocol frames and returns one request's rows to another. It also inherits the
loop's internal state while owning none of the threads that maintain it.

Worse, `fork` in a process with threads is undefined behaviour at the POSIX
level — only the calling thread survives into the child, so any lock held by a
thread that did not survive is now held forever by nobody. uvicorn runs threads.
CPython 3.12 warns about exactly this, and 3.14 changed the Linux default to
`forkserver` because of it.

`spawn` starts a clean interpreter that inherits no descriptors and no locks.
It costs a fresh interpreter start-up and a re-import per worker — which is why
workers are started once, at application start-up, and reused.

**The consequence to know about:** a spawned child re-imports the parent's
`__main__` module. Under uvicorn or gunicorn that is a module of theirs and
harmless. In a *script* it is your own file, so a script that builds a pool at
module scope will recursively spawn until multiprocessing refuses — which it
does, with a `RuntimeError` naming `freeze_support`. The fix is the standard
`if __name__ == "__main__":` guard.

## What a timeout can and cannot do

`asyncio.wait_for` around an executor future cancels *the wait*, never the work.
The worker keeps computing, and — since a pool has a fixed number of workers —
a runaway call would hold its slot forever while the parent politely reported a
timeout. Do that a few times and the pool has no capacity left, having told
nobody.

So where the platform allows it the deadline is enforced **inside the worker**,
with `signal.setitimer`. That is what makes the slot come back, and it is why
there is no parent-side timer racing it: a parent-side wait measured from
*submission* would include however long the call sat queued behind other work,
so it would fire first under load and report a timeout for a call that had not
started. The worker's timer starts when the work does, which is the only clock
that can answer "did this call take too long".

Two limits, neither worked around here because neither can be:

- **Not on Windows.** `signal.setitimer` is POSIX-only. There, and only there,
  `run` falls back to a parent-side `wait_for` — which bounds the request and
  leaks the slot, the lesser of the two available evils.
  `CpuPool.deadline_enforced` reports which regime you are in.
- **Not through a C extension holding the GIL.** A signal handler runs at a
  bytecode boundary, so a tight `numpy` loop or a regex in catastrophic
  backtracking will overrun its deadline and nothing here will stop it. For
  attacker-influenced input to such a library a pool is not enough: the work
  needs its own killable process, or a limit inside the library. What bounds
  the *request* in that case is the timeout at the edge (uvicorn, the ingress),
  which is the right layer for an end-to-end bound and is not this module's to
  impose.

`max_tasks_per_child` is the other half of that story and is on by default: a
worker is retired after a set number of calls, so a slow leak in a third-party
decoder is bounded by the recycle rather than by the pod's memory limit.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import pickle
import signal
from collections.abc import Callable
from concurrent.futures import BrokenExecutor, ProcessPoolExecutor
from types import FrameType
from typing import Any, Final

import structlog

from src.parallel.errors import (
    CpuPoolOverloadedError,
    CpuPoolUnavailableError,
    CpuTaskTimeoutError,
    NotOffloadableError,
    WorkerDeadline,
)

logger = structlog.get_logger(__name__)

#: `signal.setitimer` and `SIGALRM` are POSIX. Without them the worker cannot
#: enforce its own deadline and only the parent-side wait applies — which
#: bounds the request but not the worker. Read `CpuPool.deadline_enforced`
#: rather than this, so a test can substitute either regime.
CAN_ENFORCE_WORKER_DEADLINE: Final[bool] = hasattr(signal, "setitimer")

#: Retire a worker after this many calls. Bounds the damage from a third-party
#: library that leaks a little per call — a real property of image and PDF
#: decoders — at the cost of an interpreter start-up every N calls. High enough
#: that the amortised cost is negligible, low enough that a leak is capped well
#: below a container memory limit.
DEFAULT_MAX_TASKS_PER_CHILD: Final[int] = 100

#: How many calls may be waiting on a full pool before admission is refused,
#: per worker. Queued payloads sit pickled in the parent's memory, so this is a
#: memory bound as much as a latency one — see `CpuPoolOverloadedError`.
DEFAULT_QUEUE_DEPTH_PER_WORKER: Final[int] = 4


def _raise_worker_deadline(signum: int, frame: FrameType | None) -> None:
    """`SIGALRM` handler installed for the duration of one deadlined call."""
    raise WorkerDeadline("Worker deadline exceeded.")


def _call_with_deadline[R](
    fn: Callable[..., R],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    timeout: float | None,
) -> R:
    """Run `fn` in the worker process, under `timeout` seconds if it can be.

    Module-level and not a closure because this is the callable that gets
    pickled and sent across; a closure could not be.

    The previous handler and timer are restored rather than cleared. Workers are
    reused, so leaving a live `ITIMER_REAL` behind would fire during a *later*
    call — a timeout attributed to whichever unlucky request came next, which is
    close to unfindable from a log.
    """
    if timeout is None or not CAN_ENFORCE_WORKER_DEADLINE:
        return fn(*args, **kwargs)

    previous_handler = signal.signal(signal.SIGALRM, _raise_worker_deadline)
    signal.setitimer(signal.ITIMER_REAL, timeout)
    try:
        return fn(*args, **kwargs)
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def _worker_initializer() -> None:
    """Runs once per worker process, before it takes any work.

    Ignoring `SIGINT` is the point. A Ctrl-C in the foreground hits the whole
    process *group*, so every worker would raise `KeyboardInterrupt` in the
    middle of whatever it held, the pool would break, and the parent's orderly
    shutdown would be racing a pile of `BrokenProcessPool` errors on its way
    out. Letting the parent be the only thing that reacts to the interrupt
    means shutdown runs once, in the right order, and the workers exit when
    their queue closes.
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def ensure_offloadable(fn: Callable[..., Any]) -> None:
    """Raise `NotOffloadableError` unless `fn` can cross a process boundary.

    Checks the *callable* only, never the arguments. Arguments can be
    arbitrarily large and pickling them here to find out would double the cost
    of every call to catch a much rarer mistake; an unpicklable argument still
    fails, just with the executor's own message.

    The callable is where the mistake actually happens, and it is nearly always
    the same one: a lambda, a closure, or a function defined inside another
    function. Pickle stores a function *by qualified name* — it never sends the
    code — so anything the child cannot look up by name is unusable, no matter
    how simple it is.
    """
    try:
        pickle.dumps(fn)
    except Exception as exc:  # noqa: BLE001 - pickle raises several unrelated types
        name = getattr(fn, "__qualname__", None) or repr(fn)
        raise NotOffloadableError(
            f"{name} cannot be sent to a worker "
            f"process ({type(exc).__name__}: {exc}). Pickle resolves functions "
            f"by qualified name, so the target must be a module-level function "
            f"or a method of a picklable object — not a lambda, a closure, or a "
            f"function defined inside another function."
        ) from exc


def default_workers() -> int:
    """A worker count that respects a container CPU limit.

    `os.cpu_count()` reports the *machine's* cores, which in a container is
    almost never what the process may use: a pod limited to 2 CPUs on a 64-core
    node still sees 64, and a pool sized from that spawns 64 interpreters to
    fight over two cores' worth of quota. `os.process_cpu_count()` (3.13+) and
    `os.sched_getaffinity` both report the real allowance, so they are
    preferred where available.

    One is subtracted to leave the event loop a core of its own — the point of
    offloading is that the loop stays responsive, and saturating every core
    with workers takes that back. Never returns less than 1.
    """
    count: int | None = None

    process_cpu_count = getattr(os, "process_cpu_count", None)
    if process_cpu_count is not None:  # pragma: no cover - 3.13+ only
        count = process_cpu_count()
    elif hasattr(os, "sched_getaffinity"):
        count = len(os.sched_getaffinity(0))

    if not count:
        count = os.cpu_count() or 1

    return max(1, count - 1)


def supported_start_methods() -> tuple[str, ...]:
    """Start methods usable here, safest first, with `fork` excluded.

    Exposed so a deployment can check its configured method rather than find
    out at the first offload. `fork` is filtered out even where the platform
    offers it; `CpuPool` refuses it for the reasons in the module docstring.
    """
    available = multiprocessing.get_all_start_methods()
    order = ("forkserver", "spawn")
    return tuple(method for method in order if method in available)


class CpuPool:
    """A process pool sized for one server process, safe to use from a loop.

    Not a `ProcessPoolExecutor` subclass. The executor's contract is
    synchronous `submit`/`map` and this deliberately exposes neither: every
    entry point is a coroutine, so there is no way to accidentally call the
    blocking one from the event loop, which is the mistake this class exists to
    prevent.

    One pool per *server worker*, built from the lifespan. Under
    `uvicorn --workers 4` there are four of these, each with its own children,
    so the real process count is the product — see `docs/parallel-execution.md`
    for why `max_workers` should be sized against a container's CPU limit
    rather than the host's core count.
    """

    def __init__(
        self,
        *,
        max_workers: int | None = None,
        max_tasks_per_child: int | None = DEFAULT_MAX_TASKS_PER_CHILD,
        queue_depth_per_worker: int = DEFAULT_QUEUE_DEPTH_PER_WORKER,
        start_method: str = "spawn",
    ) -> None:
        if max_workers is not None and max_workers < 1:
            raise ValueError(f"max_workers must be at least 1, got {max_workers}.")
        if queue_depth_per_worker < 0:
            raise ValueError(
                f"queue_depth_per_worker must not be negative, "
                f"got {queue_depth_per_worker}."
            )
        if start_method == "fork":
            # Refused rather than warned about: the failure it produces is
            # corrupted database traffic and hung locks, which surface far from
            # here and look like anything but a start-method choice. See the
            # module docstring.
            raise ValueError(
                "The 'fork' start method is unsafe in an async server process: "
                "the child inherits open database and Redis sockets and any "
                "lock held by a thread that did not survive the fork. Use "
                "'spawn' or 'forkserver'."
            )

        self._max_workers = (
            max_workers if max_workers is not None else default_workers()
        )
        self._max_tasks_per_child = max_tasks_per_child
        self._start_method = start_method
        self._executor: ProcessPoolExecutor | None = None
        # Sized to workers plus a bounded queue, so admission is refused before
        # the executor's own unbounded queue can grow. Created here rather than
        # in `start` because `asyncio.Semaphore` no longer binds to a loop at
        # construction (3.10+), and a pool is always started and used on one.
        self._capacity = self._max_workers * (1 + queue_depth_per_worker)
        self._slots = asyncio.Semaphore(self._capacity)

    @property
    def max_workers(self) -> int:
        """Worker processes this pool runs."""
        return self._max_workers

    @property
    def capacity(self) -> int:
        """Calls that may be in flight or queued before admission is refused."""
        return self._capacity

    @property
    def running(self) -> bool:
        """Whether `start` has been called and `shutdown` has not."""
        return self._executor is not None

    @property
    def deadline_enforced(self) -> bool:
        """Whether a `timeout` actually stops the worker, or only the wait.

        False on a platform without `setitimer`, where a timed-out call keeps
        its slot until it finishes on its own. Exposed so a caller that cares —
        anything running attacker-influenced input — can refuse to start rather
        than discover it under load.
        """
        return CAN_ENFORCE_WORKER_DEADLINE

    def start(self) -> None:
        """Build the executor. Idempotent.

        Worker processes are *not* started here: `ProcessPoolExecutor` spawns
        them lazily, on the first submission. That is deliberate on the
        standard library's part and left alone — a deployment that never
        offloads anything pays nothing, and pre-warming would put several
        interpreter start-ups in front of the first health check.
        """
        if self._executor is not None:
            return

        self._executor = self._new_executor()
        logger.info(
            "cpu_pool.started",
            max_workers=self._max_workers,
            capacity=self._capacity,
            start_method=self._start_method,
            deadline_enforced=self.deadline_enforced,
        )

    def _new_executor(self) -> ProcessPoolExecutor:
        return ProcessPoolExecutor(
            max_workers=self._max_workers,
            mp_context=multiprocessing.get_context(self._start_method),
            initializer=_worker_initializer,
            max_tasks_per_child=self._max_tasks_per_child,
        )

    def with_timeout(self, seconds: float) -> DeadlinedCpuPool:
        """A view of this pool whose `run` carries a deadline.

            await pool.with_timeout(2.0).run(render, image, 320)

        The indirection is not decoration. `run` forwards its arguments to `fn`
        under a `ParamSpec`, and a `ParamSpec` signature may not carry
        parameters of its own *after* `*args` — so a `timeout=` keyword on `run`
        would cost the call-site argument checking that makes offloading typed
        at all. The same shape as `DeadlockRetryPolicy` in `src/locking/retry.py`,
        for the same reason: knobs on an object, forwarded arguments on the
        call.

        `seconds` is a budget for the *call*, not for the wait. It starts when a
        worker picks the work up, so time spent queued behind other calls does
        not count against it; queue time is bounded separately, by admission
        control. See the module docstring for why there is no second timer in
        the parent racing this one.
        """
        if seconds <= 0:
            raise ValueError(f"timeout must be positive, got {seconds}.")
        return DeadlinedCpuPool(self, seconds)

    async def run[**P, R](
        self, fn: Callable[P, R], *args: P.args, **kwargs: P.kwargs
    ) -> R:
        """Run `fn(*args, **kwargs)` in a worker process and return its result.

        No deadline: the call runs to completion. Use `with_timeout` for one.

        Typed with a `ParamSpec`, so a wrong argument list is a type error at
        the call site rather than an unpickling failure in a child process.

        Exceptions raised by `fn` propagate unchanged, with the worker's
        traceback attached as the cause. Nothing here converts them: a
        `ValueError` from a parser is the caller's to interpret, and wrapping it
        would only hide it.
        """
        return await self._submit(fn, args, kwargs, None)

    async def _submit[R](
        self,
        fn: Callable[..., R],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        timeout: float | None,
    ) -> R:
        """The single path both `run` and `DeadlinedCpuPool.run` take.

        Untyped in its arguments by necessity — a `ParamSpec` cannot survive
        being packed into a tuple — which is exactly why the two public entry
        points above exist to put it back.
        """
        executor = self._executor
        if executor is None:
            raise CpuPoolUnavailableError(
                "CPU pool is not running. It is started from the application "
                "lifespan; a test that uses it directly must call `start`."
            )

        ensure_offloadable(fn)

        if self._slots.locked():
            # `locked()` is true only when the count is zero, so this refuses
            # exactly the call that would have had to wait. Checked instead of
            # a zero-timeout acquire because a semaphore has no such form, and
            # `wait_for(..., 0)` still yields to the loop — which lets a burst
            # queue up behind the admission check meant to stop it.
            raise CpuPoolOverloadedError(
                f"CPU pool is at capacity ({self._capacity} in flight or queued).",
                details={"capacity": self._capacity, "max_workers": self._max_workers},
            )

        await self._slots.acquire()

        try:
            future = executor.submit(_call_with_deadline, fn, args, kwargs, timeout)
        except BrokenExecutor as exc:
            self._slots.release()
            self._replace_broken_executor(executor)
            raise CpuPoolUnavailableError(
                "CPU pool worker processes died and the pool is being replaced."
            ) from exc
        except RuntimeError as exc:
            # Raised by `submit` after `shutdown` — reachable when the lifespan
            # tears the pool down while a request is still resolving.
            self._slots.release()
            raise CpuPoolUnavailableError("CPU pool is shutting down.") from exc

        wrapped = asyncio.wrap_future(future)
        # The slot is released when the *work* ends, not when this coroutine
        # stops waiting for it. That distinction only bites in the fallback
        # regime below, where the coroutine can walk away from a call that is
        # still running — releasing the slot there would let the pool admit
        # more work than it has workers to run.
        wrapped.add_done_callback(lambda _: self._slots.release())

        try:
            if timeout is None or self.deadline_enforced:
                # The worker owns the deadline and always answers: a result, the
                # callable's exception, or `WorkerDeadline`. Adding a second
                # timer here would only race it, and would lose — see the module
                # docstring.
                return await wrapped
            return await asyncio.wait_for(asyncio.shield(wrapped), timeout)
        except WorkerDeadline as exc:
            raise CpuTaskTimeoutError(
                f"CPU-bound task exceeded its {timeout}s deadline.",
                details={"timeout_seconds": timeout, "enforced_by": "worker"},
            ) from exc
        except TimeoutError as exc:
            logger.warning(
                "cpu_pool.wait_timeout",
                timeout_seconds=timeout,
                detail=(
                    "This platform cannot interrupt a worker, so the call is "
                    "still running and holds its slot until it finishes."
                ),
            )
            raise CpuTaskTimeoutError(
                f"CPU-bound task exceeded its {timeout}s deadline.",
                details={"timeout_seconds": timeout, "enforced_by": "wait"},
            ) from exc
        except BrokenExecutor as exc:
            self._replace_broken_executor(executor)
            raise CpuPoolUnavailableError(
                "CPU pool worker processes died and the pool is being replaced."
            ) from exc

    def _replace_broken_executor(self, broken: ProcessPoolExecutor) -> None:
        """Swap a broken executor for a fresh one.

        A `ProcessPoolExecutor` whose worker was killed — the OOM killer is the
        realistic case — fails *every* subsequent submission with
        `BrokenProcessPool` forever. There is no reset, so the only recovery is
        a new executor, and without one a single OOM turns into an endpoint
        that is down until the pod restarts.

        Guarded on identity so that several requests failing on the same break
        replace it once. `shutdown(wait=False)` because this may run on the
        event loop thread and joining dead children there would block it.
        """
        if self._executor is not broken:
            return

        logger.error(
            "cpu_pool.broken",
            detail=(
                "A worker process died (an OOM kill is the usual cause). "
                "Replacing the executor; in-flight calls are lost."
            ),
        )
        self._executor = self._new_executor()
        broken.shutdown(wait=False)

    async def shutdown(self, *, wait: bool = True) -> None:
        """Stop the pool and let its workers exit. Idempotent.

        `wait=True` blocks until in-flight calls finish, which is what an
        orderly shutdown wants and why this is a coroutine: the join happens in
        a thread so the event loop keeps serving whatever else is draining
        alongside it. A synchronous `shutdown(wait=True)` on the loop thread
        would stall every other request for as long as the slowest offloaded
        call.
        """
        executor = self._executor
        if executor is None:
            return

        self._executor = None
        await asyncio.to_thread(executor.shutdown, wait)
        logger.info("cpu_pool.shutdown", waited=wait)


class DeadlinedCpuPool:
    """A `CpuPool` plus a deadline, returned by `CpuPool.with_timeout`.

    Deliberately tiny and deliberately not a pool: it owns no processes, holds
    no slots, and exists only so that `run` can stay a pure `ParamSpec`
    signature while the timeout rides on the object. Cheap enough to build per
    call, which is how it is meant to be used.
    """

    __slots__ = ("_pool", "_timeout")

    def __init__(self, pool: CpuPool, timeout: float) -> None:
        self._pool = pool
        self._timeout = timeout

    @property
    def timeout(self) -> float:
        """The deadline, in seconds, applied to each call through this view."""
        return self._timeout

    async def run[**P, R](
        self, fn: Callable[P, R], *args: P.args, **kwargs: P.kwargs
    ) -> R:
        """Run `fn(*args, **kwargs)` in a worker, under this view's deadline.

        Raises `CpuTaskTimeoutError` when it overruns.
        `details["enforced_by"]` distinguishes the two regimes, which differ in
        the way that matters operationally: `worker` means the deadline
        interrupted the call and the slot came back, `wait` means the platform
        could not interrupt it and the slot is still occupied.
        """
        return await self._pool._submit(fn, args, kwargs, self._timeout)
