"""Picklable workloads for the `CpuPool` tests.

A separate module because every one of these has to be importable *by name* in
a freshly spawned interpreter. Defined inside a test function they would be
unpicklable; defined in the test module they would be importable, but the child
would then re-import that module and everything it pulls in — the app, the
settings, the fixtures — for each worker.
"""

from __future__ import annotations

import os
import signal
import time
from typing import Any, NoReturn


def double(value: int) -> int:
    return value * 2


def add(left: int, right: int = 0, *, offset: int = 0) -> int:
    """Exercises positional, default and keyword-only argument passing."""
    return left + right + offset


def echo(value: Any) -> Any:
    return value


def current_pid() -> int:
    """The worker's PID — used to prove the work left the parent process."""
    return os.getpid()


def raise_value_error(message: str = "workload failed") -> NoReturn:
    raise ValueError(message)


def spin(seconds: float) -> str:
    """Burn CPU in pure Python for `seconds`.

    A busy loop rather than `time.sleep`, because sleeping releases the GIL and
    would be interruptible in ways real compute is not — and because a deadline
    that only works against sleeping code proves nothing about the case this
    exists for.

    Interruptible by `SIGALRM` precisely because it is pure Python: the handler
    runs at a bytecode boundary, and this loop has one every few instructions.
    """
    started = time.monotonic()
    while time.monotonic() - started < seconds:
        pass
    return "finished"


def spin_and_pid(seconds: float) -> int:
    """Occupy a worker for `seconds`, then say which one it was.

    Two of these running at once must report different PIDs — the check that a
    two-worker pool really overlaps work, which a wall-clock assertion can only
    hint at on a loaded CI runner.
    """
    spin(seconds)
    return os.getpid()


def spin_with_alarm_blocked(seconds: float) -> str:
    """Spin with `SIGALRM` blocked, so the worker's own deadline cannot fire.

    A faithful stand-in for the case the module docstring calls out and cannot
    fix: a C extension holding the GIL through a long call, where the signal
    handler never reaches a bytecode boundary. Blocking the signal reproduces
    "the worker will not stop" deterministically, which a `numpy` loop would
    only do by luck of timing.

    Used to exercise the parent-side fallback — the regime Windows is always in
    — on a POSIX box, so that path is not left to run for the first time in
    somebody's deployment.
    """
    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
    try:
        return spin(seconds)
    finally:
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGALRM})


def spin_swallowing_exceptions(seconds: float) -> str:
    """Spin inside a broad `except Exception`, as defensive code so often is.

    The point of the test that uses this: `WorkerDeadline` derives from
    `BaseException`, so this handler does not catch it. Were it an `Exception`,
    this workload would return "swallowed" and the deadline would be advisory.
    """
    try:
        return spin(seconds)
    except Exception:  # noqa: BLE001 - the whole point of this workload
        return "swallowed"


def alarm_is_disarmed() -> bool:
    """Whether this worker has a live `ITIMER_REAL` left over from a past call.

    Workers are reused, so a call that armed a timer and did not disarm it would
    fire during somebody else's work. Run after a deadlined call to prove the
    timer was restored.
    """
    remaining, _interval = signal.getitimer(signal.ITIMER_REAL)
    return remaining == 0.0


def sigint_is_ignored() -> bool:
    """Whether the worker initializer's `SIGINT` disposition took effect."""
    return signal.getsignal(signal.SIGINT) is signal.SIG_IGN


def kill_own_process() -> NoReturn:
    """Terminate this worker abruptly, as an OOM kill would.

    `SIGKILL` rather than raising: an exception travels back to the parent as a
    result, which is the opposite of what this needs to reproduce. The pool must
    observe a worker that simply stopped existing, which is what breaks a
    `ProcessPoolExecutor` permanently.
    """
    os.kill(os.getpid(), signal.SIGKILL)
    raise AssertionError("unreachable")  # pragma: no cover


class Unpicklable:
    """Rejected by pickle, to exercise an unpicklable *result*."""

    def __reduce__(self) -> NoReturn:
        raise TypeError("Unpicklable cannot be pickled")


def return_unpicklable() -> Unpicklable:
    return Unpicklable()
