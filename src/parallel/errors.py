"""Failure modes of offloaded and fanned-out work.

Split from the two implementation modules because `cpu.py` raises things
`io.py` also needs to talk about, and because one of them — `WorkerDeadline` —
has to be importable in a *worker* process that has no reason to import an
executor.
"""

from __future__ import annotations

from src.exceptions import AppException


class WorkerDeadline(BaseException):
    """Raised inside a worker process when its per-call deadline elapses.

    `BaseException`, not `Exception`, and that is the whole point of it. This is
    delivered by a `SIGALRM` handler, so it surfaces at whatever bytecode the
    worker happened to be executing — which is very often inside somebody's
    `try: ... except Exception:` block. Deriving from `Exception` would let
    ordinary defensive error handling swallow the deadline, the call would run
    to completion anyway, and the timeout would be a suggestion. Deriving from
    `BaseException` means only code that catches `BaseException` can absorb it,
    and `concurrent.futures`' worker loop does exactly that — which is how it
    gets sent back to the parent instead of killing the process.

    Never raised in the parent: `CpuPool.run` translates it to
    `CpuTaskTimeoutError` so callers have one exception to catch regardless of
    which side of the process boundary ran out of time.

    Kept free of any executor import so a worker paying the cost of unpickling
    it is not also paying for `concurrent.futures`.
    """


class CpuTaskTimeoutError(AppException):
    """A CPU-bound call did not finish within its deadline.

    504 rather than 500: the call was well-formed and the server simply did not
    produce an answer in time, which is what a gateway timeout means. Retrying
    it unchanged is reasonable — the same input on a less loaded pool may well
    fit — so this is deliberately not a 400.

    Raised for both halves of the timeout: the worker-side `SIGALRM` deadline
    (the usual one, and the only one that reclaims the worker) and the
    parent-side wait. See `CpuPool.run` for why there are two.
    """

    status_code = 504
    error_code = "CPU_TASK_TIMEOUT"

    def __init__(
        self, message: str = "CPU-bound task timed out", details: object = None
    ) -> None:
        super().__init__(message, details)


class CpuPoolOverloadedError(AppException):
    """The pool's queue is full and this call was not admitted.

    503 with a `Retry-After`, and the reason it exists at all is that the
    alternative is worse than an error. `ProcessPoolExecutor`'s work queue is
    unbounded: submissions that cannot run yet sit in the parent's memory as
    *pickled payloads*, so an endpoint that offloads a megabyte of input under
    a traffic spike grows the parent's heap without limit while every one of
    those requests is still waiting on a socket that has probably already timed
    out. Refusing at the door sheds the load where it can still be reported.

    Distinct from `CpuTaskTimeoutError`: nothing ran, so nothing was wasted and
    a retry costs the pool nothing it has not already spent.
    """

    status_code = 503
    error_code = "CPU_POOL_OVERLOADED"
    headers = {"Retry-After": "1"}

    def __init__(
        self, message: str = "CPU pool is at capacity", details: object = None
    ) -> None:
        super().__init__(message, details)


class CpuPoolUnavailableError(AppException):
    """The pool is not running, or its worker processes died under it.

    503 for both, because both are recoverable and neither is the caller's
    fault. "Not running" is a wiring mistake (nothing called `start`, or the
    lifespan already tore it down). "Died under it" is
    `BrokenProcessPool`, which `concurrent.futures` raises for every subsequent
    submission once a worker has been killed — most often by the OOM killer,
    which is a realistic outcome for a pool doing memory-hungry work. A broken
    executor never recovers on its own, so `CpuPool` replaces it; this error is
    what the requests caught mid-break receive.
    """

    status_code = 503
    error_code = "CPU_POOL_UNAVAILABLE"
    headers = {"Retry-After": "1"}

    def __init__(
        self, message: str = "CPU pool unavailable", details: object = None
    ) -> None:
        super().__init__(message, details)


class NotOffloadableError(ValueError):
    """The callable cannot cross a process boundary.

    A `ValueError` rather than an `AppException`, matching
    `LockNameInvalidError` in `src/distributed_lock/base.py`: what gets
    offloaded is chosen by this codebase and never by a client, so a lambda
    here is a programming error that deserves a 500 and a stack trace pointing
    at the call site — not a 4xx telling a user to fix something they did not
    send.

    Worth its own type because the failure it replaces is genuinely hard to
    read. `ProcessPoolExecutor` pickles the payload on a background queue-
    management thread, so passing a lambda surfaces as an `AttributeError` or
    `PicklingError` attached to the future, with a traceback through
    `concurrent.futures.process` and no mention of the call site that made the
    mistake.
    """
