"""Failures that come from a *scope* ending, rather than from the work in it.

Split from the three implementation modules for the same reason
`src/parallel/errors.py` is: `deadline.py` and `cancel.py` both raise
`DeadlineExceeded`, and a caller catching it should not have to know which one
produced it.
"""

from __future__ import annotations

from src.exceptions import AppException


class DeadlineExceeded(AppException):
    """A scope ran out of time before the work inside it finished.

    504 rather than 500, matching `CpuTaskTimeoutError`: the request was
    well-formed and the server simply did not produce an answer in time.

    **Deliberately not a subclass of `TimeoutError`.** That inheritance is
    tempting — every `except TimeoutError:` already written would keep working
    — and it is exactly wrong here. `TimeoutError` is what a socket read, an
    `asyncio.wait_for` and an `httpx` call raise, so a handler that catches it
    to retry an upstream would silently catch *the enclosing budget expiring*
    and retry inside a scope that has no time left. The two mean opposite
    things: one says "that call failed, try another", the other says "stop".
    Keeping them distinct is what makes the second one un-retryable by
    accident, and it is why `src/decorators/retry.py` needs no special case.

    `scope` is the name of the deadline that actually expired, which is not
    always the innermost one: a nested `deadline()` clamped by an enclosing
    budget does not arm a timer of its own, so the enclosing scope is what
    fires and what gets named. That attribution is the whole reason this
    carries a name at all — "the request budget ran out" and "the Stripe call
    was slow" call for different fixes, and a bare `TimeoutError` cannot tell
    them apart.
    """

    status_code = 504
    error_code = "DEADLINE_EXCEEDED"

    def __init__(self, scope: str, seconds: float, details: object = None) -> None:
        self.scope = scope
        self.seconds = seconds
        super().__init__(
            f"Deadline {scope!r} of {seconds:g}s exceeded",
            details,
        )


class TaskScopeClosedError(RuntimeError):
    """`start_soon` was called on a `TaskScope` that is not open.

    A `RuntimeError` rather than an `AppException`, matching
    `NotOffloadableError` in `src/parallel/errors.py`: what starts a background
    task is chosen by this codebase and never by a client, so starting one
    after its scope has closed is a programming error deserving a 500 and a
    traceback, not a status code a user is asked to interpret.

    Worth its own type because the alternative is `asyncio.TaskGroup`'s own
    message — "TaskGroup … is finished" — which names neither the scope nor the
    task somebody tried to start in it, and which is the single most likely way
    to reintroduce an unowned task: code that catches the `RuntimeError`,
    shrugs, and falls back to `asyncio.create_task`.
    """
