"""Time budgets that nest, clamp, and say which one ran out.

## The bug this exists to prevent

A handler with a five second budget makes three upstream calls, each given a
five second timeout. Nothing is wrong with any one of those numbers and the
handler can take fifteen seconds. Per-call timeouts do not compose: they bound
a call, and what a client waiting on a socket cares about is the *request*.

`deadline()` fixes that by making the innermost scope unable to outlive the
outermost one. A nested scope asking for longer than the enclosing budget has
left silently gets the remainder instead — a timeout is a *ceiling*, and a
nested one that could raise the ceiling would not be a budget at all.

## Attribution, and why nesting does not double-arm

When a nested scope is clamped, it does not start a timer of its own. There
would be two timers set to the same instant, both firing, both cancelling the
same task, and which one won the race would decide the error message — for a
distinction that matters, since "the request budget ran out" and "the payment
gateway was slow" have different fixes. The enclosing scope owns the instant,
so the enclosing scope arms it and the enclosing scope names it in
`DeadlineExceeded.scope`. An inner scope arms a timer only when it is genuinely
shorter, and then it is genuinely the one that expired.

## Loop time, not wall time, and not `time.monotonic`

`Deadline.expires_at` is `loop.time()`, and that is load-bearing rather than
incidental. `asyncio.timeout_at` compares against the running loop's clock, and
under `uvloop` — which this application runs on in production — that clock is
libuv's, whose epoch is *not* `time.monotonic()`'s. Building the instant from
`time.monotonic()` and handing it to `timeout_at` therefore produces a deadline
wrong by the difference between two arbitrary epochs: usually far in the past,
so the scope expires immediately. It fails on uvloop and passes on the default
event loop, which is the worst way for it to fail.

The consequence is that a `Deadline` belongs to the loop that created it, and
`remaining()` needs a running loop. Both are true of everything else the
deadline touches, so neither is a real constraint.

## What this is not

It is not a retry budget and it is not a rate limit. It also does not bound
anything that never awaits: a scope around a tight CPU loop expires and nothing
happens until the loop yields, which is what `src/parallel/cpu.py` exists for.

See `docs/structured-concurrency.md`.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass

import structlog

from src.structured.errors import DeadlineExceeded

logger = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class Deadline:
    """An instant on the running loop's clock, and the scope that owns it."""

    #: The scope whose expiry this instant represents. Not necessarily the
    #: innermost scope in scope — see the module docstring on clamping.
    name: str

    #: `asyncio.get_running_loop().time()`, never `time.monotonic()`.
    expires_at: float

    #: The number the owning scope was opened with. Kept so an error can say
    #: how large the budget was rather than only that it is gone, and so a
    #: clamped inner scope reports the enclosing scope's figure instead of the
    #: one it asked for and did not get.
    budget: float

    def remaining(self) -> float:
        """Seconds left, negative once the deadline has passed.

        Negative rather than clamped at zero on purpose: "half a second over"
        and "four minutes over" are very different things to find in a log, and
        a caller that wants the clamped form can say `max(0.0, ...)`.
        """
        return self.expires_at - asyncio.get_running_loop().time()

    @property
    def expired(self) -> bool:
        return self.remaining() <= 0.0


_current_deadline: ContextVar[Deadline | None] = ContextVar(
    "structured_current_deadline", default=None
)


def current_deadline() -> Deadline | None:
    """The innermost effective deadline, or `None` outside every scope.

    A `ContextVar`, so it follows the request rather than the module: a task
    started inside a scope inherits the budget as of the moment it was created,
    and a task started before it does not. That is the behaviour to want —
    inheritance by lexical accident would give a relay tick started at boot the
    budget of whichever request happened to be running.
    """
    return _current_deadline.get()


def clamp_to_deadline(seconds: float) -> float:
    """How long a call may take: `seconds`, or the enclosing budget's remainder.

    This is the one line that has to appear at every boundary where a timeout
    is handed to something else — an `httpx` client, a lock acquisition, a
    subscriber — because those timeouts are numbers, not scopes, and a number
    cannot know what is left. Passing a flat five seconds to an upstream when
    the request has 300ms of budget spends 4.7 seconds producing an answer
    nobody is waiting for, on a connection nobody has closed yet.

    Raises:
        DeadlineExceeded: the enclosing budget is already spent, so the call
            has no time to run in. Raised here rather than returning zero,
            because most clients read a zero or negative timeout as "no
            timeout" and would wait forever at precisely the wrong moment.
        ValueError: `seconds` is not positive.
    """
    if seconds <= 0:
        raise ValueError(f"seconds must be positive, got {seconds}.")
    enclosing = _current_deadline.get()
    if enclosing is None:
        return seconds
    remaining = enclosing.remaining()
    if remaining <= 0.0:
        raise DeadlineExceeded(enclosing.name, enclosing.budget)
    return min(seconds, remaining)


@asynccontextmanager
async def deadline(seconds: float, *, name: str) -> AsyncIterator[Deadline]:
    """Bound everything inside the block by `seconds`, or by what is left.

    Nesting is the point:

        async with deadline(30, name="request"):
            async with deadline(10, name="stripe"):   # arms its own timer
                ...
            async with deadline(60, name="report"):   # clamped to "request"
                ...

    The second inner scope cannot extend the request past 30 seconds, and when
    the request's budget runs out inside it the error names `"request"`.

    `name` is required rather than defaulted because it is the entire value of
    the error: a `DeadlineExceeded` with no name is a `TimeoutError` with extra
    steps.

    Raises:
        DeadlineExceeded: this scope, or the enclosing one that clamped it, ran
            out of time. A `TimeoutError` raised by the body itself is passed
            through untouched — see the guard on `timer.expired()`, which is
            what keeps a slow socket from being reported as a spent budget.
        ValueError: `seconds` is not positive.
    """
    if seconds <= 0:
        raise ValueError(f"seconds must be positive, got {seconds}.")

    loop = asyncio.get_running_loop()
    own_expiry = loop.time() + seconds
    enclosing = _current_deadline.get()
    # `<=` and not `<`: on a tie the enclosing scope keeps ownership, so two
    # timers are never armed for the same instant. See the module docstring.
    if enclosing is not None and enclosing.expires_at <= own_expiry:
        clamped = True
        effective = enclosing
    else:
        clamped = False
        effective = Deadline(name=name, expires_at=own_expiry, budget=seconds)

    token = _current_deadline.set(effective)
    try:
        if clamped:
            # No timer: the enclosing scope's is already set to this instant or
            # earlier, and it is the one that will fire and name itself.
            yield effective
            return

        timer = asyncio.timeout_at(own_expiry)
        try:
            async with timer:
                yield effective
        except TimeoutError as exc:
            # `asyncio.timeout` converts *its own* cancellation into
            # `TimeoutError`, but the body may have raised one of its own from
            # a socket or a `wait_for`. Only the former is this scope
            # expiring, and reporting the latter as a spent budget would send
            # somebody looking for a slow request instead of a slow upstream.
            if not timer.expired():
                raise
            logger.info(
                "structured.deadline_exceeded",
                scope=name,
                seconds=seconds,
            )
            raise DeadlineExceeded(name, seconds) from exc
    finally:
        _current_deadline.reset(token)


__all__ = [
    "Deadline",
    "clamp_to_deadline",
    "current_deadline",
    "deadline",
]
