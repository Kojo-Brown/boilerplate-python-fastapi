"""`@timed` — emit a structured duration for every call.

The decorator exists so that "how long does this take in production" is a
question the logs already answer, rather than one that needs a new deploy. It
is deliberately not a metrics client: it emits a structlog event with a
`duration_ms` field, and whatever ships logs decides whether that becomes a
histogram. That keeps the boilerplate free of a Prometheus/OTel dependency
while still putting the number somewhere it can be aggregated.

Timing that only records successes measures the wrong thing — the pathological
call is usually the one that timed out — so failures are timed too, and
cancellation is reported as its own outcome rather than as an error. Nothing is
swallowed: the exception propagates exactly as it was raised.
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Awaitable, Callable
from typing import Any, ParamSpec, TypeVar, overload

import structlog

from src.decorators.base import (
    DEFAULT_TIMER,
    Clock,
    default_event_name,
    duration_ms,
    is_async_callable,
)

logger = structlog.get_logger(__name__)

P = ParamSpec("P")
R = TypeVar("R")

DEFAULT_EVENT_SUFFIX = "duration"


class TimedDecorator:
    """Applies timing instrumentation to one function. Returned by `timed()`."""

    def __init__(
        self,
        *,
        event: str | None,
        slow_after: float | None,
        timer: Clock,
    ) -> None:
        if slow_after is not None and slow_after <= 0:
            raise ValueError("slow_after must be greater than 0 seconds.")
        self._event = event
        self._slow_after = slow_after
        self._timer = timer

    @overload
    def __call__(
        self, func: Callable[P, Awaitable[R]]
    ) -> Callable[P, Awaitable[R]]: ...

    @overload
    def __call__(self, func: Callable[P, R]) -> Callable[P, R]: ...

    # The overloads above are the checked contract; `Any` here only keeps the
    # implementation compatible with both of them.
    def __call__(self, func: Callable[P, Any]) -> Callable[P, Any]:
        event = self._event or default_event_name(func)

        if is_async_callable(func):

            @functools.wraps(func)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
                start = self._timer()
                try:
                    result = await func(*args, **kwargs)
                except BaseException as exc:
                    self._log_failure(event, self._timer() - start, exc)
                    raise
                self._log_success(event, self._timer() - start)
                return result

            return async_wrapper

        @functools.wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            start = self._timer()
            try:
                result = func(*args, **kwargs)
            except BaseException as exc:
                self._log_failure(event, self._timer() - start, exc)
                raise
            self._log_success(event, self._timer() - start)
            return result

        return sync_wrapper

    def _log_success(self, event: str, elapsed: float) -> None:
        if self._slow_after is not None and elapsed >= self._slow_after:
            logger.warning(
                f"{event}.{DEFAULT_EVENT_SUFFIX}",
                duration_ms=duration_ms(elapsed),
                outcome="ok",
                slow=True,
                slow_after_ms=duration_ms(self._slow_after),
            )
            return
        logger.debug(
            f"{event}.{DEFAULT_EVENT_SUFFIX}",
            duration_ms=duration_ms(elapsed),
            outcome="ok",
        )

    def _log_failure(self, event: str, elapsed: float, exc: BaseException) -> None:
        # A cancelled call is not a fault — the caller went away, or a timeout
        # fired — and counting it as an error makes every deploy look like an
        # incident. It still gets a duration, because "how long before we gave
        # up" is exactly the number a timeout investigation needs.
        outcome = "cancelled" if isinstance(exc, asyncio.CancelledError) else "error"
        logger.warning(
            f"{event}.{DEFAULT_EVENT_SUFFIX}",
            duration_ms=duration_ms(elapsed),
            outcome=outcome,
            error=type(exc).__name__,
        )


def timed(
    *,
    event: str | None = None,
    slow_after: float | None = None,
    timer: Clock = DEFAULT_TIMER,
) -> TimedDecorator:
    """Log how long each call to the decorated function took.

    Works on `async def` and plain `def` alike; the returned wrapper keeps the
    signature of what it wraps, so a mistyped argument is still caught at the
    call site rather than inside the decorator.

    Args:
        event: Log event name. Defaults to `module.qualname` of the wrapped
            function, with `.duration` appended — `src.repositories.user.get.duration`.
        slow_after: Seconds above which a *successful* call is logged at
            warning instead of debug, with `slow=True`. Leave unset and every
            success stays at debug. Failures are always warnings.
        timer: Source of elapsed time. `time.perf_counter` by default; pass a
            counter a test controls to assert on an exact `duration_ms`.

    Example:
        >>> @timed(event="db.users.by_email", slow_after=0.25)
        ... async def get_by_email(email: str) -> User | None:
        ...     ...
    """
    return TimedDecorator(event=event, slow_after=slow_after, timer=timer)
