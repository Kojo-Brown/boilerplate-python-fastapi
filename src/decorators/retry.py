"""`@retry` — re-run a call that failed for a reason that might not recur.

Retrying is only safe for *transient* failures, and only for operations that
can be repeated without doubling their effect. Nothing here can check either
property, so both are the caller's to declare: `on` narrows which exceptions
are worth another attempt, and applying this to a non-idempotent write is a bug
the decorator cannot catch. The default `on=Exception` is broad enough to be a
poor production choice on purpose — narrow it at the decoration site.

Two decisions worth stating outright:

**The original exception propagates.** There is no `RetryError` wrapper. This
API turns an `AppException` into a status code by inspecting its type, so
wrapping a `ConflictError` in something else would quietly convert a 409 into a
500 after the third attempt. The last failure is re-raised unchanged; the fact
that attempts happened is in the logs, not in the exception type.

**Cancellation is never retried.** `asyncio.CancelledError` means the caller
stopped caring — a disconnected client, an expired timeout. Retrying through it
keeps work alive that nothing is waiting for and makes shutdown hang. It is
refused even when `on` is broad enough to match it.
"""

from __future__ import annotations

import asyncio
import functools
import random
import time
from collections.abc import Awaitable, Callable
from typing import Any, ParamSpec, TypeVar, overload

import structlog

from src.decorators.base import (
    DEFAULT_RNG,
    AsyncSleeper,
    SyncSleeper,
    backoff_delay,
    default_event_name,
    is_async_callable,
)

logger = structlog.get_logger(__name__)

P = ParamSpec("P")
R = TypeVar("R")

ExceptionTypes = type[BaseException] | tuple[type[BaseException], ...]

RetryPredicate = Callable[[BaseException], bool]


class RetryDecorator:
    """Applies a retry policy to one function. Returned by `retry()`."""

    def __init__(
        self,
        *,
        attempts: int,
        on: ExceptionTypes,
        give_up_on: ExceptionTypes,
        should_retry: RetryPredicate | None,
        base_delay: float,
        max_delay: float,
        jitter: bool,
        rng: random.Random,
        sleep: SyncSleeper,
        asleep: AsyncSleeper,
        event: str | None,
    ) -> None:
        if attempts < 1:
            raise ValueError("attempts must be at least 1.")
        if base_delay < 0:
            raise ValueError("base_delay must not be negative.")
        if max_delay < base_delay:
            raise ValueError("max_delay must be at least base_delay.")

        self._attempts = attempts
        self._on = on
        self._give_up_on = give_up_on
        self._should_retry = should_retry
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._jitter = jitter
        self._rng = rng
        self._sleep = sleep
        self._asleep = asleep
        self._event = event

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
                for attempt in range(1, self._attempts + 1):
                    try:
                        return await func(*args, **kwargs)
                    except BaseException as exc:
                        delay = self._delay_after(attempt, exc, event)
                        if delay is None:
                            raise
                        await self._asleep(delay)
                raise AssertionError("unreachable")  # pragma: no cover

            return async_wrapper

        @functools.wraps(func)
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            for attempt in range(1, self._attempts + 1):
                try:
                    return func(*args, **kwargs)
                except BaseException as exc:
                    delay = self._delay_after(attempt, exc, event)
                    if delay is None:
                        raise
                    self._sleep(delay)
            raise AssertionError("unreachable")  # pragma: no cover

        return sync_wrapper

    def _delay_after(
        self, attempt: int, exc: BaseException, event: str
    ) -> float | None:
        """Seconds to wait before attempt `attempt + 1`, or None to give up.

        Returning None rather than raising keeps the two wrappers to one
        `raise` each, so the traceback the caller sees is the original
        exception re-raised from its own `except` block and carries no frames
        from this method.
        """
        if not self._is_retryable_type(exc):
            logger.debug(
                f"{event}.retry_declined",
                attempt=attempt,
                error=type(exc).__name__,
            )
            return None

        # Exhaustion is checked before `should_retry` on purpose. The predicate
        # exists to influence a decision, and on the last attempt there is no
        # decision left — calling it anyway would run caller code (which may
        # touch a response body, or count something) for an answer that cannot
        # matter.
        if attempt >= self._attempts:
            logger.warning(
                f"{event}.retry_exhausted",
                attempts=self._attempts,
                error=type(exc).__name__,
            )
            return None

        if self._should_retry is not None and not self._should_retry(exc):
            logger.debug(
                f"{event}.retry_declined",
                attempt=attempt,
                error=type(exc).__name__,
            )
            return None

        delay = self._delay_for(attempt)
        logger.warning(
            f"{event}.retry_scheduled",
            attempt=attempt,
            of=self._attempts,
            delay_ms=round(delay * 1000, 3),
            error=type(exc).__name__,
        )
        return delay

    def _is_retryable_type(self, exc: BaseException) -> bool:
        """The type-only half of the decision. `should_retry` is applied later."""
        if isinstance(exc, asyncio.CancelledError):
            return False
        if isinstance(exc, self._give_up_on):
            return False
        return isinstance(exc, self._on)

    def _delay_for(self, attempt: int) -> float:
        """The wait after `attempt`. See `backoff_delay` for the policy."""
        return backoff_delay(
            attempt,
            base_delay=self._base_delay,
            max_delay=self._max_delay,
            jitter=self._jitter,
            rng=self._rng,
        )


def retry(
    *,
    attempts: int = 3,
    on: ExceptionTypes = Exception,
    give_up_on: ExceptionTypes = (),
    should_retry: RetryPredicate | None = None,
    base_delay: float = 0.1,
    max_delay: float = 5.0,
    jitter: bool = True,
    rng: random.Random = DEFAULT_RNG,
    sleep: SyncSleeper = time.sleep,
    asleep: AsyncSleeper = asyncio.sleep,
    event: str | None = None,
) -> RetryDecorator:
    """Retry the decorated call on transient failure, with backoff.

    Works on `async def` and plain `def` alike and preserves the signature of
    what it wraps. Note that the synchronous form sleeps with `time.sleep`: on
    a request path that blocks the event loop and the whole process stalls, so
    reserve it for Celery tasks, CLI entry points and startup checks. Inside
    the application, decorate the coroutine.

    Args:
        attempts: Total calls, not extra ones — `attempts=3` means one try and
            two retries. Must be at least 1.
        on: Exception types worth retrying. Narrow this: the default catches
            everything short of `BaseException`, which will happily retry a
            `TypeError` from a bug three times before surfacing it.
        give_up_on: Checked first and wins over `on`, for carving a durable
            failure out of a retryable family — `on=AppException`,
            `give_up_on=BadRequestError`.
        should_retry: Final say when the type alone is not enough, e.g.
            inspecting `exc.response.status_code` on an HTTP error. Only
            consulted for exceptions that already passed `on`/`give_up_on`,
            and never on the last attempt, where no retry is possible either
            way.
        base_delay: Seconds to wait after the first failure, before jitter.
        max_delay: Ceiling on the backoff, before jitter. Must be >= base_delay.
        jitter: Draw each wait uniformly from `[0, ceiling]` instead of using
            the ceiling. Leave on outside tests.
        rng: Jitter source. Pass a seeded `random.Random` to make delays
            reproducible in a test.
        sleep: Blocking sleep used by the synchronous wrapper.
        asleep: Awaitable sleep used by the async wrapper.
        event: Log event prefix. Defaults to `module.qualname` of the wrapped
            function; events are `<prefix>.retry_scheduled`,
            `.retry_exhausted` and `.retry_declined`.

    Raises:
        ValueError: The policy is unusable — non-positive `attempts`, a
            negative `base_delay`, or a `max_delay` below `base_delay`. Raised
            at decoration time, so a bad policy fails at import rather than on
            the first failure in production.

    Example:
        >>> @retry(attempts=4, on=httpx.TransportError, base_delay=0.2)
        ... async def fetch_rates() -> Rates:
        ...     ...
    """
    return RetryDecorator(
        attempts=attempts,
        on=on,
        give_up_on=give_up_on,
        should_retry=should_retry,
        base_delay=base_delay,
        max_delay=max_delay,
        jitter=jitter,
        rng=rng,
        sleep=sleep,
        asleep=asleep,
        event=event,
    )
