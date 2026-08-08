"""The three decorators stacked, in the order `docs/decorators.md` recommends.

Each decorator is exercised on its own elsewhere; this asserts the claim the
docs actually make about putting them together — a cache hit skips the retry
loop entirely, a failure is retried but never stored, and the duration `@timed`
reports covers all of it.
"""

from __future__ import annotations

import functools
import inspect

import pytest
from structlog.testing import capture_logs

from src.decorators import cached, retry, timed
from src.decorators.base import is_async_callable
from tests.test_decorator_cached import FakeClock


class RecordingSleeper:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


async def test_stack_caches_hits_retries_failures_and_times_the_whole_thing() -> None:
    sleeper = RecordingSleeper()
    calls = 0

    @timed(event="rates.fetch")
    @retry(attempts=3, on=ConnectionError, jitter=False, asleep=sleeper)
    @cached(ttl=60, clock=FakeClock())
    async def fetch_rates(base: str) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("upstream down")
        return f"rates-{base}"

    with capture_logs() as logs:
        assert await fetch_rates("usd") == "rates-usd"
        assert await fetch_rates("usd") == "rates-usd"

    # One failure, one success, then a cache hit that never reaches the body.
    assert calls == 2
    assert sleeper.delays == [0.1]

    # `@timed` is outermost, so it logged once per *call*, not once per attempt.
    durations = [entry for entry in logs if entry["event"] == "rates.fetch.duration"]
    assert len(durations) == 2
    assert all(entry["outcome"] == "ok" for entry in durations)


async def test_the_failed_attempt_is_not_left_in_the_cache() -> None:
    sleeper = RecordingSleeper()

    async def always_fails(base: str) -> str:
        raise ConnectionError("upstream down")

    # `cache_info` lives on the object `@cached` returns, and the `@retry`
    # wrapper above it is a plain function, so the stacked name cannot reach it.
    # Build the stack by hand when a test — or an invalidation hook — needs the
    # cache API.
    inner = cached(ttl=60, clock=FakeClock())(always_fails)
    decorated = retry(attempts=2, on=ConnectionError, jitter=False, asleep=sleeper)(
        inner
    )

    with pytest.raises(ConnectionError):
        await decorated("usd")

    assert inner.cache_info().size == 0
    assert sleeper.delays == [0.1]


async def test_a_cached_function_is_recognised_as_awaitable_by_the_stack() -> None:
    """The bug this guards: `@retry` taking the sync branch over `@cached`.

    An `AsyncCachedFunction` is a callable object, not a coroutine function, so
    a plain `inspect.iscoroutinefunction` check would hand the coroutine back
    un-awaited — the call could not fail inside the `try`, and nothing would
    ever be retried.
    """
    calls = 0
    sleeper = RecordingSleeper()

    @retry(attempts=3, on=ConnectionError, jitter=False, asleep=sleeper)
    @cached(ttl=60, clock=FakeClock())
    async def flaky() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ConnectionError("down")
        return "up"

    assert await flaky() == "up"
    assert calls == 3
    assert sleeper.delays == [0.1, 0.2]


async def test_a_cached_coroutine_reports_itself_as_a_coroutine_function() -> None:
    """FastAPI reads this to decide between awaiting and a thread pool."""

    @cached(ttl=60, clock=FakeClock())
    async def load() -> int:
        return 1

    assert inspect.iscoroutinefunction(load)


async def test_a_partial_over_an_async_callable_object_is_seen_as_async() -> None:
    """The case the stdlib check misses on its own.

    `inspect.iscoroutinefunction` unwraps `functools.partial`, but what it finds
    underneath still has to be a coroutine function or carry the coroutine
    marker. An ordinary object with an `async def __call__` — someone's own
    client class, say — is neither, so `is_async_callable` unwraps the partial
    itself and then asks about `__call__`.
    """

    class Client:
        async def __call__(self, key: str) -> str:
            return key

    bound = functools.partial(Client(), "a")

    assert inspect.iscoroutinefunction(bound) is False
    assert is_async_callable(bound) is True

    sleeper = RecordingSleeper()
    decorated = retry(attempts=2, on=ValueError, asleep=sleeper)(bound)
    assert await decorated() == "a"
