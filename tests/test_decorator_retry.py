"""`@retry` — what gets retried, what does not, and how long it waits.

Nothing here sleeps. Both sleepers are injected and record the delays they were
asked for, so the backoff curve is asserted exactly and the suite stays fast
enough that nobody is tempted to delete these tests later.
"""

from __future__ import annotations

import asyncio
import inspect
import random

import pytest

from src.decorators import retry
from src.exceptions import BadRequestError, ConflictError


class RecordingSleeper:
    """Stands in for `asyncio.sleep`/`time.sleep` and remembers the delays."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    async def asleep(self, delay: float) -> None:
        self.delays.append(delay)

    def sleep(self, delay: float) -> None:
        self.delays.append(delay)


class Flaky:
    """Fails `failures` times, then returns `value`. Counts every call."""

    def __init__(self, failures: int, value: str = "ok") -> None:
        self.failures = failures
        self.value = value
        self.calls = 0

    async def __call__(self) -> str:
        self.calls += 1
        if self.calls <= self.failures:
            raise ConnectionError(f"attempt {self.calls}")
        return self.value


async def test_succeeds_after_transient_failures() -> None:
    sleeper = RecordingSleeper()
    flaky = Flaky(failures=2)

    @retry(attempts=3, on=ConnectionError, jitter=False, asleep=sleeper.asleep)
    async def call() -> str:
        return await flaky()

    assert await call() == "ok"
    assert flaky.calls == 3
    assert sleeper.delays == [0.1, 0.2]


async def test_first_call_wins_and_nothing_sleeps() -> None:
    sleeper = RecordingSleeper()

    @retry(attempts=5, asleep=sleeper.asleep)
    async def call() -> int:
        return 7

    assert await call() == 7
    assert sleeper.delays == []


async def test_exhausted_retries_re_raise_the_original_exception() -> None:
    """No wrapper type: the status code the app derives must survive retrying."""
    sleeper = RecordingSleeper()

    @retry(attempts=3, on=ConflictError, jitter=False, asleep=sleeper.asleep)
    async def call() -> None:
        raise ConflictError("row moved")

    with pytest.raises(ConflictError) as caught:
        await call()

    assert caught.value.status_code == 409
    assert len(sleeper.delays) == 2


async def test_an_unlisted_exception_is_not_retried() -> None:
    sleeper = RecordingSleeper()
    calls = 0

    @retry(attempts=4, on=ConnectionError, asleep=sleeper.asleep)
    async def call() -> None:
        nonlocal calls
        calls += 1
        raise TimeoutError("different family")

    with pytest.raises(TimeoutError):
        await call()

    assert calls == 1
    assert sleeper.delays == []


async def test_give_up_on_wins_over_on() -> None:
    """A 400 inside a retryable family is still a durable rejection."""
    sleeper = RecordingSleeper()
    calls = 0

    @retry(
        attempts=4,
        on=Exception,
        give_up_on=BadRequestError,
        asleep=sleeper.asleep,
    )
    async def call() -> None:
        nonlocal calls
        calls += 1
        raise BadRequestError("malformed")

    with pytest.raises(BadRequestError):
        await call()

    assert calls == 1


async def test_should_retry_predicate_has_the_final_say() -> None:
    sleeper = RecordingSleeper()
    seen: list[int] = []

    def only_503(exc: BaseException) -> bool:
        code = getattr(exc, "code", 0)
        seen.append(code)
        return code == 503

    class Upstream(Exception):
        def __init__(self, code: int) -> None:
            super().__init__(code)
            self.code = code

    @retry(
        attempts=3,
        on=Upstream,
        should_retry=only_503,
        jitter=False,
        asleep=sleeper.asleep,
    )
    async def call(code: int) -> None:
        raise Upstream(code)

    with pytest.raises(Upstream):
        await call(400)
    assert sleeper.delays == []

    with pytest.raises(Upstream):
        await call(503)
    assert len(sleeper.delays) == 2
    assert seen == [400, 503, 503]


async def test_cancellation_is_never_retried() -> None:
    """Even with `on=BaseException`, a cancelled call must stop immediately."""
    sleeper = RecordingSleeper()
    calls = 0

    @retry(attempts=5, on=BaseException, asleep=sleeper.asleep)
    async def call() -> None:
        nonlocal calls
        calls += 1
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await call()

    assert calls == 1
    assert sleeper.delays == []


async def test_backoff_is_capped_by_max_delay() -> None:
    sleeper = RecordingSleeper()

    @retry(
        attempts=6,
        on=ConnectionError,
        base_delay=1.0,
        max_delay=4.0,
        jitter=False,
        asleep=sleeper.asleep,
    )
    async def call() -> None:
        raise ConnectionError("still down")

    with pytest.raises(ConnectionError):
        await call()

    assert sleeper.delays == [1.0, 2.0, 4.0, 4.0, 4.0]


async def test_jitter_draws_from_zero_to_the_ceiling_and_is_seedable() -> None:
    sleeper = RecordingSleeper()

    @retry(
        attempts=4,
        on=ConnectionError,
        base_delay=1.0,
        max_delay=10.0,
        rng=random.Random(1234),
        asleep=sleeper.asleep,
    )
    async def call() -> None:
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        await call()

    expected = random.Random(1234)
    assert sleeper.delays == [
        expected.uniform(0.0, 1.0),
        expected.uniform(0.0, 2.0),
        expected.uniform(0.0, 4.0),
    ]


async def test_a_huge_attempt_count_does_not_overflow_the_backoff() -> None:
    """`0.1 * 2 ** 2000` raises OverflowError; the exponent cap prevents it."""
    sleeper = RecordingSleeper()

    @retry(
        attempts=200,
        on=ConnectionError,
        base_delay=0.1,
        max_delay=3.0,
        jitter=False,
        asleep=sleeper.asleep,
    )
    async def call() -> None:
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        await call()

    assert len(sleeper.delays) == 199
    assert sleeper.delays[-1] == 3.0


def test_sync_functions_retry_with_the_blocking_sleeper() -> None:
    sleeper = RecordingSleeper()
    calls = 0

    @retry(attempts=3, on=ValueError, jitter=False, sleep=sleeper.sleep)
    def call() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ValueError("not yet")
        return "done"

    assert call() == "done"
    assert calls == 3
    assert sleeper.delays == [0.1, 0.2]


async def test_arguments_are_passed_through_on_every_attempt() -> None:
    sleeper = RecordingSleeper()
    seen: list[tuple[int, str]] = []

    @retry(attempts=3, on=ValueError, jitter=False, asleep=sleeper.asleep)
    async def call(number: int, *, label: str) -> str:
        seen.append((number, label))
        if len(seen) < 3:
            raise ValueError("again")
        return f"{label}-{number}"

    assert await call(5, label="x") == "x-5"
    assert seen == [(5, "x"), (5, "x"), (5, "x")]


async def test_signature_is_preserved_at_runtime() -> None:
    async def work(value: int, *, flag: bool = False) -> str:
        return f"{value}{flag}"

    decorated = retry(attempts=2)(work)

    assert inspect.signature(decorated) == inspect.signature(work)
    assert decorated.__name__ == "work"
    assert inspect.iscoroutinefunction(decorated)


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"attempts": 0}, "attempts"),
        ({"base_delay": -1.0}, "base_delay"),
        ({"base_delay": 5.0, "max_delay": 1.0}, "max_delay"),
    ],
)
def test_an_unusable_policy_is_rejected_at_decoration(
    kwargs: dict[str, float], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        retry(**kwargs)  # type: ignore[arg-type]


def test_sync_retries_exhaust_and_re_raise_the_original() -> None:
    sleeper = RecordingSleeper()
    calls = 0

    @retry(attempts=3, on=ValueError, jitter=False, sleep=sleeper.sleep)
    def call() -> None:
        nonlocal calls
        calls += 1
        raise ValueError("never works")

    with pytest.raises(ValueError, match="never works"):
        call()

    assert calls == 3
    assert sleeper.delays == [0.1, 0.2]
