"""`@timed` — what gets logged, and what is left alone.

The decorator's whole output is a log event, so the assertions capture
structlog rather than inspect return values. `structlog.testing.capture_logs`
bypasses the configured processors, which is the point: these tests describe
the event and its fields, not how the app happens to render them today.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable

import pytest
from structlog.testing import capture_logs

from src.decorators import timed


class FakeTimer:
    """A `Clock` that advances by a fixed amount on each reading pair.

    `@timed` reads the timer exactly twice per call — before and after — so
    handing back a scripted sequence makes `duration_ms` exact instead of
    approximately-some-microseconds.
    """

    def __init__(self, *readings: float) -> None:
        self._readings = list(readings)

    def __call__(self) -> float:
        return self._readings.pop(0)


def timer_for(*durations: float) -> Callable[[], float]:
    """Timer whose n-th call pair spans `durations[n]` seconds."""
    readings: list[float] = []
    now = 0.0
    for duration in durations:
        readings.extend((now, now + duration))
        now += duration + 1
    return FakeTimer(*readings)


async def test_async_success_logs_duration_at_debug() -> None:
    @timed(event="work", timer=timer_for(0.25))
    async def work(value: int) -> int:
        return value * 2

    with capture_logs() as logs:
        assert await work(21) == 42

    assert logs == [
        {
            "event": "work.duration",
            "log_level": "debug",
            "duration_ms": 250.0,
            "outcome": "ok",
        }
    ]


def test_sync_success_logs_duration_at_debug() -> None:
    @timed(event="work", timer=timer_for(0.001))
    def work(value: int) -> int:
        return value + 1

    with capture_logs() as logs:
        assert work(1) == 2

    assert logs[0]["duration_ms"] == 1.0
    assert logs[0]["log_level"] == "debug"


async def test_default_event_name_is_module_and_qualname() -> None:
    @timed()
    async def work() -> None:
        return None

    with capture_logs() as logs:
        await work()

    assert logs[0]["event"] == (
        "tests.test_decorator_timed."
        "test_default_event_name_is_module_and_qualname.<locals>.work.duration"
    )


async def test_slow_call_is_warned_with_the_threshold_attached() -> None:
    @timed(event="work", slow_after=0.1, timer=timer_for(0.3))
    async def work() -> None:
        return None

    with capture_logs() as logs:
        await work()

    assert logs[0]["log_level"] == "warning"
    assert logs[0]["slow"] is True
    assert logs[0]["slow_after_ms"] == 100.0
    assert logs[0]["outcome"] == "ok"


async def test_fast_call_stays_at_debug_when_a_threshold_is_set() -> None:
    @timed(event="work", slow_after=0.5, timer=timer_for(0.01))
    async def work() -> None:
        return None

    with capture_logs() as logs:
        await work()

    assert logs[0]["log_level"] == "debug"
    assert "slow" not in logs[0]


async def test_failure_is_timed_logged_and_re_raised_unchanged() -> None:
    sentinel = ValueError("boom")

    @timed(event="work", timer=timer_for(0.05))
    async def work() -> None:
        raise sentinel

    with capture_logs() as logs:
        with pytest.raises(ValueError) as caught:
            await work()

    assert caught.value is sentinel
    assert logs[0] == {
        "event": "work.duration",
        "log_level": "warning",
        "duration_ms": 50.0,
        "outcome": "error",
        "error": "ValueError",
    }


async def test_cancellation_is_reported_as_its_own_outcome() -> None:
    """A cancelled call is not an error, and must not be counted as one."""
    started = asyncio.Event()

    @timed(event="work", timer=timer_for(2.0))
    async def work() -> None:
        started.set()
        await asyncio.sleep(3600)

    with capture_logs() as logs:
        task = asyncio.create_task(work())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert logs[0]["outcome"] == "cancelled"
    assert logs[0]["error"] == "CancelledError"


async def test_signature_is_preserved_at_runtime() -> None:
    async def work(value: int, *, flag: bool = False) -> str:
        return f"{value}{flag}"

    decorated = timed(event="work")(work)

    assert inspect.signature(decorated) == inspect.signature(work)
    assert decorated.__name__ == "work"
    assert decorated.__wrapped__ is work  # type: ignore[attr-defined]
    assert inspect.iscoroutinefunction(decorated)


def test_non_positive_slow_after_is_rejected_at_decoration() -> None:
    with pytest.raises(ValueError, match="slow_after"):
        timed(slow_after=0)


def test_sync_failure_is_timed_logged_and_re_raised() -> None:
    @timed(event="work", timer=timer_for(0.2))
    def work() -> None:
        raise KeyError("missing")

    with capture_logs() as logs:
        with pytest.raises(KeyError):
            work()

    assert logs[0]["outcome"] == "error"
    assert logs[0]["error"] == "KeyError"
    assert logs[0]["duration_ms"] == 200.0
