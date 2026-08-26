"""Nested time budgets: clamping, attribution, and what is *not* a deadline.

Several of these assert on `asyncio` itself rather than on this module —
`TestTheProblemBeingSolved` — for the reason `tests/test_parallel_io.py` does:
the justification for `deadline()` is that per-call timeouts do not compose,
and a docstring claiming that is worth much less than a test that fails if it
ever stops being true.

Durations are small but real. Everything here is measured against the loop's
own clock, and a fake clock would have to be injected into `asyncio.timeout_at`
as well to prove anything — at which point the test would be exercising the
fake rather than the interaction with the timer that is the whole subject.
"""

from __future__ import annotations

import asyncio
import importlib
from contextvars import ContextVar, Token

import pytest

from src.structured.deadline import (
    Deadline,
    clamp_to_deadline,
    current_deadline,
    deadline,
)
from src.structured.errors import DeadlineExceeded

# Long enough that scheduling jitter cannot make an assertion flap, short
# enough that the file stays quick.
TICK = 0.05


class TestTheProblemBeingSolved:
    """The `asyncio` behaviour `deadline()` exists because of."""

    async def test_per_call_timeouts_do_not_add_up_to_a_budget(self) -> None:
        """Three calls of 'at most one tick' take three ticks, not one."""
        loop = asyncio.get_running_loop()
        started = loop.time()

        for _ in range(3):
            async with asyncio.timeout(TICK * 4):
                await asyncio.sleep(TICK)

        assert loop.time() - started >= TICK * 3

    async def test_asyncio_timeout_cannot_say_which_scope_expired(self) -> None:
        """Nested `asyncio.timeout` raises a `TimeoutError` naming nothing."""
        with pytest.raises(TimeoutError) as caught:
            async with asyncio.timeout(TICK):
                async with asyncio.timeout(TICK * 10):
                    await asyncio.sleep(TICK * 30)

        assert caught.value.args == ()


class TestDeadlineScope:
    async def test_body_within_budget_is_untouched(self) -> None:
        async with deadline(TICK * 10, name="request"):
            await asyncio.sleep(TICK)

    async def test_expiry_raises_deadline_exceeded_naming_the_scope(self) -> None:
        with pytest.raises(DeadlineExceeded) as caught:
            async with deadline(TICK, name="request"):
                await asyncio.sleep(TICK * 30)

        assert caught.value.scope == "request"
        assert caught.value.seconds == TICK
        assert caught.value.status_code == 504
        assert caught.value.error_code == "DEADLINE_EXCEEDED"

    async def test_deadline_exceeded_is_not_a_timeout_error(self) -> None:
        """The distinction retry loops and upstream handlers depend on."""
        assert not issubclass(DeadlineExceeded, TimeoutError)

        with pytest.raises(DeadlineExceeded):
            try:
                async with deadline(TICK, name="request"):
                    await asyncio.sleep(TICK * 30)
            except TimeoutError:  # pragma: no cover - must not be reached
                pytest.fail("DeadlineExceeded was caught as a TimeoutError")

    async def test_body_raising_its_own_timeout_error_is_passed_through(self) -> None:
        """A slow socket is not a spent budget, even inside a live scope."""
        with pytest.raises(TimeoutError) as caught:
            async with deadline(TICK * 30, name="request"):
                raise TimeoutError("upstream read timed out")

        assert not isinstance(caught.value, DeadlineExceeded)

    async def test_body_exception_propagates_unchanged(self) -> None:
        with pytest.raises(ValueError, match="boom"):
            async with deadline(TICK * 10, name="request"):
                raise ValueError("boom")

    @pytest.mark.parametrize("seconds", [0.0, -1.0])
    async def test_non_positive_budget_is_rejected(self, seconds: float) -> None:
        with pytest.raises(ValueError, match="must be positive"):
            async with deadline(seconds, name="request"):  # pragma: no cover
                pass


class TestNesting:
    async def test_inner_scope_cannot_extend_the_enclosing_budget(self) -> None:
        """The clamp. An inner 30-tick scope inside a 1-tick one still expires."""
        loop = asyncio.get_running_loop()
        started = loop.time()

        with pytest.raises(DeadlineExceeded) as caught:
            async with deadline(TICK * 2, name="request"):
                async with deadline(TICK * 60, name="report"):
                    await asyncio.sleep(TICK * 120)

        # Named for the scope that owns the instant, not the innermost one.
        assert caught.value.scope == "request"
        assert loop.time() - started < TICK * 30

    async def test_a_clamped_scope_reports_the_enclosing_budget(self) -> None:
        with pytest.raises(DeadlineExceeded) as caught:
            async with deadline(TICK * 2, name="request"):
                async with deadline(TICK * 60, name="report"):
                    await asyncio.sleep(TICK * 120)

        assert caught.value.seconds == TICK * 2

    async def test_a_genuinely_shorter_inner_scope_owns_its_expiry(self) -> None:
        async with deadline(TICK * 60, name="request"):
            with pytest.raises(DeadlineExceeded) as caught:
                async with deadline(TICK, name="stripe"):
                    await asyncio.sleep(TICK * 30)

        assert caught.value.scope == "stripe"

    async def test_inner_expiry_leaves_the_enclosing_scope_usable(self) -> None:
        """Catching the inner failure must not have cancelled the outer scope."""
        async with deadline(TICK * 60, name="request"):
            with pytest.raises(DeadlineExceeded):
                async with deadline(TICK, name="stripe"):
                    await asyncio.sleep(TICK * 30)
            await asyncio.sleep(0)
            remaining = current_deadline()
            assert remaining is not None
            assert remaining.name == "request"


class TestCurrentDeadline:
    async def test_none_outside_every_scope(self) -> None:
        assert current_deadline() is None

    async def test_names_the_innermost_owning_scope(self) -> None:
        async with deadline(TICK * 60, name="request"):
            first = current_deadline()
            async with deadline(TICK * 2, name="stripe"):
                second = current_deadline()
            third = current_deadline()

        assert first is not None and first.name == "request"
        assert second is not None and second.name == "stripe"
        assert third is not None and third.name == "request"

    async def test_a_clamped_scope_does_not_replace_the_enclosing_one(self) -> None:
        async with deadline(TICK * 2, name="request"):
            async with deadline(TICK * 60, name="report"):
                inner = current_deadline()

        assert inner is not None
        assert inner.name == "request"

    async def test_reset_after_the_block_even_when_it_raises(self) -> None:
        with pytest.raises(ValueError):
            async with deadline(TICK * 10, name="request"):
                raise ValueError

        assert current_deadline() is None

    async def test_a_child_task_inherits_the_budget_it_was_started_under(
        self,
    ) -> None:
        seen: list[str | None] = []

        async def observe() -> None:
            found = current_deadline()
            seen.append(None if found is None else found.name)

        async with deadline(TICK * 60, name="request"):
            inside = asyncio.create_task(observe())
            await inside
        outside = asyncio.create_task(observe())
        await outside

        assert seen == ["request", None]


class TestDeadlineValue:
    async def test_remaining_counts_down_and_goes_negative(self) -> None:
        loop = asyncio.get_running_loop()
        value = Deadline(name="request", expires_at=loop.time() + TICK, budget=TICK)

        assert 0 < value.remaining() <= TICK
        assert value.expired is False

        await asyncio.sleep(TICK * 2)

        assert value.remaining() < 0
        assert value.expired is True

    async def test_expires_at_is_loop_time_not_monotonic(self) -> None:
        """The uvloop trap: the two clocks have unrelated epochs.

        Asserting the deadline is built from `loop.time()` is asserting that it
        can be handed to `asyncio.timeout_at`, which is the only thing anyone
        does with it.
        """
        loop = asyncio.get_running_loop()
        async with deadline(TICK * 10, name="request") as value:
            assert abs(value.expires_at - (loop.time() + TICK * 10)) < TICK

    def test_is_frozen(self) -> None:
        value = Deadline(name="request", expires_at=1.0, budget=1.0)
        with pytest.raises(AttributeError):
            value.expires_at = 2.0  # type: ignore[misc]


class TestClampToDeadline:
    async def test_passes_the_request_through_outside_any_scope(self) -> None:
        assert clamp_to_deadline(5.0) == 5.0

    async def test_returns_the_remainder_when_it_is_smaller(self) -> None:
        async with deadline(TICK, name="request"):
            assert clamp_to_deadline(30.0) <= TICK

    async def test_returns_the_request_when_it_is_smaller(self) -> None:
        async with deadline(TICK * 60, name="request"):
            assert clamp_to_deadline(TICK) == TICK

    async def test_raises_rather_than_returning_zero_on_a_spent_budget(self) -> None:
        """Most clients read a non-positive timeout as 'wait forever'."""
        loop = asyncio.get_running_loop()
        spent = Deadline(name="request", expires_at=loop.time() - 1.0, budget=TICK)
        token = _set_current(spent)
        try:
            with pytest.raises(DeadlineExceeded) as caught:
                clamp_to_deadline(5.0)
        finally:
            _reset_current(token)

        assert caught.value.scope == "request"
        assert caught.value.seconds == TICK

    @pytest.mark.parametrize("seconds", [0.0, -1.0])
    async def test_non_positive_request_is_rejected(self, seconds: float) -> None:
        with pytest.raises(ValueError, match="must be positive"):
            clamp_to_deadline(seconds)


# `clamp_to_deadline` on an already-spent budget needs a deadline in the past,
# and there is no way to reach one through the public interface: a real scope
# that has expired has also already raised, because the timer fires at the same
# instant. So these reach the module's `ContextVar` directly, rather than
# exporting a setter that only a test would ever call.
#
# `importlib` rather than `from src.structured import deadline`, which binds
# the re-exported *function* of that name from the package `__init__`.
_DEADLINE_MODULE = importlib.import_module("src.structured.deadline")


def _set_current(value: Deadline) -> Token[Deadline | None]:
    variable: ContextVar[Deadline | None] = _DEADLINE_MODULE._current_deadline
    return variable.set(value)


def _reset_current(token: Token[Deadline | None]) -> None:
    _DEADLINE_MODULE._current_deadline.reset(token)
