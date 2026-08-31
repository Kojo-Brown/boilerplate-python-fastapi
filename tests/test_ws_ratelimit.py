"""The per-connection budget, over a clock the test moves by hand.

Every timing assertion here advances a list-backed clock rather than sleeping.
A limiter tested with `asyncio.sleep` is a limiter tested at whatever precision
the machine happened to have that morning, and the refill arithmetic is exactly
the part worth pinning exactly.
"""

from __future__ import annotations

import pytest

from src.ws.ratelimit import Decision, RateLimiter, TokenBucket


class ManualClock:
    """A monotonic clock that only moves when a test says so."""

    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


class TestTokenBucket:
    def test_it_starts_full(self) -> None:
        """A connection's first message must not be the one that is refused."""
        bucket = TokenBucket(capacity=3, rate=1)

        assert bucket.tokens == 3

    def test_a_burst_is_admitted_up_to_capacity(self, clock: ManualClock) -> None:
        bucket = TokenBucket(capacity=3, rate=1, clock=clock)

        assert [bucket.take() for _ in range(4)] == [True, True, True, False]

    def test_a_refusal_spends_nothing(self, clock: ManualClock) -> None:
        """Charging for rejected attempts would let a flood deepen its own hole."""
        bucket = TokenBucket(capacity=2, rate=1, clock=clock)
        bucket.take(2)

        for _ in range(50):
            assert bucket.take() is False

        clock.advance(1.0)
        assert bucket.take() is True

    def test_tokens_refill_continuously_rather_than_on_a_boundary(
        self, clock: ManualClock
    ) -> None:
        """No window edge to line up against and spend two budgets across."""
        bucket = TokenBucket(capacity=10, rate=2, clock=clock)
        bucket.take(10)

        clock.advance(0.5)
        assert bucket.tokens == pytest.approx(1.0)
        clock.advance(0.25)
        assert bucket.tokens == pytest.approx(1.5)

    def test_refill_never_exceeds_capacity(self, clock: ManualClock) -> None:
        """An idle connection banks a burst, not an afternoon's worth of them."""
        bucket = TokenBucket(capacity=5, rate=1, clock=clock)
        bucket.take(5)

        clock.advance(3600)

        assert bucket.tokens == 5

    def test_a_clock_that_goes_backwards_does_not_remove_tokens(self) -> None:
        """`time.monotonic` will not, but an injected clock might.

        Subtracting on a negative elapsed would make the limiter *stricter*
        whenever someone's test double rewound — a limiter that gets tighter
        under a condition nobody is thinking about.
        """
        clock = ManualClock()
        bucket = TokenBucket(capacity=5, rate=1, clock=clock)
        bucket.take(2)

        clock.advance(-100)

        assert bucket.tokens == pytest.approx(3.0)

    def test_retry_after_is_zero_when_the_tokens_are_there(self) -> None:
        assert TokenBucket(capacity=2, rate=1).retry_after() == 0.0

    def test_retry_after_is_the_wait_that_would_help(self, clock: ManualClock) -> None:
        bucket = TokenBucket(capacity=4, rate=2, clock=clock)
        bucket.take(4)

        assert bucket.retry_after(3) == pytest.approx(1.5)

    def test_a_cost_above_capacity_never_becomes_affordable(self) -> None:
        """`inf` rather than a large number: the remedy is a smaller message.

        Reporting a finite wait here would have a well-behaved client sleeping
        and retrying the identical message forever.
        """
        bucket = TokenBucket(capacity=10, rate=1)

        assert bucket.retry_after(11) == float("inf")
        assert bucket.take(11) is False

    @pytest.mark.parametrize(("capacity", "rate"), [(0, 1), (-1, 1), (1, 0), (1, -1)])
    def test_a_bound_that_is_not_positive_is_refused(
        self, capacity: float, rate: float
    ) -> None:
        with pytest.raises(ValueError, match="must be positive"):
            TokenBucket(capacity=capacity, rate=rate)


def limiter(clock: ManualClock, **overrides: float) -> RateLimiter:
    settings: dict[str, float] = {
        "messages_per_second": 2,
        "message_burst": 3,
        "bytes_per_second": 100,
        "byte_burst": 200,
        "max_consecutive_violations": 3,
    }
    settings.update(overrides)
    return RateLimiter(
        messages_per_second=settings["messages_per_second"],
        message_burst=int(settings["message_burst"]),
        bytes_per_second=settings["bytes_per_second"],
        byte_burst=int(settings["byte_burst"]),
        max_consecutive_violations=int(settings["max_consecutive_violations"]),
        clock=clock,
    )


class TestRateLimiter:
    def test_messages_within_both_budgets_are_admitted(
        self, clock: ManualClock
    ) -> None:
        limits = limiter(clock)

        assert all(limits.offer(10).allowed for _ in range(3))

    def test_the_message_budget_binds_on_a_flood_of_small_frames(
        self, clock: ManualClock
    ) -> None:
        """A byte limit alone is defeated by a flood of `{}`."""
        limits = limiter(clock)
        for _ in range(3):
            limits.offer(1)

        decision = limits.offer(1)

        assert not decision.allowed
        assert decision.retry_after == pytest.approx(0.5)

    def test_the_byte_budget_binds_on_a_few_large_frames(
        self, clock: ManualClock
    ) -> None:
        """A message limit alone is defeated by one-megabyte messages."""
        limits = limiter(clock)

        assert limits.offer(200).allowed
        decision = limits.offer(200)

        assert not decision.allowed
        assert decision.retry_after == pytest.approx(2.0)

    def test_a_rejected_message_spends_neither_budget(self, clock: ManualClock) -> None:
        """A message that fails on volume must not consume the message budget.

        Otherwise a client held under the byte limit is also being charged for
        the attempts, and drains an allowance it never got to use.
        """
        limits = limiter(clock, byte_burst=200, bytes_per_second=1)
        limits.offer(200)
        for _ in range(5):
            assert not limits.offer(200).allowed

        clock.advance(200)

        # Three message tokens are still there: the rejections took none.
        assert [limits.offer(1).allowed for _ in range(4)] == [
            True,
            True,
            True,
            False,
        ]

    def test_consecutive_violations_end_the_connection(
        self, clock: ManualClock
    ) -> None:
        limits = limiter(clock, max_consecutive_violations=3)
        for _ in range(3):
            limits.offer(10)

        outcomes = [limits.offer(10).disconnect for _ in range(3)]

        assert outcomes == [False, False, True]

    def test_an_admitted_message_forgives_the_violations_before_it(
        self, clock: ManualClock
    ) -> None:
        """An application that occasionally bumps the ceiling is not an abuser."""
        limits = limiter(clock, max_consecutive_violations=3)
        for _ in range(3):
            limits.offer(10)
        limits.offer(10)
        limits.offer(10)
        assert limits.violations == 2

        clock.advance(5)
        assert limits.offer(10).allowed

        assert limits.violations == 0

    def test_an_allowed_decision_never_asks_for_a_disconnect(
        self, clock: ManualClock
    ) -> None:
        assert limiter(clock).offer(1) == Decision(allowed=True, retry_after=0.0)

    def test_byte_capacity_is_published_so_a_caller_can_check_its_ceiling(
        self, clock: ManualClock
    ) -> None:
        """`Connection` refuses to be built when a legal message cannot fit."""
        assert limiter(clock, byte_burst=200).byte_capacity == 200

    def test_a_violation_budget_below_one_is_refused(self, clock: ManualClock) -> None:
        with pytest.raises(ValueError, match="max_consecutive_violations"):
            limiter(clock, max_consecutive_violations=0)
