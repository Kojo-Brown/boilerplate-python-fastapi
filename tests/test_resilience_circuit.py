"""The state machine, driven against a clock rather than against a server."""

from __future__ import annotations

import pytest

from src.resilience.base import CircuitBreakerConfig, CircuitOpenError, CircuitState
from src.resilience.circuit import CircuitBreaker, CircuitBreakerRegistry


class FakeClock:
    """A monotonic clock a test advances by hand."""

    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


CONFIG = CircuitBreakerConfig(
    failure_threshold=3,
    window_size=5,
    reset_timeout=10.0,
    success_threshold=2,
    half_open_max_calls=1,
)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def breaker(clock: FakeClock) -> CircuitBreaker:
    return CircuitBreaker("https://api.test", config=CONFIG, clock=clock)


def state_of(breaker: CircuitBreaker) -> CircuitState:
    """`breaker.state`, behind a call.

    A property read twice in one test body is narrowed by mypy to whatever the
    first assertion proved, so the second — the one asserting the state has
    *changed* — reads as a non-overlapping comparison. Going through a function
    keeps the assertions honest without weakening either of them.
    """
    return breaker.state


def fail(breaker: CircuitBreaker, times: int = 1) -> None:
    for _ in range(times):
        breaker.acquire().failed()


def succeed(breaker: CircuitBreaker, times: int = 1) -> None:
    for _ in range(times):
        breaker.acquire().succeeded()


def test_a_new_breaker_is_closed_and_admits(breaker: CircuitBreaker) -> None:
    assert state_of(breaker) is CircuitState.CLOSED
    assert breaker.acquire().probe is False


def test_failures_below_the_threshold_do_not_open_it(breaker: CircuitBreaker) -> None:
    fail(breaker, 2)
    assert state_of(breaker) is CircuitState.CLOSED
    assert breaker.failures_in_window == 2


def test_the_threshold_opens_it(breaker: CircuitBreaker) -> None:
    fail(breaker, 3)
    assert state_of(breaker) is CircuitState.OPEN


def test_a_success_does_not_reset_the_count(breaker: CircuitBreaker) -> None:
    """The window is what makes a 50% error rate trip the breaker at all.

    Under a consecutive-failure rule this sequence — fail, succeed, fail,
    succeed, fail — never opens the circuit, however obviously broken the
    dependency looks on a dashboard.
    """
    fail(breaker)
    succeed(breaker)
    fail(breaker)
    succeed(breaker)
    assert state_of(breaker) is CircuitState.CLOSED
    fail(breaker)
    assert state_of(breaker) is CircuitState.OPEN


def test_failures_that_age_out_of_the_window_stop_counting(
    clock: FakeClock,
) -> None:
    config = CircuitBreakerConfig(
        failure_threshold=3, window_size=4, reset_timeout=10.0
    )
    breaker = CircuitBreaker("https://api.test", config=config, clock=clock)

    fail(breaker, 2)
    # Four successes push both failures out of a window that holds four.
    succeed(breaker, 4)
    assert breaker.failures_in_window == 0
    fail(breaker, 2)
    assert state_of(breaker) is CircuitState.CLOSED


def test_an_open_circuit_refuses_with_the_time_left(
    breaker: CircuitBreaker, clock: FakeClock
) -> None:
    fail(breaker, 3)
    clock.advance(4.0)

    with pytest.raises(CircuitOpenError) as excinfo:
        breaker.acquire()

    assert excinfo.value.origin == "https://api.test"
    assert excinfo.value.retry_after == pytest.approx(6.0)


def test_the_reset_timeout_moves_it_to_half_open(
    breaker: CircuitBreaker, clock: FakeClock
) -> None:
    fail(breaker, 3)
    clock.advance(10.0)
    assert state_of(breaker) is CircuitState.HALF_OPEN


def test_half_open_admits_one_probe_and_refuses_the_rest(
    breaker: CircuitBreaker, clock: FakeClock
) -> None:
    fail(breaker, 3)
    clock.advance(10.0)

    call = breaker.acquire()
    assert call.probe is True

    with pytest.raises(CircuitOpenError) as excinfo:
        breaker.acquire()
    # The probe is deciding right now, so "come back in thirty seconds" would
    # be wrong in the direction that costs the caller an answer it could have.
    assert excinfo.value.retry_after == 0.0


def test_a_settled_probe_frees_its_slot(
    breaker: CircuitBreaker, clock: FakeClock
) -> None:
    fail(breaker, 3)
    clock.advance(10.0)

    breaker.acquire().succeeded()
    assert breaker.acquire().probe is True


def test_a_released_probe_frees_its_slot_without_an_opinion(
    breaker: CircuitBreaker, clock: FakeClock
) -> None:
    """Cancellation must not strand a half-open circuit with no slots left."""
    fail(breaker, 3)
    clock.advance(10.0)

    breaker.acquire().release()

    assert state_of(breaker) is CircuitState.HALF_OPEN
    assert breaker.acquire().probe is True


def test_a_probe_failure_reopens_and_restarts_the_timer(
    breaker: CircuitBreaker, clock: FakeClock
) -> None:
    fail(breaker, 3)
    clock.advance(10.0)
    breaker.acquire().failed()

    assert state_of(breaker) is CircuitState.OPEN
    clock.advance(9.0)
    assert state_of(breaker) is CircuitState.OPEN
    clock.advance(1.0)
    assert state_of(breaker) is CircuitState.HALF_OPEN


def test_one_success_is_not_enough_to_close_it(
    breaker: CircuitBreaker, clock: FakeClock
) -> None:
    """A restarting dependency can answer once and fall over on the next."""
    fail(breaker, 3)
    clock.advance(10.0)
    breaker.acquire().succeeded()
    assert state_of(breaker) is CircuitState.HALF_OPEN


def test_the_success_threshold_closes_it(
    breaker: CircuitBreaker, clock: FakeClock
) -> None:
    fail(breaker, 3)
    clock.advance(10.0)
    succeed(breaker, 2)
    assert state_of(breaker) is CircuitState.CLOSED


def test_closing_forgets_the_failures_that_opened_it(
    breaker: CircuitBreaker, clock: FakeClock
) -> None:
    """Otherwise the first failure after recovery re-opens the circuit."""
    fail(breaker, 3)
    clock.advance(10.0)
    succeed(breaker, 2)

    assert breaker.failures_in_window == 0
    fail(breaker)
    assert state_of(breaker) is CircuitState.CLOSED


def test_a_settled_call_cannot_be_settled_twice(breaker: CircuitBreaker) -> None:
    call = breaker.acquire()
    call.failed()
    call.failed()
    call.succeeded()
    call.release()
    assert breaker.failures_in_window == 1


def test_an_outcome_from_a_superseded_generation_is_ignored(
    clock: FakeClock,
) -> None:
    """A probe still in flight when another probe re-opens the circuit comes
    back with an answer about a circuit that has moved on. Counting it would
    let a stale success close a circuit that was just re-opened."""
    config = CircuitBreakerConfig(
        failure_threshold=1,
        window_size=5,
        reset_timeout=10.0,
        success_threshold=1,
        half_open_max_calls=2,
    )
    breaker = CircuitBreaker("https://api.test", config=config, clock=clock)

    fail(breaker)
    clock.advance(10.0)
    slow_probe = breaker.acquire()
    quick_probe = breaker.acquire()

    quick_probe.failed()
    assert state_of(breaker) is CircuitState.OPEN

    slow_probe.succeeded()
    assert state_of(breaker) is CircuitState.OPEN


def test_a_closed_call_that_lands_after_a_trip_is_ignored(
    breaker: CircuitBreaker,
) -> None:
    in_flight = breaker.acquire()
    fail(breaker, 3)
    assert state_of(breaker) is CircuitState.OPEN

    in_flight.succeeded()
    assert state_of(breaker) is CircuitState.OPEN


def test_reset_forces_it_closed(breaker: CircuitBreaker) -> None:
    fail(breaker, 3)
    breaker.reset()

    assert state_of(breaker) is CircuitState.CLOSED
    assert breaker.failures_in_window == 0


def test_reset_invalidates_calls_that_were_already_in_flight(
    breaker: CircuitBreaker,
) -> None:
    in_flight = breaker.acquire()
    breaker.reset()
    in_flight.failed()
    assert breaker.failures_in_window == 0


def test_the_registry_keeps_one_breaker_per_origin() -> None:
    registry = CircuitBreakerRegistry(config=CONFIG, clock=FakeClock())

    first = registry.get("https://a.test")
    assert registry.get("https://a.test") is first
    assert registry.get("https://b.test") is not first


def test_one_dependency_failing_does_not_stop_calls_to_another() -> None:
    registry = CircuitBreakerRegistry(config=CONFIG, clock=FakeClock())
    fail(registry.get("https://a.test"), 3)

    assert state_of(registry.get("https://a.test")) is CircuitState.OPEN
    assert state_of(registry.get("https://b.test")) is CircuitState.CLOSED
    registry.get("https://b.test").acquire()


def test_the_registry_reports_a_snapshot_and_can_reset_everything() -> None:
    registry = CircuitBreakerRegistry(config=CONFIG, clock=FakeClock())
    fail(registry.get("https://a.test"), 3)
    registry.get("https://b.test")

    assert registry.states() == {
        "https://a.test": CircuitState.OPEN,
        "https://b.test": CircuitState.CLOSED,
    }

    registry.reset()
    assert registry.states() == {
        "https://a.test": CircuitState.CLOSED,
        "https://b.test": CircuitState.CLOSED,
    }


def test_the_origin_is_reported_back(breaker: CircuitBreaker) -> None:
    assert breaker.origin == "https://api.test"


def test_a_breaker_built_without_a_config_uses_the_defaults() -> None:
    breaker = CircuitBreaker("https://api.test")
    fail(breaker, CircuitBreakerConfig().failure_threshold - 1)
    assert state_of(breaker) is CircuitState.CLOSED
    fail(breaker)
    assert state_of(breaker) is CircuitState.OPEN
