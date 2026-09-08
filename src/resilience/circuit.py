"""The breaker itself: a three-state machine per origin.

```
            failures in window >= threshold
   CLOSED ─────────────────────────────────▶ OPEN
      ▲                                        │
      │ success_threshold probes succeed       │ reset_timeout elapses
      │                                        ▼
      └──────────────── HALF_OPEN ◀────────────┘
                            │
                            └── a probe fails ──▶ OPEN (timer restarted)
```

## Why none of this awaits

Every method here is synchronous, and that is deliberate rather than
incidental: a state machine with an `await` in the middle of it is a state
machine that can be observed halfway through a transition by whatever runs at
that suspension point. Keeping the transitions atomic under asyncio means no
lock, no lock ordering, and no way for two coroutines to both read "one probe
slot free" and both take it. The cost is that a breaker belongs to one event
loop in one process, which is written down in `docs/resilience.md` as the
limitation it is: five replicas hold five independent opinions about whether a
dependency is up, and each spends its own probe to find out.

## Why a call carries a generation

A probe that is still in flight when a *different* probe fails and re-opens the
circuit will eventually come back with an answer about a circuit that has since
moved on. Recording it would let a stale success count towards closing a
circuit that was re-opened after it started. So `acquire` stamps the call with
the generation it was admitted in, and an outcome from an earlier generation is
released without being counted.
"""

from __future__ import annotations

from collections import deque
from typing import NoReturn

import structlog

from src.decorators.base import DEFAULT_CLOCK, Clock
from src.resilience.base import CircuitBreakerConfig, CircuitOpenError, CircuitState

logger = structlog.get_logger(__name__)


class CircuitCall:
    """One admitted attempt. Settle it exactly once; `release` is idempotent.

    Returned by `CircuitBreaker.acquire`. The transport settles it with the
    outcome it got and releases it in a `finally`, so a cancelled request gives
    its half-open probe slot back instead of stranding the circuit in a state
    where nothing may pass and nothing will ever report.

    Not a dataclass: `settled` is a latch this object flips on itself, which is
    the opposite of the value semantics `tests/test_immutability_gate.py` holds
    dataclasses in `src/` to.
    """

    __slots__ = ("breaker", "generation", "probe", "settled")

    def __init__(
        self, breaker: CircuitBreaker, generation: int, *, probe: bool
    ) -> None:
        self.breaker = breaker
        self.generation = generation
        self.probe = probe
        self.settled = False

    def succeeded(self) -> None:
        self._settle(failed=False)

    def failed(self) -> None:
        self._settle(failed=True)

    def release(self) -> None:
        """Give the probe slot back without recording an outcome.

        For cancellation and for pool exhaustion — neither says anything about
        the dependency's health, and both would otherwise leave a half-open
        circuit permanently out of slots.
        """
        if self.settled:
            return
        self.settled = True
        if self.probe:
            self.breaker._release_probe()

    def _settle(self, *, failed: bool) -> None:
        if self.settled:
            return
        self.settled = True
        if self.probe:
            self.breaker._release_probe()
        self.breaker._record(self.generation, failed=failed)


class CircuitBreaker:
    """Admission control for one origin.

    Not constructed directly in application code — `CircuitBreakerRegistry`
    owns one per origin and hands them to the transport.
    """

    def __init__(
        self,
        origin: str,
        *,
        config: CircuitBreakerConfig | None = None,
        clock: Clock = DEFAULT_CLOCK,
    ) -> None:
        self._origin = origin
        self._config = config if config is not None else CircuitBreakerConfig()
        self._clock = clock
        self._state = CircuitState.CLOSED
        self._window: deque[bool] = deque(maxlen=self._config.window_size)
        self._open_until = 0.0
        self._generation = 0
        self._probes_in_flight = 0
        self._probe_successes = 0

    @property
    def origin(self) -> str:
        return self._origin

    @property
    def state(self) -> CircuitState:
        """The state as of now, resolving an elapsed open period first.

        Reading this can transition OPEN to HALF_OPEN, which looks surprising
        for a property and is the only correct answer: the circuit becomes
        half-open when the clock says so, not when the next request happens to
        ask. Without it a dashboard would report OPEN indefinitely on an idle
        origin and `acquire` would report HALF_OPEN for the same instant.
        """
        self._expire_open_period()
        return self._state

    @property
    def failures_in_window(self) -> int:
        return sum(self._window)

    def acquire(self) -> CircuitCall:
        """Admit one attempt, or refuse it.

        Raises:
            CircuitOpenError: the circuit is open, or half-open with every
                probe slot taken.
        """
        self._expire_open_period()

        if self._state is CircuitState.OPEN:
            self._reject(self._open_until - self._clock())
        if (
            self._state is CircuitState.HALF_OPEN
            and self._probes_in_flight >= self._config.half_open_max_calls
        ):
            # 0.0 rather than the reset timeout: a probe is in flight and the
            # circuit is seconds from an answer, so telling the caller to come
            # back in thirty is wrong in the direction that hurts.
            self._reject(0.0)

        probe = self._state is CircuitState.HALF_OPEN
        if probe:
            self._probes_in_flight += 1
        return CircuitCall(self, self._generation, probe=probe)

    def reset(self) -> None:
        """Force the circuit closed and forget every recorded outcome."""
        self._state = CircuitState.CLOSED
        self._window.clear()
        self._probes_in_flight = 0
        self._probe_successes = 0
        self._generation += 1

    def _reject(self, retry_after: float) -> NoReturn:
        logger.debug("circuit.rejected", origin=self._origin, state=str(self._state))
        raise CircuitOpenError(self._origin, max(0.0, retry_after))

    def _expire_open_period(self) -> None:
        if self._state is CircuitState.OPEN and self._clock() >= self._open_until:
            self._state = CircuitState.HALF_OPEN
            self._probes_in_flight = 0
            self._probe_successes = 0
            logger.info("circuit.half_open", origin=self._origin)

    def _release_probe(self) -> None:
        self._probes_in_flight = max(0, self._probes_in_flight - 1)

    def _record(self, generation: int, *, failed: bool) -> None:
        if generation != self._generation:
            # An answer about a circuit that has since re-opened. See the
            # module docstring.
            return

        if self._state is CircuitState.HALF_OPEN:
            self._record_probe(failed=failed)
            return

        self._window.append(failed)
        if failed and self.failures_in_window >= self._config.failure_threshold:
            self._open()

    def _record_probe(self, *, failed: bool) -> None:
        if failed:
            self._open()
            return

        self._probe_successes += 1
        if self._probe_successes < self._config.success_threshold:
            return

        self._state = CircuitState.CLOSED
        # Cleared rather than carried over: the window holds the failures that
        # opened the circuit, and keeping them would trip it again on the first
        # failure after recovery, however healthy the dependency now is.
        self._window.clear()
        self._probe_successes = 0
        self._generation += 1
        logger.info("circuit.closed", origin=self._origin)

    def _open(self) -> None:
        self._state = CircuitState.OPEN
        self._open_until = self._clock() + self._config.reset_timeout
        self._window.clear()
        self._probes_in_flight = 0
        self._probe_successes = 0
        self._generation += 1
        logger.warning(
            "circuit.opened",
            origin=self._origin,
            reset_timeout=self._config.reset_timeout,
        )


class CircuitBreakerRegistry:
    """One breaker per origin, created on first sight of that origin.

    Shared by every client that is handed the same registry, which is how two
    `httpx.AsyncClient`s talking to the same dependency come to share one
    opinion about it. Growth is bounded by the number of distinct origins this
    application calls out to, which is a property of its own code — do not hand
    this a registry keyed by a URL taken from user input.
    """

    __slots__ = ("_breakers", "clock", "config")

    def __init__(
        self,
        *,
        config: CircuitBreakerConfig | None = None,
        clock: Clock = DEFAULT_CLOCK,
    ) -> None:
        self.config = config if config is not None else CircuitBreakerConfig()
        self.clock = clock
        self._breakers: dict[str, CircuitBreaker] = {}

    def get(self, origin: str) -> CircuitBreaker:
        breaker = self._breakers.get(origin)
        if breaker is None:
            breaker = CircuitBreaker(origin, config=self.config, clock=self.clock)
            self._breakers[origin] = breaker
        return breaker

    def states(self) -> dict[str, CircuitState]:
        """A snapshot for a health endpoint or a log line."""
        return {origin: b.state for origin, b in self._breakers.items()}

    def reset(self) -> None:
        """Close every breaker. For tests, and for an operator override."""
        for breaker in self._breakers.values():
            breaker.reset()


#: The registry the default client factory uses, so that two clients built by
#: `resilient_async_client` for the same dependency share its state instead of
#: each learning it is down separately. Pass an explicit registry to isolate.
DEFAULT_REGISTRY: CircuitBreakerRegistry = CircuitBreakerRegistry()
