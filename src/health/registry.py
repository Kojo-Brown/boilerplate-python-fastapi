"""Running the checks: concurrently, under a timeout, at most once per TTL.

Three properties, each of which is a bug if it is missing.

**Concurrent.** Probes run under `asyncio.gather`, so a readiness answer costs
the *slowest* dependency rather than the sum of all of them. Sequential checks
turn a probe into a latency budget nobody sized: four dependencies at 200ms
each is an 800ms probe, which is already past Kubernetes' default
`timeoutSeconds: 1`.

**Bounded.** Every probe runs inside `asyncio.timeout`. Without one, the
failure mode of a wedged dependency — a TCP connection that is open and
answering nothing, which is what a partitioned or overloaded server looks like
— is a probe that never returns. The orchestrator then kills the request at its
own timeout and records a failure with no body, so the one endpoint that should
explain the outage says nothing at all. A timeout here turns that into a
reported, attributed failure.

The bound is not absolute: `asyncio.timeout` cancels at an await point, so a
probe that blocks the event loop cannot be interrupted by it. That is why the
Redis client in `checks.py` is also given driver-level socket timeouts — the
two are belt and braces for different failure shapes.

**Coalesced.** Results are cached for a TTL, and concurrent callers that miss
the cache share one run rather than starting one each. A readiness endpoint is
polled by the kubelet, by every load balancer in front of it and by whatever
scrapes it, and each of those multiplies by the replica count against a single
Postgres. Worse, the multiplication peaks exactly when the dependency is
already struggling, because that is when everything retries. The TTL is what
makes probe traffic a function of time rather than of how many probers exist.

Failures are cached for the same TTL as successes, deliberately. Re-probing a
dependency that was unreachable 200ms ago rarely learns anything, and doing it
per request is the same thundering herd pointed at a server that is already
down.

## What is not cached

Nothing is remembered across a failure→recovery boundary beyond one TTL, so
recovery latency is bounded by it: set `HEALTH_CACHE_TTL_SECONDS` below the
orchestrator's `periodSeconds` and every poll still sees a fresh answer.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Sequence
from typing import Final

import structlog

from src.decorators.base import DEFAULT_CLOCK, DEFAULT_TIMER, Clock
from src.health.base import (
    CheckOutcome,
    HealthCheck,
    ReadinessReport,
    aggregate,
)

logger = structlog.get_logger(__name__)

#: Long enough for a TCP connect and a round trip to a healthy dependency on a
#: bad day, short enough to sit under an orchestrator's probe timeout with room
#: for the response itself. See `docs/health.md` for the manifest this pairs
#: with.
DEFAULT_TIMEOUT_SECONDS: Final[float] = 2.0

#: One second collapses a burst of probers into a single run while keeping the
#: answer fresher than any sane `periodSeconds`.
DEFAULT_CACHE_TTL_SECONDS: Final[float] = 1.0


class DuplicateCheckNameError(ValueError):
    """Two checks claimed the same name.

    Refused at construction rather than allowed to overwrite each other in the
    response body, where the survivor would be decided by iteration order and
    one dependency would silently stop being probed.
    """


class HealthRegistry:
    """The set of checks this process runs, and the policy for running them.

    Constructed once per process by `src/health/wiring.py` and resolved into
    the route as a dependency, so a test can substitute a registry of fakes
    without touching the real dependencies.
    """

    def __init__(
        self,
        checks: Iterable[HealthCheck] = (),
        *,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
        cache_ttl_seconds: float = DEFAULT_CACHE_TTL_SECONDS,
        clock: Clock = DEFAULT_CLOCK,
        timer: Clock = DEFAULT_TIMER,
    ) -> None:
        self._checks: tuple[HealthCheck, ...] = tuple(checks)
        self._reject_duplicate_names()
        self._timeout = timeout_seconds
        self._cache_ttl = cache_ttl_seconds
        # Two clocks on purpose, as everywhere else in this codebase:
        # `monotonic` for expiry decisions, `perf_counter` for measuring a
        # duration. See `src/decorators/base.py`.
        self._clock = clock
        self._timer = timer
        self._lock = asyncio.Lock()
        self._cached: tuple[float, ReadinessReport] | None = None

    def _reject_duplicate_names(self) -> None:
        seen: set[str] = set()
        for check in self._checks:
            if check.name in seen:
                raise DuplicateCheckNameError(
                    f"Two health checks are named '{check.name}'. "
                    "Names are the keys of the probe body and must be unique."
                )
            seen.add(check.name)

    @property
    def checks(self) -> Sequence[HealthCheck]:
        """The registered checks, in the order they are reported."""
        return self._checks

    @property
    def timeout_seconds(self) -> float:
        """The ceiling on one probe, and very nearly on the whole endpoint."""
        return self._timeout

    @property
    def cache_ttl_seconds(self) -> float:
        """How long one run's results are served to every caller."""
        return self._cache_ttl

    async def report(self) -> ReadinessReport:
        """The current readiness, from cache when it is fresh enough."""
        fresh = self._fresh_report()
        if fresh is not None:
            return fresh

        async with self._lock:
            # Checked again under the lock: everything that queued here while
            # one caller ran the checks takes that caller's result rather than
            # running its own, which is the coalescing this lock exists for.
            fresh = self._fresh_report()
            if fresh is not None:
                return fresh

            report = await self._run_all()
            self._cached = (self._clock(), report)
            return report

    def invalidate(self) -> None:
        """Drop the cached result so the next `report()` re-probes.

        For tests and for a deployment that wants a forced re-check after a
        known event; ordinary traffic should let the TTL do its work.
        """
        self._cached = None

    async def aclose(self) -> None:
        """Release whatever the checks hold open.

        Checks are not required to own resources, so anything without an
        `aclose` is skipped. Called from the application lifespan, which is
        what keeps a reload from leaking one socket per probe client.
        """
        for check in self._checks:
            closer = getattr(check, "aclose", None)
            if closer is None:
                continue
            try:
                await closer()
            except Exception as exc:  # pragma: no cover - defensive
                # A failed close must not stop the shutdown: everything after
                # it in the lifespan still has to run.
                logger.warning(
                    "health.check_close_failed",
                    check=check.name,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )

    def _fresh_report(self) -> ReadinessReport | None:
        if self._cached is None:
            return None
        cached_at, report = self._cached
        if self._clock() - cached_at >= self._cache_ttl:
            return None
        return report

    async def _run_all(self) -> ReadinessReport:
        outcomes = tuple(
            await asyncio.gather(*(self._run_one(check) for check in self._checks))
        )
        return ReadinessReport(status=aggregate(outcomes), outcomes=outcomes)

    async def _run_one(self, check: HealthCheck) -> CheckOutcome:
        """Run one probe, converting any outcome into a reportable result.

        Never raises. `gather` would otherwise abandon the other checks'
        results the moment one of them failed, and a probe body that lists only
        the first failure is the one thing an operator cannot work from.
        """
        started = self._timer()
        try:
            async with asyncio.timeout(self._timeout):
                await check.probe()
        except TimeoutError:
            # Reported as its own error rather than folded into the generic
            # branch: "did not answer within 2s" and "refused the connection"
            # are different incidents, and an operator reads them differently.
            elapsed_ms = (self._timer() - started) * 1000
            logger.warning(
                "health.check_timed_out",
                check=check.name,
                criticality=check.criticality,
                timeout_seconds=self._timeout,
            )
            return CheckOutcome(
                name=check.name,
                criticality=check.criticality,
                status="failed",
                duration_ms=elapsed_ms,
                description=check.description,
                error="TimeoutError",
            )
        except Exception as exc:
            # `Exception`, not `BaseException`: a `CancelledError` from the
            # client disconnecting is not this dependency's failure and must
            # keep propagating, or the probe would report a healthy dependency
            # as broken every time a prober gave up early.
            elapsed_ms = (self._timer() - started) * 1000
            logger.warning(
                "health.check_failed",
                check=check.name,
                criticality=check.criticality,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            return CheckOutcome(
                name=check.name,
                criticality=check.criticality,
                status="failed",
                duration_ms=elapsed_ms,
                description=check.description,
                error=type(exc).__name__,
            )

        return CheckOutcome(
            name=check.name,
            criticality=check.criticality,
            status="ok",
            duration_ms=(self._timer() - started) * 1000,
            description=check.description,
        )
