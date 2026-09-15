"""How the registry runs checks: concurrently, bounded, coalesced.

These are the properties a readiness probe is worth having *because of*. Each
one is tested against a stub that fails or hangs on demand (`tests/fakes.py`),
since a real dependency reaches those states only by being broken.
"""

from __future__ import annotations

import asyncio

import pytest

from src.health.base import CheckOutcome, CheckStatus, Criticality, aggregate
from src.health.registry import (
    DuplicateCheckNameError,
    HealthRegistry,
)
from tests.fakes import ClosingStubHealthCheck, StubHealthCheck


class FakeClock:
    """A monotonic clock a test advances by hand."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def outcome(
    name: str = "x",
    *,
    status: CheckStatus = "ok",
    criticality: Criticality = "required",
) -> CheckOutcome:
    return CheckOutcome(
        name=name,
        criticality=criticality,
        status=status,
        duration_ms=0.0,
        description="",
    )


# ── Aggregation ───────────────────────────────────────────────────────────────


def test_no_checks_is_ready() -> None:
    """A process configured with in-process backends has nothing to wait for.

    The defensive answer — `unavailable` until proven otherwise — would mean a
    deployment with no external dependency never enters its load balancer.
    """
    assert aggregate(()) == "ready"


def test_all_passing_is_ready() -> None:
    assert aggregate((outcome("a"), outcome("b"))) == "ready"


def test_a_failed_optional_check_degrades_but_keeps_serving() -> None:
    outcomes = (
        outcome("db"),
        outcome("cache", status="failed", criticality="optional"),
    )

    assert aggregate(outcomes) == "degraded"


def test_a_failed_required_check_makes_the_process_unavailable() -> None:
    outcomes = (outcome("db", status="failed"), outcome("cache"))

    assert aggregate(outcomes) == "unavailable"


def test_a_required_failure_outranks_an_optional_one() -> None:
    """Both kinds failing is still a 503: the worst answer wins."""
    outcomes = (
        outcome("db", status="failed"),
        outcome("cache", status="failed", criticality="optional"),
    )

    assert aggregate(outcomes) == "unavailable"


@pytest.mark.asyncio
async def test_degraded_still_routes_traffic_and_unavailable_does_not() -> None:
    """`ready` is the load-balancer decision, and it is not `status == "ready"`."""
    degraded = HealthRegistry(
        [StubHealthCheck("cache", criticality="optional", error=OSError())],
        cache_ttl_seconds=0,
    )
    unavailable = HealthRegistry(
        [StubHealthCheck("db", criticality="required", error=OSError())],
        cache_ttl_seconds=0,
    )

    assert (await degraded.report()).ready is True
    assert (await unavailable.report()).ready is False


# ── Running the checks ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_every_check_is_reported_even_when_one_fails() -> None:
    """A body listing only the first failure is one an operator cannot work from.

    This is what `gather` would do by default; `_run_one` swallowing its own
    exception is what stops it.
    """
    registry = HealthRegistry(
        [
            StubHealthCheck("first", error=ConnectionRefusedError(111, "refused")),
            StubHealthCheck("second"),
        ],
        cache_ttl_seconds=0,
    )

    report = await registry.report()

    assert [o.name for o in report.outcomes] == ["first", "second"]
    assert [o.status for o in report.outcomes] == ["failed", "ok"]


@pytest.mark.asyncio
async def test_a_failure_is_reported_as_a_type_name_never_a_message() -> None:
    """The probe is unauthenticated and driver errors quote the DSN."""
    registry = HealthRegistry(
        [
            StubHealthCheck(
                "db",
                error=ConnectionRefusedError(
                    111, "connect to postgres://admin:hunter2@db:5432 refused"
                ),
            )
        ],
        cache_ttl_seconds=0,
    )

    report = await registry.report()

    assert report.outcomes[0].error == "ConnectionRefusedError"
    assert "hunter2" not in repr(report.outcomes)


@pytest.mark.asyncio
async def test_checks_run_concurrently_rather_than_one_after_another() -> None:
    """Four 100ms probes must cost 100ms, not 400ms.

    Measured as wall clock against a real `asyncio.sleep`, because that is the
    thing being claimed. The margin is wide enough that a loaded CI runner does
    not fail it and narrow enough that a sequential implementation cannot pass.
    """
    checks = [StubHealthCheck(f"slow-{i}", delay=0.1) for i in range(4)]
    registry = HealthRegistry(checks, timeout_seconds=5, cache_ttl_seconds=0)

    started = asyncio.get_running_loop().time()
    report = await registry.report()
    elapsed = asyncio.get_running_loop().time() - started

    assert report.status == "ready"
    assert elapsed < 0.3


@pytest.mark.asyncio
async def test_a_hanging_check_times_out_instead_of_hanging_the_probe() -> None:
    """The failure mode a readiness probe exists to survive.

    A dependency whose socket is open and answering nothing would otherwise
    hold the request until the orchestrator gave up, recording a failure with
    no body — the one moment the endpoint has to be able to explain itself.
    """
    registry = HealthRegistry(
        [StubHealthCheck("wedged", delay=30)],
        timeout_seconds=0.05,
        cache_ttl_seconds=0,
    )

    report = await registry.report()

    assert report.status == "unavailable"
    assert report.outcomes[0].error == "TimeoutError"


@pytest.mark.asyncio
async def test_one_slow_check_does_not_fail_the_others() -> None:
    registry = HealthRegistry(
        [StubHealthCheck("wedged", delay=30), StubHealthCheck("fine")],
        timeout_seconds=0.05,
        cache_ttl_seconds=0,
    )

    report = await registry.report()

    assert {o.name: o.status for o in report.outcomes} == {
        "wedged": "failed",
        "fine": "ok",
    }


@pytest.mark.asyncio
async def test_a_cancelled_request_is_not_reported_as_a_failed_dependency() -> None:
    """A prober that gives up early must not make Postgres look broken.

    `CancelledError` is a `BaseException`, so `except Exception` lets it
    through — this test is what keeps that from being "tidied up" into a bare
    `except`.
    """
    registry = HealthRegistry(
        [StubHealthCheck("slow", delay=30)], timeout_seconds=30, cache_ttl_seconds=0
    )

    task = asyncio.create_task(registry.report())
    await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_duration_is_measured_for_every_check() -> None:
    registry = HealthRegistry(
        [StubHealthCheck("slow", delay=0.05)], cache_ttl_seconds=0
    )

    report = await registry.report()

    assert report.outcomes[0].duration_ms >= 45


@pytest.mark.asyncio
async def test_a_timed_out_check_still_reports_how_long_it_waited() -> None:
    registry = HealthRegistry(
        [StubHealthCheck("wedged", delay=30)],
        timeout_seconds=0.05,
        cache_ttl_seconds=0,
    )

    report = await registry.report()

    assert report.outcomes[0].duration_ms >= 45


# ── Caching and coalescing ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_results_are_cached_for_the_ttl() -> None:
    clock = FakeClock()
    check = StubHealthCheck("db")
    registry = HealthRegistry([check], cache_ttl_seconds=1.0, clock=clock)

    await registry.report()
    await registry.report()

    assert check.calls == 1


@pytest.mark.asyncio
async def test_the_cache_expires() -> None:
    clock = FakeClock()
    check = StubHealthCheck("db")
    registry = HealthRegistry([check], cache_ttl_seconds=1.0, clock=clock)

    await registry.report()
    clock.advance(1.0)
    await registry.report()

    assert check.calls == 2


@pytest.mark.asyncio
async def test_a_zero_ttl_probes_every_time() -> None:
    clock = FakeClock()
    check = StubHealthCheck("db")
    registry = HealthRegistry([check], cache_ttl_seconds=0.0, clock=clock)

    await registry.report()
    await registry.report()

    assert check.calls == 2


@pytest.mark.asyncio
async def test_failures_are_cached_too() -> None:
    """Re-probing a dependency that was down 200ms ago is the same herd.

    Pointed at a server that is already struggling, which is when a probe is
    least welcome and most frequent.
    """
    clock = FakeClock()
    check = StubHealthCheck("db", error=OSError("down"))
    registry = HealthRegistry([check], cache_ttl_seconds=1.0, clock=clock)

    first = await registry.report()
    second = await registry.report()

    assert check.calls == 1
    assert first.status == second.status == "unavailable"


@pytest.mark.asyncio
async def test_concurrent_probes_share_one_run() -> None:
    """Twenty simultaneous probers must produce one round trip, not twenty.

    The cache alone does not give this: without the lock, every caller misses
    the empty cache at the same instant and starts its own run.
    """
    clock = FakeClock()
    check = StubHealthCheck("db", delay=0.05)
    registry = HealthRegistry([check], cache_ttl_seconds=1.0, clock=clock)

    reports = await asyncio.gather(*(registry.report() for _ in range(20)))

    assert check.calls == 1
    assert all(report.status == "ready" for report in reports)


@pytest.mark.asyncio
async def test_invalidate_forces_the_next_probe_to_re_run() -> None:
    clock = FakeClock()
    check = StubHealthCheck("db")
    registry = HealthRegistry([check], cache_ttl_seconds=60.0, clock=clock)

    await registry.report()
    registry.invalidate()
    await registry.report()

    assert check.calls == 2


# ── Construction and teardown ─────────────────────────────────────────────────


def test_duplicate_names_are_refused_at_construction() -> None:
    """The names are the keys of the response body.

    Allowing two would let iteration order decide which dependency silently
    stops being reported.
    """
    with pytest.raises(DuplicateCheckNameError, match="redis"):
        HealthRegistry([StubHealthCheck("redis"), StubHealthCheck("redis")])


def test_checks_are_exposed_in_registration_order() -> None:
    registry = HealthRegistry([StubHealthCheck("a"), StubHealthCheck("b")])

    assert [check.name for check in registry.checks] == ["a", "b"]


@pytest.mark.asyncio
async def test_aclose_closes_what_it_can_and_skips_what_it_cannot() -> None:
    """Checks are not required to own resources; the lifespan calls this anyway."""
    closing = ClosingStubHealthCheck("redis")
    plain = StubHealthCheck("database")
    registry = HealthRegistry([closing, plain])

    await registry.aclose()

    assert closing.closed == 1
