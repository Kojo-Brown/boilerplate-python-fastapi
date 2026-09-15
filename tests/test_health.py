"""What the two probe endpoints answer, and what an orchestrator does with it.

The registry is substituted through `app.dependency_overrides`, so these are
tests of the endpoints — status codes, body shape, headers — rather than of the
checks underneath them. Those are in `test_health_checks.py`, and how a
registry runs them is in `test_health_registry.py`.

The readiness probe is also asserted against a *real* Postgres by
`scripts/smoke_start.py`, which CI runs against the service container before the
suite. That is the one place the whole path — uvicorn, lifespan, engine, driver,
server — is exercised end to end, because nothing short of it can fail the way a
misconfigured deployment fails.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from src.health.registry import HealthRegistry
from src.health.wiring import get_health_registry
from src.main import app
from tests.fakes import StubHealthCheck


@pytest.fixture
async def probe_client() -> AsyncGenerator[AsyncClient, None]:
    """A client whose readiness registry the test replaces per case."""
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client
    app.dependency_overrides.clear()


def use(registry: HealthRegistry) -> None:
    app.dependency_overrides[get_health_registry] = lambda: registry


def registry_of(*checks: StubHealthCheck) -> HealthRegistry:
    # No caching: each case wants its own checks probed, and the TTL is tested
    # where it belongs, in test_health_registry.py.
    return HealthRegistry(checks, cache_ttl_seconds=0)


# ── Liveness ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_liveness_returns_ok(async_client: AsyncClient) -> None:
    response = await async_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_liveness_does_not_touch_the_database(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    """A liveness probe that queried Postgres would get healthy pods killed
    during a database blip, so it must issue no statements at all."""
    mock_db.execute = AsyncMock()

    await async_client.get("/health")

    mock_db.execute.assert_not_called()


@pytest.mark.asyncio
async def test_liveness_does_not_run_the_dependency_checks(
    probe_client: AsyncClient,
) -> None:
    """The same rule, stated against the registry rather than one session.

    A check added to the registry must never start being probed by `/health`
    as a side effect of being registered.
    """
    check = StubHealthCheck("database")
    use(registry_of(check))

    await probe_client.get("/health")

    assert check.calls == 0


@pytest.mark.asyncio
async def test_liveness_is_not_cacheable(async_client: AsyncClient) -> None:
    """A proxy replaying a stale `ok` is a dead process that still looks alive."""
    response = await async_client.get("/health")

    assert response.headers["cache-control"] == "no-store"


# ── Readiness ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_readiness_reports_every_check_when_they_all_pass(
    probe_client: AsyncClient,
) -> None:
    use(
        registry_of(
            StubHealthCheck("database", description="postgresql"),
            StubHealthCheck("redis", description="redis, used by celery"),
        )
    )

    response = await probe_client.get("/health/ready")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert sorted(body["checks"]) == ["database", "redis"]
    assert body["checks"]["database"]["status"] == "ok"
    assert body["checks"]["database"]["criticality"] == "required"
    assert body["checks"]["database"]["error"] is None
    assert body["checks"]["redis"]["description"] == "redis, used by celery"


@pytest.mark.asyncio
async def test_readiness_reports_a_duration_per_check(
    probe_client: AsyncClient,
) -> None:
    """Which dependency is slow is most of the diagnosis during an incident."""
    use(registry_of(StubHealthCheck("database")))

    body = (await probe_client.get("/health/ready")).json()

    assert isinstance(body["checks"]["database"]["duration_ms"], float)
    assert body["checks"]["database"]["duration_ms"] >= 0


@pytest.mark.asyncio
async def test_readiness_returns_503_when_a_required_check_fails(
    probe_client: AsyncClient,
) -> None:
    use(
        registry_of(
            StubHealthCheck("database", error=ConnectionRefusedError(111, "refused")),
            StubHealthCheck("redis"),
        )
    )

    response = await probe_client.get("/health/ready")

    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "unavailable"
    assert body["checks"]["database"]["error"] == "ConnectionRefusedError"
    assert body["checks"]["redis"]["status"] == "ok"


@pytest.mark.asyncio
async def test_readiness_stays_200_when_only_an_optional_check_fails(
    probe_client: AsyncClient,
) -> None:
    """The case this whole design exists for.

    Taking a process out of its load balancer because something it can serve
    without is unreachable turns one dependency's incident into an outage.
    """
    use(
        registry_of(
            StubHealthCheck("database"),
            StubHealthCheck(
                "redis:redis-streams", criticality="optional", error=OSError("down")
            ),
        )
    )

    response = await probe_client.get("/health/ready")

    assert response.status_code == 200
    assert response.json()["status"] == "degraded"


@pytest.mark.asyncio
async def test_readiness_returns_503_when_a_required_check_times_out(
    probe_client: AsyncClient,
) -> None:
    """A wedged dependency must answer 503, not hold the connection open."""
    registry = HealthRegistry(
        [StubHealthCheck("database", delay=30)],
        timeout_seconds=0.05,
        cache_ttl_seconds=0,
    )
    use(registry)

    response = await probe_client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["checks"]["database"]["error"] == "TimeoutError"


@pytest.mark.asyncio
async def test_readiness_failure_does_not_leak_driver_details(
    probe_client: AsyncClient,
) -> None:
    """The connection string appears in driver errors; it must not reach the
    response body, which is served unauthenticated to any prober."""
    use(
        registry_of(
            StubHealthCheck(
                "database",
                error=ConnectionRefusedError(
                    111, "connect to postgres://admin:hunter2@db:5432 refused"
                ),
            )
        )
    )

    response = await probe_client.get("/health/ready")

    assert response.status_code == 503
    assert "hunter2" not in response.text
    assert "5432" not in response.text


@pytest.mark.asyncio
async def test_readiness_is_not_cacheable(probe_client: AsyncClient) -> None:
    """A CDN serving a cached `ready` would route traffic to a dead replica."""
    use(registry_of(StubHealthCheck("database")))

    response = await probe_client.get("/health/ready")

    assert response.headers["cache-control"] == "no-store"


@pytest.mark.asyncio
async def test_readiness_with_no_checks_is_ready(probe_client: AsyncClient) -> None:
    use(registry_of())

    response = await probe_client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "checks": {}}


@pytest.mark.asyncio
async def test_the_probes_are_documented_in_the_openapi_schema() -> None:
    """Including the 503, which is the answer a client most needs to expect."""
    schema = app.openapi()

    assert "200" in schema["paths"]["/health"]["get"]["responses"]
    ready = schema["paths"]["/health/ready"]["get"]["responses"]
    assert "200" in ready
    assert "503" in ready


@pytest.mark.asyncio
async def test_the_real_registry_is_wired_into_the_app(
    probe_client: AsyncClient,
) -> None:
    """Without an override the endpoint resolves the configured registry.

    A dependency that only ever ran under a test double would be a probe that
    never touched a real dependency in production.
    """
    app.dependency_overrides.clear()
    registry = get_health_registry()

    assert "database" in {check.name for check in registry.checks}
