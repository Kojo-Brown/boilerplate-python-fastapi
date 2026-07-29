"""Liveness and readiness probe behaviour.

The readiness probe is what CI's start-up smoke test asserts against a real
Postgres, so its failure path needs unit coverage here: a database that is down
must produce a 503, never an unhandled 500.

Session/client fixtures come from ``tests/conftest.py``.
"""

from unittest.mock import AsyncMock

import pytest
from httpx import AsyncClient
from sqlalchemy.exc import OperationalError

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


# ── Readiness ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_readiness_reports_ready_when_the_database_answers(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    mock_db.execute = AsyncMock()

    response = await async_client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "database": "ok"}


@pytest.mark.asyncio
async def test_readiness_round_trips_a_statement_to_the_database(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    """The probe is only meaningful if it actually reaches Postgres."""
    mock_db.execute = AsyncMock()

    await async_client.get("/health/ready")

    mock_db.execute.assert_awaited_once()
    executed = str(mock_db.execute.await_args.args[0])
    assert "SELECT 1" in executed


@pytest.mark.asyncio
async def test_readiness_returns_503_when_postgres_refuses_the_connection(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    """A refused connect raises ConnectionRefusedError (an OSError), not a
    SQLAlchemyError — the case a SQLAlchemy-only except clause would miss."""
    mock_db.execute = AsyncMock(side_effect=ConnectionRefusedError(111, "refused"))

    response = await async_client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "unavailable", "database": "unreachable"}


@pytest.mark.asyncio
async def test_readiness_returns_503_on_sqlalchemy_operational_error(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    mock_db.execute = AsyncMock(
        side_effect=OperationalError("SELECT 1", {}, Exception("server closed"))
    )

    response = await async_client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "unavailable", "database": "unreachable"}


@pytest.mark.asyncio
async def test_readiness_failure_does_not_leak_driver_details(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    """The connection string can appear in driver errors; it must not reach the
    response body, which is served unauthenticated to any prober."""
    mock_db.execute = AsyncMock(
        side_effect=ConnectionRefusedError(
            111, "connect to postgres://admin:hunter2@db:5432 refused"
        )
    )

    response = await async_client.get("/health/ready")

    assert response.status_code == 503
    assert "hunter2" not in response.text
    assert response.json() == {"status": "unavailable", "database": "unreachable"}
