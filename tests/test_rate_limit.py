import os
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from src.database import get_db  # noqa: E402
from src.limiter import limiter  # noqa: E402
from src.main import app  # noqa: E402
from tests.conftest import apply_column_defaults  # noqa: E402


@pytest.fixture(autouse=True)
def reset_limiter() -> None:
    """Clear the in-memory rate limit storage between tests."""
    storage = limiter._limiter
    if hasattr(storage, "storage") and hasattr(storage.storage, "reset"):
        storage.storage.reset()
    yield  # type: ignore[misc]


@pytest.fixture
def mock_db() -> AsyncMock:
    """Session stub that resolves column defaults the way a real flush does.

    Without this, ``refresh()`` leaves ``id``/``role``/``is_active`` at None and
    the register handler fails to serialise its own 201 response. That used to
    reach the client as a 400 — the router caught ``ValueError``, and
    ``pydantic.ValidationError`` is one — so the rate-limit assertions below
    still passed while every "successful" registration was really an error.
    """
    session = AsyncMock()
    pending: list[object] = []

    def _add(instance: object) -> None:
        pending.append(instance)

    async def _flush(*_args: object, **_kwargs: object) -> None:
        for instance in pending:
            apply_column_defaults(instance)

    async def _refresh(instance: object, *_args: object, **_kwargs: object) -> None:
        apply_column_defaults(instance)

    session.add = MagicMock(side_effect=_add)
    session.commit = AsyncMock()
    session.flush = AsyncMock(side_effect=_flush)
    session.refresh = AsyncMock(side_effect=_refresh)
    return session


@pytest.fixture
async def async_client(mock_db: AsyncMock) -> AsyncClient:
    async def override_get_db() -> AsyncMock:
        yield mock_db  # type: ignore[misc]

    app.dependency_overrides[get_db] = override_get_db

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client  # type: ignore[misc]

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_login_rate_limit_allows_requests_under_limit(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    """Requests below the rate limit threshold succeed normally."""
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=result_mock)

    payload = {"email": "user@example.com", "password": "wrongpassword"}
    response = await async_client.post("/api/v1/auth/login", json=payload)
    # 401 means the route was reached (credentials rejected, not rate-limited)
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_login_rate_limit_returns_429_after_limit(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    """POST /auth/login returns 429 after exceeding 5 requests per minute."""
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=result_mock)

    payload = {"email": "user@example.com", "password": "wrongpassword"}

    # Send 5 requests (at the limit). Each one reaches the route and is
    # rejected on its merits: unknown email is 401, and nothing else.
    for _ in range(5):
        response = await async_client.post("/api/v1/auth/login", json=payload)
        assert response.status_code == 401

    # The 6th request should be rate-limited
    response = await async_client.post("/api/v1/auth/login", json=payload)
    assert response.status_code == 429


@pytest.mark.asyncio
async def test_register_rate_limit_returns_429_after_limit(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    """POST /auth/register returns 429 after exceeding 5 requests per minute."""
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=result_mock)

    payload = {"email": "new@example.com", "password": "password123"}

    for _ in range(5):
        response = await async_client.post("/api/v1/auth/register", json=payload)
        assert response.status_code == 201

    response = await async_client.post("/api/v1/auth/register", json=payload)
    assert response.status_code == 429


@pytest.mark.asyncio
async def test_refresh_rate_limit_allows_10_per_minute(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    """POST /auth/refresh allows 10 requests before rate-limiting."""
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=result_mock)

    payload = {"refresh_token": "invalid.token.here"}

    for _ in range(10):
        response = await async_client.post("/api/v1/auth/refresh", json=payload)
        assert response.status_code == 401

    response = await async_client.post("/api/v1/auth/refresh", json=payload)
    assert response.status_code == 429


@pytest.mark.asyncio
async def test_rate_limit_response_has_retry_after_header(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    """429 responses include a Retry-After header."""
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=result_mock)

    payload = {"email": "user@example.com", "password": "wrongpassword"}

    for _ in range(5):
        await async_client.post("/api/v1/auth/login", json=payload)

    response = await async_client.post("/api/v1/auth/login", json=payload)
    assert response.status_code == 429
    assert "Retry-After" in response.headers or "retry-after" in response.headers


@pytest.mark.asyncio
async def test_health_endpoint_not_rate_limited(async_client: AsyncClient) -> None:
    """Health check endpoint is not subject to rate limiting."""
    for _ in range(10):
        response = await async_client.get("/health")
        assert response.status_code == 200
