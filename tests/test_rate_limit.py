import os
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")

from src.database import get_db  # noqa: E402
from src.limiter import limiter  # noqa: E402
from src.main import app  # noqa: E402


@pytest.fixture(autouse=True)
def reset_limiter() -> None:
    """Clear the in-memory rate limit storage between tests."""
    storage = limiter._limiter
    if hasattr(storage, "storage") and hasattr(storage.storage, "reset"):
        storage.storage.reset()
    yield  # type: ignore[misc]


@pytest.fixture
def mock_db() -> AsyncMock:
    session = AsyncMock()
    session.add = MagicMock()
    session.commit = AsyncMock()
    session.flush = AsyncMock()
    session.refresh = AsyncMock()
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
    response = await async_client.post("/auth/login", json=payload)
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

    # Send 5 requests (at the limit)
    for _ in range(5):
        response = await async_client.post("/auth/login", json=payload)
        assert response.status_code in {200, 401, 400}

    # The 6th request should be rate-limited
    response = await async_client.post("/auth/login", json=payload)
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
        response = await async_client.post("/auth/register", json=payload)
        assert response.status_code in {200, 201, 400, 422}

    response = await async_client.post("/auth/register", json=payload)
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
        response = await async_client.post("/auth/refresh", json=payload)
        assert response.status_code in {200, 401}

    response = await async_client.post("/auth/refresh", json=payload)
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
        await async_client.post("/auth/login", json=payload)

    response = await async_client.post("/auth/login", json=payload)
    assert response.status_code == 429
    assert "Retry-After" in response.headers or "retry-after" in response.headers


@pytest.mark.asyncio
async def test_health_endpoint_not_rate_limited(async_client: AsyncClient) -> None:
    """Health check endpoint is not subject to rate limiting."""
    for _ in range(10):
        response = await async_client.get("/health")
        assert response.status_code == 200
