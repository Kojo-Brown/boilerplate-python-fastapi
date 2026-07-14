import os

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
os.environ.setdefault("ALGORITHM", "HS256")
os.environ.setdefault("ACCESS_TOKEN_EXPIRE_MINUTES", "30")
os.environ.setdefault("REFRESH_TOKEN_EXPIRE_DAYS", "7")

import pytest
from httpx import ASGITransport, AsyncClient
from unittest.mock import AsyncMock, MagicMock

from src.database import get_db
from src.main import app


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
    async def override_get_db():
        yield mock_db

    app.dependency_overrides[get_db] = override_get_db

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client

    app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_health_endpoint_still_reachable(async_client: AsyncClient) -> None:
    response = await async_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_auth_register_mounted_under_api_v1(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=result_mock)

    response = await async_client.post(
        "/api/v1/auth/register",
        json={"email": "test@example.com", "password": "password123"},
    )
    # 201 Created means the route is mounted at /api/v1/auth/register
    assert response.status_code == 201


@pytest.mark.asyncio
async def test_auth_login_mounted_under_api_v1(async_client: AsyncClient) -> None:
    response = await async_client.post(
        "/api/v1/auth/login",
        json={"email": "nobody@example.com", "password": "wrong"},
    )
    # 401 Unauthorized means the route exists at /api/v1/auth/login
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_old_auth_route_not_reachable(async_client: AsyncClient) -> None:
    response = await async_client.post(
        "/auth/login",
        json={"email": "nobody@example.com", "password": "wrong"},
    )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_openapi_schema_includes_v1_paths(async_client: AsyncClient) -> None:
    response = await async_client.get("/openapi.json")
    assert response.status_code == 200
    schema = response.json()
    paths = schema.get("paths", {})
    api_v1_paths = [p for p in paths if p.startswith("/api/v1/")]
    assert len(api_v1_paths) > 0, "No /api/v1/ paths found in OpenAPI schema"


@pytest.mark.asyncio
async def test_v1_router_prefix_in_openapi(async_client: AsyncClient) -> None:
    response = await async_client.get("/openapi.json")
    assert response.status_code == 200
    schema = response.json()
    paths = schema.get("paths", {})
    assert "/api/v1/auth/register" in paths
    assert "/api/v1/auth/login" in paths
    assert "/api/v1/auth/refresh" in paths
    assert "/api/v1/auth/logout" in paths
