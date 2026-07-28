"""Every route must be reachable under the /api/v1 prefix and nowhere else.

Session/client fixtures come from ``tests/conftest.py``; this module deliberately
does not redefine them, so the stub session behaves identically everywhere.
"""

from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import AsyncClient


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
async def test_auth_login_mounted_under_api_v1(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    mock_db.execute = AsyncMock(return_value=result_mock)

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
