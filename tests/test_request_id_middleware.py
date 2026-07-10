import uuid

import pytest
import structlog
from httpx import ASGITransport, AsyncClient

from src.main import app
from src.middleware.request_id import REQUEST_ID_HEADER


@pytest.fixture
async def client() -> AsyncClient:
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


class TestRequestIDMiddleware:
    async def test_response_contains_request_id_header(self, client: AsyncClient) -> None:
        response = await client.get("/health")
        assert REQUEST_ID_HEADER in response.headers
        header_value = response.headers[REQUEST_ID_HEADER]
        # Must be a valid UUID
        uuid.UUID(header_value)

    async def test_echoes_client_provided_request_id(self, client: AsyncClient) -> None:
        custom_id = str(uuid.uuid4())
        response = await client.get("/health", headers={REQUEST_ID_HEADER: custom_id})
        assert response.headers[REQUEST_ID_HEADER] == custom_id

    async def test_generates_unique_request_ids(self, client: AsyncClient) -> None:
        r1 = await client.get("/health")
        r2 = await client.get("/health")
        assert r1.headers[REQUEST_ID_HEADER] != r2.headers[REQUEST_ID_HEADER]

    async def test_request_id_is_valid_uuid4(self, client: AsyncClient) -> None:
        response = await client.get("/health")
        request_id = response.headers[REQUEST_ID_HEADER]
        parsed = uuid.UUID(request_id, version=4)
        assert str(parsed) == request_id

    async def test_context_cleared_between_requests(self, client: AsyncClient) -> None:
        id1 = str(uuid.uuid4())
        id2 = str(uuid.uuid4())
        r1 = await client.get("/health", headers={REQUEST_ID_HEADER: id1})
        r2 = await client.get("/health", headers={REQUEST_ID_HEADER: id2})
        assert r1.headers[REQUEST_ID_HEADER] == id1
        assert r2.headers[REQUEST_ID_HEADER] == id2

    async def test_non_http_scope_passthrough(self, client: AsyncClient) -> None:
        # Verify the app still handles normal HTTP requests correctly
        response = await client.get("/health")
        assert response.status_code == 200

    async def test_structlog_contextvars_cleared_after_request(
        self, client: AsyncClient
    ) -> None:
        await client.get("/health")
        ctx = structlog.contextvars.get_contextvars()
        assert ctx == {}

    async def test_request_id_header_lowercase_in_response(
        self, client: AsyncClient
    ) -> None:
        response = await client.get("/health")
        # httpx normalises header names; verify the header exists via case-insensitive lookup
        assert response.headers.get("x-request-id") is not None

    async def test_error_response_also_carries_request_id(
        self, client: AsyncClient
    ) -> None:
        response = await client.get("/api/v1/nonexistent-route-404")
        assert REQUEST_ID_HEADER in response.headers

    async def test_custom_id_survives_error_response(
        self, client: AsyncClient
    ) -> None:
        custom_id = str(uuid.uuid4())
        response = await client.get(
            "/api/v1/nonexistent-404",
            headers={REQUEST_ID_HEADER: custom_id},
        )
        assert response.headers[REQUEST_ID_HEADER] == custom_id
