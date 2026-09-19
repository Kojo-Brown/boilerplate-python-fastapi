"""The policy as observed from outside, on the real application.

`tests/test_security_headers.py` covers the value and the middleware in
isolation; this file is about which *responses* carry the headers, which is
the property that is easy to believe and hard to keep.
"""

from collections.abc import AsyncGenerator, Iterator

import pytest
from fastapi import Request
from fastapi.responses import JSONResponse, PlainTextResponse
from httpx import ASGITransport, AsyncClient
from starlette.routing import Route

from src.docs import DOCS_URL, OAUTH2_REDIRECT_URL, REDOC_URL
from src.exception_handlers import apply_security_headers
from src.main import app
from src.middleware.security_headers import (
    CONTENT_TYPE_OPTIONS_HEADER,
    CSP_HEADER,
    FRAME_OPTIONS_HEADER,
    HSTS_HEADER,
    REFERRER_POLICY_HEADER,
)

ALWAYS_PRESENT = (
    CSP_HEADER,
    CONTENT_TYPE_OPTIONS_HEADER,
    REFERRER_POLICY_HEADER,
    FRAME_OPTIONS_HEADER,
)


async def _boom(request: Request) -> JSONResponse:  # pragma: no cover - raises
    raise RuntimeError("deliberate failure")


async def _own_policy(request: Request) -> PlainTextResponse:
    return PlainTextResponse("ok", headers={CSP_HEADER: "sandbox"})


@pytest.fixture(scope="module", autouse=True)
def probe_routes() -> Iterator[None]:
    """Two routes that only exist while this module runs.

    Registered on the real application rather than on a copy of its wiring,
    because a copy is exactly the thing that stops reproducing the bug: the
    500 below is interesting *because* of where Starlette installs the handler
    for bare `Exception` relative to the middleware stack, and a hand-built app
    would be asserting against my reconstruction of that instead of against it.
    """
    added = [
        Route("/__test__/boom", _boom, methods=["GET"]),
        Route("/__test__/own-policy", _own_policy, methods=["GET"]),
    ]
    app.router.routes.extend(added)
    yield
    for route in added:
        app.router.routes.remove(route)


@pytest.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://test",
    ) as c:
        yield c


@pytest.fixture
async def tls_client() -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False),
        base_url="https://test",
    ) as c:
        yield c


class TestEveryResponseIsCovered:
    @pytest.mark.parametrize(
        ("path", "expected_status"),
        [
            ("/health", 200),
            ("/openapi.json", 200),
            (DOCS_URL, 200),
            (REDOC_URL, 200),
            (OAUTH2_REDIRECT_URL, 200),
            ("/no-such-route", 404),
            ("/api/v1/users/me", 401),
            ("/__test__/boom", 500),
        ],
    )
    async def test_headers_are_present(
        self, client: AsyncClient, path: str, expected_status: int
    ) -> None:
        response = await client.get(path)
        assert response.status_code == expected_status
        for header in ALWAYS_PRESENT:
            assert header in response.headers, f"{header} missing from {path}"

    async def test_the_unhandled_500_is_the_interesting_one(
        self, client: AsyncClient
    ) -> None:
        # ServerErrorMiddleware sits outside the user middleware stack, so this
        # response never passes through SecurityHeadersMiddleware's `send`. If
        # `apply_security_headers` is dropped from the handler this is the only
        # assertion in the suite that notices.
        response = await client.get("/__test__/boom")
        assert response.status_code == 500
        assert response.json()["error"] == "INTERNAL_SERVER_ERROR"
        assert response.headers[CSP_HEADER].startswith("default-src 'none'")

    async def test_a_route_that_sets_its_own_policy_keeps_it(
        self, client: AsyncClient
    ) -> None:
        response = await client.get("/__test__/own-policy")
        assert response.headers[CSP_HEADER] == "sandbox"
        # The rest of the policy is still a floor under it.
        assert response.headers[CONTENT_TYPE_OPTIONS_HEADER] == "nosniff"


class TestTransportDecidesHSTS:
    async def test_absent_over_plain_http(self, client: AsyncClient) -> None:
        response = await client.get("/health")
        assert HSTS_HEADER not in response.headers

    async def test_present_over_https(self, tls_client: AsyncClient) -> None:
        response = await tls_client.get("/health")
        assert response.headers[HSTS_HEADER] == "max-age=31536000; includeSubDomains"

    async def test_present_on_the_unhandled_500_over_https(
        self, tls_client: AsyncClient
    ) -> None:
        response = await tls_client.get("/__test__/boom")
        assert response.status_code == 500
        assert HSTS_HEADER in response.headers


class TestApiPolicy:
    async def test_api_responses_forbid_everything(self, client: AsyncClient) -> None:
        csp = (await client.get("/health")).headers[CSP_HEADER]
        assert "default-src 'none'" in csp
        assert "frame-ancestors 'none'" in csp
        assert "base-uri 'none'" in csp
        assert "form-action 'none'" in csp

    async def test_api_responses_carry_no_nonce(self, client: AsyncClient) -> None:
        assert "nonce-" not in (await client.get("/health")).headers[CSP_HEADER]

    async def test_nosniff_and_referrer_policy(self, client: AsyncClient) -> None:
        headers = (await client.get("/health")).headers
        assert headers[CONTENT_TYPE_OPTIONS_HEADER] == "nosniff"
        assert headers[REFERRER_POLICY_HEADER] == "strict-origin-when-cross-origin"
        assert headers[FRAME_OPTIONS_HEADER] == "DENY"


class TestDisabledPolicy:
    async def test_the_exception_handler_is_a_no_op_without_a_policy(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "src.exception_handlers.get_security_headers_policy", lambda: None
        )
        response = JSONResponse(status_code=500, content={})
        request = Request({"type": "http", "path": "/x", "headers": [], "scheme": "*"})
        assert apply_security_headers(request, response) is response
        assert CSP_HEADER not in response.headers
