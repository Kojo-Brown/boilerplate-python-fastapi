"""The middleware, against the real app and against a bare ASGI app.

Two reasons for both. The bare app is where the ordering claim can be made
precisely — the handler records what the tenant was *when it ran*, which is
the only thing the middleware promises — and the real app is where the error
envelope and the header name come from configuration rather than from a
constructor argument.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from starlette.types import Receive, Scope, Send

from src.auth.utils import create_access_token
from src.tenancy.context import current_tenant_id
from src.tenancy.middleware import WS_POLICY_VIOLATION, TenantContextMiddleware

ALPHA = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")
BETA = uuid.UUID("bbbbbbbb-0000-0000-0000-000000000002")
USER = uuid.UUID("11111111-2222-3333-4444-555555555555")

HEADER = "X-Tenant-ID"


class _Recorder:
    """A minimal ASGI app that answers with the tenant it saw."""

    def __init__(self) -> None:
        self.seen: list[uuid.UUID | None] = []

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        self.seen.append(current_tenant_id())
        await send(
            {
                "type": "http.response.start",
                "status": 204,
                "headers": [],
            }
        )
        await send({"type": "http.response.body", "body": b""})


def _client(app: Any) -> AsyncClient:
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


def _wrapped(*, trust_header: bool = True) -> tuple[_Recorder, Any]:
    recorder = _Recorder()
    return recorder, TenantContextMiddleware(
        recorder, header_name=HEADER, trust_header=trust_header
    )


def _auth(tenant_id: uuid.UUID) -> dict[str, str]:
    token = create_access_token(str(USER), "u@example.com", "user", tenant_id)
    return {"Authorization": f"Bearer {token}"}


class TestWhatTheHandlerSees:
    async def test_the_header_reaches_the_handler(self) -> None:
        recorder, app = _wrapped()
        async with _client(app) as client:
            response = await client.get("/", headers={HEADER: str(ALPHA)})
        assert response.status_code == 204
        assert recorder.seen == [ALPHA]

    async def test_the_token_reaches_the_handler(self) -> None:
        recorder, app = _wrapped()
        async with _client(app) as client:
            await client.get("/", headers=_auth(BETA))
        assert recorder.seen == [BETA]

    async def test_a_request_with_neither_runs_unscoped(self) -> None:
        """Unscoped, not refused: the database is what says no."""
        recorder, app = _wrapped()
        async with _client(app) as client:
            response = await client.get("/")
        assert response.status_code == 204
        assert recorder.seen == [None]

    async def test_the_tenant_does_not_outlive_the_request(self) -> None:
        """Restored, not cleared.

        What is in scope here is the bootstrap tenant `tests/conftest.py`
        wraps the whole suite in, so this asserts the stronger of the two
        properties: the middleware puts back whatever it found rather than
        resetting to `None`. A worker that serves two tenants in turn inside
        one outer scope depends on that.
        """
        before = current_tenant_id()
        assert before is not None and before != ALPHA
        recorder, app = _wrapped()
        async with _client(app) as client:
            await client.get("/", headers={HEADER: str(ALPHA)})
        assert recorder.seen == [ALPHA]
        assert current_tenant_id() == before

    async def test_an_untrusted_header_does_not_reach_the_handler(self) -> None:
        recorder, app = _wrapped(trust_header=False)
        async with _client(app) as client:
            await client.get("/", headers={HEADER: str(ALPHA)})
        assert recorder.seen == [None]

    async def test_the_header_name_is_the_configured_one(self) -> None:
        recorder, app = _wrapped()
        async with _client(app) as client:
            await client.get("/", headers={"X-Wrong-Header": str(ALPHA)})
        assert recorder.seen == [None]

    async def test_the_header_is_matched_case_insensitively(self) -> None:
        """HTTP field names are, and Starlette's `Headers` honours that."""
        recorder, app = _wrapped()
        async with _client(app) as client:
            await client.get("/", headers={"x-tenant-id": str(ALPHA)})
        assert recorder.seen == [ALPHA]


class TestRefusals:
    async def test_a_malformed_header_is_a_400_in_the_usual_envelope(self) -> None:
        recorder, app = _wrapped()
        async with _client(app) as client:
            response = await client.get("/", headers={HEADER: "not-a-uuid"})
        assert response.status_code == 400
        assert response.json() == {
            "error": "INVALID_TENANT",
            "message": "Malformed tenant identifier",
            "status": 400,
        }
        assert recorder.seen == [], "the handler must not have run"

    async def test_a_mismatch_is_a_403(self) -> None:
        recorder, app = _wrapped()
        async with _client(app) as client:
            response = await client.get(
                "/", headers={HEADER: str(BETA), **_auth(ALPHA)}
            )
        assert response.status_code == 403
        assert response.json()["error"] == "TENANT_MISMATCH"
        assert recorder.seen == []


class TestOtherScopes:
    async def test_a_lifespan_message_passes_straight_through(self) -> None:
        """Nothing to resolve, and `Headers(scope=...)` would not work on one."""
        received: list[str] = []

        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            received.append(scope["type"])

        wrapped = TenantContextMiddleware(app, header_name=HEADER, trust_header=True)
        await wrapped({"type": "lifespan"}, _nothing, _nothing)
        assert received == ["lifespan"]

    async def test_a_websocket_handshake_carries_the_tenant(self) -> None:
        seen: list[uuid.UUID | None] = []

        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            seen.append(current_tenant_id())

        wrapped = TenantContextMiddleware(app, header_name=HEADER, trust_header=True)
        await wrapped(_ws_scope({HEADER.lower(): str(ALPHA)}), _nothing, _nothing)
        assert seen == [ALPHA]

    async def test_a_refused_websocket_is_closed_before_it_is_accepted(self) -> None:
        """There is no status line to put a 400 in; uvicorn renders this as 403."""
        sent: list[dict[str, Any]] = []
        reached = False

        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            nonlocal reached
            reached = True

        async def send(message: Any) -> None:
            sent.append(message)

        wrapped = TenantContextMiddleware(app, header_name=HEADER, trust_header=True)
        await wrapped(_ws_scope({HEADER.lower(): "nope"}), _nothing, send)

        assert sent == [{"type": "websocket.close", "code": WS_POLICY_VIOLATION}]
        assert not reached


def _ws_scope(headers: dict[str, str]) -> Scope:
    return {
        "type": "websocket",
        "path": "/api/v1/ws",
        "headers": [(k.encode(), v.encode()) for k, v in headers.items()],
    }


async def _nothing(*_args: Any, **_kwargs: Any) -> Any:
    return None


class TestAgainstTheRealApp:
    """The wiring in `src/main.py`, not the class in isolation."""

    @pytest.fixture
    def app(self) -> Any:
        from src.main import app as real_app

        return real_app

    async def test_a_malformed_tenant_is_refused_with_a_request_id(
        self, app: Any
    ) -> None:
        """Inside RequestIDMiddleware, so the refusal is logged and stamped."""
        async with _client(app) as client:
            response = await client.get("/health", headers={HEADER: "nope"})
        assert response.status_code == 400
        assert response.json()["error"] == "INVALID_TENANT"
        assert response.headers.get("X-Request-ID")

    async def test_an_ordinary_request_is_unaffected(self, app: Any) -> None:
        async with _client(app) as client:
            response = await client.get("/health", headers={HEADER: str(ALPHA)})
        assert response.status_code == 200
