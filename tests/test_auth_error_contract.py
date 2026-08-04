"""The error contract the SOLID audit fixed in place (see ``docs/solid.md``).

Each test here pins one property that used to depend on which route the caller
happened to enter through, because ``AuthService`` signalled every rejection as
a bare ``ValueError`` and each router re-derived a status code from it.

The rate limiter's storage is process-wide and these tests share ``/auth/login``
with ``tests/test_api_client.py``, so the bucket is cleared around every test —
otherwise the first request here would arrive already over the 5/minute limit.
"""

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from src.auth.password import hash_password
from src.database import get_db
from src.limiter import limiter
from src.main import app
from src.models.user import User
from tests.conftest import apply_column_defaults


@pytest.fixture(autouse=True)
def reset_limiter() -> AsyncGenerator[None, None]:
    def _clear() -> None:
        storage = limiter._limiter
        if hasattr(storage, "storage") and hasattr(storage.storage, "reset"):
            storage.storage.reset()

    _clear()
    yield
    _clear()


def _result_mock(value: object) -> MagicMock:
    m = MagicMock()
    m.scalar_one_or_none.return_value = value
    return m


def _user(*, is_active: bool = True) -> User:
    user = User(
        email="user@example.com",
        hashed_password=hash_password("password123"),
        is_active=is_active,
        is_verified=True,
    )
    apply_column_defaults(user)
    return user


@pytest.mark.asyncio
async def test_inactive_account_is_forbidden_not_unauthorized(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    """The same condition gets the same answer on every entry path.

    ``get_current_user`` and the OAuth callback have always returned 403 for a
    switched-off account. ``/auth/login`` returned 401 purely because its own
    ``except ValueError`` block named 401, and it could not tell "wrong
    password" from "account disabled".
    """
    mock_db.execute = AsyncMock(return_value=_result_mock(_user(is_active=False)))

    response = await async_client.post(
        "/api/v1/auth/login",
        json={"email": "user@example.com", "password": "password123"},
    )

    assert response.status_code == 403
    assert response.json()["error"] == "FORBIDDEN"


@pytest.mark.asyncio
async def test_auth_failures_report_their_own_error_code(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    """Clients can branch on the error code instead of parsing the message.

    Re-raising as ``HTTPException`` routed these through the generic HTTP
    handler, so every auth failure — conflict, bad password, expired token —
    arrived labelled ``HTTP_ERROR``.
    """
    mock_db.execute = AsyncMock(return_value=_result_mock(_user()))

    response = await async_client.post(
        "/api/v1/auth/login",
        json={"email": "user@example.com", "password": "wrong-password"},
    )

    assert response.status_code == 401
    assert response.json()["error"] == "UNAUTHORIZED"


@pytest.mark.asyncio
async def test_unauthorized_responses_carry_the_bearer_challenge(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    """RFC 9110 requires a 401 to name the scheme; the class supplies it.

    All five hand-written 401s did carry the header, and nothing made them —
    ``UnauthorizedError`` now does, so this holds for every future one too.
    """
    mock_db.execute = AsyncMock(return_value=_result_mock(None))

    response = await async_client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": "not.a.real.token"},
    )

    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"


@pytest.mark.asyncio
async def test_incidental_value_error_is_not_laundered_into_a_401(
    mock_db: AsyncMock,
) -> None:
    """An internal failure must not be reported to the client as a rejection.

    ``except ValueError`` caught far more than the service meant to raise —
    ``pydantic.ValidationError`` is a ``ValueError``, so a response the API
    failed to serialise came back as a crisp 400 or 401 naming the *client* as
    the problem. It is a 500 now, and the traceback reaches the logs.
    """

    async def _override_db() -> AsyncGenerator[AsyncMock, None]:
        yield mock_db

    app.dependency_overrides[get_db] = _override_db
    try:
        with patch(
            "src.auth.service.AuthService.login",
            side_effect=ValueError("upstream serialiser blew up"),
        ):
            transport = ASGITransport(app=app, raise_app_exceptions=False)
            async with AsyncClient(transport=transport, base_url="http://test") as c:
                response = await c.post(
                    "/api/v1/auth/login",
                    json={"email": "user@example.com", "password": "password123"},
                )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 500
    body = response.json()
    assert body["error"] == "INTERNAL_SERVER_ERROR"
    # The generic message must not leak the internal failure to the caller.
    assert "serialiser" not in body["message"]
