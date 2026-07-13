"""
Pytest + HTTPX async test client patterns for FastAPI.

Demonstrates how to test every layer of the HTTP contract without a real
database or network connection:

  - ASGITransport spins the app up in-process (no socket, no server process)
  - Dependency overrides swap out get_db and get_current_user per test
  - MagicMock / AsyncMock stubs DB results without a running PostgreSQL instance
  - patch() replaces third-party calls (S3, Celery) that have their own test modules

Fixtures are defined in conftest.py and shared across all test modules:
  async_client          – unauthenticated client with mock DB
  authenticated_client  – client carrying a valid user JWT + get_current_user override
  admin_client          – same but with an admin JWT
  mock_db               – AsyncMock session; configure .execute per test as needed
  mock_user / mock_admin – ready-made User ORM objects
  auth_headers / admin_headers – raw {"Authorization": "Bearer ..."} dicts
"""
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient

from src.auth.password import hash_password
from src.auth.utils import create_refresh_token
from src.models.refresh_token import RefreshToken
from src.models.user import User


# ── Helpers ───────────────────────────────────────────────────────────────────


def _result_mock(value: object) -> MagicMock:
    """Return a MagicMock whose .scalar_one_or_none() returns *value*."""
    m = MagicMock()
    m.scalar_one_or_none.return_value = value
    return m


def _make_user(
    *,
    email: str = "test@example.com",
    password: str = "password123",
    role: str = "user",
    is_active: bool = True,
) -> User:
    return User(
        id=uuid.uuid4(),
        email=email,
        hashed_password=hash_password(password),
        is_active=is_active,
        is_verified=True,
        role=role,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )


# ── Health ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_health_returns_ok(async_client: AsyncClient) -> None:
    response = await async_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_health_returns_json_content_type(async_client: AsyncClient) -> None:
    response = await async_client.get("/health")

    assert "application/json" in response.headers["content-type"]


# ── Registration ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_register_new_user_returns_201(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    mock_db.execute = AsyncMock(return_value=_result_mock(None))

    response = await async_client.post(
        "/api/v1/auth/register",
        json={"email": "new@example.com", "password": "password123"},
    )

    assert response.status_code == 201


@pytest.mark.asyncio
async def test_register_response_shape(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    mock_db.execute = AsyncMock(return_value=_result_mock(None))

    response = await async_client.post(
        "/api/v1/auth/register",
        json={"email": "new@example.com", "password": "password123"},
    )

    body = response.json()
    assert body["email"] == "new@example.com"
    assert body["role"] == "user"
    assert body["is_active"] is True
    assert body["is_verified"] is False
    assert "id" in body


@pytest.mark.asyncio
async def test_register_duplicate_email_returns_400(
    async_client: AsyncClient, mock_db: AsyncMock, mock_user: User
) -> None:
    mock_db.execute = AsyncMock(return_value=_result_mock(mock_user))

    response = await async_client.post(
        "/api/v1/auth/register",
        json={"email": mock_user.email, "password": "password123"},
    )

    assert response.status_code == 400
    assert "already registered" in response.json()["detail"]


@pytest.mark.asyncio
async def test_register_password_too_short_returns_422(
    async_client: AsyncClient,
) -> None:
    response = await async_client.post(
        "/api/v1/auth/register",
        json={"email": "user@example.com", "password": "short"},
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_register_invalid_email_returns_422(
    async_client: AsyncClient,
) -> None:
    response = await async_client.post(
        "/api/v1/auth/register",
        json={"email": "not-an-email", "password": "password123"},
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_register_missing_body_returns_422(
    async_client: AsyncClient,
) -> None:
    response = await async_client.post("/api/v1/auth/register", json={})

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_register_validation_error_body_has_detail(
    async_client: AsyncClient,
) -> None:
    response = await async_client.post(
        "/api/v1/auth/register",
        json={"email": "bad"},
    )

    assert response.status_code == 422
    assert "detail" in response.json()


# ── Login ─────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_login_success_returns_token_pair(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    user = _make_user(email="user@example.com", password="password123")
    mock_db.execute = AsyncMock(return_value=_result_mock(user))

    response = await async_client.post(
        "/api/v1/auth/login",
        json={"email": "user@example.com", "password": "password123"},
    )

    assert response.status_code == 200
    body = response.json()
    assert "access_token" in body
    assert "refresh_token" in body
    assert body["token_type"] == "bearer"


@pytest.mark.asyncio
async def test_login_wrong_password_returns_401(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    user = _make_user(email="user@example.com", password="correct-password")
    mock_db.execute = AsyncMock(return_value=_result_mock(user))

    response = await async_client.post(
        "/api/v1/auth/login",
        json={"email": "user@example.com", "password": "wrong-password"},
    )

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_login_unknown_email_returns_401(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    mock_db.execute = AsyncMock(return_value=_result_mock(None))

    response = await async_client.post(
        "/api/v1/auth/login",
        json={"email": "nobody@example.com", "password": "password123"},
    )

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_login_inactive_user_returns_401(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    user = _make_user(email="user@example.com", password="password123", is_active=False)
    mock_db.execute = AsyncMock(return_value=_result_mock(user))

    response = await async_client.post(
        "/api/v1/auth/login",
        json={"email": "user@example.com", "password": "password123"},
    )

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_login_missing_fields_returns_422(
    async_client: AsyncClient,
) -> None:
    response = await async_client.post("/api/v1/auth/login", json={})

    assert response.status_code == 422


# ── Refresh token ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_refresh_malformed_token_returns_401(
    async_client: AsyncClient,
) -> None:
    response = await async_client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": "not.a.valid.token"},
    )

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_refresh_access_token_instead_of_refresh_returns_401(
    async_client: AsyncClient, auth_headers: dict[str, str]
) -> None:
    access_token = auth_headers["Authorization"].split(" ", 1)[1]

    response = await async_client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": access_token},
    )

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_refresh_revoked_token_returns_401(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    user_id = uuid.uuid4()
    token_str, expires_at = create_refresh_token(str(user_id), str(uuid.uuid4()))
    stored = RefreshToken(
        id=uuid.uuid4(),
        token=token_str,
        user_id=user_id,
        expires_at=expires_at,
        revoked=True,
        created_at=datetime.now(UTC),
    )
    mock_db.execute = AsyncMock(return_value=_result_mock(stored))

    response = await async_client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": token_str},
    )

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_refresh_missing_field_returns_422(
    async_client: AsyncClient,
) -> None:
    response = await async_client.post("/api/v1/auth/refresh", json={})

    assert response.status_code == 422


# ── Logout ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_logout_valid_token_returns_204(
    async_client: AsyncClient, mock_db: AsyncMock
) -> None:
    user_id = uuid.uuid4()
    token_str, expires_at = create_refresh_token(str(user_id), str(uuid.uuid4()))
    stored = RefreshToken(
        id=uuid.uuid4(),
        token=token_str,
        user_id=user_id,
        expires_at=expires_at,
        revoked=False,
        created_at=datetime.now(UTC),
    )
    mock_db.execute = AsyncMock(return_value=_result_mock(stored))

    response = await async_client.post(
        "/api/v1/auth/logout",
        json={"refresh_token": token_str},
    )

    assert response.status_code == 204


@pytest.mark.asyncio
async def test_logout_missing_field_returns_422(
    async_client: AsyncClient,
) -> None:
    response = await async_client.post("/api/v1/auth/logout", json={})

    assert response.status_code == 422


# ── Auth guards (protected endpoints) ────────────────────────────────────────


@pytest.mark.asyncio
async def test_protected_upload_without_token_returns_403(
    async_client: AsyncClient,
) -> None:
    response = await async_client.post(
        "/api/v1/uploads/presigned-upload",
        json={
            "folder": "images",
            "filename": "photo.jpg",
            "content_type": "image/jpeg",
        },
    )
    # HTTPBearer returns 403 when the Authorization header is absent
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_protected_upload_with_invalid_token_returns_401(
    async_client: AsyncClient,
) -> None:
    response = await async_client.post(
        "/api/v1/uploads/presigned-upload",
        json={
            "folder": "images",
            "filename": "photo.jpg",
            "content_type": "image/jpeg",
        },
        headers={"Authorization": "Bearer bad.token.here"},
    )

    assert response.status_code == 401


@pytest.mark.asyncio
async def test_protected_upload_with_valid_token_returns_200(
    authenticated_client: AsyncClient,
) -> None:
    presigned: dict[str, object] = {
        "key": "images/abc.jpg",
        "url": "https://bucket.s3.amazonaws.com/",
        "fields": {"Content-Type": "image/jpeg", "key": "images/abc.jpg"},
        "expires_in": 3600,
    }
    with patch("src.api.v1.uploads.generate_presigned_upload", return_value=presigned):
        response = await authenticated_client.post(
            "/api/v1/uploads/presigned-upload",
            json={
                "folder": "images",
                "filename": "photo.jpg",
                "content_type": "image/jpeg",
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["key"] == "images/abc.jpg"
    assert "url" in body
    assert "fields" in body
    assert body["expires_in"] == 3600


@pytest.mark.asyncio
async def test_protected_download_with_valid_token_returns_200(
    authenticated_client: AsyncClient,
) -> None:
    presigned: dict[str, object] = {
        "url": "https://bucket.s3.amazonaws.com/images/abc.jpg?Signature=xyz",
        "expires_in": 3600,
    }
    with patch(
        "src.api.v1.uploads.generate_presigned_download", return_value=presigned
    ):
        response = await authenticated_client.post(
            "/api/v1/uploads/presigned-download",
            json={"key": "images/abc.jpg"},
        )

    assert response.status_code == 200
    body = response.json()
    assert "url" in body
    assert body["expires_in"] == 3600


# ── OpenAPI schema ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_openapi_schema_is_accessible(async_client: AsyncClient) -> None:
    response = await async_client.get("/openapi.json")

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_openapi_schema_has_correct_title(async_client: AsyncClient) -> None:
    schema = (await async_client.get("/openapi.json")).json()

    assert schema["info"]["title"] == "boilerplate-python-fastapi"


@pytest.mark.asyncio
async def test_openapi_schema_includes_auth_routes(async_client: AsyncClient) -> None:
    paths = (await async_client.get("/openapi.json")).json()["paths"]

    assert "/api/v1/auth/register" in paths
    assert "/api/v1/auth/login" in paths
    assert "/api/v1/auth/refresh" in paths
    assert "/api/v1/auth/logout" in paths


@pytest.mark.asyncio
async def test_openapi_schema_includes_upload_routes(async_client: AsyncClient) -> None:
    paths = (await async_client.get("/openapi.json")).json()["paths"]

    assert "/api/v1/uploads/presigned-upload" in paths
    assert "/api/v1/uploads/presigned-download" in paths


# ── 404 / not found ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_unknown_root_route_returns_404(async_client: AsyncClient) -> None:
    response = await async_client.get("/nonexistent")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_unknown_v1_route_returns_404(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/nonexistent")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_wrong_http_method_returns_405(async_client: AsyncClient) -> None:
    response = await async_client.get("/api/v1/auth/register")

    assert response.status_code == 405
