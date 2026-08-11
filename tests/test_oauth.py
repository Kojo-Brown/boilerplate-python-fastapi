import os
import secrets
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("GOOGLE_CLIENT_ID", "ci-placeholder-client-id")
os.environ.setdefault("GOOGLE_CLIENT_SECRET", secrets.token_hex(16))

from src.auth.service import AuthService  # noqa: E402
from src.dependencies import get_auth_service  # noqa: E402
from src.exceptions import ForbiddenError  # noqa: E402
from src.main import app  # noqa: E402
from src.models.user import User  # noqa: E402
from tests.fakes import (  # noqa: E402
    CollectingPublisher,
    InMemoryRefreshTokenStore,
    InMemoryUserStore,
    RecordingUnitOfWork,
)

# --- Service tests ---


@pytest.mark.asyncio
async def test_oauth_login_creates_new_user(
    auth_service: AuthService,
    user_store: InMemoryUserStore,
    uow: RecordingUnitOfWork,
) -> None:
    """oauth_login creates a new verified user when no existing user is found."""
    result = await auth_service.oauth_login(
        "google", "google-sub-123", "user@gmail.com"
    )

    assert result.access_token
    assert result.refresh_token
    assert result.token_type == "bearer"
    assert uow.commits == 1

    created = user_store.users[0]
    assert created.email == "user@gmail.com"
    assert created.oauth_provider == "google"
    assert created.oauth_sub == "google-sub-123"
    # The provider already proved the address; a confirmation mail would ask the
    # user to verify something Google just verified.
    assert created.is_verified is True
    # An OAuth-only account has no password, and must not be given a usable one.
    assert created.hashed_password is None


@pytest.mark.asyncio
async def test_oauth_login_links_existing_email_account(
    auth_service: AuthService, user_store: InMemoryUserStore
) -> None:
    """oauth_login links an OAuth identity to an existing email-only account."""
    existing_user = User(
        id=uuid.uuid4(),
        email="user@gmail.com",
        hashed_password="hashed",
        is_active=True,
        is_verified=False,
        role="user",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )

    user_store.users.append(existing_user)

    result = await auth_service.oauth_login(
        "google", "google-sub-456", "user@gmail.com"
    )

    assert result.access_token
    # Linked, not duplicated: a second row for the same address would be two
    # accounts one person can log into by picking a different button.
    assert len(user_store.users) == 1
    assert existing_user.oauth_provider == "google"
    assert existing_user.oauth_sub == "google-sub-456"
    assert existing_user.is_verified is True


@pytest.mark.asyncio
async def test_oauth_login_finds_existing_oauth_user(
    auth_service: AuthService, user_store: InMemoryUserStore
) -> None:
    """oauth_login reuses a user already linked to the given oauth_sub."""
    existing_user = User(
        id=uuid.uuid4(),
        email="user@gmail.com",
        hashed_password=None,
        is_active=True,
        is_verified=True,
        role="user",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        oauth_provider="google",
        oauth_sub="google-sub-789",
    )

    user_store.users.append(existing_user)

    result = await auth_service.oauth_login(
        "google", "google-sub-789", "user@gmail.com"
    )

    assert result.access_token
    assert result.refresh_token
    assert len(user_store.users) == 1


@pytest.mark.asyncio
async def test_oauth_login_inactive_user_raises(
    auth_service: AuthService, user_store: InMemoryUserStore
) -> None:
    """oauth_login raises ForbiddenError when the matched account is inactive."""
    inactive_user = User(
        id=uuid.uuid4(),
        email="inactive@gmail.com",
        hashed_password=None,
        is_active=False,
        is_verified=True,
        role="user",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        oauth_provider="google",
        oauth_sub="google-sub-inactive",
    )

    user_store.users.append(inactive_user)

    with pytest.raises(ForbiddenError, match="inactive") as exc_info:
        await auth_service.oauth_login(
            "google", "google-sub-inactive", "inactive@gmail.com"
        )

    # Credentials were valid; the account is switched off. Retrying cannot help,
    # so this is 403 and not 401 — on this path and on /auth/login alike.
    assert exc_info.value.status_code == 403


# --- Route tests ---


@pytest.mark.asyncio
async def test_google_login_initiates_redirect(async_client: AsyncClient) -> None:
    """GET /auth/google returns a redirect to the Google OAuth consent screen."""
    from starlette.responses import RedirectResponse

    mock_redirect = RedirectResponse(
        url="https://accounts.google.com/o/oauth2/auth?client_id=test"
    )

    with patch("src.auth.router.oauth") as mock_oauth:
        mock_oauth.google.authorize_redirect = AsyncMock(return_value=mock_redirect)
        response = await async_client.get("/api/v1/auth/google", follow_redirects=False)

    assert response.status_code in {301, 302, 303, 307, 308}


@pytest.mark.asyncio
async def test_google_callback_returns_tokens() -> None:
    """GET /auth/google/callback issues JWT tokens after a successful OAuth exchange."""
    service = AuthService(
        users=InMemoryUserStore(),
        tokens=InMemoryRefreshTokenStore(),
        uow=RecordingUnitOfWork(),
        events=CollectingPublisher(),
    )
    app.dependency_overrides[get_auth_service] = lambda: service

    mock_token = {
        "userinfo": {
            "sub": "google-user-123",
            "email": "user@gmail.com",
            "name": "Test User",
            "email_verified": True,
        }
    }

    try:
        with patch("src.auth.router.oauth") as mock_oauth:
            mock_oauth.google.authorize_access_token = AsyncMock(
                return_value=mock_token
            )
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.get("/api/v1/auth/google/callback")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert "refresh_token" in data
    assert data["token_type"] == "bearer"


@pytest.mark.asyncio
async def test_google_callback_oauth_error_returns_400() -> None:
    """GET /auth/google/callback returns 400 when the OAuth token exchange fails."""
    try:
        with patch("src.auth.router.oauth") as mock_oauth:
            mock_oauth.google.authorize_access_token = AsyncMock(
                side_effect=Exception("invalid_grant")
            )
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.get("/api/v1/auth/google/callback")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 400
    assert "OAuth error" in response.json()["message"]
