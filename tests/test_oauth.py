import os
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("GOOGLE_CLIENT_ID", "test-google-client-id")
os.environ.setdefault("GOOGLE_CLIENT_SECRET", "test-google-client-secret")

from src.database import get_db  # noqa: E402
from src.main import app  # noqa: E402
from src.models.user import User  # noqa: E402


# --- Service tests ---


@pytest.mark.asyncio
async def test_oauth_login_creates_new_user() -> None:
    """oauth_login creates a new verified user when no existing user is found."""
    from src.auth.service import AuthService

    db = AsyncMock()
    no_result = MagicMock()
    no_result.scalar_one_or_none.return_value = None
    db.execute = AsyncMock(return_value=no_result)
    db.add = MagicMock()
    db.flush = AsyncMock()
    db.commit = AsyncMock()

    service = AuthService(db)
    result = await service.oauth_login("google", "google-sub-123", "user@gmail.com")

    assert result.access_token
    assert result.refresh_token
    assert result.token_type == "bearer"
    db.commit.assert_called_once()


@pytest.mark.asyncio
async def test_oauth_login_links_existing_email_account() -> None:
    """oauth_login links an OAuth identity to an existing email-only account."""
    from src.auth.service import AuthService

    db = AsyncMock()
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

    no_result = MagicMock()
    no_result.scalar_one_or_none.return_value = None
    found_result = MagicMock()
    found_result.scalar_one_or_none.return_value = existing_user

    db.execute = AsyncMock(side_effect=[no_result, found_result])
    db.add = MagicMock()
    db.flush = AsyncMock()
    db.commit = AsyncMock()

    service = AuthService(db)
    result = await service.oauth_login("google", "google-sub-456", "user@gmail.com")

    assert result.access_token
    assert existing_user.oauth_provider == "google"
    assert existing_user.oauth_sub == "google-sub-456"
    assert existing_user.is_verified is True


@pytest.mark.asyncio
async def test_oauth_login_finds_existing_oauth_user() -> None:
    """oauth_login reuses a user already linked to the given oauth_sub."""
    from src.auth.service import AuthService

    db = AsyncMock()
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

    found_result = MagicMock()
    found_result.scalar_one_or_none.return_value = existing_user
    db.execute = AsyncMock(return_value=found_result)
    db.add = MagicMock()
    db.flush = AsyncMock()
    db.commit = AsyncMock()

    service = AuthService(db)
    result = await service.oauth_login("google", "google-sub-789", "user@gmail.com")

    assert result.access_token
    assert result.refresh_token


@pytest.mark.asyncio
async def test_oauth_login_inactive_user_raises() -> None:
    """oauth_login raises ValueError when the matched user account is inactive."""
    from src.auth.service import AuthService

    db = AsyncMock()
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

    found_result = MagicMock()
    found_result.scalar_one_or_none.return_value = inactive_user
    db.execute = AsyncMock(return_value=found_result)

    service = AuthService(db)
    with pytest.raises(ValueError, match="inactive"):
        await service.oauth_login("google", "google-sub-inactive", "inactive@gmail.com")


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
        response = await async_client.get("/auth/google", follow_redirects=False)

    assert response.status_code in {301, 302, 303, 307, 308}


@pytest.mark.asyncio
async def test_google_callback_returns_tokens() -> None:
    """GET /auth/google/callback issues JWT tokens after a successful OAuth exchange."""
    db = AsyncMock()
    no_result = MagicMock()
    no_result.scalar_one_or_none.return_value = None
    db.execute = AsyncMock(return_value=no_result)
    db.add = MagicMock()
    db.flush = AsyncMock()
    db.commit = AsyncMock()

    async def override_get_db() -> AsyncMock:
        yield db

    app.dependency_overrides[get_db] = override_get_db

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
                response = await client.get("/auth/google/callback")
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
    db = AsyncMock()

    async def override_get_db() -> AsyncMock:
        yield db

    app.dependency_overrides[get_db] = override_get_db

    try:
        with patch("src.auth.router.oauth") as mock_oauth:
            mock_oauth.google.authorize_access_token = AsyncMock(
                side_effect=Exception("invalid_grant")
            )
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://test"
            ) as client:
                response = await client.get("/auth/google/callback")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 400
    assert "OAuth error" in response.json()["detail"]
