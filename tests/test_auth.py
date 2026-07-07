import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.auth.schemas import RegisterRequest
from src.auth.utils import (
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_password,
    verify_password,
)


# --- Utility tests (no DB needed) ---


def test_hash_password_returns_different_string() -> None:
    plain = "supersecret123"
    hashed = hash_password(plain)
    assert hashed != plain
    assert len(hashed) > 0


def test_verify_password_correct() -> None:
    plain = "supersecret123"
    hashed = hash_password(plain)
    assert verify_password(plain, hashed) is True


def test_verify_password_wrong() -> None:
    hashed = hash_password("correct-password")
    assert verify_password("wrong-password", hashed) is False


def test_verify_password_invalid_hash() -> None:
    assert verify_password("any", "not-a-valid-hash") is False


def test_create_and_decode_access_token() -> None:
    user_id = str(uuid.uuid4())
    token = create_access_token(user_id, "user@example.com", "user")
    payload = decode_token(token)

    assert payload["sub"] == user_id
    assert payload["email"] == "user@example.com"
    assert payload["role"] == "user"
    assert payload["type"] == "access"


def test_access_token_has_expiry() -> None:
    token = create_access_token("123", "a@b.com", "user")
    payload = decode_token(token)
    assert "exp" in payload


def test_create_and_decode_refresh_token() -> None:
    user_id = str(uuid.uuid4())
    jti = str(uuid.uuid4())
    token, expires_at = create_refresh_token(user_id, jti)

    payload = decode_token(token)
    assert payload["sub"] == user_id
    assert payload["jti"] == jti
    assert payload["type"] == "refresh"
    assert expires_at > datetime.now(UTC)


def test_decode_invalid_token_raises() -> None:
    with pytest.raises(ValueError, match="Invalid token"):
        decode_token("not.a.valid.token")


def test_decode_tampered_token_raises() -> None:
    token = create_access_token("id", "a@b.com", "user")
    tampered = token[:-4] + "xxxx"
    with pytest.raises(ValueError):
        decode_token(tampered)


# --- Service tests (mocked DB) ---


@pytest.mark.asyncio
async def test_auth_service_register_success() -> None:
    from src.auth.service import AuthService
    from src.models.user import User

    db = AsyncMock()

    # First execute: check existing user → None
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    db.execute = AsyncMock(return_value=result_mock)

    user_instance = User(
        id=uuid.uuid4(),
        email="new@example.com",
        hashed_password="hashed",
        is_active=True,
        is_verified=False,
        role="user",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    db.refresh = AsyncMock(side_effect=lambda u: None)
    db.add = MagicMock()
    db.commit = AsyncMock()

    # After commit+refresh, the user object should have correct values
    service = AuthService(db)
    data = RegisterRequest(email="new@example.com", password="password123")

    # Patch refresh to populate user attributes from user_instance
    async def fake_refresh(obj: object) -> None:
        if isinstance(obj, User):
            obj.id = user_instance.id
            obj.created_at = user_instance.created_at
            obj.updated_at = user_instance.updated_at

    db.refresh = AsyncMock(side_effect=fake_refresh)

    response = await service.register(data)
    assert response.email == "new@example.com"
    assert response.role == "user"
    assert response.is_active is True
    db.commit.assert_called_once()


@pytest.mark.asyncio
async def test_auth_service_register_duplicate_email() -> None:
    from src.auth.service import AuthService
    from src.models.user import User

    db = AsyncMock()
    existing = User(
        id=uuid.uuid4(),
        email="taken@example.com",
        hashed_password="hashed",
        is_active=True,
        is_verified=False,
        role="user",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = existing
    db.execute = AsyncMock(return_value=result_mock)

    service = AuthService(db)
    data = RegisterRequest(email="taken@example.com", password="password123")

    with pytest.raises(ValueError, match="already registered"):
        await service.register(data)


@pytest.mark.asyncio
async def test_auth_service_login_invalid_credentials() -> None:
    from src.auth.service import AuthService

    db = AsyncMock()
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    db.execute = AsyncMock(return_value=result_mock)

    service = AuthService(db)
    with pytest.raises(ValueError, match="Invalid credentials"):
        await service.login("nobody@example.com", "password")


@pytest.mark.asyncio
async def test_auth_service_refresh_revoked_token() -> None:
    from src.auth.service import AuthService
    from src.models.refresh_token import RefreshToken

    user_id = uuid.uuid4()
    jti = str(uuid.uuid4())
    token_str, expires_at = create_refresh_token(str(user_id), jti)

    revoked = RefreshToken(
        id=uuid.uuid4(),
        token=token_str,
        user_id=user_id,
        expires_at=expires_at,
        revoked=True,
        created_at=datetime.now(UTC),
    )

    db = AsyncMock()
    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = revoked
    db.execute = AsyncMock(return_value=result_mock)

    service = AuthService(db)
    with pytest.raises(ValueError, match="revoked"):
        await service.refresh(token_str)
