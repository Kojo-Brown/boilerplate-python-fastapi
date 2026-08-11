import uuid
from datetime import UTC, datetime

import pytest

from src.auth.schemas import RegisterRequest
from src.auth.service import AuthService
from src.auth.utils import (
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_password,
    verify_password,
)
from src.exceptions import ConflictError, UnauthorizedError
from src.models.user import User
from tests.fakes import (
    InMemoryRefreshTokenStore,
    InMemoryUserStore,
    RecordingUnitOfWork,
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


# --- Service tests (in-memory stores, no session) ---
#
# `AuthService` takes protocols, so these hand it the fakes from `tests/fakes.py`
# and assert on policy. Nothing here teaches a mock how SQLAlchemy behaves; the
# repositories' own behaviour is covered in `test_repository.py`.


async def test_auth_service_register_success(
    auth_service: AuthService,
    user_store: InMemoryUserStore,
    uow: RecordingUnitOfWork,
) -> None:
    data = RegisterRequest(email="new@example.com", password="password123")

    response = await auth_service.register(data)

    assert response.email == "new@example.com"
    assert response.role == "user"
    assert response.is_active is True
    assert uow.commits == 1
    assert [u.email for u in user_store.users] == ["new@example.com"]


async def test_auth_service_register_stores_a_hash_not_the_password(
    auth_service: AuthService, user_store: InMemoryUserStore
) -> None:
    await auth_service.register(
        RegisterRequest(email="new@example.com", password="password123")
    )

    stored = user_store.users[0].hashed_password
    assert stored is not None
    assert stored != "password123"
    assert verify_password("password123", stored)


async def test_auth_service_register_duplicate_email(
    auth_service: AuthService, user_store: InMemoryUserStore, uow: RecordingUnitOfWork
) -> None:
    user_store.users.append(
        User(
            id=uuid.uuid4(),
            email="taken@example.com",
            hashed_password="hashed",
            is_active=True,
            is_verified=False,
            role="user",
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )
    )

    with pytest.raises(ConflictError, match="already registered") as exc_info:
        await auth_service.register(
            RegisterRequest(email="taken@example.com", password="password123")
        )

    assert exc_info.value.status_code == 409
    # A rejected registration must not commit a transaction it never opened.
    assert uow.commits == 0


async def test_auth_service_login_invalid_credentials(
    auth_service: AuthService,
) -> None:
    with pytest.raises(UnauthorizedError, match="Invalid credentials") as exc_info:
        await auth_service.login("nobody@example.com", "password")

    assert exc_info.value.status_code == 401


async def test_auth_service_refresh_revoked_token(
    auth_service: AuthService, token_store: InMemoryRefreshTokenStore
) -> None:
    user_id = uuid.uuid4()
    jti = str(uuid.uuid4())
    token_str, expires_at = create_refresh_token(str(user_id), jti)

    stored = await token_store.create(
        token=token_str, user_id=user_id, expires_at=expires_at
    )
    stored.revoked = True

    with pytest.raises(UnauthorizedError, match="revoked") as exc_info:
        await auth_service.refresh(token_str)

    assert exc_info.value.status_code == 401
