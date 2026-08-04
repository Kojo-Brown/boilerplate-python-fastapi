import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from fastapi.security import HTTPAuthorizationCredentials

from src.auth.utils import create_access_token, create_refresh_token
from src.exceptions import ForbiddenError, UnauthorizedError
from src.models.user import User


def _make_user(
    role: str = "user",
    is_active: bool = True,
) -> User:
    return User(
        id=uuid.uuid4(),
        email="test@example.com",
        hashed_password="hashed",
        is_active=is_active,
        is_verified=True,
        role=role,
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )


def _make_credentials(token: str) -> HTTPAuthorizationCredentials:
    return HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)


def _db_returning(user: User | None) -> AsyncMock:
    """Session stub for the lookup ``UserRepository.get`` performs.

    The dependency resolves the caller through the repository rather than
    assembling its own ``select(User)``, so what a test has to stand in for is
    ``session.get(User, id)`` — a primary-key load — not an arbitrary query.
    """
    db = AsyncMock()
    db.get = AsyncMock(return_value=user)
    return db


@pytest.mark.asyncio
async def test_get_current_user_valid_token() -> None:
    from src.auth.dependencies import get_current_user

    user = _make_user()
    token = create_access_token(str(user.id), user.email, user.role)
    db = _db_returning(user)

    found = await get_current_user(credentials=_make_credentials(token), db=db)

    assert found is user
    # The subject claim is a string in the JWT and a UUID in the database; the
    # dependency owns that conversion.
    db.get.assert_awaited_once_with(User, user.id)


@pytest.mark.asyncio
async def test_get_current_user_invalid_token_raises_401() -> None:
    from src.auth.dependencies import get_current_user

    credentials = _make_credentials("not.a.valid.token")

    with pytest.raises(UnauthorizedError) as exc_info:
        await get_current_user(credentials=credentials, db=_db_returning(None))

    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_get_current_user_refresh_token_raises_401() -> None:
    from src.auth.dependencies import get_current_user

    jti = str(uuid.uuid4())
    token, _ = create_refresh_token(str(uuid.uuid4()), jti)
    credentials = _make_credentials(token)

    with pytest.raises(UnauthorizedError) as exc_info:
        await get_current_user(credentials=credentials, db=_db_returning(None))

    assert exc_info.value.status_code == 401
    assert "token type" in exc_info.value.message.lower()


@pytest.mark.asyncio
async def test_get_current_user_not_found_raises_401() -> None:
    from src.auth.dependencies import get_current_user

    token = create_access_token(str(uuid.uuid4()), "gone@example.com", "user")
    credentials = _make_credentials(token)

    with pytest.raises(UnauthorizedError) as exc_info:
        await get_current_user(credentials=credentials, db=_db_returning(None))

    assert exc_info.value.status_code == 401
    assert "not found" in exc_info.value.message.lower()


@pytest.mark.asyncio
async def test_get_current_user_non_uuid_subject_raises_401() -> None:
    """A signed token whose ``sub`` is not a user id is rejected, not queried.

    Passing the raw claim through to the database made Postgres reject it
    instead, which surfaces as a 500 for what is plainly an unusable credential.
    """
    from src.auth.dependencies import get_current_user

    token = create_access_token("not-a-uuid", "someone@example.com", "user")
    db = _db_returning(_make_user())

    with pytest.raises(UnauthorizedError) as exc_info:
        await get_current_user(credentials=_make_credentials(token), db=db)

    assert exc_info.value.status_code == 401
    db.get.assert_not_awaited()


@pytest.mark.asyncio
async def test_get_current_user_inactive_raises_403() -> None:
    from src.auth.dependencies import get_current_user

    user = _make_user(is_active=False)
    token = create_access_token(str(user.id), user.email, user.role)
    credentials = _make_credentials(token)

    with pytest.raises(ForbiddenError) as exc_info:
        await get_current_user(credentials=credentials, db=_db_returning(user))

    assert exc_info.value.status_code == 403
    assert "inactive" in exc_info.value.message.lower()


@pytest.mark.asyncio
async def test_require_role_allowed() -> None:
    from src.auth.dependencies import get_current_user, require_role

    user = _make_user(role="admin")
    token = create_access_token(str(user.id), user.email, user.role)

    # require_role guards an already-authenticated user; FastAPI resolves that
    # user via Depends(get_current_user), so do the same explicitly here.
    current = await get_current_user(
        credentials=_make_credentials(token), db=_db_returning(user)
    )
    dep = require_role("admin", "superuser")
    found = await dep(current_user=current)
    assert found is user


@pytest.mark.asyncio
async def test_require_role_denied_raises_403() -> None:
    from src.auth.dependencies import get_current_user, require_role

    user = _make_user(role="user")
    token = create_access_token(str(user.id), user.email, user.role)

    current = await get_current_user(
        credentials=_make_credentials(token), db=_db_returning(user)
    )
    dep = require_role("admin")
    with pytest.raises(ForbiddenError) as exc_info:
        await dep(current_user=current)

    assert exc_info.value.status_code == 403
    assert "insufficient permissions" in exc_info.value.message.lower()


@pytest.mark.asyncio
async def test_require_role_multiple_allowed_roles() -> None:
    from src.auth.dependencies import get_current_user, require_role

    for role in ("editor", "moderator"):
        user = _make_user(role=role)
        token = create_access_token(str(user.id), user.email, user.role)

        current = await get_current_user(
            credentials=_make_credentials(token), db=_db_returning(user)
        )
        dep = require_role("editor", "moderator", "admin")
        found = await dep(current_user=current)
        assert found.role == role
