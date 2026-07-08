import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from src.auth.utils import create_access_token, create_refresh_token
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


@pytest.mark.asyncio
async def test_get_current_user_valid_token() -> None:
    from src.auth.dependencies import get_current_user

    user = _make_user()
    token = create_access_token(str(user.id), user.email, user.role)
    credentials = _make_credentials(token)

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = user
    db = AsyncMock()
    db.execute = AsyncMock(return_value=result_mock)

    found = await get_current_user(credentials=credentials, db=db)
    assert found is user


@pytest.mark.asyncio
async def test_get_current_user_invalid_token_raises_401() -> None:
    from src.auth.dependencies import get_current_user

    credentials = _make_credentials("not.a.valid.token")
    db = AsyncMock()

    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(credentials=credentials, db=db)

    assert exc_info.value.status_code == 401


@pytest.mark.asyncio
async def test_get_current_user_refresh_token_raises_401() -> None:
    from src.auth.dependencies import get_current_user

    jti = str(uuid.uuid4())
    token, _ = create_refresh_token(str(uuid.uuid4()), jti)
    credentials = _make_credentials(token)
    db = AsyncMock()

    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(credentials=credentials, db=db)

    assert exc_info.value.status_code == 401
    assert "token type" in exc_info.value.detail.lower()


@pytest.mark.asyncio
async def test_get_current_user_not_found_raises_401() -> None:
    from src.auth.dependencies import get_current_user

    token = create_access_token(str(uuid.uuid4()), "gone@example.com", "user")
    credentials = _make_credentials(token)

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = None
    db = AsyncMock()
    db.execute = AsyncMock(return_value=result_mock)

    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(credentials=credentials, db=db)

    assert exc_info.value.status_code == 401
    assert "not found" in exc_info.value.detail.lower()


@pytest.mark.asyncio
async def test_get_current_user_inactive_raises_403() -> None:
    from src.auth.dependencies import get_current_user

    user = _make_user(is_active=False)
    token = create_access_token(str(user.id), user.email, user.role)
    credentials = _make_credentials(token)

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = user
    db = AsyncMock()
    db.execute = AsyncMock(return_value=result_mock)

    with pytest.raises(HTTPException) as exc_info:
        await get_current_user(credentials=credentials, db=db)

    assert exc_info.value.status_code == 403
    assert "inactive" in exc_info.value.detail.lower()


@pytest.mark.asyncio
async def test_require_role_allowed() -> None:
    from src.auth.dependencies import require_role

    user = _make_user(role="admin")
    token = create_access_token(str(user.id), user.email, user.role)
    credentials = _make_credentials(token)

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = user
    db = AsyncMock()
    db.execute = AsyncMock(return_value=result_mock)

    dep = require_role("admin", "superuser")
    found = await dep(credentials=credentials, db=db)
    assert found is user


@pytest.mark.asyncio
async def test_require_role_denied_raises_403() -> None:
    from src.auth.dependencies import require_role

    user = _make_user(role="user")
    token = create_access_token(str(user.id), user.email, user.role)
    credentials = _make_credentials(token)

    result_mock = MagicMock()
    result_mock.scalar_one_or_none.return_value = user
    db = AsyncMock()
    db.execute = AsyncMock(return_value=result_mock)

    dep = require_role("admin")
    with pytest.raises(HTTPException) as exc_info:
        await dep(credentials=credentials, db=db)

    assert exc_info.value.status_code == 403
    assert "insufficient permissions" in exc_info.value.detail.lower()


@pytest.mark.asyncio
async def test_require_role_multiple_allowed_roles() -> None:
    from src.auth.dependencies import require_role

    for role in ("editor", "moderator"):
        user = _make_user(role=role)
        token = create_access_token(str(user.id), user.email, user.role)
        credentials = _make_credentials(token)

        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = user
        db = AsyncMock()
        db.execute = AsyncMock(return_value=result_mock)

        dep = require_role("editor", "moderator", "admin")
        found = await dep(credentials=credentials, db=db)
        assert found.role == role
