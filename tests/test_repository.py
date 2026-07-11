"""Tests for BaseRepository, UserRepository, and RefreshTokenRepository."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.repositories.base import BaseRepository
from src.repositories.refresh_token import RefreshTokenRepository
from src.repositories.user import UserRepository


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_session() -> AsyncMock:
    session = AsyncMock()
    session.add = MagicMock()
    session.delete = AsyncMock()
    session.flush = AsyncMock()
    session.refresh = AsyncMock()
    return session


def _make_user(
    *,
    id: uuid.UUID | None = None,
    email: str = "user@example.com",
    is_active: bool = True,
    role: str = "user",
    oauth_provider: str | None = None,
    oauth_sub: str | None = None,
) -> MagicMock:
    user = MagicMock()
    user.id = id or uuid.uuid4()
    user.email = email
    user.is_active = is_active
    user.role = role
    user.hashed_password = "hashed"
    user.oauth_provider = oauth_provider
    user.oauth_sub = oauth_sub
    return user


def _make_token(
    *,
    id: uuid.UUID | None = None,
    token: str = "tok",
    user_id: uuid.UUID | None = None,
    revoked: bool = False,
    expires_at: datetime | None = None,
) -> MagicMock:
    rt = MagicMock()
    rt.id = id or uuid.uuid4()
    rt.token = token
    rt.user_id = user_id or uuid.uuid4()
    rt.revoked = revoked
    rt.expires_at = expires_at or datetime.now(UTC) + timedelta(days=7)
    return rt


# ---------------------------------------------------------------------------
# BaseRepository
# ---------------------------------------------------------------------------


class TestBaseRepository:
    def test_init_stores_session_and_model(self) -> None:
        from src.models.user import User

        session = _make_session()
        repo: BaseRepository[User] = BaseRepository(session, User)
        assert repo.session is session
        assert repo._model is User

    @pytest.mark.asyncio
    async def test_get_delegates_to_session_get(self) -> None:
        from src.models.user import User

        session = _make_session()
        expected = _make_user()
        session.get = AsyncMock(return_value=expected)

        repo: BaseRepository[User] = BaseRepository(session, User)
        pk = expected.id
        result = await repo.get(pk)

        session.get.assert_awaited_once_with(User, pk)
        assert result is expected

    @pytest.mark.asyncio
    async def test_get_returns_none_when_missing(self) -> None:
        from src.models.user import User

        session = _make_session()
        session.get = AsyncMock(return_value=None)

        repo: BaseRepository[User] = BaseRepository(session, User)
        assert await repo.get(uuid.uuid4()) is None

    @pytest.mark.asyncio
    async def test_get_by_returns_first_match(self) -> None:
        from src.models.user import User

        session = _make_session()
        expected = _make_user(email="a@b.com")
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = expected
        session.execute = AsyncMock(return_value=mock_result)

        repo: BaseRepository[User] = BaseRepository(session, User)
        result = await repo.get_by(email="a@b.com")
        assert result is expected

    @pytest.mark.asyncio
    async def test_list_returns_all_scalars(self) -> None:
        from src.models.user import User

        session = _make_session()
        users = [_make_user(), _make_user()]
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = users
        mock_result = MagicMock()
        mock_result.scalars.return_value = mock_scalars
        session.execute = AsyncMock(return_value=mock_result)

        repo: BaseRepository[User] = BaseRepository(session, User)
        result = await repo.list(limit=10, offset=0)
        assert result == users

    @pytest.mark.asyncio
    async def test_count_returns_scalar(self) -> None:
        from src.models.user import User

        session = _make_session()
        mock_result = MagicMock()
        mock_result.scalar_one.return_value = 42
        session.execute = AsyncMock(return_value=mock_result)

        repo: BaseRepository[User] = BaseRepository(session, User)
        assert await repo.count() == 42

    @pytest.mark.asyncio
    async def test_create_adds_flushes_and_refreshes(self) -> None:
        from src.models.user import User

        session = _make_session()
        created = _make_user(email="new@x.com")

        with patch.object(User, "__init__", return_value=None) as mock_init:
            instance = MagicMock(spec=User)
            with patch("src.repositories.base.BaseRepository._model", User, create=True):
                pass  # just verifying patch works

        # Simpler: test via UserRepository
        repo: BaseRepository[Any] = BaseRepository(session, User)
        # Patch User(...) constructor to return our mock
        with patch("src.repositories.base.BaseRepository._model") as MockModel:
            MockModel.return_value = created
            MockModel.__name__ = "User"
            repo._model = MockModel  # type: ignore[assignment]
            result = await repo.create(email="new@x.com")

        session.add.assert_called_once_with(created)
        session.flush.assert_awaited()
        session.refresh.assert_awaited_with(created)
        assert result is created

    @pytest.mark.asyncio
    async def test_update_modifies_and_returns_instance(self) -> None:
        from src.models.user import User

        session = _make_session()
        user = _make_user()
        session.get = AsyncMock(return_value=user)

        repo: BaseRepository[User] = BaseRepository(session, User)
        result = await repo.update(user.id, is_active=False)

        assert result is user
        assert user.is_active is False
        session.flush.assert_awaited()
        session.refresh.assert_awaited_with(user)

    @pytest.mark.asyncio
    async def test_update_returns_none_when_missing(self) -> None:
        from src.models.user import User

        session = _make_session()
        session.get = AsyncMock(return_value=None)

        repo: BaseRepository[User] = BaseRepository(session, User)
        result = await repo.update(uuid.uuid4(), is_active=False)
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_removes_row_and_returns_true(self) -> None:
        from src.models.user import User

        session = _make_session()
        user = _make_user()
        session.get = AsyncMock(return_value=user)
        session.delete = AsyncMock()

        repo: BaseRepository[User] = BaseRepository(session, User)
        result = await repo.delete(user.id)

        assert result is True
        session.delete.assert_awaited_once_with(user)
        session.flush.assert_awaited()

    @pytest.mark.asyncio
    async def test_delete_returns_false_when_missing(self) -> None:
        from src.models.user import User

        session = _make_session()
        session.get = AsyncMock(return_value=None)

        repo: BaseRepository[User] = BaseRepository(session, User)
        result = await repo.delete(uuid.uuid4())
        assert result is False


# ---------------------------------------------------------------------------
# UserRepository
# ---------------------------------------------------------------------------


class TestUserRepository:
    def test_init_uses_user_model(self) -> None:
        from src.models.user import User

        session = _make_session()
        repo = UserRepository(session)
        assert repo._model is User

    @pytest.mark.asyncio
    async def test_get_by_email_executes_query(self) -> None:
        session = _make_session()
        user = _make_user(email="a@b.com")
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = user
        session.execute = AsyncMock(return_value=mock_result)

        repo = UserRepository(session)
        result = await repo.get_by_email("a@b.com")
        assert result is user
        session.execute.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_by_oauth_executes_query(self) -> None:
        session = _make_session()
        user = _make_user(oauth_provider="google", oauth_sub="sub123")
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = user
        session.execute = AsyncMock(return_value=mock_result)

        repo = UserRepository(session)
        result = await repo.get_by_oauth("google", "sub123")
        assert result is user

    @pytest.mark.asyncio
    async def test_exists_by_email_true_when_found(self) -> None:
        session = _make_session()
        user = _make_user()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = user
        session.execute = AsyncMock(return_value=mock_result)

        repo = UserRepository(session)
        assert await repo.exists_by_email(user.email) is True

    @pytest.mark.asyncio
    async def test_exists_by_email_false_when_not_found(self) -> None:
        session = _make_session()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        session.execute = AsyncMock(return_value=mock_result)

        repo = UserRepository(session)
        assert await repo.exists_by_email("x@y.com") is False

    @pytest.mark.asyncio
    async def test_list_active_executes_filtered_query(self) -> None:
        session = _make_session()
        users = [_make_user(is_active=True), _make_user(is_active=True)]
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = users
        mock_result = MagicMock()
        mock_result.scalars.return_value = mock_scalars
        session.execute = AsyncMock(return_value=mock_result)

        repo = UserRepository(session)
        result = await repo.list_active(limit=10)
        assert result == users

    @pytest.mark.asyncio
    async def test_deactivate_updates_is_active(self) -> None:
        session = _make_session()
        user = _make_user(is_active=True)
        session.get = AsyncMock(return_value=user)

        repo = UserRepository(session)
        result = await repo.deactivate(user.id)

        assert result is user
        assert user.is_active is False


# ---------------------------------------------------------------------------
# RefreshTokenRepository
# ---------------------------------------------------------------------------


class TestRefreshTokenRepository:
    def test_init_uses_refresh_token_model(self) -> None:
        from src.models.refresh_token import RefreshToken

        session = _make_session()
        repo = RefreshTokenRepository(session)
        assert repo._model is RefreshToken

    @pytest.mark.asyncio
    async def test_get_by_token_executes_query(self) -> None:
        session = _make_session()
        token_obj = _make_token(token="mytoken")
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = token_obj
        session.execute = AsyncMock(return_value=mock_result)

        repo = RefreshTokenRepository(session)
        result = await repo.get_by_token("mytoken")
        assert result is token_obj

    @pytest.mark.asyncio
    async def test_revoke_marks_token_revoked(self) -> None:
        session = _make_session()
        token_obj = _make_token(revoked=False)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = token_obj
        session.execute = AsyncMock(return_value=mock_result)

        repo = RefreshTokenRepository(session)
        result = await repo.revoke(token_obj.token)

        assert result is True
        assert token_obj.revoked is True
        session.flush.assert_awaited()

    @pytest.mark.asyncio
    async def test_revoke_returns_false_when_not_found(self) -> None:
        session = _make_session()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        session.execute = AsyncMock(return_value=mock_result)

        repo = RefreshTokenRepository(session)
        result = await repo.revoke("nonexistent")
        assert result is False

    @pytest.mark.asyncio
    async def test_revoke_all_for_user_revokes_active_tokens(self) -> None:
        user_id = uuid.uuid4()
        session = _make_session()
        tokens = [_make_token(user_id=user_id, revoked=False) for _ in range(3)]
        mock_scalars = MagicMock()
        mock_scalars.all.return_value = tokens
        mock_result = MagicMock()
        mock_result.scalars.return_value = mock_scalars
        session.execute = AsyncMock(return_value=mock_result)

        repo = RefreshTokenRepository(session)
        count = await repo.revoke_all_for_user(user_id)

        assert count == 3
        for t in tokens:
            assert t.revoked is True
        session.flush.assert_awaited()

    @pytest.mark.asyncio
    async def test_delete_expired_executes_bulk_delete(self) -> None:
        session = _make_session()
        mock_result = MagicMock()
        mock_result.rowcount = 5
        session.execute = AsyncMock(return_value=mock_result)

        repo = RefreshTokenRepository(session)
        count = await repo.delete_expired()

        assert count == 5
        session.flush.assert_awaited()
