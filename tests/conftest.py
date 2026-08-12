import os
import secrets
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

# Must be set before any application imports so pydantic-settings can validate
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
os.environ.setdefault("SECRET_KEY", secrets.token_hex(32))
os.environ.setdefault("ALGORITHM", "HS256")
os.environ.setdefault("ACCESS_TOKEN_EXPIRE_MINUTES", "30")
os.environ.setdefault("REFRESH_TOKEN_EXPIRE_DAYS", "7")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
# The app under test gets the in-process idempotency store, so importing
# `src.main` never opens a Redis connection pool and the suite runs without a
# server. The Redis store itself is covered directly, against a real server,
# in `test_idempotency_contract.py` and `test_idempotency_redis.py`.
os.environ.setdefault("IDEMPOTENCY_BACKEND", "memory")

import pytest
from httpx import ASGITransport, AsyncClient
from pytest_factoryboy import register

from src.auth.dependencies import get_current_user
from src.auth.service import AuthService
from src.auth.utils import create_access_token
from src.database import get_db
from src.dependencies import get_auth_service
from src.main import app
from src.models.user import User
from src.worker import celery_app as _celery_app
from tests.factories import AdminUserFactory, RefreshTokenFactory, UserFactory
from tests.fakes import (
    CollectingPublisher,
    InMemoryRefreshTokenStore,
    InMemoryUserStore,
    RecordingUnitOfWork,
    apply_column_defaults,
)

# Re-exported: it lived here before `tests/fakes.py` existed, and the fakes now
# need it too. Kept importable from both so no test had to move for it.
__all__ = ["apply_column_defaults"]

# Register factories as pytest fixtures.
# UserFactory      → fixtures: user_factory, user
# AdminUserFactory → fixtures: admin_user_factory, admin_user
# RefreshTokenFactory → fixtures: refresh_token_factory, refresh_token
register(UserFactory)
register(AdminUserFactory, "admin_user")
register(RefreshTokenFactory)


@pytest.fixture(autouse=True)
def celery_eager() -> None:
    """Run all Celery tasks synchronously and propagate exceptions in tests."""
    _celery_app.conf.update(task_always_eager=True, task_eager_propagates=True)


@pytest.fixture
def mock_db() -> AsyncMock:
    """In-memory async SQLAlchemy session stub.

    Tracks pending instances so ``flush()``/``refresh()`` can populate column
    defaults, mirroring the behaviour handlers rely on after ``repo.create()``.
    """
    session = AsyncMock()
    pending: list[object] = []

    def _add(instance: object) -> None:
        pending.append(instance)

    async def _flush(*_args: object, **_kwargs: object) -> None:
        for instance in pending:
            apply_column_defaults(instance)

    async def _refresh(instance: object, *_args: object, **_kwargs: object) -> None:
        apply_column_defaults(instance)

    session.add = MagicMock(side_effect=_add)
    session.commit = AsyncMock()
    session.flush = AsyncMock(side_effect=_flush)
    session.refresh = AsyncMock(side_effect=_refresh)
    return session


@pytest.fixture
async def async_client(mock_db: AsyncMock) -> AsyncGenerator[AsyncClient, None]:
    """Unauthenticated HTTPX async client wired to the FastAPI app via ASGITransport."""

    async def _override_db() -> AsyncGenerator[AsyncMock, None]:
        yield mock_db

    app.dependency_overrides[get_db] = _override_db

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client

    app.dependency_overrides.clear()


@pytest.fixture
def mock_user() -> User:
    """A standard active user for use in fixture-driven tests."""
    return User(
        id=uuid.uuid4(),
        email="user@example.com",
        hashed_password="hashed",
        is_active=True,
        is_verified=True,
        role="user",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        notification_channel="email",
    )


@pytest.fixture
def mock_admin() -> User:
    """An admin user for role-guard tests."""
    return User(
        id=uuid.uuid4(),
        email="admin@example.com",
        hashed_password="hashed",
        is_active=True,
        is_verified=True,
        role="admin",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        notification_channel="email",
    )


@pytest.fixture
def auth_headers(mock_user: User) -> dict[str, str]:
    """Bearer token header for a regular user."""
    token = create_access_token(str(mock_user.id), mock_user.email, mock_user.role)
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def admin_headers(mock_admin: User) -> dict[str, str]:
    """Bearer token header for an admin user."""
    token = create_access_token(str(mock_admin.id), mock_admin.email, mock_admin.role)
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
async def authenticated_client(
    mock_db: AsyncMock,
    mock_user: User,
    auth_headers: dict[str, str],
) -> AsyncGenerator[AsyncClient, None]:
    """HTTPX client carrying a valid user JWT plus a get_current_user override."""

    async def _override_db() -> AsyncGenerator[AsyncMock, None]:
        yield mock_db

    async def _override_current_user() -> User:
        return mock_user

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_current_user] = _override_current_user

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers=auth_headers,
    ) as client:
        yield client

    app.dependency_overrides.clear()


@pytest.fixture
async def admin_client(
    mock_db: AsyncMock,
    mock_admin: User,
    admin_headers: dict[str, str],
) -> AsyncGenerator[AsyncClient, None]:
    """HTTPX client pre-configured with an admin JWT and get_current_user override."""

    async def _override_db() -> AsyncGenerator[AsyncMock, None]:
        yield mock_db

    async def _override_current_user() -> User:
        return mock_admin

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_current_user] = _override_current_user

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers=admin_headers,
    ) as client:
        yield client

    app.dependency_overrides.clear()


# --- Protocol-typed fakes -------------------------------------------------
#
# The fixtures above hand the app a stub *session*; these hand it stores. Prefer
# these for anything testing authentication policy — they need no database, and
# nothing in them can be wrong about how SQLAlchemy behaves, because none of
# them is pretending to be SQLAlchemy. See `tests/fakes.py`.


@pytest.fixture
def user_store() -> InMemoryUserStore:
    """An empty in-memory `UserStore`. Append to `.users` to seed it."""
    return InMemoryUserStore()


@pytest.fixture
def token_store() -> InMemoryRefreshTokenStore:
    """An empty in-memory `RefreshTokenStore`."""
    return InMemoryRefreshTokenStore()


@pytest.fixture
def uow() -> RecordingUnitOfWork:
    """A `UnitOfWork` that counts commits and flushes instead of doing them."""
    return RecordingUnitOfWork()


@pytest.fixture
def publisher() -> CollectingPublisher:
    """An `EventPublisher` that keeps every event it is handed."""
    return CollectingPublisher()


@pytest.fixture
def auth_service(
    user_store: InMemoryUserStore,
    token_store: InMemoryRefreshTokenStore,
    uow: RecordingUnitOfWork,
    publisher: CollectingPublisher,
) -> AuthService:
    """The real `AuthService` with every collaborator faked.

    The service under test is the production one — only what it talks to is
    substituted, which is the difference between testing the policy and testing
    a mock's `side_effect` list.
    """
    return AuthService(users=user_store, tokens=token_store, uow=uow, events=publisher)


@pytest.fixture
async def fake_backed_client(
    auth_service: AuthService,
) -> AsyncGenerator[AsyncClient, None]:
    """A client whose auth routes run the real service over in-memory stores.

    Overrides `get_auth_service` rather than the four providers underneath it,
    so the fixture composing the service and the app resolving it agree by
    construction. `get_db` is left untouched and never called: nothing in the
    resolved dependency tree reaches it, which is the assertion
    `test_dependency_inversion.py` makes explicitly.
    """
    app.dependency_overrides[get_auth_service] = lambda: auth_service

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client

    app.dependency_overrides.clear()
