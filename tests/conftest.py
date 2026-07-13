import os
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

# Must be set before any application imports so pydantic-settings can validate
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
os.environ.setdefault("SECRET_KEY", "test-secret-key-minimum-32-characters-long!")
os.environ.setdefault("ALGORITHM", "HS256")
os.environ.setdefault("ACCESS_TOKEN_EXPIRE_MINUTES", "30")
os.environ.setdefault("REFRESH_TOKEN_EXPIRE_DAYS", "7")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

import pytest
from httpx import ASGITransport, AsyncClient

from src.auth.dependencies import get_current_user
from src.auth.utils import create_access_token
from src.database import get_db
from src.main import app
from src.models.user import User
from src.worker import celery_app as _celery_app


@pytest.fixture(autouse=True)
def celery_eager() -> None:
    """Run all Celery tasks synchronously and propagate exceptions in tests."""
    _celery_app.conf.update(task_always_eager=True, task_eager_propagates=True)


@pytest.fixture
def mock_db() -> AsyncMock:
    """In-memory async SQLAlchemy session stub."""
    session = AsyncMock()
    session.add = MagicMock()
    session.commit = AsyncMock()
    session.flush = AsyncMock()
    session.refresh = AsyncMock()
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
    )


@pytest.fixture
def auth_headers(mock_user: User) -> dict[str, str]:
    """Bearer token header for a regular user."""
    token = create_access_token(str(mock_user.id), mock_user.email, mock_user.role)
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def admin_headers(mock_admin: User) -> dict[str, str]:
    """Bearer token header for an admin user."""
    token = create_access_token(
        str(mock_admin.id), mock_admin.email, mock_admin.role
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
async def authenticated_client(
    mock_db: AsyncMock,
    mock_user: User,
    auth_headers: dict[str, str],
) -> AsyncGenerator[AsyncClient, None]:
    """HTTPX client pre-configured with a valid user JWT and get_current_user override."""
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
