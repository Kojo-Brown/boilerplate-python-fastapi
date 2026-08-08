"""What `AuthService` publishes, and what it deliberately does not.

The service is exercised against a stub session, the same way `test_auth.py`
does it: what is under test here is which events come out and when, not
SQLAlchemy.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.auth.schemas import RegisterRequest
from src.auth.service import AuthService
from src.auth.utils import hash_password
from src.events.base import DomainEvent
from src.events.bus import EventBus
from src.events.catalog import UserLoggedIn, UserRegistered
from src.exceptions import ConflictError
from src.models.user import User
from tests.conftest import apply_column_defaults

PASSWORD = "not-a-real-password"


@pytest.fixture
def bus() -> EventBus:
    return EventBus()


@pytest.fixture
def published(bus: EventBus) -> list[DomainEvent]:
    """Every event the service publishes, in order."""
    seen: list[DomainEvent] = []

    async def record(event: DomainEvent) -> None:
        seen.append(event)

    bus.subscribe(DomainEvent, record)
    return seen


def make_user(email: str = "alice@example.com", **overrides: object) -> User:
    user = User(
        id=uuid.uuid4(),
        email=email,
        hashed_password=hash_password(PASSWORD),
        is_active=True,
        is_verified=False,
        role="user",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        **overrides,
    )
    apply_column_defaults(user)
    return user


def session_returning(*rows: object) -> AsyncMock:
    """A stub session whose successive `execute` calls yield `rows`."""
    db = AsyncMock()
    results = []
    for row in rows:
        result = MagicMock()
        result.scalar_one_or_none.return_value = row
        results.append(result)
    db.execute = AsyncMock(side_effect=results)
    db.add = MagicMock()
    db.commit = AsyncMock()
    db.flush = AsyncMock()

    async def fake_refresh(obj: object, *_a: object, **_kw: object) -> None:
        if isinstance(obj, User):
            apply_column_defaults(obj)

    db.refresh = AsyncMock(side_effect=fake_refresh)
    return db


# --- registration ---


async def test_register_publishes_user_registered(
    bus: EventBus, published: list[DomainEvent]
) -> None:
    db = session_returning(None)
    service = AuthService(db, events=bus)

    response = await service.register(
        RegisterRequest(email="new@example.com", password=PASSWORD)
    )

    assert len(published) == 1
    event = published[0]
    assert isinstance(event, UserRegistered)
    assert event.email == "new@example.com"
    assert event.user_id == str(response.id)
    assert event.via == "password"


async def test_register_publishes_after_the_commit(bus: EventBus) -> None:
    """A subscriber that fired before the commit could react to a
    registration the database then rolled back."""
    order: list[str] = []
    db = session_returning(None)

    async def original_commit() -> None:
        order.append("commit")

    db.commit = AsyncMock(side_effect=original_commit)

    async def record(event: DomainEvent) -> None:
        order.append("publish")

    bus.subscribe(DomainEvent, record)
    await AuthService(db, events=bus).register(
        RegisterRequest(email="new@example.com", password=PASSWORD)
    )

    assert order == ["commit", "publish"]


async def test_a_rejected_registration_publishes_nothing(
    bus: EventBus, published: list[DomainEvent]
) -> None:
    db = session_returning(make_user("taken@example.com"))
    service = AuthService(db, events=bus)

    with pytest.raises(ConflictError):
        await service.register(
            RegisterRequest(email="taken@example.com", password=PASSWORD)
        )

    assert published == []


async def test_a_failing_subscriber_does_not_fail_the_registration(
    bus: EventBus,
) -> None:
    async def explodes(event: DomainEvent) -> None:
        raise RuntimeError("the mail queue is down")

    bus.subscribe(DomainEvent, explodes)
    db = session_returning(None)

    response = await AuthService(db, events=bus).register(
        RegisterRequest(email="new@example.com", password=PASSWORD)
    )

    assert response.email == "new@example.com"


# --- login ---


async def test_login_publishes_user_logged_in(
    bus: EventBus, published: list[DomainEvent]
) -> None:
    user = make_user()
    db = session_returning(user)
    service = AuthService(db, events=bus)

    await service.login(user.email, PASSWORD)

    assert len(published) == 1
    event = published[0]
    assert isinstance(event, UserLoggedIn)
    assert event.user_id == str(user.id)
    assert event.method == "password"


async def test_a_rejected_login_publishes_nothing(
    bus: EventBus, published: list[DomainEvent]
) -> None:
    from src.exceptions import UnauthorizedError

    db = session_returning(None)
    service = AuthService(db, events=bus)

    with pytest.raises(UnauthorizedError):
        await service.login("nobody@example.com", PASSWORD)

    assert published == []


async def test_a_refresh_is_not_a_login(
    bus: EventBus, published: list[DomainEvent]
) -> None:
    """Rotating a token is the same session continuing. Publishing a login for
    it would make "last seen" mean "last polled"."""
    from src.auth.utils import create_refresh_token
    from src.models.refresh_token import RefreshToken

    user = make_user()
    token_str, expires_at = create_refresh_token(str(user.id), str(uuid.uuid4()))
    stored = RefreshToken(
        id=uuid.uuid4(),
        token=token_str,
        user_id=user.id,
        expires_at=expires_at,
        revoked=False,
        created_at=datetime.now(UTC),
    )
    db = session_returning(stored)
    # `refresh` looks the user up by primary key, which is `session.get`
    # rather than a `select`.
    db.get = AsyncMock(return_value=user)

    await AuthService(db, events=bus).refresh(token_str)

    assert published == []


# --- OAuth ---


async def test_first_oauth_sign_in_is_both_a_registration_and_a_login(
    bus: EventBus, published: list[DomainEvent]
) -> None:
    db = session_returning(None, None)
    service = AuthService(db, events=bus)

    await service.oauth_login("google", "sub-123", "new@example.com")

    assert [type(e).event_name for e in published] == [
        "user.registered",
        "user.logged_in",
    ]
    registered, logged_in = published
    assert isinstance(registered, UserRegistered) and registered.via == "oauth"
    assert isinstance(logged_in, UserLoggedIn) and logged_in.method == "oauth"


async def test_a_returning_oauth_user_only_logs_in(
    bus: EventBus, published: list[DomainEvent]
) -> None:
    user = make_user(oauth_provider="google", oauth_sub="sub-123")
    db = session_returning(user)

    await AuthService(db, events=bus).oauth_login("google", "sub-123", user.email)

    assert [type(e).event_name for e in published] == ["user.logged_in"]


async def test_linking_a_provider_to_an_existing_account_is_not_a_registration(
    bus: EventBus, published: list[DomainEvent]
) -> None:
    """That account was registered long ago; only the provider is new."""
    user = make_user()
    db = session_returning(None, user)

    await AuthService(db, events=bus).oauth_login("google", "sub-123", user.email)

    assert [type(e).event_name for e in published] == ["user.logged_in"]


# --- wiring ---


def test_the_service_defaults_to_the_process_wide_bus() -> None:
    from src.events.bus import event_bus

    assert AuthService(AsyncMock()).events is event_bus
