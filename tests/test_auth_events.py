"""What `AuthService` publishes, and what it deliberately does not.

The service runs over the in-memory stores from `tests/fakes.py`, the same way
`test_auth.py` does it: what is under test here is which events come out and
when, not SQLAlchemy. The *bus* is real, because subscriber isolation and
ordering are the bus's behaviour and a collecting fake would not have them.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

from src.auth.schemas import RegisterRequest
from src.auth.service import AuthService
from src.auth.utils import hash_password
from src.events.base import DomainEvent
from src.events.bus import EventBus
from src.events.catalog import UserLoggedIn, UserRegistered
from src.exceptions import ConflictError
from src.models.user import User
from tests.fakes import (
    InMemoryRefreshTokenStore,
    InMemoryUserStore,
    RecordingUnitOfWork,
    apply_column_defaults,
)

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


def service_over(
    bus: EventBus,
    *,
    users: list[User] | None = None,
    tokens: InMemoryRefreshTokenStore | None = None,
    uow: RecordingUnitOfWork | None = None,
) -> AuthService:
    """The real service over in-memory stores, publishing to `bus`.

    Seeding is by *content* — the users that exist — rather than by the order
    the service happens to query in. The stub session this replaced took a list
    of rows to return from successive `execute` calls, so adding a lookup to
    the service silently shifted every later answer onto the wrong question.
    """
    return AuthService(
        users=InMemoryUserStore(users),
        tokens=tokens if tokens is not None else InMemoryRefreshTokenStore(),
        uow=uow if uow is not None else RecordingUnitOfWork(),
        events=bus,
    )


# --- registration ---


async def test_register_publishes_user_registered(
    bus: EventBus, published: list[DomainEvent]
) -> None:
    service = service_over(bus)

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

    class OrderingUnitOfWork(RecordingUnitOfWork):
        async def commit(self) -> None:
            await super().commit()
            order.append("commit")

    async def record(event: DomainEvent) -> None:
        order.append("publish")

    bus.subscribe(DomainEvent, record)
    await service_over(bus, uow=OrderingUnitOfWork()).register(
        RegisterRequest(email="new@example.com", password=PASSWORD)
    )

    assert order == ["commit", "publish"]


async def test_a_rejected_registration_publishes_nothing(
    bus: EventBus, published: list[DomainEvent]
) -> None:
    service = service_over(bus, users=[make_user("taken@example.com")])

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

    response = await service_over(bus).register(
        RegisterRequest(email="new@example.com", password=PASSWORD)
    )

    assert response.email == "new@example.com"


# --- login ---


async def test_login_publishes_user_logged_in(
    bus: EventBus, published: list[DomainEvent]
) -> None:
    user = make_user()
    service = service_over(bus, users=[user])

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

    service = service_over(bus)

    with pytest.raises(UnauthorizedError):
        await service.login("nobody@example.com", PASSWORD)

    assert published == []


async def test_a_refresh_is_not_a_login(
    bus: EventBus, published: list[DomainEvent]
) -> None:
    """Rotating a token is the same session continuing. Publishing a login for
    it would make "last seen" mean "last polled"."""
    from src.auth.utils import create_refresh_token

    user = make_user()
    token_str, expires_at = create_refresh_token(str(user.id), str(uuid.uuid4()))
    tokens = InMemoryRefreshTokenStore()
    await tokens.create(token=token_str, user_id=user.id, expires_at=expires_at)

    await service_over(bus, users=[user], tokens=tokens).refresh(token_str)

    assert published == []


# --- OAuth ---


async def test_first_oauth_sign_in_is_both_a_registration_and_a_login(
    bus: EventBus, published: list[DomainEvent]
) -> None:
    service = service_over(bus)

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

    await service_over(bus, users=[user]).oauth_login("google", "sub-123", user.email)

    assert [type(e).event_name for e in published] == ["user.logged_in"]


async def test_linking_a_provider_to_an_existing_account_is_not_a_registration(
    bus: EventBus, published: list[DomainEvent]
) -> None:
    """That account was registered long ago; only the provider is new."""
    user = make_user()

    await service_over(bus, users=[user]).oauth_login("google", "sub-123", user.email)

    assert [type(e).event_name for e in published] == ["user.logged_in"]
