"""In-memory implementations of the ports in `src/repositories/protocols.py`.

These exist because the alternative was worse. Before `AuthService` took
protocols, a service test had to hand it an `AsyncMock` session and then teach
that mock to behave like SQLAlchemy: `execute()` returning a result object whose
`scalar_one_or_none()` answers the next row in a list, `flush()` and `refresh()`
resolving column defaults the ORM would have resolved. Tests written that way
assert on the stub as much as on the code, and they go quietly wrong when the
service adds a query — the `side_effect` list shifts by one and every row after
it is answered to the wrong question.

A store that holds a list of users has none of those failure modes. It also
cannot drift from the interface: the conformance test annotates each class with
its protocol, so mypy fails the build the day a fake stops implementing one.

What these deliberately do *not* do is emulate a database. There is no unique
constraint, no cascade, no transaction isolation, and `RecordingUnitOfWork`
cannot roll anything back. Anything that turns on those is integration
territory and belongs against the real Postgres in CI, not here.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import ColumnDefault, DateTime
from sqlalchemy.orm import class_mapper

from src.events.base import DomainEvent
from src.models.refresh_token import RefreshToken
from src.models.user import User


def apply_column_defaults(instance: object) -> None:
    """Populate unset columns the way a real INSERT flush would.

    SQLAlchemy resolves ``default=`` and ``server_default=`` at flush time, not
    at construction time, so a freshly constructed model keeps ``None`` for
    ``id``, ``role``, ``is_active`` and the timestamps. Response-model
    validation then fails for reasons that have nothing to do with the code
    under test. Reading the defaults off the mapper rather than restating them
    keeps this honest when a column gains one.
    """
    mapper = class_mapper(type(instance))

    for column in mapper.columns:
        key = mapper.get_property_by_column(column).key
        if getattr(instance, key, None) is not None:
            continue

        default = column.default
        if default is not None:
            # Narrowed rather than duck-typed: `column.default` is a
            # `DefaultGenerator`, and only a `ColumnDefault` carries `arg`. A
            # `Sequence` default would reach `.arg` and raise.
            if isinstance(default, ColumnDefault):
                if default.is_callable:
                    setattr(instance, key, default.arg({}))
                elif default.is_scalar:
                    setattr(instance, key, default.arg)
        elif column.server_default is not None and isinstance(column.type, DateTime):
            setattr(instance, key, datetime.now(UTC))


class InMemoryUserStore:
    """A `UserStore` backed by a list.

    Seed it with the rows a test needs to already exist:

        store = InMemoryUserStore([make_user("taken@example.com")])
    """

    def __init__(self, users: list[User] | None = None) -> None:
        self.users: list[User] = list(users) if users else []

    async def get(self, id: uuid.UUID) -> User | None:
        return next((u for u in self.users if u.id == id), None)

    async def get_by_email(self, email: str) -> User | None:
        return next((u for u in self.users if u.email == email), None)

    async def get_by_oauth(self, provider: str, sub: str) -> User | None:
        return next(
            (
                u
                for u in self.users
                if u.oauth_provider == provider and u.oauth_sub == sub
            ),
            None,
        )

    async def exists_by_email(self, email: str) -> bool:
        return await self.get_by_email(email) is not None

    async def create(
        self,
        *,
        email: str,
        hashed_password: str | None,
        is_active: bool = True,
        is_verified: bool = False,
        oauth_provider: str | None = None,
        oauth_sub: str | None = None,
    ) -> User:
        """Insert a user, populated as a flush would leave it.

        The two defaults restated here — `is_active`, `is_verified` — are the
        model's own, and `test_fakes.py` asserts they still match the mapper, so
        a column whose default changes fails a test instead of quietly making
        the fake disagree with the database.
        """
        user = User(
            email=email,
            hashed_password=hashed_password,
            is_active=is_active,
            is_verified=is_verified,
            oauth_provider=oauth_provider,
            oauth_sub=oauth_sub,
        )
        apply_column_defaults(user)
        self.users.append(user)
        return user


class InMemoryRefreshTokenStore:
    """A `RefreshTokenStore` backed by a list."""

    def __init__(self, tokens: list[RefreshToken] | None = None) -> None:
        self.tokens: list[RefreshToken] = list(tokens) if tokens else []

    async def get_by_token(self, token: str) -> RefreshToken | None:
        return next((t for t in self.tokens if t.token == token), None)

    async def create(
        self,
        *,
        token: str,
        user_id: uuid.UUID,
        expires_at: datetime,
    ) -> RefreshToken:
        stored = RefreshToken(token=token, user_id=user_id, expires_at=expires_at)
        apply_column_defaults(stored)
        self.tokens.append(stored)
        return stored

    async def revoke(self, token: str) -> bool:
        stored = await self.get_by_token(token)
        if stored is None:
            return False
        stored.revoked = True
        return True


class RecordingUnitOfWork:
    """A `UnitOfWork` that counts calls instead of doing anything.

    Counting is the point: "did this commit before it published?" is a question
    about ordering that a real session answers just as well, and far less
    readably. `flush` is a genuine no-op rather than a stub — writes to an
    in-memory store are visible to the next read the moment they happen, which
    is exactly what flushing buys against a real session.
    """

    def __init__(self) -> None:
        self.commits = 0
        self.flushes = 0
        #: Every call in order, as `"commit"` / `"flush"`. For tests that care
        #: which came first rather than how many there were.
        self.calls: list[str] = []

    async def flush(self) -> None:
        self.flushes += 1
        self.calls.append("flush")

    async def commit(self) -> None:
        self.commits += 1
        self.calls.append("commit")


class CollectingPublisher:
    """An `EventPublisher` that keeps what it was given.

    Enough for "was anything published?"; a test that cares how the *bus*
    behaves — subscriber isolation, timeouts, nesting — should use a real
    `EventBus`, since that behaviour is the bus's and not this one's.
    """

    def __init__(self) -> None:
        self.events: list[DomainEvent] = []

    async def publish(self, event: DomainEvent) -> object:
        self.events.append(event)
        return None


if TYPE_CHECKING:
    # Static conformance. Nothing calls this; mypy checking it is the whole
    # point, and it fails the build if a fake drifts from its protocol.
    from src.events.base import EventPublisher
    from src.repositories.protocols import RefreshTokenStore, UserStore
    from src.unit_of_work import UnitOfWork

    def _assert_conformance() -> None:
        _users: UserStore = InMemoryUserStore()
        _tokens: RefreshTokenStore = InMemoryRefreshTokenStore()
        _uow: UnitOfWork = RecordingUnitOfWork()
        _events: EventPublisher = CollectingPublisher()
