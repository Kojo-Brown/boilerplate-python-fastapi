"""The fakes' own tests.

A fake that is subtly wrong is worse than no fake: every suite that uses it goes
green against behaviour the database does not have. These pin the two places
`tests/fakes.py` could disagree with the real thing — the column defaults it
restates, and the read-your-writes behaviour it claims makes `flush` a no-op.

Conformance to the protocols is asserted in `test_dependency_inversion.py`,
which is where the seam as a whole is checked.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import ColumnDefault
from sqlalchemy.orm import class_mapper

from src.events.catalog import UserRegistered
from src.models.refresh_token import RefreshToken
from src.models.user import User
from tests.fakes import (
    CollectingPublisher,
    InMemoryRefreshTokenStore,
    InMemoryUserStore,
    RecordingUnitOfWork,
    apply_column_defaults,
)


def _column_default(model: type[object], column: str) -> object:
    """The scalar the ORM would have inserted for an unset column."""
    default = class_mapper(model).columns[column].default
    assert isinstance(default, ColumnDefault)
    return default.arg


async def test_the_stores_create_defaults_match_the_model() -> None:
    """`InMemoryUserStore.create` restates two of the model's defaults in its
    signature. If a column's default changes, this fails rather than letting the
    fake quietly disagree with what an INSERT would have done."""
    store = InMemoryUserStore()

    user = await store.create(email="a@example.com", hashed_password="hashed")

    assert user.is_active is _column_default(User, "is_active")
    assert user.is_verified is _column_default(User, "is_verified")


async def test_a_created_user_looks_like_a_flushed_row() -> None:
    """Nothing downstream should have to know the row never reached Postgres:
    `UserResponse.model_validate` needs an id, a role and timestamps."""
    store = InMemoryUserStore()

    user = await store.create(email="a@example.com", hashed_password="hashed")

    assert isinstance(user.id, uuid.UUID)
    assert user.role == "user"
    assert user.notification_channel == "email"
    assert user.created_at is not None
    assert user.updated_at is not None


async def test_a_created_user_is_immediately_findable() -> None:
    """The read-your-writes property that makes `RecordingUnitOfWork.flush` an
    honest no-op rather than a stub that skips work."""
    store = InMemoryUserStore()

    created = await store.create(email="a@example.com", hashed_password="hashed")

    assert await store.get(created.id) is created
    assert await store.get_by_email("a@example.com") is created
    assert await store.exists_by_email("a@example.com") is True
    assert await store.exists_by_email("b@example.com") is False


async def test_oauth_lookup_matches_on_both_provider_and_subject() -> None:
    """Matching on the subject alone would hand one provider's account to
    another provider that happened to issue the same id."""
    store = InMemoryUserStore()
    await store.create(
        email="a@example.com",
        hashed_password=None,
        oauth_provider="google",
        oauth_sub="sub-1",
    )

    assert await store.get_by_oauth("google", "sub-1") is not None
    assert await store.get_by_oauth("github", "sub-1") is None
    assert await store.get_by_oauth("google", "sub-2") is None


async def test_seeded_rows_are_copied_not_aliased() -> None:
    """A store that held the caller's list would let one test's `create` append
    to the seed another test is still using."""
    seed: list[User] = []
    store = InMemoryUserStore(seed)

    await store.create(email="a@example.com", hashed_password="hashed")

    assert seed == []


async def test_the_token_store_round_trips_and_revokes() -> None:
    store = InMemoryRefreshTokenStore()
    user_id = uuid.uuid4()

    stored = await store.create(
        token="mock-refresh-token",
        user_id=user_id,
        expires_at=datetime.now(UTC) + timedelta(days=7),
    )

    assert isinstance(stored.id, uuid.UUID)
    assert stored.revoked is _column_default(RefreshToken, "revoked")
    assert await store.get_by_token("mock-refresh-token") is stored

    assert await store.revoke("mock-refresh-token") is True
    assert stored.revoked is True
    # Revoking something that was never stored is not an error: the caller
    # wanted it unusable and it is.
    assert await store.revoke("mock-unknown-token") is False


async def test_the_unit_of_work_records_order_not_just_counts() -> None:
    uow = RecordingUnitOfWork()

    await uow.flush()
    await uow.commit()
    await uow.commit()

    assert uow.flushes == 1
    assert uow.commits == 2
    assert uow.calls == ["flush", "commit", "commit"]


async def test_the_publisher_keeps_events_in_order() -> None:
    publisher = CollectingPublisher()
    first = UserRegistered(user_id="1", email="a@example.com", via="password")
    second = UserRegistered(user_id="2", email="b@example.com", via="oauth")

    assert await publisher.publish(first) is None
    await publisher.publish(second)

    assert publisher.events == [first, second]


def test_applying_defaults_never_overwrites_a_set_value() -> None:
    """It stands in for a flush, and a flush does not rewrite what you gave
    it — a test that pins a user's id would otherwise lose it."""
    fixed = uuid.uuid4()
    user = User(id=fixed, email="a@example.com", hashed_password=None, is_active=False)

    apply_column_defaults(user)

    assert user.id == fixed
    assert user.is_active is False
