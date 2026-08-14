"""Optimistic concurrency measured against a real Postgres.

The route tests next door prove what the API does with a client's `If-Match`.
They cannot prove the part that makes it sound: that a stale UPDATE is refused
by the database rather than by a comparison the application made a moment
earlier. Only two real transactions racing over one row can show that, so these
tests open two sessions and let them fight.

They are skipped when `DATABASE_URL` names nothing reachable, and CI always has
a Postgres service, so the claim is measured on every pull request. A skip here
cannot hide a broken CI database either: the `alembic upgrade head` step runs
before pytest and fails the job first.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm.exc import StaleDataError
from sqlalchemy.pool import NullPool

from src.auth.utils import create_access_token
from src.concurrency import IfMatch, resource_version_tag
from src.config import settings
from src.database import Base, get_db
from src.exceptions import PreconditionFailedError
from src.main import app
from src.models.user import User
from src.users.schemas import ProfileUpdateRequest
from src.users.service import ProfileService

ENDPOINT = "/api/v1/users/me"


@pytest.fixture
async def engine() -> AsyncGenerator[AsyncEngine]:
    """An engine on `DATABASE_URL`, or a skip if there is nothing there.

    `NullPool` because each test opens two sessions that must be two *distinct*
    connections — a pooled engine handing the same one to both would serialise
    the very race being measured.

    Reachability and schema are checked separately on purpose: a connection
    failure is an environment without a database and skips, while anything that
    goes wrong afterwards is a real defect and fails.
    """
    engine = create_async_engine(settings.DATABASE_URL, poolclass=NullPool)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except (OSError, SQLAlchemyError) as exc:
        await engine.dispose()
        pytest.skip(f"no usable Postgres at DATABASE_URL: {exc}")

    # No-op in CI, where `alembic upgrade head` has already run — which is what
    # makes these tests a check on the *migration* as well as the mapper. If
    # 0004 forgot the column, the table already exists, `create_all` skips it,
    # and every test below fails on an undefined column.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine
    await engine.dispose()


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
async def user_id(
    sessions: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[uuid.UUID]:
    """A committed user row, removed again afterwards.

    The email is randomised rather than fixed so a rerun after an interrupted
    test does not trip the unique index, and so two of these tests can never
    collide on one row.
    """
    async with sessions() as session:
        user = User(
            email=f"concurrency-{uuid.uuid4()}@example.test",
            hashed_password="not-a-real-hash",
        )
        session.add(user)
        await session.commit()
        created_id = user.id

    yield created_id

    async with sessions() as session:
        row = await session.get(User, created_id)
        if row is not None:
            await session.delete(row)
            await session.commit()


class TestVersionCounter:
    async def test_insert_starts_the_counter_at_one(
        self, sessions: async_sessionmaker[AsyncSession], user_id: uuid.UUID
    ) -> None:
        async with sessions() as session:
            user = await session.get(User, user_id)
            assert user is not None
            assert user.version == 1

    async def test_update_increments_it(
        self, sessions: async_sessionmaker[AsyncSession], user_id: uuid.UUID
    ) -> None:
        async with sessions() as session:
            user = await session.get(User, user_id)
            assert user is not None
            user.notification_channel = "none"
            await session.commit()
            assert user.version == 2

    async def test_a_write_that_changes_nothing_does_not_bump_it(
        self, sessions: async_sessionmaker[AsyncSession], user_id: uuid.UUID
    ) -> None:
        """Which is why an unchanged representation keeps its ETag.

        SQLAlchemy emits no UPDATE for an assignment that does not alter the
        value, so there is nothing for the counter to count.
        """
        async with sessions() as session:
            user = await session.get(User, user_id)
            assert user is not None
            user.notification_channel = user.notification_channel
            await session.commit()
            assert user.version == 1


class TestConcurrentWrites:
    async def test_the_second_writer_loses(
        self, sessions: async_sessionmaker[AsyncSession], user_id: uuid.UUID
    ) -> None:
        """Two transactions, one row, both holding version 1."""
        async with sessions() as first, sessions() as second:
            mine = await first.get(User, user_id)
            theirs = await second.get(User, user_id)
            assert mine is not None and theirs is not None

            theirs.notification_channel = "none"
            await second.commit()

            mine.notification_channel = "webhook"
            with pytest.raises(StaleDataError):
                await first.commit()

    async def test_the_loser_does_not_overwrite_the_winner(
        self, sessions: async_sessionmaker[AsyncSession], user_id: uuid.UUID
    ) -> None:
        """The lost update, not lost. Without the version column this passes
        silently with the *second* writer's value in the row."""
        async with sessions() as first, sessions() as second:
            mine = await first.get(User, user_id)
            theirs = await second.get(User, user_id)
            assert mine is not None and theirs is not None

            theirs.notification_channel = "none"
            await second.commit()

            mine.notification_channel = "webhook"
            with pytest.raises(StaleDataError):
                await first.commit()
            await first.rollback()

        async with sessions() as session:
            row = await session.get(User, user_id)
            assert row is not None
            assert row.notification_channel == "none"
            assert row.version == 2


class TestServiceUnderRace:
    async def test_a_conflict_between_the_check_and_the_write_is_a_412(
        self, sessions: async_sessionmaker[AsyncSession], user_id: uuid.UUID
    ) -> None:
        """The window `require_match` alone cannot close.

        The precondition is evaluated against a row loaded before the other
        transaction committed, so it *passes* — the tag genuinely described the
        resource when it was read. Only the versioned UPDATE catches it.
        """
        async with sessions() as mine, sessions() as theirs:
            user = await mine.get(User, user_id)
            other_copy = await theirs.get(User, user_id)
            assert user is not None and other_copy is not None

            precondition = IfMatch.parse(
                resource_version_tag(user.id, user.version).serialize()
            )

            other_copy.notification_channel = "none"
            await theirs.commit()

            assert precondition.matches(resource_version_tag(user.id, user.version))

            with pytest.raises(PreconditionFailedError) as excinfo:
                await ProfileService(mine).update(
                    user,
                    ProfileUpdateRequest(notification_channel="webhook"),
                    precondition,
                )

            assert excinfo.value.status_code == 412
            # Nothing to hand back: the failed flush left the session unusable.
            assert excinfo.value.headers is None
            await mine.rollback()


class TestOverHttp:
    @pytest.fixture
    async def client(
        self,
        sessions: async_sessionmaker[AsyncSession],
        user_id: uuid.UUID,
    ) -> AsyncGenerator[AsyncClient]:
        """The real app, the real database, a real token for `user_id`."""

        async def _override_db() -> AsyncGenerator[AsyncSession]:
            async with sessions() as session:
                yield session

        app.dependency_overrides[get_db] = _override_db
        token = create_access_token(str(user_id), "concurrency@example.test", "user")

        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"Authorization": f"Bearer {token}"},
        ) as client:
            yield client

        app.dependency_overrides.clear()

    async def test_an_etag_can_be_used_once(
        self, client: AsyncClient, user_id: uuid.UUID
    ) -> None:
        """The whole protocol, end to end, against Postgres."""
        first_read = await client.get(ENDPOINT)
        assert first_read.status_code == 200
        original = first_read.headers["etag"]
        assert original == f'"{user_id}.1"'

        updated = await client.patch(
            ENDPOINT,
            json={"notification_channel": "none"},
            headers={"If-Match": original},
        )
        assert updated.status_code == 200
        assert updated.headers["etag"] == f'"{user_id}.2"'

        replayed = await client.patch(
            ENDPOINT,
            json={"notification_channel": "webhook"},
            headers={"If-Match": original},
        )
        assert replayed.status_code == 412
        assert replayed.headers["etag"] == f'"{user_id}.2"'

        final = await client.get(ENDPOINT)
        assert final.json()["notification_channel"] == "none"
        assert final.headers["etag"] == f'"{user_id}.2"'
