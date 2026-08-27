"""The export measured against a real Postgres, cursor and all.

The route tests next door prove what the API does with a faked source. They
cannot prove the part that makes the export a stream rather than a `SELECT *`
with extra steps: that one cursor serves every batch, that the password hash is
never fetched at all — measured against the entity query it replaces, which
does fetch it — and that abandoning the download closes the cursor. Those need
a database.

Skipped when `DATABASE_URL` names nothing reachable; CI always has a Postgres
service, so the claims are measured on every pull request.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import delete, event, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from src.auth.dependencies import get_current_user
from src.config import settings
from src.database import Base, get_db
from src.main import app
from src.models.user import User
from src.repositories.user import UserRepository
from src.streaming.ndjson import TERMINAL_KEY
from src.users.export import UserExportRecord

ENDPOINT = "/api/v1/exports/users"
SEEDED = 40
MARKER = "streaming-export"


@pytest.fixture
async def engine() -> AsyncGenerator[AsyncEngine]:
    """An engine on `DATABASE_URL`, or a skip if there is nothing there.

    `NullPool` so that a test asserting a cursor was released is not quietly
    satisfied by a pool handing back a connection it never really freed.
    """
    engine = create_async_engine(settings.DATABASE_URL, poolclass=NullPool)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except (OSError, SQLAlchemyError) as exc:
        await engine.dispose()
        pytest.skip(f"no usable Postgres at DATABASE_URL: {exc}")

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine
    await engine.dispose()


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
async def seeded(
    sessions: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[list[uuid.UUID]]:
    """`SEEDED` committed users, every fourth one deactivated, then removed."""
    async with sessions() as session:
        users = [
            User(
                email=f"{MARKER}-{uuid.uuid4()}@example.test",
                hashed_password="not-a-real-hash",
                is_active=index % 4 != 0,
            )
            for index in range(SEEDED)
        ]
        session.add_all(users)
        await session.commit()
        ids = [user.id for user in users]

    yield ids

    async with sessions() as session:
        await session.execute(delete(User).where(User.id.in_(ids)))
        await session.commit()


def _capture_sql(engine: AsyncEngine) -> list[str]:
    """Record every statement the engine emits from here on.

    Registered on the sync engine because that is where SQLAlchemy's execution
    events live; the async facade is a wrapper around it.
    """
    statements: list[str] = []

    @event.listens_for(engine.sync_engine, "before_cursor_execute")
    def _record(
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        statements.append(statement)

    return statements


async def _export(
    session: AsyncSession, *, active_only: bool = False
) -> list[UserExportRecord]:
    return [
        record
        async for record in UserRepository(session).stream_export(
            batch_size=7, active_only=active_only
        )
    ]


class TestStreamExport:
    async def test_it_returns_every_row_in_primary_key_order(
        self, sessions: async_sessionmaker[AsyncSession], seeded: list[uuid.UUID]
    ) -> None:
        async with sessions() as session:
            records = await _export(session)

        exported = [record.id for record in records if record.id in set(seeded)]
        assert exported == sorted(seeded)

    async def test_the_columns_are_the_published_schema(
        self, sessions: async_sessionmaker[AsyncSession], seeded: list[uuid.UUID]
    ) -> None:
        async with sessions() as session:
            records = await _export(session)

        mine = next(record for record in records if record.id in set(seeded))
        assert MARKER in mine.email
        assert mine.role == "user"
        assert mine.created_at is not None

    async def test_active_only_filters_in_the_database(
        self, sessions: async_sessionmaker[AsyncSession], seeded: list[uuid.UUID]
    ) -> None:
        wanted = set(seeded)
        async with sessions() as session:
            everyone = await _export(session)
            active = await _export(session, active_only=True)

        assert len([r for r in everyone if r.id in wanted]) == SEEDED
        assert len([r for r in active if r.id in wanted]) == SEEDED - SEEDED // 4
        assert all(record.is_active for record in active)

    async def test_the_password_hash_is_never_fetched(
        self,
        engine: AsyncEngine,
        sessions: async_sessionmaker[AsyncSession],
        seeded: list[uuid.UUID],
    ) -> None:
        """The reason the query names columns instead of `select(User)`.

        A key dropped during serialisation is a decision eight layers can undo;
        a column never selected is one the database enforces.
        """
        statements = _capture_sql(engine)

        async with sessions() as session:
            await _export(session)

        assert statements, "no statement was captured"
        assert all("hashed_password" not in sql for sql in statements)
        assert all("oauth_sub" not in sql for sql in statements)

    async def test_the_entity_query_this_replaces_would_fetch_it(
        self,
        engine: AsyncEngine,
        sessions: async_sessionmaker[AsyncSession],
        seeded: list[uuid.UUID],
    ) -> None:
        """The obvious implementation, measured rather than asserted about."""
        statements = _capture_sql(engine)

        async with sessions() as session:
            result = await session.stream_scalars(
                select(User).order_by(User.id).execution_options(yield_per=7)
            )
            async for _ in result:
                pass
            await result.close()

        assert any("hashed_password" in sql for sql in statements)

    async def test_it_takes_one_statement_however_many_batches(
        self,
        engine: AsyncEngine,
        sessions: async_sessionmaker[AsyncSession],
        seeded: list[uuid.UUID],
    ) -> None:
        """A cursor, not `LIMIT`/`OFFSET` paging dressed up as a stream."""
        statements = _capture_sql(engine)

        async with sessions() as session:
            records = await _export(session)

        assert len(records) >= SEEDED
        assert len(statements) == 1
        assert "LIMIT" not in statements[0].upper()

    async def test_no_orm_entity_is_created_per_row(
        self, sessions: async_sessionmaker[AsyncSession], seeded: list[uuid.UUID]
    ) -> None:
        """Nothing is instrumented, identity-mapped or version-checked per row."""
        async with sessions() as session:
            await _export(session)

            assert len(session.identity_map) == 0

    async def test_stopping_early_closes_the_cursor(
        self, sessions: async_sessionmaker[AsyncSession], seeded: list[uuid.UUID]
    ) -> None:
        """An abandoned download must not leave a portal open on the server.

        asyncpg tolerates an abandoned cursor — a second statement on the same
        session still works — so nothing *fails* without the `finally`. The
        cost is a portal held until the transaction ends, once per cancelled
        download, which is why this asserts on the result rather than on a
        query that would succeed either way.
        """
        async with sessions() as session:
            results: list[Any] = []
            original = AsyncSession.stream

            async def spy(self: AsyncSession, *args: Any, **kwargs: Any) -> Any:
                result = await original(self, *args, **kwargs)
                results.append(result)
                return result

            with patch.object(AsyncSession, "stream", spy):
                stream = UserRepository(session).stream_export(
                    batch_size=7, active_only=False
                )
                assert await anext(stream) is not None
                assert results[0].closed is False
                await stream.aclose()

            assert results[0].closed is True
            assert (await session.execute(text("SELECT 1"))).scalar_one() == 1


class TestThroughTheRoute:
    async def test_the_endpoint_streams_the_real_table(
        self,
        sessions: async_sessionmaker[AsyncSession],
        seeded: list[uuid.UUID],
        mock_admin: User,
        admin_headers: dict[str, str],
    ) -> None:
        """End to end: HTTP in, server-side cursor out, terminal record last."""

        async def _override_db() -> AsyncGenerator[AsyncSession]:
            async with sessions() as session:
                yield session

        app.dependency_overrides[get_db] = _override_db
        app.dependency_overrides[get_current_user] = lambda: mock_admin
        try:
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
                headers=admin_headers,
            ) as client:
                response = await client.get(ENDPOINT)
        finally:
            app.dependency_overrides.clear()

        assert response.status_code == 200
        lines: list[dict[str, Any]] = [
            json.loads(line) for line in response.content.splitlines()
        ]
        terminal = lines[-1]
        assert terminal[TERMINAL_KEY] == "complete"
        assert terminal["records"] == len(lines) - 1

        exported = {line["id"] for line in lines[:-1]}
        assert {str(user_id) for user_id in seeded} <= exported
        assert all("hashed_password" not in line for line in lines[:-1])
