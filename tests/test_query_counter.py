"""The recorder in `tests/querycount.py`, measured against its own claims.

Everything `test_n_plus_one.py` asserts is only as true as this file: a counter
that quietly missed a statement would turn every gate next door into a test
that passes for the wrong reason. So the recorder is exercised directly here —
what it counts, what it separates, what its failure messages say, and that it
detaches itself afterwards.

A throwaway table in a registry of its own, following `test_locking_db.py`, so
none of this depends on the application schema and nothing here adds a table to
the `create_all` that the rest of the suite runs.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator

import pytest
from sqlalchemy import Integer, String, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.pool import NullPool

from src.config import settings
from tests.querycount import (
    EXCERPT_LENGTH,
    NPlusOneDetected,
    QueryLog,
    RelationshipLoad,
    Statement,
    assert_no_repeated_statements,
    capture_queries,
)


class ProbeBase(DeclarativeBase):
    """Its own registry, so `Base.metadata` never learns about this table."""


class CounterProbe(ProbeBase):
    __tablename__ = "querycount_probe"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    label: Mapped[str] = mapped_column(String(64), nullable=False)


@pytest.fixture
async def engine() -> AsyncGenerator[AsyncEngine]:
    engine = create_async_engine(settings.DATABASE_URL, poolclass=NullPool)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except (OSError, SQLAlchemyError) as exc:
        await engine.dispose()
        pytest.skip(f"no usable Postgres at DATABASE_URL: {exc}")

    async with engine.begin() as conn:
        await conn.run_sync(ProbeBase.metadata.create_all)

    yield engine

    async with engine.begin() as conn:
        await conn.run_sync(ProbeBase.metadata.drop_all)
    await engine.dispose()


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


class TestCounting:
    async def test_counts_one_statement_per_round_trip(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        async with sessions() as session:
            with capture_queries(session) as log:
                await session.execute(text("SELECT 1"))
                await session.execute(text("SELECT 2"))

        assert len(log) == 2
        assert log.parameter_sets == 2

    async def test_records_nothing_before_or_after_the_block(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        """The listeners are removed on the way out.

        Worth its own test because the failure mode is silent and remote: a
        recorder left attached keeps filling a log that some *later* test
        asserts on, and the test that breaks is never the one at fault.
        """
        async with sessions() as session:
            await session.execute(text("SELECT 1"))
            with capture_queries(session) as log:
                await session.execute(text("SELECT 2"))
            await session.execute(text("SELECT 3"))

        assert len(log) == 1
        assert log.statements[0].shape == "SELECT 2"

    async def test_an_exception_in_the_block_still_detaches(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        async with sessions() as session:
            with pytest.raises(RuntimeError):
                with capture_queries(session) as log:
                    await session.execute(text("SELECT 1"))
                    raise RuntimeError("boom")
            await session.execute(text("SELECT 2"))

        assert len(log) == 1

    async def test_a_batched_write_is_one_statement_and_many_parameter_sets(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        """The two numbers the recorder deliberately keeps apart.

        SQLAlchemy's unit of work turns N single-row UPDATEs into one
        `executemany`, so counting parameter sets as statements would report an
        N+1 that is not there, and counting only statements would hide how much
        was really written.
        """
        marker = uuid.uuid4().hex[:8]
        async with sessions() as session:
            session.add_all(CounterProbe(label=f"{marker}-{n}") for n in range(3))
            await session.commit()

        async with sessions() as session:
            rows = await session.execute(
                select(CounterProbe).where(CounterProbe.label.like(f"{marker}-%"))
            )
            probes = list(rows.scalars().all())
            with capture_queries(session) as log:
                for index, probe in enumerate(probes):
                    probe.label = f"{marker}-updated-{index}"
                await session.flush()

        assert len(log) == 1
        assert log.statements[0].executemany is True
        assert log.statements[0].parameter_sets == 3
        assert log.parameter_sets == 3


class TestGrouping:
    async def test_groups_by_shape_not_by_parameters(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        """Two queries differing only in their bound values are one shape.

        This is the property the repeat detector rests on: an N+1 sends the
        same SQL with a different id each time, and if the parameters were part
        of the identity every iteration would look unique.
        """
        async with sessions() as session:
            with capture_queries(session) as log:
                for label in ("a", "b", "c"):
                    await session.execute(
                        select(CounterProbe).where(CounterProbe.label == label)
                    )

        shapes = log.shapes()
        assert len(shapes) == 1
        assert next(iter(shapes.values())) == 3
        assert log.repeated(limit=1) == [(log.statements[0].shape, 3)]
        assert log.repeated(limit=3) == []

    def test_shape_collapses_whitespace(self) -> None:
        statement = Statement(
            sql="SELECT 1\n  FROM   users\n", parameter_sets=1, executemany=False
        )
        assert statement.shape == "SELECT 1 FROM users"

    def test_excerpt_is_bounded(self) -> None:
        statement = Statement(
            sql="SELECT " + ("x" * (EXCERPT_LENGTH * 2)),
            parameter_sets=1,
            executemany=False,
        )
        assert len(statement.excerpt) == EXCERPT_LENGTH + 1
        assert statement.excerpt.endswith("…")


class TestAssertions:
    async def test_repeated_statements_pass_at_the_limit_and_fail_above_it(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        async with sessions() as session:
            with capture_queries(session) as log:
                for _ in range(3):
                    await session.execute(
                        select(CounterProbe).where(CounterProbe.label == "x")
                    )

        assert_no_repeated_statements(log, limit=3)
        with pytest.raises(NPlusOneDetected) as caught:
            assert_no_repeated_statements(log, limit=2)

        message = str(caught.value)
        # The message has to carry the offending SQL and how often it was sent,
        # or the gate tells you only that something is wrong.
        assert "3x" in message
        assert "querycount_probe" in message
        assert "docs/n-plus-one.md" in message

    async def test_summary_names_every_statement(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        async with sessions() as session:
            with capture_queries(session) as log:
                await session.execute(text("SELECT 1"))
                await session.execute(text("SELECT 1"))
                await session.execute(text("SELECT 2"))

        summary = log.summary()
        assert "3 statement(s)" in summary
        assert "0 lazy relationship load(s)" in summary
        assert "2x SELECT 1" in summary
        assert "1x SELECT 2" in summary

    def test_summary_groups_repeated_loads_instead_of_listing_them(self) -> None:
        """A thousand-row N+1 must not answer with a thousand lines.

        Built by hand rather than provoked, because the size that makes the
        point is the size nobody wants to seed.
        """
        log = QueryLog(
            statements=[
                Statement(sql="SELECT 1", parameter_sets=1, executemany=False)
                for _ in range(1000)
            ],
            relationship_loads=[
                RelationshipLoad(path="User.refresh_tokens", lazy=True)
                for _ in range(1000)
            ],
        )

        summary = log.summary()
        assert "1000x [lazy relationship load] User.refresh_tokens" in summary
        assert len(summary.splitlines()) == 3
