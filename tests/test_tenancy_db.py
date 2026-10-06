"""Tenant isolation measured against a real Postgres, as a bounded role.

Everything else in this package can be proven in process. This cannot: the
boundary is a row-level security policy, and the only way to know a policy
works is to ask the database as a role the policy applies to. So these tests
create one — `rls_probe`, an ordinary `LOGIN` role with table grants and
nothing else — and run every claim through it.

That role is also the point. The suite's own `DATABASE_URL` is a superuser in
CI and on most developers' machines, and **a superuser bypasses every policy**.
A test that seeded two tenants and asserted it saw one would pass under a
correctly configured database and fail under a superuser, which is backwards;
run as superuser it would see both and fail for the right reason, but only by
accident of the credentials. Connecting as a role that cannot bypass is what
makes the result mean something either way.

They are skipped when `DATABASE_URL` names nothing reachable, and CI always
has a Postgres service, so every claim below is measured on every pull
request. They assert on the schema migration `0008` produces — not on one
`create_all` builds — because `FORCE ROW LEVEL SECURITY` and the policies
exist only in the migration.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator

import pytest
from sqlalchemy import make_url, text
from sqlalchemy.exc import DBAPIError, IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from src.config import settings
from src.models.tenant import DEFAULT_TENANT_ID, DEFAULT_TENANT_SLUG
from src.models.user import User
from src.tenancy.context import tenant_scope
from src.tenancy.isolation import inspect_isolation
from src.tenancy.sql import READ_TENANT, TENANT_SCOPED_TABLES
from tests.querycount import capture_queries

#: The bounded role these tests connect as. Dropped and recreated per session,
#: so a run interrupted halfway leaves nothing that changes the next one.
PROBE_ROLE = "rls_probe"
PROBE_PASSWORD = "mock-rls-probe-password"

ALPHA = uuid.UUID("aaaaaaaa-1111-1111-1111-111111111111")
BETA = uuid.UUID("bbbbbbbb-2222-2222-2222-222222222222")

ALPHA_USER = uuid.UUID("aaaaaaaa-0000-0000-0000-00000000000a")
BETA_USER = uuid.UUID("bbbbbbbb-0000-0000-0000-00000000000b")

#: The same address in both tenants, which is the point of it.
SHARED_EMAIL = "shared@example.test"


# Function-scoped, like every other DB fixture in this suite: pytest-asyncio
# gives each test its own event loop, and an engine built on one loop cannot
# be disposed on another. Creating the probe role per test costs a few
# milliseconds and removes a whole class of cross-test coupling.
@pytest.fixture
async def admin_engine() -> AsyncGenerator[AsyncEngine]:
    """An engine on `DATABASE_URL`, or a skip if there is nothing there.

    Reachability and schema are checked separately, following
    `test_optimistic_concurrency_db.py`: a connection failure is an
    environment without a database and skips, while anything that goes wrong
    afterwards is a real defect and fails.
    """
    engine = create_async_engine(settings.DATABASE_URL, poolclass=NullPool)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except (OSError, SQLAlchemyError) as exc:
        await engine.dispose()
        pytest.skip(f"no usable Postgres at DATABASE_URL: {exc}")
    yield engine
    await engine.dispose()


@pytest.fixture
async def migrated(admin_engine: AsyncEngine) -> None:
    """Fail loudly if the policies are not there.

    Not a skip. `create_all` builds these tables without RLS, so a database
    in that state would silently turn every assertion below into a tautology
    — which is exactly the failure this file exists to catch in production.
    """
    async with admin_engine.connect() as conn:
        report = await inspect_isolation(conn)
    table_problems = [p for table in report.tables for p in table.problems]
    if table_problems:
        raise AssertionError(
            "the database has not had migration 0008 applied: "
            + "; ".join(table_problems)
        )


@pytest.fixture
async def probe_engine(
    admin_engine: AsyncEngine, migrated: None
) -> AsyncGenerator[AsyncEngine]:
    """An engine connected as a role the policies actually apply to.

    `DROP OWNED BY` before `DROP ROLE` because a role holding grants cannot
    be dropped, and the grants below are exactly that.
    """
    async with admin_engine.begin() as conn:
        await _drop_role(conn)
        await conn.execute(
            text(f"CREATE ROLE {PROBE_ROLE} LOGIN PASSWORD '{PROBE_PASSWORD}'")
        )
        await conn.execute(text(f"GRANT USAGE ON SCHEMA public TO {PROBE_ROLE}"))
        for table in TENANT_SCOPED_TABLES:
            await conn.execute(
                text(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO {PROBE_ROLE}")
            )

    url = make_url(settings.DATABASE_URL).set(
        username=PROBE_ROLE, password=PROBE_PASSWORD
    )
    engine = create_async_engine(url)
    yield engine
    await engine.dispose()

    async with admin_engine.begin() as conn:
        await _drop_role(conn)


async def _drop_role(conn: AsyncConnection) -> None:
    exists = await conn.scalar(
        text("SELECT 1 FROM pg_roles WHERE rolname = :name"), {"name": PROBE_ROLE}
    )
    if exists:
        await conn.execute(text(f"DROP OWNED BY {PROBE_ROLE}"))
        await conn.execute(text(f"DROP ROLE {PROBE_ROLE}"))


@pytest.fixture
async def seeded(admin_engine: AsyncEngine, migrated: None) -> AsyncGenerator[None]:
    """Two tenants, one user each, written as the superuser that can see both.

    Written through the admin engine on purpose: seeding through the probe
    role would already be exercising the thing under test, and a seeding bug
    would then look like an isolation result.
    """
    async with admin_engine.begin() as conn:
        await _clean(conn)
        for tenant_id, slug in ((ALPHA, "alpha"), (BETA, "beta")):
            await conn.execute(
                text("INSERT INTO tenants (id, slug, name) VALUES (:id, :slug, :slug)"),
                {"id": tenant_id, "slug": f"rls-probe-{slug}"},
            )
        for user_id, tenant_id in ((ALPHA_USER, ALPHA), (BETA_USER, BETA)):
            await conn.execute(
                text(
                    "INSERT INTO users (id, tenant_id, email, hashed_password)"
                    " VALUES (:id, :tenant_id, :email, 'mock-argon2-hash')"
                ),
                {"id": user_id, "tenant_id": tenant_id, "email": SHARED_EMAIL},
            )
    yield
    async with admin_engine.begin() as conn:
        await _clean(conn)


async def _clean(conn: AsyncConnection) -> None:
    """Tenants cascade to users and refresh tokens, so one statement does it."""
    await conn.execute(
        text("DELETE FROM tenants WHERE id = ANY(:ids)"), {"ids": [ALPHA, BETA]}
    )


@pytest.fixture
def probe_sessions(probe_engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(probe_engine, expire_on_commit=False)


async def _emails(session: AsyncSession) -> list[uuid.UUID]:
    result = await session.execute(text("SELECT id FROM users ORDER BY id"))
    return [row.id for row in result.all()]


class TestTheMigrationItself:
    async def test_every_tenant_scoped_table_is_enabled_forced_and_has_a_policy(
        self, admin_engine: AsyncEngine, migrated: None
    ) -> None:
        async with admin_engine.connect() as conn:
            report = await inspect_isolation(conn)
        assert [t.problems for t in report.tables] == [[] for _ in report.tables]
        assert report.function_exists

    async def test_the_bootstrap_tenant_is_the_one_the_application_names(
        self, admin_engine: AsyncEngine, migrated: None
    ) -> None:
        """`DEFAULT_TENANT_ID` is duplicated in migration 0008 on purpose; this
        is what stops the two copies drifting."""
        async with admin_engine.connect() as conn:
            slug = await conn.scalar(
                text("SELECT slug FROM tenants WHERE id = :id"),
                {"id": DEFAULT_TENANT_ID},
            )
        assert slug == DEFAULT_TENANT_SLUG

    async def test_the_superuser_the_suite_runs_as_is_reported_as_bypassing(
        self, admin_engine: AsyncEngine, migrated: None
    ) -> None:
        """Which is why every test below connects as somebody else."""
        async with admin_engine.connect() as conn:
            report = await inspect_isolation(conn)
        assert not report.enforced
        assert any("superuser" in problem for problem in report.problems)

    async def test_the_probe_role_is_reported_as_enforced(
        self, probe_engine: AsyncEngine
    ) -> None:
        async with probe_engine.connect() as conn:
            report = await inspect_isolation(conn)
        assert report.problems == []
        assert report.enforced


class TestReads:
    async def test_an_unbound_connection_sees_nothing(
        self, probe_sessions: async_sessionmaker[AsyncSession], seeded: None
    ) -> None:
        """Fails closed. The whole design rests on this being the direction."""
        with tenant_scope(None):
            async with probe_sessions() as session:
                assert await _emails(session) == []

    async def test_a_bound_connection_sees_only_its_own_tenant(
        self, probe_sessions: async_sessionmaker[AsyncSession], seeded: None
    ) -> None:
        with tenant_scope(ALPHA):
            async with probe_sessions() as session:
                assert await _emails(session) == [ALPHA_USER]

    async def test_the_other_tenant_sees_the_other_row(
        self, probe_sessions: async_sessionmaker[AsyncSession], seeded: None
    ) -> None:
        with tenant_scope(BETA):
            async with probe_sessions() as session:
                assert await _emails(session) == [BETA_USER]

    async def test_a_primary_key_lookup_across_the_boundary_finds_nothing(
        self, probe_sessions: async_sessionmaker[AsyncSession], seeded: None
    ) -> None:
        """Not a 403. The row does not exist as far as this tenant is
        concerned, and saying anything else makes the boundary enumerable."""
        with tenant_scope(ALPHA):
            async with probe_sessions() as session:
                assert await session.get(User, BETA_USER) is None

    async def test_the_tenants_table_shows_one_row(
        self, probe_sessions: async_sessionmaker[AsyncSession], seeded: None
    ) -> None:
        """Otherwise the customer list is readable by every customer."""
        with tenant_scope(ALPHA):
            async with probe_sessions() as session:
                result = await session.execute(text("SELECT id FROM tenants"))
                assert [row.id for row in result.all()] == [ALPHA]


class TestWrites:
    async def test_an_insert_takes_its_tenant_from_the_connection(
        self, probe_sessions: async_sessionmaker[AsyncSession], seeded: None
    ) -> None:
        """Which is what lets `AuthService.register` stay unaware of tenancy."""
        with tenant_scope(ALPHA):
            async with probe_sessions() as session:
                # No `tenant_id`, exactly as `AuthService.register` builds it.
                session.add(
                    User(email="fresh@example.test", hashed_password="mock-hash")
                )
                await session.commit()

            # Read back rather than asserted on the instance: the value came
            # from a *server* default, which SQLAlchemy does not fetch unless
            # asked, so the in-memory object is not the evidence here.
            async with probe_sessions() as session:
                result = await session.execute(
                    text("SELECT tenant_id FROM users WHERE email = :email"),
                    {"email": "fresh@example.test"},
                )
                assert [row.tenant_id for row in result.all()] == [ALPHA]

    async def test_writing_into_another_tenant_is_refused(
        self, probe_sessions: async_sessionmaker[AsyncSession], seeded: None
    ) -> None:
        """`WITH CHECK`. `USING` alone would let this through and then hide it."""
        with tenant_scope(ALPHA):
            async with probe_sessions() as session:
                session.add(
                    User(
                        tenant_id=BETA,
                        email="smuggled@example.test",
                        hashed_password="mock-hash",
                    )
                )
                with pytest.raises(DBAPIError) as exc:
                    await session.commit()
        assert "row-level security" in str(exc.value)

    async def test_moving_a_row_to_another_tenant_is_refused(
        self, probe_sessions: async_sessionmaker[AsyncSession], seeded: None
    ) -> None:
        with tenant_scope(ALPHA):
            async with probe_sessions() as session:
                with pytest.raises(DBAPIError) as exc:
                    await session.execute(
                        text("UPDATE users SET tenant_id = :beta WHERE id = :id"),
                        {"beta": BETA, "id": ALPHA_USER},
                    )
        assert "row-level security" in str(exc.value)

    async def test_a_delete_cannot_reach_across_the_boundary(
        self, probe_sessions: async_sessionmaker[AsyncSession], seeded: None
    ) -> None:
        """It affects no rows rather than failing: the row is not visible, so
        there is nothing for the statement to match."""
        with tenant_scope(ALPHA):
            async with probe_sessions() as session:
                result = await session.execute(
                    text("DELETE FROM users WHERE id = :id"), {"id": BETA_USER}
                )
                assert result.rowcount == 0
                await session.commit()

        with tenant_scope(BETA):
            async with probe_sessions() as session:
                assert await _emails(session) == [BETA_USER]


class TestEmailUniqueness:
    async def test_the_same_address_exists_in_both_tenants(
        self, probe_sessions: async_sessionmaker[AsyncSession], seeded: None
    ) -> None:
        """The seed wrote `shared@example.test` twice. Under the old global
        unique index on `email` that insert would have failed — which is the
        whole reason the constraint moved."""
        for tenant_id, expected in ((ALPHA, ALPHA_USER), (BETA, BETA_USER)):
            with tenant_scope(tenant_id):
                async with probe_sessions() as session:
                    found = await session.execute(
                        text("SELECT id FROM users WHERE email = :email"),
                        {"email": SHARED_EMAIL},
                    )
                    assert [row.id for row in found.all()] == [expected]

    async def test_a_duplicate_inside_one_tenant_is_still_a_conflict(
        self, probe_sessions: async_sessionmaker[AsyncSession], seeded: None
    ) -> None:
        with tenant_scope(ALPHA):
            async with probe_sessions() as session:
                session.add(User(email=SHARED_EMAIL, hashed_password="mock-hash"))
                with pytest.raises(IntegrityError) as exc:
                    await session.commit()
        assert "uq_users_tenant_email" in str(exc.value)


class TestTheBindingItself:
    async def test_the_setting_does_not_survive_the_transaction(
        self, probe_engine: AsyncEngine, seeded: None
    ) -> None:
        """The leak this whole mechanism is built to avoid.

        One physical connection, two transactions. A session-level `SET` would
        still be in place for the second — which, on a pooled connection, is
        the next request, belonging to somebody else.

        Note what the second transaction reads back: `''`, not `NULL`. A
        custom parameter that has ever been set in a session stays *known* for
        the rest of it and reverts to the empty string rather than becoming
        unset again. That is exactly why `app_current_tenant_id()` wraps the
        read in `NULLIF(…, '')`: without it, this connection's next statement
        would evaluate `''::uuid` inside a policy and raise, turning a read
        that should return nothing into a 500.
        """
        async with probe_engine.connect() as conn:
            with tenant_scope(ALPHA):
                async with conn.begin():
                    first_pid = await conn.scalar(text("SELECT pg_backend_pid()"))
                    assert await conn.scalar(text(READ_TENANT)) == str(ALPHA)

            with tenant_scope(None):
                async with conn.begin():
                    second_pid = await conn.scalar(text("SELECT pg_backend_pid()"))
                    assert await conn.scalar(text(READ_TENANT)) == ""
                    assert (
                        await conn.scalar(text("SELECT app_current_tenant_id()"))
                        is None
                    )
                    rows = await conn.execute(text("SELECT id FROM users"))
                    assert rows.all() == []

        assert first_pid == second_pid, "the two transactions must share a connection"

    async def test_an_empty_setting_reads_as_no_tenant_rather_than_an_error(
        self, probe_engine: AsyncEngine, seeded: None
    ) -> None:
        """`NULLIF(…, '')`, measured rather than asserted about.

        Without it this is `invalid input syntax for type uuid: ""` raised
        from inside a policy — a 500 on a request that should simply have
        found nothing, and reachable by anything that clears the setting.
        """
        with tenant_scope(None):
            async with probe_engine.begin() as conn:
                await conn.execute(text("SELECT set_config('app.tenant_id', '', true)"))
                assert await conn.scalar(text("SELECT app_current_tenant_id()")) is None
                rows = await conn.execute(text("SELECT id FROM users"))
                assert rows.all() == []

    async def test_a_second_transaction_rebinds_rather_than_losing_the_tenant(
        self, probe_sessions: async_sessionmaker[AsyncSession], seeded: None
    ) -> None:
        """A session that commits mid-request keeps working.

        This is why the listener is on `begin` and not on session creation:
        binding once would leave everything after the first `COMMIT` unscoped,
        and unscoped reads nothing.
        """
        with tenant_scope(ALPHA):
            async with probe_sessions() as session:
                assert await _emails(session) == [ALPHA_USER]
                await session.commit()
                assert await _emails(session) == [ALPHA_USER]

    async def test_the_tenant_is_bound_once_per_transaction(
        self, probe_sessions: async_sessionmaker[AsyncSession], seeded: None
    ) -> None:
        """`tests/querycount.py` keeps this statement out of its statement
        count; this is the assertion that keeps it honest."""
        with tenant_scope(ALPHA):
            async with probe_sessions() as session:
                with capture_queries(session) as log:
                    await _emails(session)
                    await session.commit()
                    await _emails(session)

        assert log.tenant_bindings == 2
        assert len(log) == 2
