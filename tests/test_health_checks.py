"""The concrete probes, and the redaction that keeps their logs clean.

The database check is exercised twice: against a double, for the failure shapes
a server does not produce on request, and against a *real* engine pointed at a
closed port, which is the one failure a unit test can have for free and the one
most likely to happen in production.
"""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from src.health.checks import DatabaseCheck, RedisCheck, redact_url
from tests.fakes import FakeEngine, FakeRedisClient

# ── redact_url ────────────────────────────────────────────────────────────────


def test_redact_url_replaces_the_password() -> None:
    assert (
        redact_url("redis://default:hunter2@cache.internal:6379/0")
        == "redis://default:***@cache.internal:6379/0"
    )


def test_redact_url_leaves_a_url_without_a_password_alone() -> None:
    assert redact_url("redis://localhost:6379/0") == "redis://localhost:6379/0"


def test_redact_url_handles_a_password_with_no_username() -> None:
    assert redact_url("redis://:hunter2@cache:6379/0") == "redis://***@cache:6379/0"


def test_redact_url_keeps_the_database_number() -> None:
    """The path is what tells two Redis logical databases apart in a log line."""
    assert redact_url("redis://user:pw@cache:6379/3").endswith("/3")


def test_redact_url_returns_nothing_for_a_url_it_cannot_parse() -> None:
    """A failed redaction must not be the thing that prints a credential."""
    assert redact_url("redis://user:hunter2@[not-an-address:6379/0") == ""


def test_redact_url_handles_a_url_with_no_port() -> None:
    assert redact_url("redis://user:pw@cache/0") == "redis://user:***@cache/0"


# ── DatabaseCheck ─────────────────────────────────────────────────────────────


def test_the_database_check_is_always_required() -> None:
    """No route in this application means anything without Postgres."""
    assert DatabaseCheck(FakeEngine()).criticality == "required"


def test_the_database_check_describes_itself_without_a_dsn() -> None:
    """The description is served publicly; a DSN carries host and credentials."""
    description = DatabaseCheck(FakeEngine()).description

    assert "postgres" in description
    assert "://" not in description


@pytest.mark.asyncio
async def test_the_database_check_round_trips_a_statement() -> None:
    """A probe that opens a connection and executes nothing proves half of it.

    A pool can hand out a connection whose server has since gone away; the
    statement is what discovers that.
    """
    engine = FakeEngine()

    await DatabaseCheck(engine).probe()

    assert [c.executed for c in engine.connections] == [["SELECT 1"]]


@pytest.mark.asyncio
async def test_the_database_check_propagates_a_refused_connect() -> None:
    """Raising is how a check fails; the registry does the catching."""
    engine = FakeEngine(connect_error=ConnectionRefusedError(111, "refused"))

    with pytest.raises(ConnectionRefusedError):
        await DatabaseCheck(engine).probe()


@pytest.mark.asyncio
async def test_the_database_check_propagates_a_failure_mid_statement() -> None:
    engine = FakeEngine(execute_error=OSError("server closed the connection"))

    with pytest.raises(OSError):
        await DatabaseCheck(engine).probe()


@pytest.mark.asyncio
async def test_the_database_check_fails_against_a_real_engine_with_nobody_listening(
    unused_tcp_port: int,
) -> None:
    """The real driver, the real URL, no server: what an outage looks like.

    Port 1 would do, but `unused_tcp_port` is guaranteed free on this host, so
    the test cannot be answered by something that happens to be listening.
    """
    engine = create_async_engine(
        f"postgresql+asyncpg://nobody:nothing@127.0.0.1:{unused_tcp_port}/nothing"
    )
    try:
        # An `OSError`, not a `SQLAlchemyError`: a refused connect never reaches
        # SQLAlchemy's wrapping layer, which is why the registry catches
        # `Exception` rather than the ORM's own base class. A check that only
        # caught `SQLAlchemyError` would let the commonest database failure of
        # all escape as a 500 from an endpoint whose entire job is to report it.
        with pytest.raises(OSError):
            await DatabaseCheck(engine).probe()
    finally:
        await engine.dispose()


# ── RedisCheck ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_the_redis_check_pings() -> None:
    client = FakeRedisClient()
    check = RedisCheck(
        lambda: client, name="redis", criticality="required", description="redis"
    )

    await check.probe()

    assert client.pings == 1


@pytest.mark.asyncio
async def test_the_redis_client_is_built_once_and_only_when_first_probed() -> None:
    """Constructing a registry — which happens at import — must open no socket."""
    built: list[FakeRedisClient] = []

    def factory() -> FakeRedisClient:
        client = FakeRedisClient()
        built.append(client)
        return client

    check = RedisCheck(
        factory, name="redis", criticality="required", description="redis"
    )
    assert built == []

    await check.probe()
    await check.probe()

    assert len(built) == 1
    assert built[0].pings == 2


@pytest.mark.asyncio
async def test_the_redis_check_propagates_a_connection_error() -> None:
    client = FakeRedisClient(error=ConnectionError("Error connecting to cache:6379"))
    check = RedisCheck(
        lambda: client, name="redis", criticality="required", description="redis"
    )

    with pytest.raises(ConnectionError):
        await check.probe()


@pytest.mark.asyncio
async def test_closing_a_redis_check_that_was_never_probed_builds_nothing() -> None:
    """Otherwise shutdown is the one path that could open a socket."""
    built: list[FakeRedisClient] = []

    def factory() -> FakeRedisClient:
        client = FakeRedisClient()
        built.append(client)
        return client

    check = RedisCheck(
        factory, name="redis", criticality="required", description="redis"
    )

    await check.aclose()

    assert built == []


@pytest.mark.asyncio
async def test_closing_a_redis_check_closes_the_client_once() -> None:
    client = FakeRedisClient()
    check = RedisCheck(
        lambda: client, name="redis", criticality="required", description="redis"
    )
    await check.probe()

    await check.aclose()
    await check.aclose()

    assert client.closed == 1


@pytest.mark.asyncio
async def test_a_redis_check_probed_after_closing_builds_a_fresh_client() -> None:
    """`aclose` releases the client rather than poisoning the check.

    A closed redis-py client raises on every command, so keeping it would make
    a post-shutdown probe report a healthy server as broken.
    """
    built: list[FakeRedisClient] = []

    def factory() -> FakeRedisClient:
        client = FakeRedisClient()
        built.append(client)
        return client

    check = RedisCheck(
        factory, name="redis", criticality="required", description="redis"
    )

    await check.probe()
    await check.aclose()
    await check.probe()

    assert len(built) == 2
    assert built[0].closed == 1
    assert [client.pings for client in built] == [1, 1]


def test_a_redis_check_reports_the_criticality_it_was_given() -> None:
    """Criticality is decided by configuration, not by the backend's identity."""
    optional = RedisCheck(
        FakeRedisClient, name="redis", criticality="optional", description="redis"
    )

    assert optional.criticality == "optional"
    assert optional.name == "redis"
    assert optional.description == "redis"
