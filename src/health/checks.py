"""The concrete probes: Postgres and Redis.

Both are deliberately the cheapest round trip the protocol offers — `SELECT 1`
and `PING`. A readiness probe is not a smoke test: it must not read a table,
touch application state or do anything whose cost grows with the data, because
it runs several times a minute on every replica forever. What it has to prove
is that a connection can be opened and a reply can come back, which is exactly
the part that breaks.

## Two different opinions about connection pools, and why

The database check goes through the **application's own engine**. If that pool
cannot hand out a connection, no request can be served either, so a probe that
opened a private connection would report a database this process cannot
actually use as healthy. Borrowing from the real pool is the point.

The Redis check uses a **dedicated client** instead, for three reasons that do
not apply to the database. The stores in `src/idempotency` and
`src/distributed_lock` own their clients privately and expose no ping; several
subsystems share one server, so there is no single pool to borrow from; and
redis-py opens connections on demand rather than queueing for a fixed pool, so
a private client costs one idle socket and takes nothing from request traffic.
It is capped at one connection and given driver-level socket timeouts, which
are what bound a probe when the event loop itself is not free to enforce
`asyncio.timeout`.

The consequence is worth stating plainly, because it decides what these probes
mean: the Redis check answers *is the server reachable*, not *is our pool
healthy*. Pool exhaustion is a saturation signal and belongs on a dashboard —
see `docs/metrics.md` — not in a readiness probe, where the response to
overload would be to remove the busiest replicas from the load balancer and
hand their traffic to the rest.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from typing import Any, Final, Protocol
from urllib.parse import urlsplit, urlunsplit

from sqlalchemy import text

from src.health.base import Criticality

#: The statement every readiness probe in every language eventually converges
#: on: it parses, it needs a live session, and it reads nothing.
_PING_STATEMENT: Final[str] = "SELECT 1"


class SupportsExecute(Protocol):
    """The one method the database check needs from a connection."""

    async def execute(self, statement: Any) -> Any: ...


class SupportsConnect(Protocol):
    """A SQLAlchemy `AsyncEngine`, narrowed to what a probe uses.

    A protocol rather than the concrete engine so a test can hand in a double
    that fails in an exact way — a refused connect, a connection that opens and
    then hangs — without needing a server that behaves like that on demand.
    """

    def connect(self) -> AbstractAsyncContextManager[SupportsExecute]: ...


class SupportsPing(Protocol):
    """A `redis.asyncio.Redis`, narrowed to what a probe uses.

    `ping` is declared as returning an awaitable rather than as `async def`
    because that is how redis-py declares it: the command methods are shared
    between the sync and async clients and typed `-> ResponseT`, so a protocol
    written with `async def` is one the real client does not satisfy.
    """

    def ping(self) -> Awaitable[Any]: ...

    async def aclose(self) -> None: ...


def redact_url(url: str) -> str:
    """Replace the password in a URL with `***`, keeping the rest readable.

    Used for *log* lines that name which server a check points at. Nothing
    redacted or otherwise reaches the response body, which carries no URL at
    all — but a log line that quotes `redis://default:hunter2@cache:6379/0`
    leaks the password to whatever ships the logs, so it is redacted at the one
    place it is produced.

    A URL that does not parse is returned as the empty string rather than
    guessed at: this is only ever decoration for a log line, and a failed
    redaction must not be the thing that prints a credential.
    """
    try:
        parts = urlsplit(url)
    except ValueError:
        return ""
    if parts.password is None:
        return url

    host = parts.hostname or ""
    if parts.port is not None:
        host = f"{host}:{parts.port}"
    userinfo = f"{parts.username}:***@" if parts.username else "***@"
    return urlunsplit(
        (parts.scheme, f"{userinfo}{host}", parts.path, parts.query, parts.fragment)
    )


class DatabaseCheck:
    """`SELECT 1` through the application's engine.

    Always `required`: there is no route in this application that means
    anything without Postgres, so a process that cannot reach it has nothing to
    serve and should be taken out of rotation.
    """

    def __init__(
        self,
        engine: SupportsConnect,
        *,
        name: str = "database",
        description: str = "postgresql, via the application connection pool",
    ) -> None:
        self._engine = engine
        self._name = name
        self._description = description

    @property
    def name(self) -> str:
        return self._name

    @property
    def criticality(self) -> Criticality:
        return "required"

    @property
    def description(self) -> str:
        return self._description

    async def probe(self) -> None:
        # `connect()` rather than a session from `get_db`: the probe wants a
        # pooled connection and a round trip, and nothing an ORM session adds
        # — identity map, transaction bookkeeping, a flush on close — is part
        # of what it is asking about.
        async with self._engine.connect() as connection:
            await connection.execute(text(_PING_STATEMENT))


class RedisCheck:
    """`PING` against one Redis server, on a client of its own.

    The client is built lazily by `client_factory` on the first probe, so
    constructing a registry — which happens at import time, in tests included —
    opens no sockets, and a deployment whose probe is never called pays
    nothing.
    """

    def __init__(
        self,
        client_factory: Callable[[], SupportsPing],
        *,
        name: str,
        criticality: Criticality,
        description: str,
    ) -> None:
        self._client_factory = client_factory
        self._client: SupportsPing | None = None
        self._name = name
        self._criticality = criticality
        self._description = description

    @property
    def name(self) -> str:
        return self._name

    @property
    def criticality(self) -> Criticality:
        return self._criticality

    @property
    def description(self) -> str:
        return self._description

    async def probe(self) -> None:
        if self._client is None:
            self._client = self._client_factory()
        await self._client.ping()

    async def aclose(self) -> None:
        """Close the client if one was ever built.

        Called by `HealthRegistry.aclose` from the lifespan. Building a client
        here to close it would be the one way this check could open a socket
        during shutdown, hence the `None` guard.
        """
        if self._client is None:
            return
        client, self._client = self._client, None
        await client.aclose()
