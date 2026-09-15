"""Which checks this process runs, derived from its configuration.

Nothing here decides that "Redis is important". It reads what *this* deployment
has switched on and works out, per dependency, whether a request can be served
without it. Four rules, each traceable to a line of application code:

* **Postgres — required.** Every route that is not a probe reads or writes it.
* **The Celery broker — required.** `src/auth/router.py` calls
  `send_welcome_email_task.delay(...)` inside the register handler, so a broker
  this process cannot reach is a registration that fails, not an email that is
  late. Move that enqueue behind the outbox and this becomes optional; the
  comment is here so the two stay in step.
* **The idempotency store — whatever `IDEMPOTENCY_FAIL_OPEN` says.** This is
  the clearest case for reading criticality out of configuration rather than
  hard-coding it. With fail-open off, an unreachable store means every request
  carrying an `Idempotency-Key` is refused — a required dependency by any
  definition. With it on, those requests are served without deduplication,
  which is a degradation the operator has explicitly chosen and must not take
  the process out of its load balancer.
* **The lock backend — required; Redis Streams — optional.** A handler that
  cannot take a lock fails (`src/locking`, `src/distributed_lock`). A stream
  consumer is a background loop that nothing in the request path touches, so
  its backend being down is lag, not an inability to serve.

Kafka is deliberately absent. Events leave this application through the
transactional outbox (`src/outbox`): a request commits a row and returns, and
the relay delivers it afterwards, which is the entire reason the outbox exists.
An unreachable broker therefore delays delivery without making this process
unable to serve traffic, and failing readiness on it would take a healthy API
out of rotation for a queue that is doing exactly what it was designed to do.
An application that adds a route publishing synchronously through
`MessagePublisherDep` should add a `required` check for it — `HealthRegistry`
takes any object matching `HealthCheck`, which is what that extension point is
for.

In-process backends (`memory` anywhere above) produce no check at all. There is
nothing to round-trip, and a check that cannot fail is noise in a body an
operator reads under pressure.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache, partial

import structlog
from redis.asyncio import Redis

from src.config import Settings, settings
from src.database import engine
from src.health.base import Criticality, HealthCheck
from src.health.checks import (
    DatabaseCheck,
    RedisCheck,
    SupportsConnect,
    SupportsPing,
    redact_url,
)
from src.health.registry import HealthRegistry

logger = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class RedisUse:
    """One subsystem's use of one Redis server."""

    subsystem: str
    url: str
    criticality: Criticality


def redis_uses(config: Settings) -> tuple[RedisUse, ...]:
    """Every Redis dependency this configuration actually has.

    Order is fixed rather than incidental: it decides the order checks appear
    in the probe body, and a body whose key order moves between processes is
    harder to diff than it needs to be.
    """
    uses: list[RedisUse] = [
        # No switch guards this one: `src/worker.py` builds the Celery app from
        # REDIS_URL unconditionally, and the register handler enqueues through
        # it on every call.
        RedisUse("celery", config.REDIS_URL, "required"),
    ]

    if config.IDEMPOTENCY_ENABLED and config.IDEMPOTENCY_BACKEND == "redis":
        uses.append(
            RedisUse(
                "idempotency",
                config.IDEMPOTENCY_REDIS_URL or config.REDIS_URL,
                "optional" if config.IDEMPOTENCY_FAIL_OPEN else "required",
            )
        )

    if config.DISTRIBUTED_LOCK_BACKEND == "redis":
        uses.append(
            RedisUse(
                "distributed-lock",
                config.DISTRIBUTED_LOCK_REDIS_URL or config.REDIS_URL,
                "required",
            )
        )

    if config.REDIS_STREAMS_BACKEND == "redis":
        uses.append(
            RedisUse(
                "redis-streams",
                config.REDIS_STREAMS_URL or config.REDIS_URL,
                "optional",
            )
        )

    return tuple(uses)


def _group_by_url(uses: tuple[RedisUse, ...]) -> dict[str, tuple[RedisUse, ...]]:
    """Collapse the uses onto the servers they actually point at.

    The three `*_REDIS_URL` overrides all fall back to `REDIS_URL`, so the
    ordinary deployment has one server wearing four hats. Probing it once per
    hat would multiply this endpoint's load on it by four and report the same
    answer four times.

    Grouping is by the literal string, not by anything resolved: two spellings
    of one server (`redis://cache:6379` and `redis://cache:6379/0`) produce two
    checks. Normalising them would mean deciding what a URL *means* — default
    ports, default databases, whether a hostname and its IP are the same server
    — which is a guess this has no business making about a value an operator
    wrote down deliberately.
    """
    grouped: defaultdict[str, list[RedisUse]] = defaultdict(list)
    for use in uses:
        grouped[use.url].append(use)
    return {url: tuple(group) for url, group in grouped.items()}


def _redis_check_name(group: tuple[RedisUse, ...], *, single_server: bool) -> str:
    """`redis` when there is one server, `redis:<subsystems>` when there are more.

    Naming the common case plainly keeps the probe body readable; naming the
    split case after its users is the only stable way to tell two servers apart
    without putting a URL — and therefore a hostname and possibly a credential
    — in a public response.
    """
    if single_server:
        return "redis"
    return "redis:" + "+".join(sorted(use.subsystem for use in group))


def _redis_client_factory(url: str, timeout_seconds: float) -> SupportsPing:
    """One capped, timeout-bounded client for probing `url`.

    `max_connections=1` because a probe is one command at a time and a pool
    that can grow under a slow server is a pool that hides the slowness. The
    socket timeouts matter more than they look: they are what bounds a probe
    whose event loop is too busy for `asyncio.timeout` to fire promptly.
    """
    # Annotated rather than returned straight through: redis-py's `from_url`
    # is typed as returning `Any`, and letting that flow out of here would make
    # every caller's `SupportsPing` an unchecked promise.
    client: Redis = Redis.from_url(
        url,
        socket_connect_timeout=timeout_seconds,
        socket_timeout=timeout_seconds,
        max_connections=1,
    )
    return client


def build_redis_checks(config: Settings) -> tuple[HealthCheck, ...]:
    """A check per distinct Redis server this configuration uses."""
    grouped = _group_by_url(redis_uses(config))
    single_server = len(grouped) == 1

    checks: list[HealthCheck] = []
    for url, group in grouped.items():
        subsystems = sorted(use.subsystem for use in group)
        criticality: Criticality = (
            "required"
            if any(use.criticality == "required" for use in group)
            else "optional"
        )
        checks.append(
            RedisCheck(
                # `partial` rather than a lambda closing over the loop
                # variable, which would give every check the last URL — the
                # oldest bug in the book, and silent here because all four
                # URLs are usually the same one anyway.
                partial(
                    _redis_client_factory, url, config.HEALTH_CHECK_TIMEOUT_SECONDS
                ),
                name=_redis_check_name(group, single_server=single_server),
                criticality=criticality,
                description=f"redis, used by {', '.join(subsystems)}",
            )
        )
        logger.debug(
            "health.redis_check_registered",
            server=redact_url(url),
            subsystems=subsystems,
            criticality=criticality,
        )
    return tuple(checks)


def build_registry(
    *,
    config: Settings | None = None,
    database: SupportsConnect | None = None,
) -> HealthRegistry:
    """The registry this process's configuration asks for.

    Both arguments default to the process-wide objects, so the no-argument call
    is the real one; a test passes its own `Settings` and a database double
    instead of mutating the global settings, for the reason given in
    `src/config.py`.
    """
    resolved = config if config is not None else settings
    checks: list[HealthCheck] = [
        DatabaseCheck(database if database is not None else engine)
    ]
    checks.extend(build_redis_checks(resolved))

    return HealthRegistry(
        checks,
        timeout_seconds=resolved.HEALTH_CHECK_TIMEOUT_SECONDS,
        cache_ttl_seconds=resolved.HEALTH_CACHE_TTL_SECONDS,
    )


@lru_cache(maxsize=1)
def get_health_registry() -> HealthRegistry:
    """The process-wide registry, resolved into the readiness route.

    Cached because the checks own clients and a cache: a registry per request
    would build a Redis client per probe and coalesce nothing. Call
    `get_health_registry.cache_clear()` in a test that needs a different
    configuration, and prefer overriding the FastAPI dependency instead.
    """
    return build_registry()
