"""Store selection.

Nothing outside this module needs to know which store is in use or how to
build one: callers depend on `IdempotencyStore`, configuration decides the
implementation.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import lru_cache
from typing import Final, Literal

import structlog

from src.config import Settings, settings
from src.idempotency.base import IdempotencyStore
from src.idempotency.memory import InMemoryIdempotencyStore
from src.idempotency.redis_store import RedisIdempotencyStore

logger = structlog.get_logger(__name__)

IdempotencyBackendName = Literal["redis", "memory"]

StoreBuilder = Callable[[Settings], IdempotencyStore]


def _build_redis(config: Settings) -> IdempotencyStore:
    return RedisIdempotencyStore.from_url(
        config.IDEMPOTENCY_REDIS_URL or config.REDIS_URL,
        record_ttl_seconds=config.IDEMPOTENCY_TTL_SECONDS,
        reservation_ttl_seconds=config.IDEMPOTENCY_RESERVATION_TTL_SECONDS,
    )


def _build_memory(config: Settings) -> IdempotencyStore:
    return InMemoryIdempotencyStore(
        record_ttl_seconds=float(config.IDEMPOTENCY_TTL_SECONDS),
        reservation_ttl_seconds=float(config.IDEMPOTENCY_RESERVATION_TTL_SECONDS),
    )


BUILDERS: Final[dict[str, StoreBuilder]] = {
    "redis": _build_redis,
    "memory": _build_memory,
}


def create_idempotency_store(
    backend: str | None = None, *, config: Settings | None = None
) -> IdempotencyStore:
    """Return a new store.

    `backend` defaults to `IDEMPOTENCY_BACKEND` and `config` to the process
    settings, so the no-argument call is the configured store.
    """
    resolved = config if config is not None else settings
    name = backend if backend is not None else resolved.IDEMPOTENCY_BACKEND

    builder = BUILDERS.get(name)
    if builder is None:
        # Unreachable through settings — the field is a Literal, so pydantic
        # rejects an unknown name at start-up — but reachable from a direct
        # call, and a silent fallback to the in-memory store would be a
        # production deployment that quietly deduplicates nothing.
        raise ValueError(
            f"Unknown idempotency backend '{name}'. "
            f"Available: {', '.join(sorted(BUILDERS))}."
        )

    if name == "memory" and resolved.ENVIRONMENT not in ("test", "development"):
        logger.warning(
            "idempotency.memory_store_outside_development",
            environment=resolved.ENVIRONMENT,
            detail=(
                "The in-memory store is per-process: a retry served by another "
                "worker will execute a second time."
            ),
        )

    logger.debug("idempotency.store_created", backend=name)
    return builder(resolved)


@lru_cache(maxsize=1)
def get_idempotency_store() -> IdempotencyStore:
    """The process-wide configured store.

    Cached because the Redis backend owns a connection pool that must not be
    rebuilt per request, and because the in-memory backend is only meaningful
    when every caller shares one instance. Call
    `get_idempotency_store.cache_clear()` after changing the backend in a test.
    """
    return create_idempotency_store()
