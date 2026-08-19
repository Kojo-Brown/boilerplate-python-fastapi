"""Backend selection.

Nothing outside this module needs to know which backend is in use or how to
build one: callers depend on `LockBackend`, configuration decides the
implementation.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import lru_cache
from typing import Final, Literal

import structlog

from src.config import Settings, settings
from src.distributed_lock.base import LockBackend
from src.distributed_lock.memory import InMemoryLockBackend
from src.distributed_lock.redis_backend import RedisLockBackend

logger = structlog.get_logger(__name__)

LockBackendName = Literal["redis", "memory"]

BackendBuilder = Callable[[Settings], LockBackend]


def _build_redis(config: Settings) -> LockBackend:
    return RedisLockBackend.from_url(
        config.DISTRIBUTED_LOCK_REDIS_URL or config.REDIS_URL,
        namespace=config.DISTRIBUTED_LOCK_NAMESPACE,
    )


def _build_memory(config: Settings) -> LockBackend:
    # Takes the settings it does not read so that both builders share one
    # signature, which is what lets `BUILDERS` be a plain dict lookup rather
    # than a match statement.
    return InMemoryLockBackend()


BUILDERS: Final[dict[str, BackendBuilder]] = {
    "redis": _build_redis,
    "memory": _build_memory,
}


def create_lock_backend(
    backend: str | None = None, *, config: Settings | None = None
) -> LockBackend:
    """Return a new backend.

    `backend` defaults to `DISTRIBUTED_LOCK_BACKEND` and `config` to the
    process settings, so the no-argument call is the configured backend.
    """
    resolved = config if config is not None else settings
    name = backend if backend is not None else resolved.DISTRIBUTED_LOCK_BACKEND

    builder = BUILDERS.get(name)
    if builder is None:
        # Unreachable through settings — the field is a Literal, so pydantic
        # rejects an unknown name at start-up — but reachable from a direct
        # call, and falling back to the in-memory backend would be a deployment
        # whose locks coordinate nothing while looking like they work.
        raise ValueError(
            f"Unknown distributed lock backend '{name}'. "
            f"Available: {', '.join(sorted(BUILDERS))}."
        )

    if name == "memory" and resolved.ENVIRONMENT not in ("test", "development"):
        logger.warning(
            "distributed_lock.memory_backend_outside_development",
            environment=resolved.ENVIRONMENT,
            detail=(
                "The in-memory backend is per-process: two workers will both "
                "acquire the same lock, and will hand out the same fencing "
                "tokens to different holders."
            ),
        )

    logger.debug("distributed_lock.backend_created", backend=name)
    return builder(resolved)


@lru_cache(maxsize=1)
def get_lock_backend() -> LockBackend:
    """The process-wide configured backend.

    Cached because the Redis backend owns a connection pool that must not be
    rebuilt per call, and because the in-memory backend only coordinates
    anything at all when every caller shares one instance. Call
    `get_lock_backend.cache_clear()` after changing the backend in a test.
    """
    return create_lock_backend()
