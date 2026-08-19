"""Lock backend selection from configuration."""

from __future__ import annotations

import pytest

from src.config import Settings, settings
from src.distributed_lock.base import LockBackend
from src.distributed_lock.factory import create_lock_backend, get_lock_backend
from src.distributed_lock.memory import InMemoryLockBackend
from src.distributed_lock.redis_backend import RedisLockBackend


def config(**overrides: object) -> Settings:
    """The process settings with fields replaced, so no `.env` is involved."""
    return settings.model_copy(update=overrides)


class TestSelection:
    def test_redis_is_the_configured_default(self) -> None:
        """A deployment that forgets to choose must not silently get a lock
        that coordinates one process and nothing else."""
        assert Settings.model_fields["DISTRIBUTED_LOCK_BACKEND"].default == "redis"

    async def test_it_builds_a_redis_backend(self) -> None:
        backend = create_lock_backend("redis", config=config())

        assert isinstance(backend, RedisLockBackend)
        assert backend.name == "redis"
        await backend.close()

    async def test_it_builds_a_memory_backend(self) -> None:
        backend = create_lock_backend("memory", config=config())

        assert isinstance(backend, InMemoryLockBackend)
        assert backend.name == "memory"

    async def test_the_backend_defaults_to_the_setting(self) -> None:
        backend = create_lock_backend(config=config(DISTRIBUTED_LOCK_BACKEND="memory"))

        assert isinstance(backend, InMemoryLockBackend)

    def test_an_unknown_backend_raises(self) -> None:
        """A silent fallback would be a deployment whose locks coordinate
        nothing while looking like they work."""
        with pytest.raises(ValueError, match="Unknown distributed lock backend"):
            create_lock_backend("zookeeper", config=config())

    async def test_the_memory_backend_outside_development_still_builds(self) -> None:
        """It warns rather than refusing — the operator may know what they
        want, and a single-process deployment is a real thing."""
        backend = create_lock_backend("memory", config=config(ENVIRONMENT="production"))

        assert isinstance(backend, InMemoryLockBackend)


class TestRedisUrl:
    async def test_it_falls_back_to_redis_url(self) -> None:
        backend = create_lock_backend(
            "redis",
            config=config(
                REDIS_URL="redis://localhost:6399/3", DISTRIBUTED_LOCK_REDIS_URL=""
            ),
        )

        # Reading the pool is the only way to see which URL was used; the
        # backend deliberately exposes no accessor for it.
        kwargs = backend._client.connection_pool.connection_kwargs  # type: ignore[attr-defined]
        assert (kwargs["port"], kwargs["db"]) == (6399, 3)
        await backend.close()

    async def test_the_dedicated_url_wins(self) -> None:
        """So a deployment can keep locks off the instance Celery is hammering
        without moving everything else."""
        backend = create_lock_backend(
            "redis",
            config=config(
                REDIS_URL="redis://localhost:6379/0",
                DISTRIBUTED_LOCK_REDIS_URL="redis://localhost:6398/5",
            ),
        )

        kwargs = backend._client.connection_pool.connection_kwargs  # type: ignore[attr-defined]
        assert (kwargs["port"], kwargs["db"]) == (6398, 5)
        await backend.close()

    async def test_the_namespace_comes_from_settings(self) -> None:
        """Separate namespaces are what let one Redis serve Celery, the
        idempotency store and this without their keys colliding."""
        backend = create_lock_backend(
            "redis", config=config(DISTRIBUTED_LOCK_NAMESPACE="tenant-a-locks")
        )

        assert backend._namespace == "tenant-a-locks"  # type: ignore[attr-defined]
        await backend.close()


class TestProcessWideBackend:
    async def test_it_is_built_once(self) -> None:
        """The Redis backend owns a connection pool that must not be rebuilt
        per call, and the in-memory one coordinates nothing unless every caller
        shares one instance."""
        get_lock_backend.cache_clear()
        try:
            assert get_lock_backend() is get_lock_backend()
        finally:
            await get_lock_backend().close()
            get_lock_backend.cache_clear()

    async def test_it_satisfies_the_protocol(self) -> None:
        get_lock_backend.cache_clear()
        try:
            assert isinstance(get_lock_backend(), LockBackend)
        finally:
            await get_lock_backend().close()
            get_lock_backend.cache_clear()
