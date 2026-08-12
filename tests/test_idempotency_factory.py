"""Backend selection from configuration."""

from __future__ import annotations

import pytest

from src.config import Settings, settings
from src.idempotency.factory import create_idempotency_store, get_idempotency_store
from src.idempotency.memory import InMemoryIdempotencyStore
from src.idempotency.redis_store import RedisIdempotencyStore


def config(**overrides: object) -> Settings:
    """The process settings with fields replaced, so no `.env` is involved."""
    return settings.model_copy(update=overrides)


class TestSelection:
    def test_redis_is_the_configured_default(self) -> None:
        """A deployment that forgets to choose must not silently get `memory`."""
        assert Settings.model_fields["IDEMPOTENCY_BACKEND"].default == "redis"

    async def test_it_builds_a_redis_store(self) -> None:
        store = create_idempotency_store("redis", config=config())

        assert isinstance(store, RedisIdempotencyStore)
        assert store.name == "redis"
        await store.close()

    async def test_it_builds_a_memory_store(self) -> None:
        store = create_idempotency_store("memory", config=config())

        assert isinstance(store, InMemoryIdempotencyStore)
        assert store.name == "memory"

    async def test_the_backend_defaults_to_the_setting(self) -> None:
        store = create_idempotency_store(config=config(IDEMPOTENCY_BACKEND="memory"))

        assert isinstance(store, InMemoryIdempotencyStore)

    def test_an_unknown_backend_raises(self) -> None:
        """A silent fallback would be a deployment that deduplicates nothing."""
        with pytest.raises(ValueError, match="Unknown idempotency backend"):
            create_idempotency_store("dynamodb", config=config())

    async def test_the_memory_store_outside_development_still_builds(self) -> None:
        """It warns rather than refusing — the operator may know what they want."""
        store = create_idempotency_store(
            "memory", config=config(ENVIRONMENT="production")
        )

        assert isinstance(store, InMemoryIdempotencyStore)


class TestRedisUrl:
    async def test_it_falls_back_to_redis_url(self) -> None:
        store = create_idempotency_store(
            "redis",
            config=config(
                REDIS_URL="redis://localhost:6399/3", IDEMPOTENCY_REDIS_URL=""
            ),
        )

        # Reading the pool is the only way to see which URL was used; the store
        # deliberately exposes no accessor for it.
        kwargs = store._client.connection_pool.connection_kwargs  # type: ignore[attr-defined]
        assert (kwargs["port"], kwargs["db"]) == (6399, 3)
        await store.close()

    async def test_the_dedicated_url_wins(self) -> None:
        """So idempotency can live on a different instance from Celery's broker."""
        store = create_idempotency_store(
            "redis",
            config=config(
                REDIS_URL="redis://localhost:6399/3",
                IDEMPOTENCY_REDIS_URL="redis://localhost:6398/7",
            ),
        )

        kwargs = store._client.connection_pool.connection_kwargs  # type: ignore[attr-defined]
        assert (kwargs["port"], kwargs["db"]) == (6398, 7)
        await store.close()

    async def test_the_ttls_come_from_settings(self) -> None:
        store = create_idempotency_store(
            "redis",
            config=config(
                IDEMPOTENCY_TTL_SECONDS=111, IDEMPOTENCY_RESERVATION_TTL_SECONDS=22
            ),
        )

        assert store._record_ttl == 111  # type: ignore[attr-defined]
        assert store._reservation_ttl == 22  # type: ignore[attr-defined]
        await store.close()


class TestProcessWideStore:
    def test_the_store_is_cached(self) -> None:
        """A per-request Redis pool would be thrown away as fast as it was built,
        and a per-request in-memory store would forget every record it held."""
        assert get_idempotency_store() is get_idempotency_store()

    def test_the_test_suite_gets_the_in_memory_store(self) -> None:
        """Set in `tests/conftest.py`, so importing the app opens no connection."""
        assert isinstance(get_idempotency_store(), InMemoryIdempotencyStore)
