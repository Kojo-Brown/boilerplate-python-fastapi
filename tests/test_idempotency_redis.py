"""Behaviour specific to the Redis store.

The shared promises are covered by `test_idempotency_contract.py`. What is here
is what only this backend can be asked: that the two TTLs really are written as
two different expirations, that an unreachable server becomes a domain error
rather than a `RedisError` escaping into a 500, and that a record left by an
older release is taken over rather than served.

TTLs are asserted by reading the key's expiration instead of sleeping through
it, so the suite stays fast and deterministic.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator

import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError

from src.idempotency.base import (
    IdempotencyRecord,
    IdempotencyStoreUnavailableError,
    StoredResponse,
)
from src.idempotency.codec import SCHEMA_VERSION
from src.idempotency.redis_store import RedisIdempotencyStore
from tests.test_idempotency_contract import (
    REDIS_SKIP_REASON,
    REDIS_URL,
    redis_reachable,
)

# Applied per class rather than to the module: `TestUnreachableServer` needs
# Redis to be *absent*, so it must run whether or not a server is up.
requires_redis = pytest.mark.skipif(not redis_reachable(), reason=REDIS_SKIP_REASON)

RECORD_TTL = 600
RESERVATION_TTL = 30


@pytest.fixture
def namespace() -> str:
    return f"test-idempotency:{uuid.uuid4()}"


@pytest.fixture
async def store(namespace: str) -> AsyncGenerator[RedisIdempotencyStore]:
    built = RedisIdempotencyStore.from_url(
        REDIS_URL,
        namespace=namespace,
        record_ttl_seconds=RECORD_TTL,
        reservation_ttl_seconds=RESERVATION_TTL,
    )
    yield built
    await built.close()


@pytest.fixture
async def client() -> AsyncGenerator[Redis]:
    """A second connection, used to look at what the store actually wrote."""
    connection = Redis.from_url(REDIS_URL, decode_responses=False)
    yield connection
    await connection.aclose()


@requires_redis
class TestTtls:
    async def test_a_reservation_gets_the_short_ttl(
        self, store: RedisIdempotencyStore, client: Redis, namespace: str
    ) -> None:
        """A crashed worker may block its own retries for a minute, not a day."""
        await store.reserve("k", "fp")

        ttl = await client.ttl(f"{namespace}:k")
        assert 0 < ttl <= RESERVATION_TTL

    async def test_completing_extends_it_to_the_record_ttl(
        self, store: RedisIdempotencyStore, client: Redis, namespace: str
    ) -> None:
        await store.reserve("k", "fp")
        await store.complete(
            "k",
            IdempotencyRecord(
                "fp", StoredResponse(status_code=200, headers=(), body=b"{}")
            ),
        )

        ttl = await client.ttl(f"{namespace}:k")
        assert RESERVATION_TTL < ttl <= RECORD_TTL

    async def test_records_always_carry_an_expiry(
        self, store: RedisIdempotencyStore, client: Redis, namespace: str
    ) -> None:
        """A key written without one would sit in Redis forever. -1 means no TTL."""
        await store.reserve("k", "fp")

        assert await client.ttl(f"{namespace}:k") != -1


@requires_redis
class TestNamespacing:
    async def test_keys_are_namespaced(
        self, store: RedisIdempotencyStore, client: Redis, namespace: str
    ) -> None:
        """Redis is shared with Celery here; the prefix is what keeps them apart."""
        await store.reserve("k", "fp")

        assert await client.exists(f"{namespace}:k") == 1
        assert await client.exists("k") == 0


@requires_redis
class TestSchemaMismatch:
    async def test_a_foreign_record_is_taken_over(
        self, store: RedisIdempotencyStore, client: Redis, namespace: str
    ) -> None:
        """Otherwise a deploy would 409 every retry until the old TTL ran out."""
        await client.set(
            f"{namespace}:k",
            f'{{"v":{SCHEMA_VERSION + 1},"fingerprint":"old","response":null}}',
            ex=RECORD_TTL,
        )

        assert await store.reserve("k", "fp") is None

        record = await store.get("k")
        assert record is not None
        assert record.fingerprint == "fp"

    async def test_the_takeover_uses_the_reservation_ttl(
        self, store: RedisIdempotencyStore, client: Redis, namespace: str
    ) -> None:
        """It is a reservation like any other — it must not inherit the old TTL."""
        await client.set(
            f"{namespace}:k",
            f'{{"v":{SCHEMA_VERSION + 1},"fingerprint":"old","response":null}}',
            ex=RECORD_TTL,
        )

        await store.reserve("k", "fp")

        assert 0 < await client.ttl(f"{namespace}:k") <= RESERVATION_TTL

    async def test_a_corrupt_record_is_an_error(
        self, store: RedisIdempotencyStore, client: Redis, namespace: str
    ) -> None:
        """Unparseable is not "old" — it is something else writing here."""
        await client.set(f"{namespace}:k", b"not json", ex=RECORD_TTL)

        with pytest.raises(IdempotencyStoreUnavailableError):
            await store.reserve("k", "fp")


class TestUnreachableServer:
    @pytest.fixture
    def dead_store(self) -> RedisIdempotencyStore:
        """Pointed at a port nothing is listening on.

        Port 1 is reserved and never bound by a service, so this fails to
        connect rather than talking to whatever happened to be running.
        """
        return RedisIdempotencyStore.from_url("redis://127.0.0.1:1/0")

    async def test_reserve_raises_a_domain_error(
        self, dead_store: RedisIdempotencyStore
    ) -> None:
        """A `RedisError` reaching the middleware would be an unhandled 500."""
        with pytest.raises(IdempotencyStoreUnavailableError):
            await dead_store.reserve("k", "fp")

    async def test_complete_raises_a_domain_error(
        self, dead_store: RedisIdempotencyStore
    ) -> None:
        with pytest.raises(IdempotencyStoreUnavailableError):
            await dead_store.complete("k", IdempotencyRecord("fp"))

    async def test_release_raises_a_domain_error(
        self, dead_store: RedisIdempotencyStore
    ) -> None:
        with pytest.raises(IdempotencyStoreUnavailableError):
            await dead_store.release("k")

    async def test_get_raises_a_domain_error(
        self, dead_store: RedisIdempotencyStore
    ) -> None:
        with pytest.raises(IdempotencyStoreUnavailableError):
            await dead_store.get("k")

    async def test_the_error_is_a_503_with_retry_after(
        self, dead_store: RedisIdempotencyStore
    ) -> None:
        with pytest.raises(IdempotencyStoreUnavailableError) as exc_info:
            await dead_store.get("k")

        assert exc_info.value.status_code == 503
        assert exc_info.value.headers == {"Retry-After": "1"}


class TestReservationRaces:
    """Interleavings a real server cannot be made to produce on demand.

    A reservation can expire between the `SET NX` that fails and the `GET` that
    asks what is there. Forcing that against a live Redis would mean a sleep
    tuned to a millisecond boundary, so the client is stubbed instead — the
    logic under test is entirely this module's.
    """

    class VanishingClient:
        """Refuses every claim, then reports the key as already gone."""

        def __init__(self) -> None:
            self.set_calls = 0

        async def set(self, *args: object, **kwargs: object) -> bool:
            self.set_calls += 1
            return False

        async def get(self, name: str) -> None:
            return None

    class ForeignRecordClient:
        """Holds a record from another schema version, and fails the takeover."""

        def __init__(self) -> None:
            self.claims = 0

        async def set(self, *args: object, **kwargs: object) -> bool:
            self.claims += 1
            if self.claims == 1:
                return False
            raise ConnectionError("connection lost during takeover")

        async def get(self, name: str) -> bytes:
            return (
                f'{{"v":{SCHEMA_VERSION + 1},"fingerprint":"old","response":null}}'
            ).encode()

    async def test_a_key_that_keeps_vanishing_gives_up(self) -> None:
        """Retrying forever would spin; the caller gets a 503 it can act on."""
        client = self.VanishingClient()
        store = RedisIdempotencyStore(client)  # type: ignore[arg-type]

        with pytest.raises(IdempotencyStoreUnavailableError) as exc_info:
            await store.reserve("k", "fp")

        assert client.set_calls == 3
        assert exc_info.value.details == {"attempts": 3}

    async def test_a_failed_takeover_is_a_domain_error(self) -> None:
        store = RedisIdempotencyStore(self.ForeignRecordClient())  # type: ignore[arg-type]

        with pytest.raises(IdempotencyStoreUnavailableError):
            await store.reserve("k", "fp")
