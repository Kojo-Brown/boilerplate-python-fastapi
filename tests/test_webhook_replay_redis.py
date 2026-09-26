"""The Redis replay guard, against a real server where there is one.

What only this backend can be asked: that a claim is written with the TTL it was
configured with, that two *processes* — not two tasks — cannot both claim one
fingerprint, and that an unreachable server becomes a domain error rather than a
`RedisError` escaping as a 500.

The TTL is asserted by reading the key's expiry rather than sleeping through it,
so the suite stays fast and deterministic.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator

import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError

from src.webhooks.errors import ReplayGuardUnavailableError
from src.webhooks.redis_guard import RedisReplayGuard
from src.webhooks.replay import ReplayGuard
from tests.test_idempotency_contract import (
    REDIS_SKIP_REASON,
    REDIS_URL,
    redis_reachable,
)

# Applied per class rather than to the module: `TestUnreachableServer` needs
# Redis to be *absent*, so it must run whether or not a server is up.
requires_redis = pytest.mark.skipif(not redis_reachable(), reason=REDIS_SKIP_REASON)

TTL = 600


@pytest.fixture
def namespace() -> str:
    return f"test-webhook-replay:{uuid.uuid4()}"


@pytest.fixture
async def guard(namespace: str) -> AsyncGenerator[RedisReplayGuard]:
    built = RedisReplayGuard.from_url(REDIS_URL, namespace=namespace, ttl_seconds=TTL)
    yield built
    await built.close()


@pytest.fixture
async def client() -> AsyncGenerator[Redis]:
    raw: Redis = Redis.from_url(REDIS_URL, decode_responses=False)
    yield raw
    await raw.aclose()


@requires_redis
class TestClaiming:
    async def test_a_first_claim_succeeds(self, guard: RedisReplayGuard) -> None:
        assert await guard.claim("fp") is True

    async def test_a_second_claim_fails(self, guard: RedisReplayGuard) -> None:
        await guard.claim("fp")

        assert await guard.claim("fp") is False

    async def test_two_guards_sharing_a_server_see_each_other(
        self, namespace: str
    ) -> None:
        """The property the in-memory guard cannot have.

        Two uvicorn workers are two guards; a replay delivered to the second
        must be recognised as one, which is the entire reason this backend is
        the default.
        """
        first = RedisReplayGuard.from_url(
            REDIS_URL, namespace=namespace, ttl_seconds=TTL
        )
        second = RedisReplayGuard.from_url(
            REDIS_URL, namespace=namespace, ttl_seconds=TTL
        )
        try:
            assert await first.claim("fp") is True
            assert await second.claim("fp") is False
        finally:
            await first.close()
            await second.close()

    async def test_namespaces_separate_senders(self, namespace: str) -> None:
        """One verifier is one counterparty, and its records are its own."""
        first = RedisReplayGuard.from_url(
            REDIS_URL, namespace=f"{namespace}:a", ttl_seconds=TTL
        )
        second = RedisReplayGuard.from_url(
            REDIS_URL, namespace=f"{namespace}:b", ttl_seconds=TTL
        )
        try:
            assert await first.claim("fp") is True
            assert await second.claim("fp") is True
        finally:
            await first.close()
            await second.close()


@requires_redis
class TestWhatIsStored:
    async def test_the_claim_carries_the_configured_expiry(
        self, guard: RedisReplayGuard, client: Redis, namespace: str
    ) -> None:
        """Read rather than waited out: a wrong TTL is otherwise invisible."""
        await guard.claim("fp")

        ttl = await client.ttl(f"{namespace}:fp")

        assert 0 < ttl <= TTL

    async def test_the_value_is_one_byte(
        self, guard: RedisReplayGuard, client: Redis, namespace: str
    ) -> None:
        """The key's existence is the whole fact; nothing reads the value.

        Asserted so that a later change which starts storing the body or the
        digest in here has to say why.
        """
        await guard.claim("fp")

        assert await client.get(f"{namespace}:fp") == b"1"

    async def test_the_ttl_is_reported(self, guard: RedisReplayGuard) -> None:
        assert guard.ttl_seconds == float(TTL)

    async def test_the_name_is_reported(self, guard: RedisReplayGuard) -> None:
        assert guard.name == "redis"

    async def test_it_satisfies_the_protocol(self, guard: RedisReplayGuard) -> None:
        assert isinstance(guard, ReplayGuard)


class TestConstruction:
    async def test_a_ttl_below_one_second_is_refused(self) -> None:
        """Redis rejects `EX 0`, and it would mean a guard that guards nothing."""
        with pytest.raises(ValueError, match="at least 1"):
            RedisReplayGuard.from_url(REDIS_URL, ttl_seconds=0)


class TestUnreachableServer:
    """No skip: this class needs Redis to be *absent* at the address it uses."""

    @pytest.fixture
    async def dead_guard(self) -> AsyncGenerator[RedisReplayGuard]:
        # Port 1 is reserved and nothing listens on it.
        built = RedisReplayGuard.from_url("redis://localhost:1/0", ttl_seconds=TTL)
        yield built
        await built.close()

    async def test_a_claim_raises_the_domain_error(
        self, dead_guard: RedisReplayGuard
    ) -> None:
        """Never `True`.

        A guard that assumes "new" when it cannot reach its store stops working
        exactly when somebody is attacking the store, and says nothing about it.
        Whether the delivery is then accepted is the verifier's decision, under
        `WEBHOOK_REPLAY_FAIL_OPEN`, and it is a decision rather than a default.
        """
        with pytest.raises(ReplayGuardUnavailableError):
            await dead_guard.claim("fp")

    async def test_the_error_carries_a_503(self, dead_guard: RedisReplayGuard) -> None:
        with pytest.raises(ReplayGuardUnavailableError) as caught:
            await dead_guard.claim("fp")

        assert caught.value.status_code == 503

    async def test_the_underlying_error_is_kept_as_the_cause(
        self, dead_guard: RedisReplayGuard
    ) -> None:
        """So an incident can see it was a connection failure and not a miss."""
        with pytest.raises(ReplayGuardUnavailableError) as caught:
            await dead_guard.claim("fp")

        assert isinstance(caught.value.__cause__, ConnectionError)

    async def test_the_error_does_not_quote_the_dsn(
        self, dead_guard: RedisReplayGuard
    ) -> None:
        """redis-py puts the URL, credentials and all, in its own message."""
        with pytest.raises(ReplayGuardUnavailableError) as caught:
            await dead_guard.claim("fp")

        assert "localhost:1" not in str(caught.value)
