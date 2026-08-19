"""The Redis backend's own behaviour: key layout, encoding, and failure modes.

The shared contract in `test_distributed_lock_contract.py` proves it is a lock.
These cover what is specific to this backend — above all the fence counter's
missing TTL, which is the difference between a fencing token and a number that
starts over — plus the pure helpers, which need no server at all.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator

import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from src.distributed_lock.base import (
    Lease,
    LockBackendUnavailableError,
    ReleaseOutcome,
)
from src.distributed_lock.redis_backend import (
    RedisLockBackend,
    split_lock_value,
)
from tests.test_distributed_lock_contract import (
    REDIS_SKIP_REASON,
    REDIS_URL,
    redis_reachable,
)


class TestSplitLockValue:
    def test_it_splits_a_token_from_an_owner(self) -> None:
        assert split_lock_value("12:owner-a") == (12, "owner-a")

    def test_it_splits_on_the_first_colon_only(self) -> None:
        """Owners are arbitrary strings — a host:pid owner carries colons of
        its own, where the token never does."""
        assert split_lock_value("12:host:4242:abcd") == (12, "host:4242:abcd")

    def test_a_value_with_no_colon_is_refused(self) -> None:
        with pytest.raises(ValueError, match="Malformed lock value"):
            split_lock_value("nonsense")

    def test_a_non_numeric_token_is_refused(self) -> None:
        with pytest.raises(ValueError):
            split_lock_value("abc:owner")


class TestTtlConversion:
    """`PX 0` is an error, so a sub-millisecond TTL must round up, not down."""

    async def test_a_sub_millisecond_ttl_becomes_one_millisecond(self) -> None:
        backend = RedisLockBackend(FakeRedis(), namespace="ns")  # type: ignore[arg-type]
        await backend.acquire("n", owner="o", ttl_seconds=0.0001)

        assert FakeRedis.last_args[1] == 1

    async def test_a_fractional_ttl_rounds_up(self) -> None:
        backend = RedisLockBackend(FakeRedis(), namespace="ns")  # type: ignore[arg-type]
        await backend.acquire("n", owner="o", ttl_seconds=1.0005)

        assert FakeRedis.last_args[1] == 1001

    async def test_a_non_positive_ttl_is_refused(self) -> None:
        backend = RedisLockBackend(FakeRedis(), namespace="ns")  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="ttl_seconds must be positive"):
            await backend.acquire("n", owner="o", ttl_seconds=0)


class FakeScript:
    """Records the arguments a script was called with and claims the lock."""

    def __init__(self, body: str) -> None:
        self._body = body

    async def __call__(
        self, keys: list[str] | None = None, args: list[object] | None = None
    ) -> object:
        FakeRedis.last_keys = list(keys or [])
        FakeRedis.last_args = list(args or [])
        return [1, b"1:o", 1]


class FakeRedis:
    """Just enough of `Redis` for the argument-shaping tests above.

    A real server cannot answer "what did you send?", and these tests are about
    exactly that. Everything with behaviour worth checking runs against the
    real thing in the contract suite.
    """

    last_keys: list[str] = []
    last_args: list[object] = []

    def register_script(self, body: str) -> FakeScript:
        return FakeScript(body)


@pytest.fixture
def namespace() -> str:
    """A namespace per test, so a developer's own Redis is never flushed."""
    return f"test-dlock-redis:{uuid.uuid4()}"


@pytest.fixture
async def backend(namespace: str) -> AsyncGenerator[RedisLockBackend]:
    if not redis_reachable():
        pytest.skip(REDIS_SKIP_REASON)
    built = RedisLockBackend.from_url(REDIS_URL, namespace=namespace)
    yield built
    await built.close()


@pytest.fixture
async def client() -> AsyncGenerator[Redis]:
    if not redis_reachable():
        pytest.skip(REDIS_SKIP_REASON)
    connection = Redis.from_url(REDIS_URL, decode_responses=False)
    yield connection
    await connection.aclose()


class TestKeyLayout:
    async def test_the_lock_key_carries_the_token_and_owner(
        self, backend: RedisLockBackend, client: Redis, namespace: str
    ) -> None:
        lease = await backend.acquire("orders", owner="worker-1", ttl_seconds=30)
        assert lease is not None

        raw = await client.get(f"{namespace}:lock:orders")
        assert raw == f"{lease.token}:worker-1".encode()

    async def test_the_lock_key_expires(
        self, backend: RedisLockBackend, client: Redis, namespace: str
    ) -> None:
        """The TTL is what keeps a killed holder from locking a name forever."""
        await backend.acquire("orders", owner="worker-1", ttl_seconds=30)

        ttl = await client.pttl(f"{namespace}:lock:orders")
        assert 0 < ttl <= 30_000

    async def test_the_fence_counter_never_expires(
        self, backend: RedisLockBackend, client: Redis, namespace: str
    ) -> None:
        """The one that must not have a TTL.

        If the counter is evicted or expires, the next INCR returns 1 and the
        store hands out tokens a resource has already accepted and moved past —
        at which point the fencing check silently stops rejecting the writers
        it exists to reject. -1 is PTTL's answer for "no expiry set".
        """
        await backend.acquire("orders", owner="worker-1", ttl_seconds=30)

        assert await client.pttl(f"{namespace}:fence:orders") == -1

    async def test_the_counter_outlives_the_lock_key(
        self, backend: RedisLockBackend, client: Redis, namespace: str
    ) -> None:
        lease = await backend.acquire("orders", owner="worker-1", ttl_seconds=30)
        assert lease is not None
        await backend.release(lease)

        assert await client.exists(f"{namespace}:lock:orders") == 0
        assert await client.exists(f"{namespace}:fence:orders") == 1

    async def test_namespaces_do_not_collide(self, client: Redis) -> None:
        """Two namespaces on one server are two independent locks, which is
        what lets Celery, the idempotency store and this share a database."""
        first = RedisLockBackend.from_url(REDIS_URL, namespace=f"ns-a:{uuid.uuid4()}")
        second = RedisLockBackend.from_url(REDIS_URL, namespace=f"ns-b:{uuid.uuid4()}")
        try:
            assert await first.acquire("same", owner="a", ttl_seconds=5) is not None
            assert await second.acquire("same", owner="b", ttl_seconds=5) is not None
        finally:
            await first.close()
            await second.close()

    async def test_an_injected_decoding_client_still_works(
        self, namespace: str
    ) -> None:
        """`from_url` turns decoding off, but the constructor takes any client.

        A caller handing in a pool it already owns may well have built it with
        `decode_responses=True`, and every reply this backend reads would then
        arrive as `str` rather than `bytes`. Parsing one and not the other would
        be an `AttributeError` on somebody's first acquisition.
        """
        built = RedisLockBackend(
            Redis.from_url(REDIS_URL, decode_responses=True), namespace=namespace
        )
        try:
            lease = await built.acquire("decoded", owner="worker-1", ttl_seconds=30)
            assert lease is not None
            assert lease.token == 1

            state = await built.inspect("decoded")
            assert state is not None
            assert state.owner == "worker-1"
            assert await built.release(lease) is ReleaseOutcome.RELEASED
        finally:
            await built.close()


class TestForeignValues:
    async def test_a_key_written_by_hand_is_reported_rather_than_ignored(
        self, backend: RedisLockBackend, client: Redis, namespace: str
    ) -> None:
        """Reporting the name as free would be a lie a caller would act on.

        The state it returns instead carries token 0, which cannot pass a
        fencing check, so nothing downstream mistakes it for a live lease.
        """
        await client.set(f"{namespace}:lock:manual", b"not-our-encoding")

        state = await backend.inspect("manual")

        assert state is not None
        assert state.token == 0
        assert state.owner == "unknown"
        assert state.ttl_seconds is None

    async def test_a_key_with_no_expiry_reports_no_ttl(
        self, backend: RedisLockBackend, client: Redis, namespace: str
    ) -> None:
        await client.set(f"{namespace}:lock:manual", b"5:someone")

        state = await backend.inspect("manual")

        assert state is not None
        assert state.token == 5
        assert state.ttl_seconds is None


class UnreachableRedis:
    """Every call fails the way a dead server does."""

    def register_script(self, body: str) -> UnreachableScript:
        return UnreachableScript()

    def pipeline(self, transaction: bool = True) -> UnreachablePipeline:
        return UnreachablePipeline()

    async def aclose(self) -> None:
        raise RedisConnectionError("gone")


class UnreachableScript:
    async def __call__(
        self, keys: list[str] | None = None, args: list[object] | None = None
    ) -> object:
        raise RedisConnectionError("gone")


class UnreachablePipeline:
    def get(self, key: str) -> None:
        return None

    def pttl(self, key: str) -> None:
        return None

    async def execute(self) -> object:
        raise RedisConnectionError("gone")


class TestUnreachableServer:
    """A store that cannot be reached is a 503, never a lock quietly skipped."""

    @pytest.fixture
    def dead(self) -> RedisLockBackend:
        return RedisLockBackend(UnreachableRedis())  # type: ignore[arg-type]

    async def test_acquire_raises(self, dead: RedisLockBackend) -> None:
        with pytest.raises(LockBackendUnavailableError):
            await dead.acquire("n", owner="o", ttl_seconds=5)

    async def test_extend_raises(self, dead: RedisLockBackend) -> None:
        lease = Lease(name="n", token=1, owner="o", ttl_seconds=5.0, expires_at=100.0)
        with pytest.raises(LockBackendUnavailableError):
            await dead.extend(lease, ttl_seconds=5)

    async def test_release_raises(self, dead: RedisLockBackend) -> None:
        lease = Lease(name="n", token=1, owner="o", ttl_seconds=5.0, expires_at=100.0)
        with pytest.raises(LockBackendUnavailableError):
            await dead.release(lease)

    async def test_inspect_raises(self, dead: RedisLockBackend) -> None:
        with pytest.raises(LockBackendUnavailableError):
            await dead.inspect("n")

    async def test_close_swallows_the_failure(self, dead: RedisLockBackend) -> None:
        """Shutdown against a server that is already gone is not a reason to
        fail a clean exit."""
        await dead.close()


class TestOutcomeMapping:
    async def test_every_script_result_maps_to_an_outcome(
        self, backend: RedisLockBackend, client: Redis, namespace: str
    ) -> None:
        lease = await backend.acquire("mapped", owner="a", ttl_seconds=30)
        assert lease is not None

        assert await backend.release(lease) is ReleaseOutcome.RELEASED
        assert await backend.release(lease) is ReleaseOutcome.EXPIRED

        await client.set(f"{namespace}:lock:mapped", b"99:somebody-else")
        assert await backend.release(lease) is ReleaseOutcome.NOT_OWNER
