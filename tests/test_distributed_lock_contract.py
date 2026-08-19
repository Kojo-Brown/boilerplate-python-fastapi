"""One suite, run against every `LockBackend` implementation.

`DistributedLock`'s correctness rests on promises the *protocol* makes — that
`acquire` is atomic, that tokens never repeat, that a release compares before
it deletes — and a promise tested against one backend is a promise about that
backend. Every backend added later runs these same tests by appending one
fixture param.

The Redis leg needs a real server. It is skipped when nothing is listening on
`REDIS_URL`, and CI always has one (see the `redis` service in ci.yml), so the
atomicity and monotonicity claims are measured on the real thing on every pull
request rather than asserted against an emulator.
"""

from __future__ import annotations

import asyncio
import os
import socket
import time
import uuid
from collections.abc import AsyncGenerator
from urllib.parse import urlparse

import pytest

from src.distributed_lock.base import Lease, LockBackend, ReleaseOutcome
from src.distributed_lock.memory import InMemoryLockBackend
from src.distributed_lock.redis_backend import RedisLockBackend

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


def redis_reachable(url: str = REDIS_URL) -> bool:
    """Cheap liveness probe used to skip, not to assert.

    A TCP connect proves a listener rather than a Redis, which is deliberate:
    if something else is on the port the tests fail loudly instead of skipping
    quietly, and a silent skip is the failure mode that lets a backend rot.
    """
    parsed = urlparse(url)
    try:
        with socket.create_connection(
            (parsed.hostname or "localhost", parsed.port or 6379), timeout=1
        ):
            return True
    except OSError:
        return False


REDIS_SKIP_REASON = f"no Redis listening on {REDIS_URL}"


@pytest.fixture(params=["memory", "redis"])
async def backend(request: pytest.FixtureRequest) -> AsyncGenerator[LockBackend]:
    """Every implementation of the protocol, one at a time."""
    if request.param == "memory":
        built: LockBackend = InMemoryLockBackend()
    else:
        if not redis_reachable():
            pytest.skip(REDIS_SKIP_REASON)
        # A namespace per test keeps concurrent runs — and a developer's own
        # Redis — from colliding, without flushing a database that may not be
        # ours to flush.
        built = RedisLockBackend.from_url(
            REDIS_URL, namespace=f"test-dlock:{uuid.uuid4()}"
        )

    yield built
    await built.close()


@pytest.fixture
def name() -> str:
    return f"resource-{uuid.uuid4()}"


class TestAcquire:
    async def test_a_free_name_is_granted(
        self, backend: LockBackend, name: str
    ) -> None:
        lease = await backend.acquire(name, owner="owner-a", ttl_seconds=5)

        assert lease is not None
        assert lease.name == name
        assert lease.owner == "owner-a"
        assert lease.ttl_seconds == 5

    async def test_a_held_name_is_refused(
        self, backend: LockBackend, name: str
    ) -> None:
        await backend.acquire(name, owner="owner-a", ttl_seconds=5)

        assert await backend.acquire(name, owner="owner-b", ttl_seconds=5) is None

    async def test_the_same_owner_is_refused_too(
        self, backend: LockBackend, name: str
    ) -> None:
        """No reentrancy: an inner release would free the outer section's lock.

        The outer section would then run on believing it held a lock nobody
        holds, which is worse than the deadlock a reentrant lock avoids.
        """
        await backend.acquire(name, owner="same", ttl_seconds=5)

        assert await backend.acquire(name, owner="same", ttl_seconds=5) is None

    async def test_different_names_do_not_interfere(self, backend: LockBackend) -> None:
        assert await backend.acquire("name-a", owner="o", ttl_seconds=5) is not None
        assert await backend.acquire("name-b", owner="o", ttl_seconds=5) is not None

    async def test_a_name_is_free_again_once_its_lease_expires(
        self, backend: LockBackend, name: str
    ) -> None:
        """The lease is what keeps a killed holder from locking a name forever.

        Deliberately a real wait against a real expiry rather than a patched
        clock: this is the one property no in-process fake can vouch for on
        Redis's behalf, and 50ms is cheap enough to pay on every run.
        """
        assert await backend.acquire(name, owner="dies", ttl_seconds=0.05) is not None
        await asyncio.sleep(0.12)

        assert await backend.acquire(name, owner="next", ttl_seconds=5) is not None

    async def test_exactly_one_of_many_concurrent_acquisitions_wins(
        self, backend: LockBackend, name: str
    ) -> None:
        """The atomicity claim, measured rather than assumed.

        A read-then-write implementation passes every other test in this file
        and fails this one, which is the bug that lets two workers run the same
        critical section.
        """
        results = await asyncio.gather(
            *(backend.acquire(name, owner=f"o{i}", ttl_seconds=5) for i in range(20))
        )

        assert sum(1 for result in results if result is not None) == 1


class TestFencingTokens:
    async def test_the_first_token_is_one(
        self, backend: LockBackend, name: str
    ) -> None:
        lease = await backend.acquire(name, owner="o", ttl_seconds=5)

        assert lease is not None
        assert lease.token == 1

    async def test_tokens_increase_across_holders(
        self, backend: LockBackend, name: str
    ) -> None:
        tokens = []
        for index in range(5):
            lease = await backend.acquire(name, owner=f"o{index}", ttl_seconds=5)
            assert lease is not None
            tokens.append(lease.token)
            await backend.release(lease)

        assert tokens == sorted(tokens)
        assert len(set(tokens)) == len(tokens)

    async def test_a_released_name_does_not_restart_its_counter(
        self, backend: LockBackend, name: str
    ) -> None:
        """The property the whole fencing scheme rests on.

        If a counter reset when the lock went idle, a later holder would carry
        a token a resource had already accepted, and the resource's `fence <
        :token` check would reject a *current* writer while admitting a stale
        one.
        """
        first = await backend.acquire(name, owner="a", ttl_seconds=5)
        assert first is not None
        await backend.release(first)

        second = await backend.acquire(name, owner="b", ttl_seconds=5)
        assert second is not None
        assert second.token > first.token

    async def test_tokens_are_per_name(self, backend: LockBackend) -> None:
        """Two names are two counters. A shared one would still be monotonic,
        but every acquisition of one name would jump the other's tokens, and a
        resource fenced on the shared value would reject writers that never
        lost anything."""
        first = await backend.acquire("alpha", owner="o", ttl_seconds=5)
        second = await backend.acquire("beta", owner="o", ttl_seconds=5)

        assert first is not None and second is not None
        assert first.token == second.token == 1

    async def test_concurrent_winners_never_share_a_token(
        self, backend: LockBackend, name: str
    ) -> None:
        """Acquire, release, repeat — from several tasks at once.

        Minting the token and claiming the key have to be one indivisible step.
        Two `INCR`s that interleave with two `SET`s can hand the same token to
        two holders even when only one of them holds the lock at a time.
        """
        seen: list[int] = []

        async def churn() -> None:
            for _ in range(10):
                lease = await backend.acquire(name, owner="o", ttl_seconds=5)
                if lease is None:
                    continue
                seen.append(lease.token)
                await backend.release(lease)

        await asyncio.gather(*(churn() for _ in range(5)))

        assert len(seen) == len(set(seen))
        assert seen == sorted(seen)


class TestExtend:
    async def test_extending_our_own_lease_moves_the_deadline(
        self, backend: LockBackend, name: str
    ) -> None:
        lease = await backend.acquire(name, owner="o", ttl_seconds=1)
        assert lease is not None

        renewed = await backend.extend(lease, ttl_seconds=30)

        assert renewed is not None
        assert renewed.expires_at > lease.expires_at
        assert renewed.token == lease.token

    async def test_extending_keeps_the_name_held(
        self, backend: LockBackend, name: str
    ) -> None:
        """A renewal that dropped the key for an instant would be a lock anyone
        could walk into, which is why `PEXPIRE` and not `SET`."""
        lease = await backend.acquire(name, owner="o", ttl_seconds=5)
        assert lease is not None
        await backend.extend(lease, ttl_seconds=5)

        assert await backend.acquire(name, owner="other", ttl_seconds=5) is None

    async def test_extending_an_expired_lease_is_refused(
        self, backend: LockBackend, name: str
    ) -> None:
        lease = await backend.acquire(name, owner="o", ttl_seconds=0.05)
        assert lease is not None
        await asyncio.sleep(0.12)

        assert await backend.extend(lease, ttl_seconds=5) is None

    async def test_extending_a_reassigned_name_is_refused(
        self, backend: LockBackend, name: str
    ) -> None:
        """The dangerous case: extending here would push out a lease belonging
        to somebody else, so the new holder loses its lock without being told."""
        stale = await backend.acquire(name, owner="slow", ttl_seconds=0.05)
        assert stale is not None
        await asyncio.sleep(0.12)
        current = await backend.acquire(name, owner="next", ttl_seconds=5)
        assert current is not None

        assert await backend.extend(stale, ttl_seconds=5) is None
        assert await backend.extend(current, ttl_seconds=5) is not None


class TestRelease:
    async def test_releasing_our_own_lease_frees_the_name(
        self, backend: LockBackend, name: str
    ) -> None:
        lease = await backend.acquire(name, owner="o", ttl_seconds=5)
        assert lease is not None

        assert await backend.release(lease) is ReleaseOutcome.RELEASED
        assert await backend.acquire(name, owner="next", ttl_seconds=5) is not None

    async def test_releasing_an_expired_lease_reports_it(
        self, backend: LockBackend, name: str
    ) -> None:
        lease = await backend.acquire(name, owner="o", ttl_seconds=0.05)
        assert lease is not None
        await asyncio.sleep(0.12)

        assert await backend.release(lease) is ReleaseOutcome.EXPIRED

    async def test_a_stale_lease_cannot_release_the_current_holder(
        self, backend: LockBackend, name: str
    ) -> None:
        """The single most common bug in hand-rolled Redis locks.

        An unconditional `DEL` here frees the *new* holder's lock, and a third
        caller then walks into the section the second one is still running.
        """
        stale = await backend.acquire(name, owner="slow", ttl_seconds=0.05)
        assert stale is not None
        await asyncio.sleep(0.12)
        current = await backend.acquire(name, owner="next", ttl_seconds=5)
        assert current is not None

        assert await backend.release(stale) is ReleaseOutcome.NOT_OWNER
        assert await backend.acquire(name, owner="third", ttl_seconds=5) is None

    async def test_releasing_twice_is_not_an_error(
        self, backend: LockBackend, name: str
    ) -> None:
        """Called from an exception path, where a second failure helps nobody."""
        lease = await backend.acquire(name, owner="o", ttl_seconds=5)
        assert lease is not None

        assert await backend.release(lease) is ReleaseOutcome.RELEASED
        assert await backend.release(lease) is ReleaseOutcome.EXPIRED


class TestInspect:
    async def test_a_free_name_is_none(self, backend: LockBackend, name: str) -> None:
        assert await backend.inspect(name) is None

    async def test_a_held_name_reports_its_holder(
        self, backend: LockBackend, name: str
    ) -> None:
        lease = await backend.acquire(name, owner="owner-a", ttl_seconds=5)
        assert lease is not None

        state = await backend.inspect(name)

        assert state is not None
        assert state.name == name
        assert state.owner == "owner-a"
        assert state.token == lease.token
        assert state.ttl_seconds is not None
        assert 0 < state.ttl_seconds <= 5

    async def test_a_released_name_is_none_again(
        self, backend: LockBackend, name: str
    ) -> None:
        lease = await backend.acquire(name, owner="o", ttl_seconds=5)
        assert lease is not None
        await backend.release(lease)

        assert await backend.inspect(name) is None


class TestLeaseDeadlines:
    async def test_the_deadline_never_overstates_the_lease(
        self, backend: LockBackend, name: str
    ) -> None:
        """Measured before the request goes out, so it is short by a round trip.

        The other direction is the dangerous one: a holder that believes it has
        time the store has already taken back will write when it should stop.
        """
        before = time.monotonic()
        lease = await backend.acquire(name, owner="o", ttl_seconds=5)
        after = time.monotonic()

        assert lease is not None
        assert before + 5 <= lease.expires_at <= after + 5


class TestProtocolConformance:
    async def test_every_backend_satisfies_the_protocol(
        self, backend: LockBackend
    ) -> None:
        assert isinstance(backend, LockBackend)

    async def test_every_backend_names_itself(self, backend: LockBackend) -> None:
        assert backend.name

    async def test_every_backend_returns_a_lease(
        self, backend: LockBackend, name: str
    ) -> None:
        assert isinstance(await backend.acquire(name, owner="o", ttl_seconds=5), Lease)
