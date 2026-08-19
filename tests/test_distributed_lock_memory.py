"""The in-process backend's own behaviour, beyond the shared contract.

The contract suite proves it is a lock. These prove the two things only this
backend can be asked about: that its expiry is driven by an injectable clock,
and that its token counters outlive the claims they belong to.
"""

from __future__ import annotations

import pytest

from src.distributed_lock.base import LockNameInvalidError, ReleaseOutcome
from src.distributed_lock.memory import InMemoryLockBackend


class FakeClock:
    """A monotonic clock a test can move without spending the time."""

    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def backend(clock: FakeClock) -> InMemoryLockBackend:
    return InMemoryLockBackend(clock=clock)


class TestExpiryUsesTheInjectedClock:
    async def test_a_lease_survives_until_its_deadline(
        self, backend: InMemoryLockBackend, clock: FakeClock
    ) -> None:
        await backend.acquire("n", owner="a", ttl_seconds=10)
        clock.advance(9.9)

        assert await backend.acquire("n", owner="b", ttl_seconds=10) is None

    async def test_a_lease_is_gone_at_its_deadline(
        self, backend: InMemoryLockBackend, clock: FakeClock
    ) -> None:
        await backend.acquire("n", owner="a", ttl_seconds=10)
        clock.advance(10)

        assert await backend.acquire("n", owner="b", ttl_seconds=10) is not None

    async def test_inspect_reports_the_remaining_time(
        self, backend: InMemoryLockBackend, clock: FakeClock
    ) -> None:
        await backend.acquire("n", owner="a", ttl_seconds=10)
        clock.advance(4)

        state = await backend.inspect("n")
        assert state is not None
        assert state.ttl_seconds == pytest.approx(6.0)

    async def test_an_expired_claim_is_purged_on_read(
        self, backend: InMemoryLockBackend, clock: FakeClock
    ) -> None:
        """Expiry is evaluated on read rather than by a sweeper task, which
        would keep this object alive for the life of the process."""
        await backend.acquire("n", owner="a", ttl_seconds=10)
        clock.advance(11)

        assert await backend.inspect("n") is None


class TestCounters:
    async def test_the_counter_survives_a_release(
        self, backend: InMemoryLockBackend
    ) -> None:
        first = await backend.acquire("n", owner="a", ttl_seconds=10)
        assert first is not None
        await backend.release(first)
        second = await backend.acquire("n", owner="b", ttl_seconds=10)

        assert second is not None
        assert second.token == first.token + 1

    async def test_the_counter_survives_an_expiry(
        self, backend: InMemoryLockBackend, clock: FakeClock
    ) -> None:
        first = await backend.acquire("n", owner="a", ttl_seconds=10)
        assert first is not None
        clock.advance(11)
        second = await backend.acquire("n", owner="b", ttl_seconds=10)

        assert second is not None
        assert second.token == first.token + 1

    async def test_clear_drops_claims_but_not_counters(
        self, backend: InMemoryLockBackend
    ) -> None:
        """`clear` exists for tests that reuse a process-wide backend. One that
        reset the counters could not tell a monotonic token from one that
        restarts, which is the property most worth protecting."""
        first = await backend.acquire("n", owner="a", ttl_seconds=10)
        assert first is not None

        await backend.clear()

        second = await backend.acquire("n", owner="b", ttl_seconds=10)
        assert second is not None
        assert second.token > first.token


class TestHousekeeping:
    async def test_it_names_itself_memory(self, backend: InMemoryLockBackend) -> None:
        assert backend.name == "memory"

    async def test_close_is_a_no_op(self, backend: InMemoryLockBackend) -> None:
        """Present so the app lifespan needs no `isinstance`."""
        await backend.close()

    async def test_an_invalid_name_is_refused_at_acquisition(
        self, backend: InMemoryLockBackend
    ) -> None:
        with pytest.raises(LockNameInvalidError):
            await backend.acquire("has space", owner="a", ttl_seconds=10)

    async def test_releasing_an_expired_lease_reports_expired(
        self, backend: InMemoryLockBackend, clock: FakeClock
    ) -> None:
        lease = await backend.acquire("n", owner="a", ttl_seconds=10)
        assert lease is not None
        clock.advance(11)

        assert await backend.release(lease) is ReleaseOutcome.EXPIRED

    async def test_extending_after_expiry_is_refused(
        self, backend: InMemoryLockBackend, clock: FakeClock
    ) -> None:
        lease = await backend.acquire("n", owner="a", ttl_seconds=10)
        assert lease is not None
        clock.advance(11)

        assert await backend.extend(lease, ttl_seconds=10) is None
