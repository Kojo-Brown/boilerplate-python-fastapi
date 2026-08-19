"""`DistributedLock`: the context manager's own policy.

The backends are tested against the shared contract elsewhere. What is under
test here is everything that happens *around* the body — how long acquisition
waits, whether a renewal keeps the lease alive, and what exit does when the
lease turns out to have ended early.

Time is injected throughout rather than slept away. Two sleepers do it, because
the two loops need different control: acquisition backs off a bounded number of
times and is happy to have the clock jumped forward for it, while the renewal
task loops until cancelled and would otherwise spin as fast as the event loop
allows. `TickSleeper` therefore holds the renewer still until a test lets one
iteration through, which is what makes "it renewed exactly twice" assertable.
"""

from __future__ import annotations

import asyncio

import pytest

from src.distributed_lock.base import (
    Lease,
    LockBackend,
    LockBackendUnavailableError,
    LockLostError,
    LockNameInvalidError,
    LockState,
    LockUnavailableError,
    ReleaseOutcome,
    StaleFencingTokenError,
    require_fence,
)
from src.distributed_lock.lock import DistributedLock, new_owner_id
from src.distributed_lock.memory import InMemoryLockBackend


class FakeClock:
    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class RecordingSleeper:
    """Spends the requested delay on the fake clock instead of the real one."""

    def __init__(self, clock: FakeClock) -> None:
        self._clock = clock
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)
        self._clock.advance(seconds)
        # Hand the loop back, as a real sleep would. Without this a caller that
        # only ever awaits this sleeper never yields, and a background task
        # waiting on it would never be scheduled.
        await asyncio.sleep(0)


class TickSleeper:
    """A sleeper that blocks until the test lets one call through.

    Used for the renewal task: it loops until cancelled, so a sleeper that
    returned immediately would turn it into a busy loop and make "how many
    renewals happened" a question about scheduler luck.
    """

    def __init__(self, clock: FakeClock) -> None:
        self._clock = clock
        self._permits = asyncio.Semaphore(0)
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        await self._permits.acquire()
        self.delays.append(seconds)
        self._clock.advance(seconds)

    async def tick(self) -> None:
        """Release one sleep and let the waiting task finish its iteration."""
        self._permits.release()
        for _ in range(8):
            await asyncio.sleep(0)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def sleeper(clock: FakeClock) -> RecordingSleeper:
    return RecordingSleeper(clock)


@pytest.fixture
def ticker(clock: FakeClock) -> TickSleeper:
    return TickSleeper(clock)


@pytest.fixture
def backend(clock: FakeClock) -> InMemoryLockBackend:
    return InMemoryLockBackend(clock=clock)


def build_lock(
    backend: LockBackend,
    clock: FakeClock,
    asleep: RecordingSleeper | TickSleeper,
    **overrides: object,
) -> DistributedLock:
    options: dict[str, object] = {
        "ttl_seconds": 30.0,
        "clock": clock,
        "asleep": asleep,
        # Jitter off so the delays a test asserts on are the policy's rather
        # than the RNG's. Full jitter itself is tested where it is implemented,
        # in `tests/test_decorator_retry.py`.
        "jitter": False,
    }
    options.update(overrides)
    return DistributedLock(backend, "resource", **options)  # type: ignore[arg-type]


class TestTheHappyPath:
    async def test_the_body_receives_a_lease(
        self, backend: InMemoryLockBackend, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        async with build_lock(backend, clock, sleeper) as lease:
            assert isinstance(lease, Lease)
            assert lease.name == "resource"
            assert lease.token == 1

    async def test_the_name_is_held_for_the_duration(
        self, backend: InMemoryLockBackend, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        async with build_lock(backend, clock, sleeper):
            intruder = await backend.acquire("resource", owner="other", ttl_seconds=5)

        assert intruder is None

    async def test_the_name_is_free_afterwards(
        self, backend: InMemoryLockBackend, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        async with build_lock(backend, clock, sleeper):
            pass

        assert await backend.inspect("resource") is None

    async def test_nothing_is_slept_when_the_lock_is_free(
        self, backend: InMemoryLockBackend, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        async with build_lock(backend, clock, sleeper):
            pass

        assert sleeper.delays == []

    async def test_the_lease_is_readable_from_the_lock(
        self, backend: InMemoryLockBackend, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        lock = build_lock(backend, clock, sleeper)
        async with lock as lease:
            assert lock.lease == lease

    async def test_the_lease_is_not_readable_outside_the_block(
        self, backend: InMemoryLockBackend, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        lock = build_lock(backend, clock, sleeper)

        with pytest.raises(RuntimeError, match="is not held"):
            _ = lock.lease

    async def test_the_lock_reports_its_name_and_owner(
        self, backend: InMemoryLockBackend, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        lock = build_lock(backend, clock, sleeper, owner="worker-7")

        assert lock.name == "resource"
        assert lock.owner == "worker-7"


class TestFailureInsideTheBody:
    async def test_the_lock_is_released(
        self, backend: InMemoryLockBackend, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        with pytest.raises(ZeroDivisionError):
            async with build_lock(backend, clock, sleeper):
                raise ZeroDivisionError

        assert await backend.inspect("resource") is None

    async def test_the_body_exception_is_the_one_that_propagates(
        self, backend: InMemoryLockBackend, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        """Even when the lease was also lost.

        Replacing the caller's failure with a consequence of it is a debug
        session spent on the wrong exception.
        """
        with pytest.raises(ZeroDivisionError):
            async with build_lock(backend, clock, sleeper, ttl_seconds=10.0):
                clock.advance(11)
                raise ZeroDivisionError


class TestContention:
    async def test_a_held_name_is_refused_immediately_by_default(
        self, backend: InMemoryLockBackend, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        """`wait_timeout` defaults to 0 for the same reason `nowait` exists in
        `src/locking/rows.py`: a retry loop is a queue."""
        await backend.acquire("resource", owner="holder", ttl_seconds=30)

        with pytest.raises(LockUnavailableError):
            async with build_lock(backend, clock, sleeper):
                pass

        assert sleeper.delays == []

    async def test_the_error_names_the_current_holder(
        self, backend: InMemoryLockBackend, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        await backend.acquire("resource", owner="holder", ttl_seconds=30)

        with pytest.raises(LockUnavailableError) as raised:
            async with build_lock(backend, clock, sleeper):
                pass

        details = raised.value.details
        assert isinstance(details, dict)
        assert details["held_by"] == "holder"
        assert details["attempts"] == 1

    async def test_a_holder_that_has_gone_is_simply_not_named(
        self, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        """The holder lookup happens after the failed acquisition, so it is
        advisory by construction — the holder may already have finished."""
        with pytest.raises(LockUnavailableError) as raised:
            async with build_lock(AlwaysBusyBackend(holder=None), clock, sleeper):
                pass

        details = raised.value.details
        assert isinstance(details, dict)
        assert "held_by" not in details

    async def test_it_waits_and_then_acquires(
        self, backend: InMemoryLockBackend, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        """The holder's lease expires while this one is backing off."""
        await backend.acquire("resource", owner="holder", ttl_seconds=0.2)

        async with build_lock(backend, clock, sleeper, wait_timeout=5.0) as lease:
            assert lease.token == 2

        assert sleeper.delays

    async def test_backoff_doubles(
        self, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        with pytest.raises(LockUnavailableError):
            async with build_lock(
                AlwaysBusyBackend(holder="holder"),
                clock,
                sleeper,
                wait_timeout=1.0,
                retry_base_delay=0.1,
                retry_max_delay=10.0,
            ):
                pass

        assert sleeper.delays[:3] == [0.1, 0.2, 0.4]

    async def test_it_never_sleeps_past_the_deadline(
        self, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        """A caller that asked to wait 500ms must not be held for ten seconds
        because the backoff had grown that far."""
        with pytest.raises(LockUnavailableError):
            async with build_lock(
                AlwaysBusyBackend(holder="holder"),
                clock,
                sleeper,
                wait_timeout=0.5,
                retry_base_delay=0.4,
                retry_max_delay=10.0,
            ):
                pass

        assert sum(sleeper.delays) == pytest.approx(0.5)

    async def test_an_unreachable_store_is_not_a_busy_lock(
        self, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        """503, not 409, and never 'carry on without the lock'."""
        with pytest.raises(LockBackendUnavailableError):
            async with build_lock(UnreachableBackend(), clock, sleeper):
                pass


class TestReuse:
    async def test_re_entering_a_held_lock_is_refused(
        self, backend: InMemoryLockBackend, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        """A second lease from one instance would let the inner exit release
        the outer section's lock."""
        lock = build_lock(backend, clock, sleeper)
        async with lock:
            with pytest.raises(RuntimeError, match="already held"):
                await lock.__aenter__()

    async def test_an_instance_can_be_used_again_after_it_exits(
        self, backend: InMemoryLockBackend, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        lock = build_lock(backend, clock, sleeper)
        async with lock as first:
            pass
        async with lock as second:
            assert second.token > first.token

    async def test_each_lock_gets_its_own_owner_by_default(self) -> None:
        assert new_owner_id() != new_owner_id()


class TestConstructorValidation:
    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"ttl_seconds": 0}, "ttl_seconds must be positive"),
            ({"ttl_seconds": -1}, "ttl_seconds must be positive"),
            ({"wait_timeout": -1}, "wait_timeout must not be negative"),
            ({"retry_base_delay": 0}, "retry_base_delay must be positive"),
            ({"retry_max_delay": 0.001}, "retry_max_delay must be at least"),
            ({"renew_interval": 0}, "renew_interval must be positive"),
            ({"renew_interval": 30.0}, "renew_interval must be positive"),
            ({"renew_interval": 60.0}, "renew_interval must be positive"),
        ],
    )
    async def test_impossible_configurations_are_refused(
        self,
        backend: InMemoryLockBackend,
        clock: FakeClock,
        sleeper: RecordingSleeper,
        kwargs: dict[str, object],
        message: str,
    ) -> None:
        """The renew-interval cases are the interesting ones: an interval at or
        above the TTL renews a lease that has already expired, which is to say
        it renews nothing and hides the fact."""
        with pytest.raises(ValueError, match=message):
            build_lock(backend, clock, sleeper, **kwargs)

    async def test_an_invalid_name_is_refused_at_construction(
        self, backend: InMemoryLockBackend
    ) -> None:
        with pytest.raises(LockNameInvalidError):
            DistributedLock(backend, "has space")


class TestRenewal:
    async def test_the_lease_deadline_moves(
        self, backend: InMemoryLockBackend, clock: FakeClock, ticker: TickSleeper
    ) -> None:
        lock = build_lock(backend, clock, ticker, ttl_seconds=10.0, renew_interval=4.0)
        async with lock as lease:
            await ticker.tick()
            assert lock.lease.expires_at > lease.expires_at

    async def test_the_token_survives_a_renewal(
        self, backend: InMemoryLockBackend, clock: FakeClock, ticker: TickSleeper
    ) -> None:
        """A token that changed per renewal would fence out the writes the same
        uninterrupted hold had already made."""
        lock = build_lock(backend, clock, ticker, ttl_seconds=10.0, renew_interval=4.0)
        async with lock as lease:
            await ticker.tick()
            assert lock.lease.token == lease.token

    async def test_a_renewed_lease_outlives_its_original_ttl(
        self, backend: InMemoryLockBackend, clock: FakeClock, ticker: TickSleeper
    ) -> None:
        """The point of renewal: a short TTL bounds how long a crashed holder
        blocks the name, and renewal is what lets a long section keep one."""
        lock = build_lock(backend, clock, ticker, ttl_seconds=10.0, renew_interval=4.0)
        async with lock:
            for _ in range(4):
                await ticker.tick()

            # 16 seconds into a 10-second lease, and still held.
            assert clock.now == 1016.0
            assert await backend.inspect("resource") is not None

    async def test_renewal_stops_at_the_end_of_the_block(
        self, backend: InMemoryLockBackend, clock: FakeClock, ticker: TickSleeper
    ) -> None:
        """A renewal landing after the release would leave the name locked with
        nobody holding it until the TTL ran out."""
        lock = build_lock(backend, clock, ticker, ttl_seconds=10.0, renew_interval=4.0)
        async with lock:
            await ticker.tick()

        await ticker.tick()
        assert await backend.inspect("resource") is None
        assert len(ticker.delays) == 1

    async def test_a_lease_lost_during_renewal_is_reported(
        self, clock: FakeClock, ticker: TickSleeper
    ) -> None:
        """The backend here releases cleanly, so the renewal is the only thing
        that can have made this a lost lease."""
        backend = LosesTheLeaseBackend()

        with pytest.raises(LockLostError):
            async with build_lock(
                backend, clock, ticker, ttl_seconds=10.0, renew_interval=4.0
            ):
                await ticker.tick()

    async def test_a_failed_renewal_attempt_is_survivable(
        self, clock: FakeClock, ticker: TickSleeper
    ) -> None:
        """A brief outage is not the end of a lease: the lease outlives it, and
        the next attempt may well succeed."""
        backend = FlakyRenewalBackend()

        async with build_lock(
            backend, clock, ticker, ttl_seconds=10.0, renew_interval=4.0
        ):
            await ticker.tick()
            await ticker.tick()

        assert backend.extend_calls == 2


class TestLostLeases:
    async def test_an_expired_lease_raises_on_exit(
        self, backend: InMemoryLockBackend, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        """A clean exit would report success for work the resource may have
        fenced out."""
        with pytest.raises(LockLostError) as raised:
            async with build_lock(backend, clock, sleeper, ttl_seconds=10.0):
                clock.advance(11)

        details = raised.value.details
        assert isinstance(details, dict)
        assert details["outcome"] == ReleaseOutcome.EXPIRED.value

    async def test_a_reassigned_name_raises_on_exit(
        self, backend: InMemoryLockBackend, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        with pytest.raises(LockLostError) as raised:
            async with build_lock(backend, clock, sleeper, ttl_seconds=10.0):
                clock.advance(11)
                await backend.acquire("resource", owner="next", ttl_seconds=30)

        details = raised.value.details
        assert isinstance(details, dict)
        assert details["outcome"] == ReleaseOutcome.NOT_OWNER.value

    async def test_a_stale_holder_does_not_release_the_current_one(
        self, backend: InMemoryLockBackend, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        """The bug an unconditional delete on exit would introduce: the late
        holder frees a lock it no longer owns, and a third caller walks into
        the section the second one is still running."""
        with pytest.raises(LockLostError):
            async with build_lock(backend, clock, sleeper, ttl_seconds=10.0):
                clock.advance(11)
                await backend.acquire("resource", owner="next", ttl_seconds=30)

        state = await backend.inspect("resource")
        assert state is not None
        assert state.owner == "next"

    async def test_an_unconfirmed_release_is_not_a_lost_lease(
        self, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        """The store not answering is no evidence the lease had ended, and a
        409 over a section that was protected throughout helps nobody. The name
        stays locked until its TTL runs out, which is what the TTL is for.
        """
        async with build_lock(UnreleasableBackend(), clock, sleeper):
            pass


class TestFencingEndToEnd:
    async def test_a_paused_holder_is_fenced_out_of_the_resource(
        self, backend: InMemoryLockBackend, clock: FakeClock, sleeper: RecordingSleeper
    ) -> None:
        """The failure the whole design exists for, start to finish.

        Holder A takes the lock and is then stopped — a GC pause, a blocked
        loop, a partition — for longer than its lease. B acquires the same name
        and writes. A wakes up still believing it holds the lock and writes
        too. Nothing in the lock itself can prevent that; the token is what
        stops the *resource* from accepting it.
        """
        ledger = Ledger()

        with pytest.raises(LockLostError):
            async with build_lock(backend, clock, sleeper, ttl_seconds=10.0) as a:
                clock.advance(11)  # A is stopped past its lease

                b = await backend.acquire("resource", owner="b", ttl_seconds=10)
                assert b is not None
                ledger.write("from-b", b.token)

                with pytest.raises(StaleFencingTokenError):
                    ledger.write("from-a", a.token)

        assert ledger.value == "from-b"


class Ledger:
    """A resource that remembers the highest token it has accepted.

    The in-memory version of `WHERE fence < :token`, which is where this check
    belongs when the resource is a database row.
    """

    def __init__(self) -> None:
        self.token: int | None = None
        self.value: str | None = None

    def write(self, value: str, token: int) -> None:
        require_fence(token, self.token, resource="ledger")
        self.token = token
        self.value = value


class AlwaysBusyBackend:
    """Never grants the lock. `inspect` reports whatever the test set up."""

    def __init__(self, holder: str | None) -> None:
        self._holder = holder

    @property
    def name(self) -> str:
        return "always-busy"

    async def acquire(
        self, name: str, *, owner: str, ttl_seconds: float
    ) -> Lease | None:
        return None

    async def extend(self, lease: Lease, *, ttl_seconds: float) -> Lease | None:
        return None

    async def release(self, lease: Lease) -> ReleaseOutcome:
        return ReleaseOutcome.RELEASED

    async def inspect(self, name: str) -> LockState | None:
        if self._holder is None:
            return None
        return LockState(name=name, token=1, owner=self._holder, ttl_seconds=5.0)

    async def close(self) -> None:
        return None


class UnreachableBackend(AlwaysBusyBackend):
    """A store that cannot be reached at all."""

    def __init__(self) -> None:
        super().__init__(holder=None)

    async def acquire(
        self, name: str, *, owner: str, ttl_seconds: float
    ) -> Lease | None:
        raise LockBackendUnavailableError()


class GrantingBackend(AlwaysBusyBackend):
    """Grants every acquisition. Renewal and release vary by subclass."""

    def __init__(self) -> None:
        super().__init__(holder=None)

    async def acquire(
        self, name: str, *, owner: str, ttl_seconds: float
    ) -> Lease | None:
        return Lease(
            name=name,
            token=1,
            owner=owner,
            ttl_seconds=ttl_seconds,
            expires_at=ttl_seconds,
        )


class LosesTheLeaseBackend(GrantingBackend):
    """Reports the lease gone at the first renewal, but releases cleanly."""


class FlakyRenewalBackend(GrantingBackend):
    """Fails one renewal with an outage, then succeeds."""

    def __init__(self) -> None:
        super().__init__()
        self.extend_calls = 0

    async def extend(self, lease: Lease, *, ttl_seconds: float) -> Lease | None:
        self.extend_calls += 1
        if self.extend_calls == 1:
            raise LockBackendUnavailableError()
        return lease.renewed_for(
            ttl_seconds=ttl_seconds, expires_at=lease.expires_at + ttl_seconds
        )


class UnreleasableBackend(GrantingBackend):
    """Grants a lease and then cannot be reached to release it."""

    async def release(self, lease: Lease) -> ReleaseOutcome:
        raise LockBackendUnavailableError()
