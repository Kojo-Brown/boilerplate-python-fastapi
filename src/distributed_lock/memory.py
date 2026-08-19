"""In-process lock backend.

The backend tests use when they want the real `DistributedLock` code path
without a Redis server, and the one a single-process development run can use so
that `DISTRIBUTED_LOCK_BACKEND=memory` is a working configuration rather than a
crash.

It is never a production choice, and the reason is worth being blunt about: a
lock that only coordinates callers inside one process coordinates nothing. Two
uvicorn workers behind the same socket would each hold their own dictionary,
both would "acquire" the same name at the same instant, and both critical
sections would run — which is the exact failure the caller reached for a lock
to prevent, now failing silently instead of loudly. The factory logs a warning
when this backend is selected outside a test or development environment.

The token counters here are per-process too, so two workers would also hand out
the *same* fencing tokens to different holders, and the resource-side check
would see nothing wrong. That is the second reason this is not deployable, and
it is the less obvious one.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from src.decorators.base import DEFAULT_CLOCK, Clock
from src.distributed_lock.base import (
    Lease,
    LockState,
    ReleaseOutcome,
    validate_lock_name,
)


@dataclass(slots=True)
class _Held:
    """One live claim, as this backend records it."""

    token: int
    owner: str
    expires_at: float


class InMemoryLockBackend:
    """Holds claims in a dict, with monotonic expiry stamps.

    The `asyncio.Lock` covers each check-then-act. Without it two tasks could
    both find a name free and both believe they hold it — the single-process
    version of the race the Redis backend uses a Lua script to avoid.

    `time.monotonic` by default rather than wall-clock time, so an NTP step
    cannot resurrect an expired claim or expire a live one. The clock is
    injectable so a test can advance time without spending it.
    """

    def __init__(self, *, clock: Clock = DEFAULT_CLOCK) -> None:
        self._clock = clock
        self._held: dict[str, _Held] = {}
        # Never pruned, and never reset by a release: monotonicity of the
        # fencing token is a property *of the name*, not of the current holder,
        # so forgetting the counter when a lock goes idle would let the next
        # holder be issued a token an earlier one already used. The cost is one
        # small entry per distinct name for the life of the process, which is
        # the same trade the Redis backend makes with a key that has no TTL.
        self._counters: dict[str, int] = {}
        self._guard = asyncio.Lock()

    @property
    def name(self) -> str:
        return "memory"

    def _live(self, name: str) -> _Held | None:
        """The claim on `name` if one is live, purging it if it has expired.

        Expiry is evaluated on read rather than by a sweeper task: a background
        loop would keep this object alive for the life of the process and would
        have to be cancelled by whoever built it.
        """
        held = self._held.get(name)
        if held is None:
            return None
        if held.expires_at <= self._clock():
            del self._held[name]
            return None
        return held

    async def acquire(
        self, name: str, *, owner: str, ttl_seconds: float
    ) -> Lease | None:
        validate_lock_name(name)
        async with self._guard:
            if self._live(name) is not None:
                return None
            # Measured before the claim is recorded, mirroring the Redis
            # backend, where the deadline has to be taken before the request
            # goes out. Here they are the same instant; keeping the order
            # identical keeps the two backends' leases comparable.
            started = self._clock()
            token = self._counters.get(name, 0) + 1
            self._counters[name] = token
            self._held[name] = _Held(
                token=token, owner=owner, expires_at=started + ttl_seconds
            )
            return Lease(
                name=name,
                token=token,
                owner=owner,
                ttl_seconds=ttl_seconds,
                expires_at=started + ttl_seconds,
            )

    async def extend(self, lease: Lease, *, ttl_seconds: float) -> Lease | None:
        async with self._guard:
            held = self._live(lease.name)
            if held is None or held.token != lease.token:
                return None
            started = self._clock()
            held.expires_at = started + ttl_seconds
            return lease.renewed_for(
                ttl_seconds=ttl_seconds, expires_at=started + ttl_seconds
            )

    async def release(self, lease: Lease) -> ReleaseOutcome:
        async with self._guard:
            held = self._live(lease.name)
            if held is None:
                return ReleaseOutcome.EXPIRED
            if held.token != lease.token:
                return ReleaseOutcome.NOT_OWNER
            del self._held[lease.name]
            return ReleaseOutcome.RELEASED

    async def inspect(self, name: str) -> LockState | None:
        async with self._guard:
            held = self._live(name)
            if held is None:
                return None
            return LockState(
                name=name,
                token=held.token,
                owner=held.owner,
                ttl_seconds=max(0.0, held.expires_at - self._clock()),
            )

    async def close(self) -> None:
        """Nothing to close. Present so the app lifespan needs no `isinstance`."""

    async def clear(self) -> None:
        """Drop every live claim, keeping the token counters.

        For tests that reuse a process-wide backend. The counters survive
        deliberately: a test that cleared them could not tell a monotonic token
        from one that restarts, which is the property most worth protecting.
        """
        async with self._guard:
            self._held.clear()
