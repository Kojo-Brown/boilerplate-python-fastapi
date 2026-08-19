"""The async context manager: acquire, optionally renew, always release.

    async with DistributedLock(backend, f"invoice:{invoice_id}") as lease:
        await settle(invoice_id, fence=lease.token)

Three things happen around that body, and each of them is a decision worth
stating.

## Acquisition does not queue by default

`wait_timeout` defaults to 0: if the name is held, the context manager raises
`LockUnavailableError` (409) immediately rather than waiting. This mirrors
`nowait` in `src/locking/rows.py`, and for the same reason — a retry loop *is*
a queue, and an unbounded one turns a slow holder into every worker in the pool
blocked on it. Set `wait_timeout` when the caller genuinely wants to wait, and
bound it by something smaller than the request timeout above it.

The wait itself is full-jitter exponential backoff through `backoff_delay` in
`src/decorators/base.py` — the same policy as the two retry loops elsewhere in
this codebase, rather than a third one that drifts. Jitter matters more here
than usual: everyone waiting on a lock was woken by the same release, and
un-jittered backoff marches them back to the store in step.

## Renewal is a convenience, not a safety mechanism

Pass `renew_interval` and a background task extends the lease while the body
runs, which is what lets a job whose duration is unpredictable hold a short TTL
instead of a long one — and a short TTL is what bounds how long a crashed
holder blocks everyone else.

It does not make the lock safe. The pause that puts a holder past its lease can
land between the last successful renewal and the write just as easily as it can
land anywhere else; all renewal changes is how likely that is. What makes the
write safe is `lease.token` and a resource that checks it. If the body never
uses the token, adding renewal has bought comfort rather than correctness.

The token deliberately does not change across renewals — see
`Lease.renewed_for`.

## Release is conditional, and losing the lease is reported

Exit releases only if the store still says the lease is ours, and says so in
one atomic step (see `src/distributed_lock/redis_backend.py`). If the lease had
already expired, or another holder has taken the name, the body ran outside the
protection it asked for, and a clean exit would report success for work whose
writes may have been fenced out. So a body that returned normally raises
`LockLostError` on the way out.

A body that raised, on the other hand, propagates its own exception unchanged.
The lock's opinion is the less interesting of the two, and replacing a
`ValueError` from the critical section with a `LockLostError` would be a debug
session spent on the wrong exception.
"""

from __future__ import annotations

import asyncio
import random
import uuid
from types import TracebackType

import structlog

from src.decorators.base import (
    DEFAULT_CLOCK,
    DEFAULT_RNG,
    AsyncSleeper,
    Clock,
    backoff_delay,
)
from src.distributed_lock.base import (
    Lease,
    LockBackend,
    LockBackendUnavailableError,
    LockLostError,
    LockUnavailableError,
    ReleaseOutcome,
    validate_lock_name,
)

logger = structlog.get_logger(__name__)

#: Long enough for a section that talks to one external service, short enough
#: that a worker killed mid-section does not block the name for minutes. It is
#: a default, not a recommendation: the right TTL is a fact about the body.
DEFAULT_TTL_SECONDS: float = 30.0

#: The first wait after a failed acquisition, doubling from there. Deliberately
#: short — the common contention here is a section measured in milliseconds.
DEFAULT_RETRY_BASE_DELAY: float = 0.05

DEFAULT_RETRY_MAX_DELAY: float = 1.0


def new_owner_id() -> str:
    """A value no other lease will carry.

    Only uniqueness matters — the token already identifies the lease for every
    comparison the backends make — so a uuid4 is enough, and it keeps a
    hostname out of a key that ends up in logs.
    """
    return uuid.uuid4().hex


class DistributedLock:
    """A lease on `name`, held for the duration of an `async with` block.

    Reusable but not concurrent: one instance holds at most one lease at a
    time, and re-entering while it is held is a `RuntimeError` rather than a
    second lease. Build a second instance for a second name.
    """

    def __init__(
        self,
        backend: LockBackend,
        name: str,
        *,
        ttl_seconds: float = DEFAULT_TTL_SECONDS,
        wait_timeout: float = 0.0,
        renew_interval: float | None = None,
        owner: str | None = None,
        retry_base_delay: float = DEFAULT_RETRY_BASE_DELAY,
        retry_max_delay: float = DEFAULT_RETRY_MAX_DELAY,
        jitter: bool = True,
        rng: random.Random = DEFAULT_RNG,
        clock: Clock = DEFAULT_CLOCK,
        asleep: AsyncSleeper = asyncio.sleep,
    ) -> None:
        validate_lock_name(name)
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive.")
        if wait_timeout < 0:
            raise ValueError("wait_timeout must not be negative.")
        if retry_base_delay <= 0:
            raise ValueError("retry_base_delay must be positive.")
        if retry_max_delay < retry_base_delay:
            raise ValueError("retry_max_delay must be at least retry_base_delay.")
        if renew_interval is not None and not 0 < renew_interval < ttl_seconds:
            # An interval at or above the TTL renews a lease that has already
            # expired, which is to say it renews nothing and hides the fact.
            # Two thirds of the TTL leaves room for one failed attempt.
            raise ValueError(
                "renew_interval must be positive and shorter than ttl_seconds."
            )

        self._backend = backend
        self._name = name
        self._ttl = ttl_seconds
        self._wait_timeout = wait_timeout
        self._renew_interval = renew_interval
        self._owner = owner if owner is not None else new_owner_id()
        self._retry_base_delay = retry_base_delay
        self._retry_max_delay = retry_max_delay
        self._jitter = jitter
        self._rng = rng
        self._clock = clock
        self._asleep = asleep

        self._lease: Lease | None = None
        self._renewer: asyncio.Task[None] | None = None
        self._lost = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def owner(self) -> str:
        return self._owner

    @property
    def lease(self) -> Lease:
        """The current lease, renewals included.

        Raises `RuntimeError` outside the block. The body is handed the lease
        by `async with`; this is for code that keeps the lock object around and
        wants the deadline after a renewal has moved it.
        """
        if self._lease is None:
            raise RuntimeError(f"Lock {self._name!r} is not held.")
        return self._lease

    async def __aenter__(self) -> Lease:
        if self._lease is not None:
            raise RuntimeError(f"Lock {self._name!r} is already held by this instance.")

        lease = await self._acquire()
        self._lease = lease
        self._lost = False
        if self._renew_interval is not None:
            self._renewer = asyncio.create_task(
                self._renew_forever(self._renew_interval),
                name=f"dlock-renew:{self._name}",
            )
        logger.debug(
            "distributed_lock.acquired",
            lock=self._name,
            token=lease.token,
            owner=self._owner,
            backend=self._backend.name,
            ttl_seconds=self._ttl,
        )
        return lease

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._lease is None:  # pragma: no cover - unreachable via `async with`
            return

        # Before the lease is dropped, so a renewal in flight still has the
        # object it is renewing, and so the lease released below is the one the
        # last renewal produced rather than the one acquisition returned.
        await self._stop_renewer()
        lease = self._lease
        self._lease = None

        outcome = await self._release(lease)
        # `None` is "the store could not be reached", which is not evidence
        # either way — see `_release`. Only an outcome the store actually
        # reported can say the lease had already ended.
        lost = self._lost or (
            outcome is not None and outcome is not ReleaseOutcome.RELEASED
        )

        if not lost:
            logger.debug(
                "distributed_lock.released", lock=self._name, token=lease.token
            )
            return

        logger.warning(
            "distributed_lock.lease_lost",
            lock=self._name,
            token=lease.token,
            owner=self._owner,
            outcome=outcome.value if outcome is not None else "unconfirmed",
            body_failed=exc is not None,
        )
        if exc is not None:
            # The body's exception is the one worth propagating. Raising over it
            # here would replace the failure the caller has to debug with a
            # consequence of it.
            return
        raise LockLostError(
            "The distributed lock lease ended before the critical section did.",
            details={
                "lock": self._name,
                "token": lease.token,
                "outcome": outcome.value if outcome is not None else "unconfirmed",
            },
        )

    async def _acquire(self) -> Lease:
        """Claim the name, waiting up to `wait_timeout` for it."""
        deadline = self._clock() + self._wait_timeout
        attempt = 0

        while True:
            lease = await self._backend.acquire(
                self._name, owner=self._owner, ttl_seconds=self._ttl
            )
            if lease is not None:
                return lease

            attempt += 1
            now = self._clock()
            if now >= deadline:
                raise await self._unavailable(attempt, waited=self._wait_timeout)

            delay = backoff_delay(
                attempt,
                base_delay=self._retry_base_delay,
                max_delay=self._retry_max_delay,
                jitter=self._jitter,
                rng=self._rng,
            )
            # Never sleep past the deadline: a caller who asked to wait 200ms
            # should not be held for a second because the backoff had grown.
            await self._asleep(min(delay, deadline - now))

    async def _unavailable(
        self, attempts: int, *, waited: float
    ) -> LockUnavailableError:
        """Build the 409, naming the holder if the store will still say who.

        The lookup is best-effort and explicitly advisory: it happens after the
        failed acquisition, so the holder it names may already have finished. A
        store that has become unreachable between the two is reported as the
        503 it is, since that is a different problem from a busy lock.
        """
        state = await self._backend.inspect(self._name)
        details: dict[str, object] = {
            "lock": self._name,
            "attempts": attempts,
            "waited_seconds": waited,
        }
        if state is not None:
            details["held_by"] = state.owner
            details["held_token"] = state.token
            details["holder_ttl_seconds"] = state.ttl_seconds
        return LockUnavailableError(
            f"The lock {self._name!r} is held by another process.", details=details
        )

    async def _release(self, lease: Lease) -> ReleaseOutcome | None:
        """Release the lease, or return None if the store could not say.

        A release that cannot reach the store is not raised over the body. The
        lock has a TTL precisely so that an unreleased lease resolves itself,
        and turning a blip on the way out into an exception would fail a
        request whose work is already done — the cost is that the name stays
        locked for the rest of its TTL, which is what the TTL is for.

        Nor is it reported as a lost lease. That the store did not answer is no
        evidence the lease had ended; treating "I could not confirm the
        release" as "somebody else held this while you worked" would raise
        `LockLostError` over a section that was protected throughout.
        """
        try:
            return await self._backend.release(lease)
        except LockBackendUnavailableError:
            logger.warning(
                "distributed_lock.release_failed",
                lock=self._name,
                token=lease.token,
                ttl_seconds=lease.ttl_seconds,
            )
            return None

    async def _stop_renewer(self) -> None:
        """Cancel the renewal task and wait for it to actually stop.

        Awaiting it matters: a renewal in flight when the body finished could
        otherwise extend a lease this method is about to release, and the
        extension would land after the release and leave the name locked with
        nobody holding it until the TTL ran out.
        """
        renewer = self._renewer
        self._renewer = None
        if renewer is None:
            return
        renewer.cancel()
        try:
            await renewer
        except asyncio.CancelledError:
            # Ours, not the caller's: this task was cancelled by the line
            # above. Re-raising would abort a caller that is merely exiting a
            # `with` block. A cancellation aimed at the *caller* arrives on the
            # caller's task and is unaffected by this.
            pass

    async def _renew_forever(self, interval: float) -> None:
        """Extend the lease on a timer until cancelled or the lease is gone.

        The interval is a parameter rather than a read of `self._renew_interval`
        so the task's schedule is fixed when it starts, and so the body of the
        loop needs no narrowing of an optional that `__aenter__` has already
        checked.
        """
        while True:
            await self._asleep(interval)
            lease = self._lease
            if lease is None:  # pragma: no cover - cancelled first in practice
                return

            try:
                renewed = await self._backend.extend(lease, ttl_seconds=self._ttl)
            except LockBackendUnavailableError:
                # Not fatal on its own: the lease outlives a brief outage, and
                # the next attempt may well succeed. If it does not, the lease
                # expires and the extend after that returns None, which is the
                # branch below.
                logger.warning(
                    "distributed_lock.renew_failed", lock=self._name, token=lease.token
                )
                continue

            if renewed is None:
                self._lost = True
                logger.warning(
                    "distributed_lock.renew_lost_lease",
                    lock=self._name,
                    token=lease.token,
                )
                return

            self._lease = renewed
            logger.debug(
                "distributed_lock.renewed", lock=self._name, token=renewed.token
            )
