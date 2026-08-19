"""Backend-agnostic distributed-lock contract, and the fencing token.

Everything here is free of redis, asyncio primitives and the settings object,
so a backend can be written against it without inheriting any of them. The
concrete backends live in `redis_backend.py` and `memory.py`; `factory.py`
chooses between them, and `lock.py` is the async context manager callers
actually use.

## A lease, not a lock

Nothing here hands out a lock that is held until you give it back. Every
acquisition is a *lease*: a claim that expires on its own after `ttl_seconds`,
whether or not the holder is still alive. That is not a convenience — it is the
only thing that keeps a process killed mid-section from locking a resource
forever, because there is nobody left to run the release.

The consequence is the whole reason this module is shaped the way it is: **a
lease can expire while its holder still believes it holds one.** A stop-the-
world pause, a blocked event loop, an over-long database call, a network
partition that makes the store unreachable — any of them can put more wall time
between "acquire returned" and "the write lands" than the TTL allows. The
holder finds out late or never. Renewal narrows the window and does not close
it: the pause can just as easily land between the last successful renewal and
the write.

## Which is what the fencing token is for

`Lease.token` is a strictly increasing integer, minted by the store on each
successful acquisition of a given name and never reused. It is the answer to
the paragraph above, and it works by moving the check to where the damage would
happen: the *resource* remembers the highest token it has accepted and refuses
anything lower. A holder that was paused past its TTL then carries a token the
resource has already moved beyond, and its write is rejected — no matter what
it believes about the lock.

In SQL that is one clause, not a subsystem:

    UPDATE jobs SET state = 'done', fence = :token
     WHERE id = :id AND fence < :token

`require_fence` is the same check for a resource that is not a database row.

This distinction is the difference between a lock that is *usually* right and
one that is *safe*, and it is the reason `Lease.token` is part of the public
contract rather than a detail of the Redis encoding: a lock whose token nobody
consumes is a lock relying on the timing assumption above. See
`docs/distributed-locking.md` for what to do when the resource cannot be
fenced.

## What this is not

It is not Redlock. A single store is the coordination point, so a failover that
loses the last few writes can hand the same name to two holders — and, worse
for this design, can lose the token counter and start it over. Redlock's
multi-master quorum does not fix that either (a lock server cannot bound a
client's pause), which is why the fencing token is the safety mechanism here
and the store is only an optimisation that keeps contention cheap. See the
docs for how the counter must be configured to survive.

It is also not a replacement for `src/locking`. Postgres row locks are the
right tool when the thing being protected is a row in the database you are
already talking to: the lock and the write are in one transaction, so there is
no window of the kind described above at all. Reach for this one when the
critical section spans something the database cannot see — an external API, a
scheduled job that must not run twice, a file in object storage.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Final, Protocol, runtime_checkable

from src.exceptions import AppException, ConflictError

#: Lock names end up in a store key, a log line and an error body, and are
#: frequently built from user-controlled ids (`order:{uuid}`). Printable ASCII
#: with no space keeps a name from injecting a newline into a log or a space
#: into a Redis key; `:` is deliberately allowed, since hierarchical names are
#: the convention this module expects.
_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[\x21-\x7e]+$")

MAX_NAME_LENGTH: Final[int] = 255

#: The first token a name can ever be given. Counters start here, so a resource
#: that has accepted nothing yet can compare against `None` rather than against
#: a sentinel that is also a legal token.
FIRST_TOKEN: Final[int] = 1


class LockNameInvalidError(ValueError):
    """The lock name is not usable as a key.

    A `ValueError` rather than an `AppException`: lock names are chosen by this
    codebase, not by a client, so a bad one is a programming error that should
    surface as a 500 with a stack trace pointing at the call site — not as a
    4xx telling a user to fix something they never sent.
    """


class LockUnavailableError(ConflictError):
    """The lock is held elsewhere and the caller would not wait for it.

    409 rather than 503: the request is well-formed and conflicts with what
    another holder is doing right now, which is what 409 means, and it carries
    its own `error_code` so a client can tell it from the durable conflicts. A
    duplicate email will fail identically forever; this will not.
    """

    error_code = "DISTRIBUTED_LOCK_UNAVAILABLE"
    headers = {"Retry-After": "1"}


class LockLostError(ConflictError):
    """The lease ended before the critical section did.

    Raised on exit when the store reports the lease already gone or already
    reassigned. It is a report, not a protection — by the time it is raised the
    section has already run, possibly beside another holder. What limits the
    damage is the fencing token; what this exception does is stop the caller
    reporting success for work whose writes may have been fenced out.
    """

    error_code = "DISTRIBUTED_LOCK_LOST"


class StaleFencingTokenError(ConflictError):
    """A write arrived carrying a token the resource has already passed.

    The holder was paused or partitioned past its lease and somebody else has
    since held the lock. Refusing the write is the point of the token; retrying
    it unchanged will fail identically, so the caller must re-acquire.
    """

    error_code = "STALE_FENCING_TOKEN"


class LockBackendUnavailableError(AppException):
    """The store could not be reached, or answered with something unreadable.

    503, and never downgraded to "carry on without the lock": a critical
    section that runs when the coordination it depends on is unreachable is
    exactly the concurrent execution the lock was taken to prevent. There is no
    fail-open switch here for that reason — unlike `IDEMPOTENCY_FAIL_OPEN`,
    where the cost of failing open is a duplicate the client asked to be spared
    rather than an unserialised section.
    """

    status_code = 503
    error_code = "DISTRIBUTED_LOCK_BACKEND_UNAVAILABLE"
    headers = {"Retry-After": "1"}

    def __init__(
        self,
        message: str = "Distributed lock backend unavailable",
        details: object = None,
    ) -> None:
        super().__init__(message, details)


class ReleaseOutcome(StrEnum):
    """What the store found when asked to release a lease.

    Three outcomes rather than a bool, because "the key was not there" and "the
    key belongs to somebody else" mean different things to the caller and only
    one of them is ordinary. `RELEASED` is the clean path; the other two both
    say the lease ended early, which is what `LockLostError` reports.
    """

    #: The lease was still ours and the key is now gone.
    RELEASED = "released"

    #: The key had already expired. Nobody holds the lock; the section ran past
    #: its lease and the tail of it was unprotected.
    EXPIRED = "expired"

    #: The key exists with a different value: the lease expired *and* another
    #: holder has since taken it. Nothing was deleted — deleting it would have
    #: released a lock this process does not hold, which is the single most
    #: common bug in hand-rolled Redis locks.
    NOT_OWNER = "not_owner"


@dataclass(frozen=True, slots=True)
class Lease:
    """A time-limited claim on `name`, plus the token that makes it safe.

    Frozen because a lease is evidence of something that already happened. A
    renewal produces a *new* lease (see `renewed_for`) carrying the same token
    and a later deadline, so a caller holding the old object cannot be silently
    told that its deadline moved.

    `expires_at` is on the monotonic clock, and is deliberately pessimistic: it
    is measured from just *before* the acquire request goes out, so it under-
    states the lease by one round trip rather than overstating it. Overstating
    is the dangerous direction — it would have a holder believe it still has
    time that the store has already taken back.
    """

    name: str
    token: int
    owner: str
    ttl_seconds: float
    expires_at: float

    def remaining(self, now: float) -> float:
        """Seconds of lease left at monotonic time `now`, floored at zero."""
        return max(0.0, self.expires_at - now)

    def is_expired(self, now: float) -> bool:
        """Whether this lease has run out, as far as the local clock knows.

        A local answer to a question only the store can settle. False here does
        not mean the store still has the key — it means nothing this process
        knows contradicts that. True is the more useful direction: it means the
        holder is certainly out of time and should stop rather than write.
        """
        return now >= self.expires_at

    def renewed_for(self, *, ttl_seconds: float, expires_at: float) -> Lease:
        """This lease with a later deadline. Same name, owner and token.

        The token is stable across renewals on purpose. It identifies the
        *lease*, not the renewal, so a resource that has accepted a write under
        this token keeps accepting writes from the same uninterrupted hold —
        bumping it per renewal would have a holder fence itself out.
        """
        return replace(self, ttl_seconds=ttl_seconds, expires_at=expires_at)


@dataclass(frozen=True, slots=True)
class LockState:
    """Who holds a name, as the store saw it at some past instant.

    Read after a failed acquisition to say something useful in the 409, and
    advisory by construction: the holder it names may already have finished by
    the time the error reaches the client. It is a debugging aid, never a basis
    for a decision — deciding on it is a check-then-act race with the lock's
    own semantics.
    """

    name: str
    token: int
    owner: str
    ttl_seconds: float | None


@runtime_checkable
class LockBackend(Protocol):
    """The operations a distributed lock needs from a store.

    Narrow on purpose: no listing, no "force unlock", no ownership transfer.
    Every method is reachable from a single name, and each of the missing ones
    would be a way to break the guarantee the rest provide.

    Every method must be atomic with respect to concurrent callers in other
    processes. `acquire` in particular has to mint the token and claim the key
    in one indivisible step — a read-then-write pair lets two callers both find
    the key free, and a lock that can be held twice is not a lock.
    """

    @property
    def name(self) -> str:
        """Short identifier, e.g. `"redis"`. Used in logs."""
        ...

    async def acquire(
        self, name: str, *, owner: str, ttl_seconds: float
    ) -> Lease | None:
        """Claim `name` for `ttl_seconds`, or return None if it is held.

        Returns None for *any* live holder, including one with the same
        `owner`. This lock is not reentrant: handing the same owner a second
        lease would let an inner section's release free the outer section's
        lock, and the outer section would then run on unprotected for as long
        as it liked, believing otherwise.
        """
        ...

    async def extend(self, lease: Lease, *, ttl_seconds: float) -> Lease | None:
        """Push `lease`'s deadline out, or return None if it is no longer ours.

        None covers both "expired" and "someone else holds it now"; neither is
        recoverable by extending, and the caller's response to both is to stop.
        """
        ...

    async def release(self, lease: Lease) -> ReleaseOutcome:
        """Give up `lease`, but only if the store still says it is ours.

        Must compare before deleting. An unconditional delete after a lease has
        expired releases whatever holder came next, which turns one late holder
        into two concurrent sections.
        """
        ...

    async def inspect(self, name: str) -> LockState | None:
        """Report the current holder of `name`, or None if it is free."""
        ...

    async def close(self) -> None:
        """Release any connections held. Called once from the app lifespan."""
        ...


def validate_lock_name(name: str) -> str:
    """Return `name` unchanged if it is usable as a key, else raise."""
    if not name:
        raise LockNameInvalidError("Lock name must not be empty.")

    if len(name) > MAX_NAME_LENGTH:
        raise LockNameInvalidError(
            f"Lock name must be at most {MAX_NAME_LENGTH} characters, got {len(name)}."
        )

    if not _NAME_RE.match(name):
        raise LockNameInvalidError(
            "Lock name must contain only printable, non-space ASCII."
        )

    return name


def fence_is_current(token: int, last_accepted: int | None) -> bool:
    """Whether a write carrying `token` may be applied.

    `last_accepted` is the highest token the resource has already applied, or
    None if it has applied none. The comparison is strictly greater: the same
    token twice is a replay of a write the resource has already taken, and
    applying it again is the double execution the lock was for.
    """
    return last_accepted is None or token > last_accepted


def require_fence(token: int, last_accepted: int | None, *, resource: str) -> None:
    """Raise unless `token` is current for `resource`.

    The non-SQL half of the fencing check — for a resource whose "last accepted
    token" lives somewhere other than the row being written, so it cannot be
    folded into a `WHERE fence < :token`. Where it *can*, prefer the SQL: one
    statement that both checks and writes has no window between the two, and
    this function has one by construction.
    """
    if not fence_is_current(token, last_accepted):
        raise StaleFencingTokenError(
            "This fencing token is no longer current; the lock was lost.",
            details={
                "resource": resource,
                "token": token,
                "last_accepted": last_accepted,
            },
        )
