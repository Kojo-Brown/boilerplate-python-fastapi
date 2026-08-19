"""Mutual exclusion across processes, with a fencing token that makes it safe.

`src/locking` serialises writers inside one Postgres transaction. This package
serialises them across machines, for critical sections the database cannot see:
a nightly job that must not run twice, a call to a payment provider that has no
idempotency key of its own, a rebuild of an object in S3.

    from src.distributed_lock import DistributedLock

    async with DistributedLock(backend, f"rebuild:{report_id}") as lease:
        await rebuild(report_id, fence=lease.token)

The `fence=lease.token` is not decoration. A distributed lock cannot stop a
holder from being paused past its own lease, so the token — strictly increasing
per name, never reused — is what lets the *resource* reject a writer the lock
has already moved on from. `src/distributed_lock/base.py` explains the failure
it prevents; `docs/distributed-locking.md` is the practical guide.
"""

from src.distributed_lock.base import (
    FIRST_TOKEN,
    MAX_NAME_LENGTH,
    Lease,
    LockBackend,
    LockBackendUnavailableError,
    LockLostError,
    LockNameInvalidError,
    LockState,
    LockUnavailableError,
    ReleaseOutcome,
    StaleFencingTokenError,
    fence_is_current,
    require_fence,
    validate_lock_name,
)
from src.distributed_lock.factory import (
    create_lock_backend,
    get_lock_backend,
)
from src.distributed_lock.lock import (
    DEFAULT_TTL_SECONDS,
    DistributedLock,
    new_owner_id,
)
from src.distributed_lock.memory import InMemoryLockBackend
from src.distributed_lock.redis_backend import RedisLockBackend

__all__ = [
    "DEFAULT_TTL_SECONDS",
    "FIRST_TOKEN",
    "MAX_NAME_LENGTH",
    "DistributedLock",
    "InMemoryLockBackend",
    "Lease",
    "LockBackend",
    "LockBackendUnavailableError",
    "LockLostError",
    "LockNameInvalidError",
    "LockState",
    "LockUnavailableError",
    "RedisLockBackend",
    "ReleaseOutcome",
    "StaleFencingTokenError",
    "create_lock_backend",
    "fence_is_current",
    "get_lock_backend",
    "new_owner_id",
    "require_fence",
    "validate_lock_name",
]
