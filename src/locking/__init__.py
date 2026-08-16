"""Pessimistic concurrency control: row locks, and retrying what loses one.

Two halves that are only useful together. `rows` takes the locks —
`SELECT ... FOR UPDATE` and its weaker relatives, with `nowait`, `skip_locked`
and a bounded wait — and `retry` re-runs a transaction that Postgres killed to
break a deadlock. Locking without the retry leaves an error nobody handles;
retrying without the locks retries a race it never had to lose.

This is the counterpart to `src/concurrency`, which solves the same lost-update
problem optimistically. Neither is the default answer: prefer the optimistic
version for anything a user edits through an API, where conflicts are rare and
blocking a request on another user's transaction is worse than asking one of
them to re-read; reach for this one when the conflict rate is high, when the
decision depends on the row being written ("is there still stock?"), or when
the work between read and write is expensive enough that discarding it hurts.

See `docs/pessimistic-locking.md`.
"""

from src.locking.errors import (
    DEADLOCK_DETECTED,
    IN_FAILED_SQL_TRANSACTION,
    LOCK_NOT_AVAILABLE,
    RETRYABLE_SQLSTATES,
    SERIALIZATION_FAILURE,
    LockNotAvailableError,
    is_deadlock,
    is_lock_unavailable,
    is_retryable_conflict,
    is_serialization_failure,
    sqlstate,
)
from src.locking.retry import (
    DeadlockRetryPolicy,
    TransactionalWork,
    retry_on_deadlock,
    run_with_deadlock_retry,
)
from src.locking.rows import LockMode, lock_row, lock_rows, lock_timeout

__all__ = [
    "DEADLOCK_DETECTED",
    "IN_FAILED_SQL_TRANSACTION",
    "LOCK_NOT_AVAILABLE",
    "RETRYABLE_SQLSTATES",
    "SERIALIZATION_FAILURE",
    "DeadlockRetryPolicy",
    "LockMode",
    "LockNotAvailableError",
    "TransactionalWork",
    "is_deadlock",
    "is_lock_unavailable",
    "is_retryable_conflict",
    "is_serialization_failure",
    "lock_row",
    "lock_rows",
    "lock_timeout",
    "retry_on_deadlock",
    "run_with_deadlock_retry",
    "sqlstate",
]
