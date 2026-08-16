"""Telling one database failure apart from another, by SQLSTATE.

Every decision in this package turns on *why* a statement failed, and the only
answer that is stable across driver versions is the five-character SQLSTATE
Postgres itself assigns (Appendix A of the manual). Matching on the exception
class is not equivalent: SQLAlchemy hands back
`sqlalchemy.exc.DBAPIError` for a deadlock, a lock timeout and a unique-key
violation alike when the asyncpg dialect is in use, so a class check that looks
precise would retry a constraint violation forever. Matching on the message
text is worse — it is localised.

Where the code actually lives is driver-specific and undocumented enough to be
worth stating: with `postgresql+asyncpg`, SQLAlchemy wraps the asyncpg error in
`sqlalchemy.dialects.postgresql.asyncpg.Error`, hangs it off `DBAPIError.orig`,
and that wrapper exposes both `sqlstate` and `pgcode`. Raw asyncpg exceptions
carry `sqlstate`; psycopg's carry `pgcode`. `sqlstate()` reads whichever is
there so this package keeps working if the driver is ever swapped.
"""

from __future__ import annotations

from typing import Final

from src.exceptions import ConflictError

#: Two transactions each held a lock the other needed. Postgres picked one and
#: killed it; the survivor committed. Retrying the victim is the textbook
#: response, and usually succeeds, because the winner's locks are gone by then.
DEADLOCK_DETECTED: Final = "40P01"

#: A `REPEATABLE READ` or `SERIALIZABLE` transaction could not be serialised
#: against a concurrent one. Surfaces at `COMMIT` rather than mid-transaction,
#: which is why the work being retried has to own its own commit.
SERIALIZATION_FAILURE: Final = "40001"

#: `NOWAIT` found the row already locked, or `lock_timeout` expired while
#: waiting. Deliberately *not* retryable by default: both are a caller saying
#: "do not queue behind this", and a retry loop is a queue.
LOCK_NOT_AVAILABLE: Final = "55P03"

#: A statement ran inside a transaction an earlier error had already aborted —
#: "current transaction is aborted, commands ignored until end of transaction
#: block". Never a cause, always a symptom: it means something reused a session
#: without rolling it back first, which is precisely the mistake
#: `src/locking/retry.py` exists to avoid making on every retry.
IN_FAILED_SQL_TRANSACTION: Final = "25P02"

#: The default retry set. Both members share the property that makes a retry
#: honest — the statement failed because of *another* transaction, not because
#: of anything wrong with this one, so the identical request may well succeed.
#: A unique violation or a check-constraint failure has the opposite property
#: and is absent on purpose.
RETRYABLE_SQLSTATES: Final[frozenset[str]] = frozenset(
    {DEADLOCK_DETECTED, SERIALIZATION_FAILURE}
)

# `DBAPIError.orig` is one hop in practice. The bounded walk costs three lines
# and survives a driver that nests one more, where an unbounded one would hang
# on an exception whose `orig` is itself.
_MAX_ORIG_DEPTH: Final = 4


class LockNotAvailableError(ConflictError):
    """A lock could not be taken without waiting, and the caller refused to.

    Raised for SQLSTATE 55P03: either `nowait=True` found the row held, or the
    statement sat in `lock_timeout` until Postgres cancelled it.

    A `ConflictError` subclass, so it reaches the edge as 409 rather than 500:
    the request was well-formed and conflicts with what another transaction is
    doing right now, which is what 409 means. It carries its own `error_code`
    so a client can distinguish "someone else is editing this, try again in a
    moment" from the durable 409s — a duplicate email will fail identically
    forever, and this will not.
    """

    error_code = "LOCK_NOT_AVAILABLE"

    def __init__(
        self,
        message: str = "Row is locked by another transaction",
        details: object = None,
    ) -> None:
        super().__init__(message, details)


def sqlstate(exc: BaseException) -> str | None:
    """The Postgres SQLSTATE behind `exc`, or None if it is not a database error.

    Checks the exception itself and then its `orig` chain, reading `sqlstate`
    first and `pgcode` second. Both are validated as five-character strings
    before being returned: a driver that sets the attribute to `None`, or to an
    integer, must read as "no SQLSTATE here" rather than produce a code that
    compares unequal to every constant in this module and silently disables
    every retry.
    """
    candidate: BaseException | None = exc
    for _ in range(_MAX_ORIG_DEPTH):
        if candidate is None:
            break
        for attribute in ("sqlstate", "pgcode"):
            value = getattr(candidate, attribute, None)
            if isinstance(value, str) and len(value) == 5:
                return value
        nxt = getattr(candidate, "orig", None)
        candidate = (
            nxt if isinstance(nxt, BaseException) and nxt is not candidate else None
        )
    return None


def is_deadlock(exc: BaseException) -> bool:
    """Whether `exc` is Postgres reporting a deadlock it just broke (40P01)."""
    return sqlstate(exc) == DEADLOCK_DETECTED


def is_serialization_failure(exc: BaseException) -> bool:
    """Whether `exc` is a failed serialisation of concurrent transactions (40001)."""
    return sqlstate(exc) == SERIALIZATION_FAILURE


def is_lock_unavailable(exc: BaseException) -> bool:
    """Whether `exc` is a refused or timed-out lock acquisition (55P03)."""
    return sqlstate(exc) == LOCK_NOT_AVAILABLE


def is_retryable_conflict(
    exc: BaseException, *, codes: frozenset[str] = RETRYABLE_SQLSTATES
) -> bool:
    """Whether re-running the transaction that raised `exc` is worth trying.

    True only for a SQLSTATE in `codes`, which by default means a deadlock or a
    serialisation failure. Anything without a SQLSTATE — a `TypeError` from a
    bug in the work itself, an `asyncio.TimeoutError` — is False, so a retry
    loop built on this never spins on a defect it cannot fix by waiting.
    """
    code = sqlstate(exc)
    return code is not None and code in codes
