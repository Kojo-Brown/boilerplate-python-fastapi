"""Row locks: `SELECT ... FOR UPDATE` and friends, with the sharp edges named.

Pessimistic locking is the other answer to the lost update that
`docs/optimistic-concurrency.md` solves optimistically. Optimistic control lets
both writers proceed and fails the loser at the end; pessimistic control makes
the second writer *wait* at the start, so by the time it reads, the first has
finished and it is reading current data. The trade is the usual one: optimistic
wins when conflicts are rare, because nobody blocks; pessimistic wins when they
are common, because a conflict costs a short wait instead of a whole discarded
attempt — and, unlike a version check, it can protect a read-modify-write whose
decision depends on the row it is about to change ("is the balance still
enough?"), where retrying is not the same as never having gone wrong.

Three properties of a row lock are easy to assume and wrong:

**The lock lives until the transaction ends, not until you are done with it.**
There is no unlock call here because Postgres offers none: `COMMIT` and
`ROLLBACK` are the only releases. A session that takes a lock and then does
something slow holds it for the whole of that something.

**A lock over a stale in-memory copy protects nothing, and the ORM will hand
you one by default.** This is the sharpest edge in the module and the reason
both functions look the way they do. A `SELECT ... FOR UPDATE` for a row the
session has already loaded takes the lock, returns the row — and gives back the
*cached* instance, because the identity map wins over the result set. The lock
is real and the values you then decide against are the ones you read before it
existed, which is precisely the race the lock was for, failing silently. Both
functions here therefore run with `populate_existing=True`, which overwrites
the in-memory instance with what the locked read returned.

The corollary is a rule for callers: **take the lock before you modify the
row.** `populate_existing` overwrites unflushed changes along with everything
else, and a modification decided on before the lock was held was decided on
stale data anyway.

`Session.get(..., with_for_update=...)` is the obvious alternative and is not
used here. It routes through the refresh path, which for a model with a
`version_id_col` — as `User` has, for the optimistic concurrency in
`docs/optimistic-concurrency.md` — validates the version and raises
`StaleDataError` when the row has moved on. That is the right answer for an
optimistic refresh and the wrong one here: a pessimistic reader is asking for
current data, not for a report that its copy is old.

**Lock order is the caller's problem.** Two transactions that lock rows A and B
in opposite orders deadlock, and nothing in this module can see that. Lock in a
consistent order — by primary key is the usual one — and treat
`src/locking/retry.py` as the safety net rather than the plan.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from enum import StrEnum
from typing import Any, Final

import structlog
from sqlalchemy import Select, inspect, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import Base
from src.locking.errors import LockNotAvailableError, is_lock_unavailable

logger = structlog.get_logger(__name__)


class LockMode(StrEnum):
    """Which row-level lock to take (Postgres manual, "Row-Level Locks").

    The four differ in what they conflict with, and the gap between the first
    two is the one that costs people production incidents:

    `UPDATE` conflicts with every other row lock, including the `KEY SHARE`
    that Postgres takes implicitly on a row when another table's row is
    inserted with a foreign key pointing at it. Locking a `users` row `FOR
    UPDATE` therefore blocks inserts into every table that references it, which
    is rarely what was intended.

    `NO_KEY_UPDATE` is what a plain `UPDATE` of a non-key column takes anyway.
    It still excludes other writers but permits those foreign-key references,
    and is the better default for the read-modify-write of an ordinary column.

    `SHARE` and `KEY_SHARE` are the read locks: several readers coexist, and
    all of them block a writer. Use `SHARE` to pin a row you are reading as a
    precondition for writing somewhere *else* — note that several holders
    upgrading a `SHARE` to an `UPDATE` at once is a deadlock, by construction.
    """

    UPDATE = "update"
    NO_KEY_UPDATE = "no_key_update"
    SHARE = "share"
    KEY_SHARE = "key_share"

    @property
    def clause(self) -> dict[str, bool]:
        """This mode as SQLAlchemy's `read`/`key_share` pair.

        SQLAlchemy spells the four modes as two orthogonal booleans, which is
        compact and unmemorable; the enum exists so call sites read as the SQL
        they emit instead of as `read=True, key_share=True`.
        """
        return {
            LockMode.UPDATE: {"read": False, "key_share": False},
            LockMode.NO_KEY_UPDATE: {"read": False, "key_share": True},
            LockMode.SHARE: {"read": True, "key_share": False},
            LockMode.KEY_SHARE: {"read": True, "key_share": True},
        }[self]


#: Postgres accepts `SET LOCAL lock_timeout` only inside a transaction, and its
#: value is a string with a unit. Set through `set_config` rather than string
#: interpolation into `SET LOCAL`, because `SET` takes no bind parameters and
#: the alternative is formatting a value into SQL.
_SET_LOCK_TIMEOUT: Final = text("SELECT set_config('lock_timeout', :value, true)")
_GET_LOCK_TIMEOUT: Final = text("SELECT current_setting('lock_timeout')")


def _for_update_argument(
    mode: LockMode, *, nowait: bool = False, skip_locked: bool = False
) -> dict[str, Any]:
    return {**mode.clause, "nowait": nowait, "skip_locked": skip_locked}


async def lock_row[ModelT: Base](
    session: AsyncSession,
    model: type[ModelT],
    ident: uuid.UUID,
    *,
    mode: LockMode = LockMode.UPDATE,
    nowait: bool = False,
) -> ModelT | None:
    """Load one row by primary key and hold it against concurrent writers.

    Returns the locked instance, or `None` if no such row exists. The lock is
    held until the surrounding transaction commits or rolls back.

    With `nowait=False` (the default) this waits for however long the current
    holder takes; wrap the call in `lock_timeout` to bound that. With
    `nowait=True` a held row raises `LockNotAvailableError` immediately.

    Note what `None` does and does not mean. It is always "no such row", never
    "someone else has it": the two outcomes stay distinguishable because
    contention raises. That is also why `skip_locked` is not offered here and
    is offered by `lock_rows` — on a single row it would collapse "gone" and
    "busy" into the same `None`, and a caller cannot tell a deleted account
    from a busy one by looking at it.

    The returned instance carries what the locked read saw, overwriting an
    older copy of the same row in this session — including unflushed changes to
    it. See the module docstring: lock first, then modify.

    Args:
        session: The transaction the lock will belong to.
        model: Mapped class to load. Must have a single-column primary key.
        ident: Primary key value.
        mode: Which row lock to take. See `LockMode` — `NO_KEY_UPDATE` is
            usually the right choice when updating non-key columns of a row
            other tables reference.
        nowait: Fail instantly instead of waiting for the current holder.

    Raises:
        LockNotAvailableError: `nowait=True` and the row was already locked
            (SQLSTATE 55P03).
        ValueError: `model` has a composite primary key, which `ident` cannot
            express. Refused rather than matched on the first column, which
            would lock the wrong row and look like it worked.
    """
    primary_key = inspect(model).primary_key
    if len(primary_key) != 1:
        raise ValueError(
            f"{model.__name__} has a composite primary key "
            f"({', '.join(column.name for column in primary_key)}); "
            "use lock_rows with an explicit select() instead."
        )

    found = await _fetch_locked(
        session,
        select(model).where(primary_key[0] == ident),
        _for_update_argument(mode, nowait=nowait),
        model=model.__name__,
        ident=ident,
    )
    return found[0] if found else None


async def lock_rows[ModelT: Base](
    session: AsyncSession,
    statement: Select[tuple[ModelT]],
    *,
    mode: LockMode = LockMode.UPDATE,
    nowait: bool = False,
    skip_locked: bool = False,
) -> list[ModelT]:
    """Run `statement` with a row lock applied, returning the rows it locked.

    This is the multi-row form, and `skip_locked=True` is what it is mostly
    for: the work-queue claim, where several workers run the same
    `SELECT ... LIMIT n FOR UPDATE SKIP LOCKED` and each walks away with a
    disjoint batch instead of queueing behind the others. Rows another
    transaction holds are omitted from the result rather than waited for, so a
    short result is normal and an empty one means "nothing free right now".

    `nowait` and `skip_locked` are mutually exclusive — Postgres rejects the
    combination, and they ask for opposite things.

    A caveat SQLAlchemy inherits from Postgres: `FOR UPDATE` cannot be applied
    to the nullable side of an outer join, so a statement with `joinedload` of
    an optional relationship fails. Lock the rows first, then load what hangs
    off them.

    Args:
        session: The transaction the locks will belong to.
        statement: A `select()` over one mapped entity. Add `.limit()` and an
            `.order_by()` — locking rows in a consistent order across workers
            is what keeps two batch claims from deadlocking on each other.
        mode: Which row lock to take.
        nowait: Fail instantly if any matched row is held.
        skip_locked: Silently omit rows that are held.

    Raises:
        ValueError: Both `nowait` and `skip_locked` were requested.
        LockNotAvailableError: `nowait=True` and some row was already locked.
    """
    if nowait and skip_locked:
        raise ValueError(
            "nowait and skip_locked are mutually exclusive: one waits for "
            "nothing and fails, the other waits for nothing and continues."
        )

    return await _fetch_locked(
        session,
        statement,
        _for_update_argument(mode, nowait=nowait, skip_locked=skip_locked),
    )


async def _fetch_locked[ModelT: Base](
    session: AsyncSession,
    statement: Select[tuple[ModelT]],
    argument: dict[str, Any],
    *,
    model: str | None = None,
    ident: uuid.UUID | None = None,
) -> list[ModelT]:
    """Run `statement` under a row lock, refreshing whatever comes back.

    The one place the lock is actually taken, so `lock_row` and `lock_rows`
    cannot drift into disagreeing about `populate_existing` — which is the
    difference between reading the row you locked and reading a copy of it from
    before the lock, and is invisible in both call sites.
    """
    locked = statement.with_for_update(**argument).execution_options(
        populate_existing=True
    )
    try:
        result = await session.execute(locked)
    except SQLAlchemyError as exc:
        unavailable = _lock_unavailable_error(exc, model=model, ident=ident)
        if unavailable is None:
            raise
        raise unavailable from exc
    return list(result.scalars().all())


@asynccontextmanager
async def lock_timeout(session: AsyncSession, seconds: float) -> AsyncIterator[None]:
    """Bound how long statements inside the block will wait for a lock.

    The middle ground between waiting forever and `nowait`: a contended row is
    usually free within milliseconds, so a short wait succeeds where `nowait`
    would have failed, while the timeout still keeps a request from parking on
    a lock until the client gives up. On expiry Postgres cancels the statement
    with SQLSTATE 55P03, which `lock_row` and `lock_rows` surface as
    `LockNotAvailableError` — the same error `nowait` produces, because it is
    the same situation with a different amount of patience.

    Scoped to the transaction (`SET LOCAL`), so a rollback or commit clears it
    whatever happens here; the previous value is restored on the way out for
    the case where the caller carries on in the same transaction.

    Note that `lock_timeout` bounds *waiting for a lock* and nothing else. It
    does not cap a slow query that is not blocked, which is `statement_timeout`.

    Args:
        session: The transaction to apply the setting to.
        seconds: Maximum wait. Rounded up to whole milliseconds, which is the
            resolution Postgres stores.

    Raises:
        ValueError: `seconds` is not positive. Postgres reads `lock_timeout=0`
            as "wait forever", so passing zero would turn a timeout the caller
            asked for into its exact opposite — refused here rather than
            silently honoured.
    """
    if seconds <= 0:
        raise ValueError(
            "lock_timeout requires a positive number of seconds; "
            "Postgres reads 0 as 'wait indefinitely'."
        )

    milliseconds = max(1, -(-int(seconds * 1_000_000) // 1000))
    previous = (await session.execute(_GET_LOCK_TIMEOUT)).scalar_one()
    await session.execute(_SET_LOCK_TIMEOUT, {"value": f"{milliseconds}ms"})
    try:
        yield
    finally:
        # Best-effort by necessity. The common way out of this block is a lock
        # timeout, which leaves the transaction aborted, so the restore would
        # itself fail with 25P02 and — raising from a `finally` — would replace
        # the `LockNotAvailableError` the caller needs to see with a confusing
        # one about a setting. Losing the restore costs nothing: the rollback
        # that has to follow an aborted transaction clears `SET LOCAL` anyway.
        try:
            await session.execute(_SET_LOCK_TIMEOUT, {"value": previous})
        except SQLAlchemyError as exc:
            logger.debug("locking.lock_timeout_restore_failed", error=str(exc))


def _lock_unavailable_error(
    exc: SQLAlchemyError, *, model: str | None = None, ident: uuid.UUID | None = None
) -> LockNotAvailableError | None:
    """`LockNotAvailableError` for a 55P03, `None` for anything else.

    Returning `None` rather than the original exception keeps the call site's
    re-raise a bare `raise`, so a deadlock — which the retry loop upstream must
    see unchanged — propagates as itself rather than as a copy chained to
    itself.
    """
    if not is_lock_unavailable(exc):
        return None

    context: Mapping[str, str] = {
        key: value
        for key, value in (("model", model), ("row_id", str(ident) if ident else None))
        if value is not None
    }
    logger.info("locking.lock_unavailable", **context)
    return LockNotAvailableError(
        "The row is locked by another transaction and could not be acquired "
        "without waiting"
    )
