"""The consumer half against Postgres: claiming rows, and finishing with them.

`SELECT ... WHERE available_at <= now() ORDER BY available_at, id LIMIT n FOR
UPDATE SKIP LOCKED` is the whole design in one statement, and each clause is
load-bearing:

**`FOR UPDATE SKIP LOCKED`** is what lets more than one relay run. Each claimer
walks away with a disjoint batch instead of queueing behind the others, and a
relay that dies mid-batch releases its rows the moment its transaction is rolled
back — no lease to expire, no reaper to write. This is the pattern
`src/locking/rows.py` was built for and named this module as the first consumer
of; `lock_rows(..., skip_locked=True)` is that code, not a second copy of it.

**A distributed lock is deliberately not used here**, even though
`src/distributed_lock` exists and `docs/distributed-locking.md` guessed this
would be its first consumer. It would make the relay a singleton — the
opposite of what SKIP LOCKED buys — to solve a problem the database has already
solved with the row locks it was going to take anyway. Fencing tokens answer
"the resource cannot tell whether its writer still holds the lease"; a row
being deleted inside the transaction that locked it does not have that problem.

**The transaction is the claim.** A row lock lives until COMMIT or ROLLBACK,
so the claim, the delivery and its outcome all belong to one transaction —
which is why the unit here is `OutboxBatch` inside a `BatchScope` rather than a
store with methods that each commit. The cost is that a slow subscriber holds
a transaction open, which is why the relay bounds every dispatch with a timeout
and claims in modest batches.

**The clock is the database's, everywhere.** `available_at` is written by
`now()` on insert and by `now() + interval` on every reschedule, and compared
against `now()` on claim. A relay host whose clock has drifted five minutes
would otherwise claim rows early or park them for an extra five, and clock
drift is not a thing that announces itself.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta

import structlog
from sqlalchemy import Interval, delete, func, literal, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.immutable import FrozenDict
from src.locking import LockMode, lock_rows
from src.models.outbox import LAST_ERROR_MAX_LENGTH, OutboxEvent
from src.outbox.base import BatchScope, OutboxBatch, PendingEvent

logger = structlog.get_logger(__name__)


class SqlAlchemyOutboxBatch:
    """One relay transaction's view of the outbox table.

    Bound to a session, and only valid for as long as that session's
    transaction is: `complete` and `fail` address rows this batch has locked,
    and after a commit or rollback it holds nothing.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def claim(self, *, limit: int) -> tuple[PendingEvent, ...]:
        """Lock up to `limit` ready rows and return them detached.

        `LockMode.UPDATE` rather than the usually-preferable `NO_KEY_UPDATE`:
        the reason to prefer the weaker mode is that `FOR UPDATE` also blocks
        inserts into tables that reference the locked row, and nothing
        references this one. The batch deletes what it delivers, so the
        stronger lock is what it needs anyway.

        Raises:
            ValueError: `limit` is not positive. A claim of zero rows is a
                caller bug that would otherwise look like an empty queue.
        """
        if limit < 1:
            raise ValueError(f"limit must be at least 1, got {limit}.")

        ready = (
            select(OutboxEvent)
            .where(OutboxEvent.available_at <= func.now())
            .order_by(OutboxEvent.available_at, OutboxEvent.id)
            .limit(limit)
        )
        rows = await lock_rows(
            self._session, ready, mode=LockMode.UPDATE, skip_locked=True
        )
        return tuple(_as_pending(row) for row in rows)

    async def complete(self, entry: PendingEvent) -> None:
        """Delete a delivered row.

        The delete is by primary key rather than through the ORM instance so
        that this method takes the same value object every other method here
        takes, and so nothing depends on the instance still being in the
        session's identity map.
        """
        await self._session.execute(
            delete(OutboxEvent).where(OutboxEvent.id == entry.id)
        )

    async def fail(self, entry: PendingEvent, *, error: str, retry_in: float) -> None:
        """Count the failed attempt and push the row's next attempt out.

        `attempts` is incremented in SQL rather than from the value this batch
        read: a read-modify-write would be correct here, since the row is
        locked, and wrong the first time someone touches the table from
        anywhere else.
        """
        await self._session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == entry.id)
            .values(
                attempts=OutboxEvent.attempts + 1,
                last_error=error[:LAST_ERROR_MAX_LENGTH],
                available_at=func.now()
                + literal(timedelta(seconds=retry_in), Interval()),
            )
        )


def _as_pending(row: OutboxEvent) -> PendingEvent:
    """Copy a locked row into a value object the relay can outlive it with."""
    return PendingEvent(
        id=row.id,
        event_id=row.event_id,
        event_name=row.event_name,
        payload=FrozenDict(row.payload),
        occurred_at=row.occurred_at,
        attempts=row.attempts,
    )


def session_batches(sessions: async_sessionmaker[AsyncSession]) -> BatchScope:
    """A `BatchScope` that opens one session — and one transaction — per batch.

    Commit on a clean exit, rollback on anything else, including cancellation.
    Rolling back is the safe direction by construction: it releases the row
    locks and leaves every claimed row exactly as it was, so the batch is
    simply redelivered. That is what makes shutting the relay down mid-batch a
    non-event, and it is the reason delivery is at-least-once rather than
    exactly-once — a crash between the dispatch and the commit repeats the
    dispatch.
    """

    @asynccontextmanager
    async def scope() -> AsyncIterator[OutboxBatch]:
        async with sessions() as session:
            batch = SqlAlchemyOutboxBatch(session)
            try:
                yield batch
            except BaseException:
                # `BaseException` so that a cancelled relay rolls back rather
                # than leaving the session to be closed by the garbage
                # collector with a transaction still open on the connection.
                await session.rollback()
                raise
            await session.commit()

    return scope


__all__ = ["SqlAlchemyOutboxBatch", "session_batches"]
