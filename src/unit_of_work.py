"""The transaction boundary, as narrow as the policy that uses it.

`AuthService` needs two things from a database session: make pending work
visible to the reads that follow it (`flush`), and make it permanent
(`commit`). It was given the whole `AsyncSession` — `execute`, `scalars`,
`merge`, `get_bind`, the connection — to reach those two, which is the
interface-segregation complaint in `docs/solid.md` finding 6, and the reason
every service test had to build a session stub faithful enough to survive
SQLAlchemy rather than an object with two methods on it.

`UnitOfWork` is that pair. It deliberately declares nothing else — no
`rollback`, because nothing in the service calls one: `get_db` closes the
session without committing, so an uncommitted transaction is already discarded,
and a protocol method no caller uses is a method every fake still has to
implement.

There is no adapter class here on purpose. `AsyncSession` already has both
methods with compatible signatures, so it satisfies this protocol structurally
and `get_unit_of_work` hands one straight over. Writing a `SqlAlchemyUnitOfWork`
wrapper would add a layer whose only job is to forward two calls.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class UnitOfWork(Protocol):
    """A transaction that can be flushed and committed.

    `runtime_checkable` buys an `isinstance` check that verifies the methods
    exist and nothing about their signatures — useful as a smoke test, not as
    the guarantee. The real check is static: `get_unit_of_work` is annotated
    `-> UnitOfWork` and returns an `AsyncSession`, so mypy rejects the day
    SQLAlchemy changes either signature.
    """

    async def flush(self) -> None:
        """Send pending changes to the database without ending the transaction.

        What it buys the caller is read-your-writes: a row created or mutated
        before the flush is visible to a query after it, within this
        transaction. An in-memory implementation where writes are already
        visible has nothing to do here, which is why the fakes in
        `tests/fakes.py` implement it as a no-op rather than as a lie.
        """
        ...

    async def commit(self) -> None:
        """Make everything done in this transaction permanent."""
        ...
