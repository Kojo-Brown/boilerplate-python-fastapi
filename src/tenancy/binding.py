"""Putting the tenant on the connection, once per transaction.

## Why `begin` and not "once per request"

The obvious implementation binds the tenant when the request's session is
created, and it is wrong in a way that does not show up until something
commits mid-request. `set_config(..., is_local => true)` is scoped to the
*transaction*, so a session that commits and then reads again is reading from
a second transaction with no tenant on it — which, under the policies, returns
nothing. The handler sees an empty list where a moment ago there was a row.

Binding on SQLAlchemy's `begin` event removes the question. Every transaction
on the engine, whoever opened it and however many of them a request uses, gets
the setting as its first statement. The symmetry is exact: `is_local` ends the
setting at `COMMIT`, and `begin` re-establishes it at the next `BEGIN`.

## The cost, stated plainly

One extra round trip per transaction, and only for work that has a tenant in
scope — the listener returns immediately when `current_tenant_id()` is `None`,
so health probes, the outbox relay and migrations pay nothing. It cannot be
pipelined with the first real statement, because it has to be in the same
transaction and SQLAlchemy emits statements one at a time. A deployment where
that round trip matters wants fewer, longer transactions rather than a
different mechanism here: the alternative — binding at checkout and trusting
nothing commits — is the leak described above.

## Scope of the listener

`bind_tenant_on_begin` takes a target rather than reaching for the application
engine, because the engine is not the only one that needs it: tests build
their own, and `scripts/` connect outside the app entirely. Passing the
`Engine` *class* registers it for every engine in the process, which is what
the test suite does — an engine created ad hoc in a test is exactly the one
that would otherwise write a row with no tenant and pass.
"""

from __future__ import annotations

from sqlalchemy import Engine, event, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncEngine

from src.tenancy.context import current_tenant_id
from src.tenancy.sql import SET_TENANT

_SET_TENANT_STATEMENT = text(SET_TENANT)


def apply_tenant(connection: Connection) -> None:
    """Bind the task's tenant to `connection`'s open transaction.

    A no-op when nothing is in scope. Public because the isolation checker and
    the tests drive it directly, and because a caller holding a connection of
    its own — a one-off script, a data fix — should not have to re-derive the
    statement.
    """
    tenant_id = current_tenant_id()
    if tenant_id is None:
        return
    connection.execute(_SET_TENANT_STATEMENT, {"tenant_id": str(tenant_id)})


def bind_tenant_on_begin(target: AsyncEngine | Engine | type[Engine]) -> None:
    """Make every transaction on `target` carry the task's tenant.

    Idempotent: registering the same listener twice would send the statement
    twice per transaction, and both the application engine and a test fixture
    have good reason to ask without knowing whether the other already did.

    Args:
        target: an `AsyncEngine` (its `sync_engine` is used), a sync `Engine`,
            or the `Engine` class itself to cover every engine in the process.
    """
    engine = target.sync_engine if isinstance(target, AsyncEngine) else target
    if event.contains(engine, "begin", apply_tenant):
        return
    event.listen(engine, "begin", apply_tenant)


__all__ = ["apply_tenant", "bind_tenant_on_begin"]
