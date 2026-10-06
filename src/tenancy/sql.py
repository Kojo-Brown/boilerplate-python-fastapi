"""The SQL the tenant boundary is actually made of.

Three strings and a name. They live in one module because the same definitions
are needed by three callers that would otherwise each keep their own copy:
Alembic migration `0008`, which installs them; `Base.metadata`'s `before_create`
hook, which installs them for a `create_all` that never ran a migration; and
`scripts/check_tenant_isolation.py`, which checks a live database against them.
A second copy of a policy predicate is a boundary that is correct in the
migration and wrong in the database.

## Why a function and not the `current_setting` call inline

`current_setting('app.tenant_id', true)` returns `text`, and the setting is
absent — not empty — on a connection nothing has bound. Writing the cast
inline in every policy means repeating `NULLIF(..., '')::uuid` once per table
and getting it wrong once: without the `NULLIF`, a connection that bound the
empty string raises `invalid input syntax for type uuid` from inside a policy,
which surfaces as a 500 on a read that should simply have returned nothing.

The function is `STABLE`, so the planner evaluates it once per statement
rather than once per row, and `PARALLEL SAFE`, so a policy using it does not
silently disable parallel plans on large tables. It is deliberately **not**
`LEAKPROOF`: marking it so needs superuser and would let the planner push
other quals below the policy, which is a performance win bought with exactly
the guarantee this package exists to make.
"""

from __future__ import annotations

from typing import Final

#: The `SET`-able run-time parameter the tenant travels in.
#:
#: A custom parameter in the reserved-for-applications `app.` namespace, which
#: is the only kind Postgres lets an unprivileged role set. The name is part of
#: the on-disk schema — it is baked into every policy predicate through the
#: function below — so changing it is a migration, not a rename.
TENANT_SETTING: Final[str] = "app.tenant_id"

#: The function every policy predicate calls.
CURRENT_TENANT_FUNCTION: Final[str] = "app_current_tenant_id"

CREATE_CURRENT_TENANT_FUNCTION: Final[str] = f"""
CREATE OR REPLACE FUNCTION {CURRENT_TENANT_FUNCTION}() RETURNS uuid
    LANGUAGE sql
    STABLE
    PARALLEL SAFE
AS $$
    SELECT NULLIF(current_setting('{TENANT_SETTING}', true), '')::uuid
$$
"""

DROP_CURRENT_TENANT_FUNCTION: Final[str] = (
    f"DROP FUNCTION IF EXISTS {CURRENT_TENANT_FUNCTION}()"
)

#: Bind the tenant for the duration of the *current transaction*.
#:
#: `set_config(..., is_local => true)` rather than `SET LOCAL` for two
#: reasons, and both of them are the whole point of this line.
#:
#: **It takes a parameter.** `SET LOCAL app.tenant_id = ...` is parsed before
#: parameters are bound, so the only way to write it is to interpolate the
#: value into the statement text — a string that reaches the database as SQL,
#: chosen by whoever sent the `X-Tenant-ID` header. `set_config` is an ordinary
#: function call, so the tenant travels as a bound parameter and can never be
#: anything but a value. The resolver already refuses anything that is not a
#: UUID; this makes that refusal a second line of defence rather than the only
#: one.
#:
#: **`is_local` is the difference between isolation and a leak.** A session-level
#: `SET` survives `COMMIT`, and a pooled connection is handed to the next
#: request with the previous request's tenant still on it. The window is small,
#: silent, and serves one customer another customer's rows; `true` scopes the
#: value to the transaction, so `COMMIT` or `ROLLBACK` removes it whatever the
#: application does next.
SET_TENANT: Final[str] = (
    f"SELECT set_config('{TENANT_SETTING}', CAST(:tenant_id AS text), true)"
)

#: Read back what the current transaction is bound to. Used by the isolation
#: checker and by the tests that measure the two claims above.
READ_TENANT: Final[str] = f"SELECT current_setting('{TENANT_SETTING}', true)"

#: Tables whose rows belong to exactly one tenant, and which therefore carry a
#: `tenant_id`, a policy and `FORCE ROW LEVEL SECURITY`.
#:
#: `outbox_events` is deliberately absent, and the omission is a decision
#: rather than an oversight. The relay (`src/outbox/relay.py`) drains that
#: table from a background task that belongs to no request and therefore to no
#: tenant: under a policy it would bind nothing, see nothing, and report an
#: empty queue forever while events accumulated. Per-tenant relays would be the
#: alternative, and they would turn one background loop into one per customer.
#: What the outbox carries instead is the tenant *inside* the payload, so a
#: subscriber that needs it re-enters `tenant_scope` explicitly — see
#: `docs/multi-tenancy.md`.
TENANT_SCOPED_TABLES: Final[tuple[str, ...]] = ("tenants", "users", "refresh_tokens")

__all__ = [
    "CREATE_CURRENT_TENANT_FUNCTION",
    "CURRENT_TENANT_FUNCTION",
    "DROP_CURRENT_TENANT_FUNCTION",
    "READ_TENANT",
    "SET_TENANT",
    "TENANT_SCOPED_TABLES",
    "TENANT_SETTING",
]
