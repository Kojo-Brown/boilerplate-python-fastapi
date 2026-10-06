"""Does this database actually enforce the boundary, for *this* role?

Row-level security has a failure mode with no symptom: it is configured
correctly, and it does nothing. Three ways to arrive there, all of them
ordinary —

* the application connects as a **superuser**, which bypasses every policy;
* the role has **`BYPASSRLS`**, which is the same thing wearing a smaller hat;
* the application is the **table owner** and the table was not given
  `FORCE ROW LEVEL SECURITY`, because an owner is exempt from its own
  policies by default.

The last one is the trap, because it is what you get by running the migrations
and the application as the same user, which is what almost every deployment
and every tutorial does. Nothing errors. Queries return rows. Every test that
asserts a tenant can see its own data passes. The only test that fails is the
one nobody wrote, where another tenant's data is also visible.

So this module asks the database, rather than the configuration, whether the
boundary exists. `scripts/check_tenant_isolation.py` runs it against a
deployment; `tests/test_tenancy_db.py` runs it against CI's.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from sqlalchemy import text

from src.tenancy.sql import CURRENT_TENANT_FUNCTION, TENANT_SCOPED_TABLES


class SupportsExecute(Protocol):
    """The one method this module needs from a connection or a session."""

    async def execute(self, statement: Any, parameters: Any = ...) -> Any: ...


@dataclass(frozen=True, slots=True)
class TableIsolation:
    """What the catalog says about one tenant-scoped table."""

    table: str
    exists: bool
    row_security_enabled: bool
    row_security_forced: bool
    policy_count: int

    @property
    def problems(self) -> list[str]:
        if not self.exists:
            return [f"table {self.table!r} does not exist"]
        found = []
        if not self.row_security_enabled:
            found.append(f"{self.table}: row-level security is not enabled")
        if not self.row_security_forced:
            # Without FORCE, the owner — which is whoever ran the migrations,
            # and very often the application too — reads and writes every row.
            found.append(f"{self.table}: row-level security is not FORCEd")
        if self.policy_count == 0:
            # RLS with no policy denies everything, which is safe but is not
            # what anyone deployed on purpose; it means a policy was dropped.
            found.append(f"{self.table}: no policy is defined")
        return found


@dataclass(frozen=True, slots=True)
class IsolationReport:
    """Whether the connected role is actually subject to the boundary."""

    role: str
    is_superuser: bool
    bypasses_rls: bool
    function_exists: bool
    tables: tuple[TableIsolation, ...] = field(default=())

    @property
    def problems(self) -> list[str]:
        found = []
        if self.is_superuser:
            found.append(f"role {self.role!r} is a superuser and bypasses every policy")
        if self.bypasses_rls:
            found.append(f"role {self.role!r} has BYPASSRLS")
        if not self.function_exists:
            found.append(f"function {CURRENT_TENANT_FUNCTION}() is missing")
        for table in self.tables:
            found.extend(table.problems)
        return found

    @property
    def enforced(self) -> bool:
        """True when a query from this role is subject to the policies."""
        return not self.problems


_ROLE = text("""
    SELECT current_user AS role, rolsuper, rolbypassrls
    FROM pg_roles WHERE rolname = current_user
""")

_FUNCTION = text("""
    SELECT EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE p.proname = :name AND pg_function_is_visible(p.oid)
    )
""")

# `relforcerowsecurity` is the column the owner exemption turns on, and it is
# the one a `\d` in psql does not show you.
_TABLES = text("""
    SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity,
           (SELECT count(*) FROM pg_policy p WHERE p.polrelid = c.oid) AS policies
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = ANY(:names) AND n.nspname = current_schema()
""")


async def inspect_isolation(
    connection: SupportsExecute,
    *,
    tables: tuple[str, ...] = TENANT_SCOPED_TABLES,
) -> IsolationReport:
    """Read the catalog and report whether isolation holds for this role."""
    role_row = (await connection.execute(_ROLE)).one()
    function_result = await connection.execute(
        _FUNCTION, {"name": CURRENT_TENANT_FUNCTION}
    )
    function_exists = bool(function_result.scalar())

    found = {
        row.relname: row
        for row in (await connection.execute(_TABLES, {"names": list(tables)})).all()
    }
    reported = tuple(
        TableIsolation(
            table=name,
            exists=name in found,
            row_security_enabled=bool(
                found[name].relrowsecurity if name in found else False
            ),
            row_security_forced=bool(
                found[name].relforcerowsecurity if name in found else False
            ),
            policy_count=int(found[name].policies if name in found else 0),
        )
        for name in tables
    )

    return IsolationReport(
        role=str(role_row.role),
        is_superuser=bool(role_row.rolsuper),
        bypasses_rls=bool(role_row.rolbypassrls),
        function_exists=function_exists,
        tables=reported,
    )


__all__ = [
    "IsolationReport",
    "SupportsExecute",
    "TableIsolation",
    "inspect_isolation",
]
