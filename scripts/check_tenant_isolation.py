#!/usr/bin/env python
"""Ask a live database whether tenant isolation is actually in force.

The thing worth checking is not the migration — that either ran or it did
not — but the *role the application connects as*, because every way of
getting this wrong leaves a database whose schema is perfect and whose
policies apply to nobody. A superuser bypasses them. A role with `BYPASSRLS`
bypasses them. A role that happens to own the tables bypasses them unless the
tables were given `FORCE ROW LEVEL SECURITY`, which is the default outcome of
running the migrations and the application as the same user.

So run this **with the application's own `DATABASE_URL`**, in the environment
you are asking about. Run it from CI against staging, and run it after any
change to roles or grants. It reads the catalog and nothing else: no rows are
read, nothing is written, and it is safe against production.

Usage::

    uv run python scripts/check_tenant_isolation.py
    DATABASE_URL=postgresql+asyncpg://app:…@host/db \\
        uv run python scripts/check_tenant_isolation.py

Exit status is 0 when the boundary holds for this role and 1 when it does
not, so it can be a deployment gate rather than something somebody reads.
`docs/multi-tenancy.md` has the fix for each message it prints.
"""

from __future__ import annotations

import asyncio
import sys

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from src.config import settings
from src.health.checks import redact_url
from src.tenancy.isolation import IsolationReport, inspect_isolation


async def check(database_url: str) -> IsolationReport:
    """Connect, read the catalog, disconnect.

    `NullPool` because this process makes exactly one connection and exits;
    a pool would hold a second idle socket open for nothing.
    """
    engine = create_async_engine(database_url, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            return await inspect_isolation(connection)
    finally:
        await engine.dispose()


def render(report: IsolationReport, database_url: str) -> str:
    """The operator-facing summary. The URL is redacted; it carries a password."""
    lines = [f"database: {redact_url(database_url)}", f"role:     {report.role}"]
    problems = report.problems
    if not problems:
        lines.append("")
        lines.append(
            f"OK — row-level security is enforced for {report.role!r} on "
            f"{len(report.tables)} tenant-scoped table(s)."
        )
        return "\n".join(lines)
    lines.append("")
    lines.append(f"NOT ENFORCED — {len(problems)} problem(s):")
    lines += [f"  - {problem}" for problem in problems]
    lines.append("")
    lines.append("See docs/multi-tenancy.md § Roles for what each of these means.")
    return "\n".join(lines)


def main() -> int:
    report = asyncio.run(check(settings.DATABASE_URL))
    print(render(report, settings.DATABASE_URL))
    return 0 if report.enforced else 1


if __name__ == "__main__":
    sys.exit(main())
