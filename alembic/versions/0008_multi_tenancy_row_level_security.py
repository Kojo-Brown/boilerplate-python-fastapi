"""multi-tenancy with row-level security

Revision ID: 0008
Revises: 0007
Create Date: 2026-10-02

The order of the steps below is the whole migration, because `FORCE ROW LEVEL
SECURITY` applies to the role running it. Every row has to be written while
the table is still unprotected; the moment the policies go on, this
connection — the owner, with no tenant bound — can no longer touch `users` or
`refresh_tokens` at all. So: columns, backfill, constraints, and only then
the boundary.

The same applies to every migration written after this one. A later data
migration against a tenant-scoped table has to either bind a tenant with
`set_config`, or run as a role with `BYPASSRLS`, or lift and restore `FORCE`
around itself. `docs/multi-tenancy.md` has the three recipes and when each is
right; the short answer is to give the migration role `BYPASSRLS` and leave
the application role without it.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op
from src.tenancy.sql import (
    CREATE_CURRENT_TENANT_FUNCTION,
    DROP_CURRENT_TENANT_FUNCTION,
)

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: Duplicated from `src.models.tenant.DEFAULT_TENANT_ID` rather than imported,
#: on the same principle as the context literal in 0007: this migration must
#: keep backfilling the rows it backfilled in October 2026 even if the
#: application's constant is ever deliberately re-pointed. The two are asserted
#: equal in `tests/test_tenancy_db.py`, so they cannot drift by accident.
DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001"

#: `(table, policy name, the expression a row must satisfy)`. One policy per
#: table covering all four commands, used for both `USING` (which rows this
#: statement may see) and `WITH CHECK` (which rows it may leave behind).
#:
#: Both halves are needed and they are not the same check. `USING` alone would
#: let a bound connection `INSERT` a row belonging to another tenant — it is
#: not reading anything, so there is nothing to filter — and then never see it
#: again. `WITH CHECK` alone would hide nothing. Postgres defaults `WITH CHECK`
#: to the `USING` expression when it is omitted, but writing it out is worth
#: the line: the default is easy to lose the next time somebody edits one half.
POLICIES: tuple[tuple[str, str, str], ...] = (
    ("tenants", "tenants_tenant_isolation", "id = app_current_tenant_id()"),
    ("users", "users_tenant_isolation", "tenant_id = app_current_tenant_id()"),
    (
        "refresh_tokens",
        "refresh_tokens_tenant_isolation",
        "tenant_id = app_current_tenant_id()",
    ),
)


def upgrade() -> None:
    op.create_table(
        "tenants",
        sa.Column("id", sa.UUID(as_uuid=True), primary_key=True, nullable=False),
        sa.Column("slug", sa.String(63), nullable=False, unique=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column(
            "is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )

    # Before the columns that default to it.
    op.execute(CREATE_CURRENT_TENANT_FUNCTION)

    # The tenant every pre-existing row joins. Inserted unconditionally rather
    # than only when `users` is non-empty: a fresh database gets it too, so
    # that `.env.example`'s `X-Tenant-ID` and the test fixtures name something
    # that exists on every deployment rather than only on upgraded ones.
    op.execute(
        sa.text(
            "INSERT INTO tenants (id, slug, name) VALUES (CAST(:id AS uuid),"
            " 'default', 'Default tenant') ON CONFLICT (id) DO NOTHING"
        ).bindparams(id=DEFAULT_TENANT_ID)
    )

    for table in ("users", "refresh_tokens"):
        # Added nullable, backfilled, then tightened. Adding it NOT NULL with
        # the function as its default would evaluate the function once for the
        # whole table — on a connection with no tenant bound — and fail on the
        # first existing row.
        op.add_column(table, sa.Column("tenant_id", sa.UUID(as_uuid=True)))
        op.execute(
            sa.text(f"UPDATE {table} SET tenant_id = CAST(:id AS uuid)").bindparams(
                id=DEFAULT_TENANT_ID
            )
        )
        op.alter_column(
            table,
            "tenant_id",
            nullable=False,
            server_default=sa.text("app_current_tenant_id()"),
        )
        op.create_foreign_key(
            f"fk_{table}_tenant_id_tenants",
            table,
            "tenants",
            ["tenant_id"],
            ["id"],
            ondelete="CASCADE",
        )
        op.create_index(f"ix_{table}_tenant_id", table, ["tenant_id"])

    # Email becomes unique per tenant. The replacement index is created before
    # the old one is dropped so the table is never momentarily without a
    # uniqueness guarantee on the column, which is the window a concurrent
    # insert would use.
    op.create_unique_constraint(
        "uq_users_tenant_email", "users", ["tenant_id", "email"]
    )
    op.drop_index("ix_users_email", table_name="users")

    # Last. Everything above writes rows; nothing below this line can.
    for table, policy, predicate in POLICIES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        # Without FORCE the owner is exempt from its own policies, which in
        # most deployments means the application is exempt. See
        # `src/tenancy/isolation.py`.
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        op.execute(
            f"CREATE POLICY {policy} ON {table}"
            f" USING ({predicate}) WITH CHECK ({predicate})"
        )


def downgrade() -> None:
    # Policies first, for the mirror image of the reason they went on last:
    # the statements below write to `users`, and this connection cannot do
    # that while FORCE is in effect.
    for table, policy, _ in reversed(POLICIES):
        op.execute(f"DROP POLICY IF EXISTS {policy} ON {table}")
        op.execute(f"ALTER TABLE {table} NO FORCE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")

    # Restoring a *globally* unique email can fail, and that is correct rather
    # than unfortunate: if two tenants have both registered the same address,
    # the single-tenant schema this reverts to has no way to hold both rows.
    # Failing here leaves the database consistent and tells an operator the
    # one thing they have to decide; silently dropping a row would not.
    op.create_index("ix_users_email", "users", ["email"], unique=True)
    op.drop_constraint("uq_users_tenant_email", "users", type_="unique")

    for table in ("refresh_tokens", "users"):
        op.drop_index(f"ix_{table}_tenant_id", table_name=table)
        op.drop_constraint(f"fk_{table}_tenant_id_tenants", table, type_="foreignkey")
        op.drop_column(table, "tenant_id")

    op.drop_table("tenants")
    op.execute(DROP_CURRENT_TENANT_FUNCTION)
