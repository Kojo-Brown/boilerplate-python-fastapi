"""encrypt users.notification_webhook_url at rest

Revision ID: 0007
Revises: 0006
Create Date: 2026-09-23

"""

from collections.abc import Sequence
from typing import Any

import sqlalchemy as sa

from alembic import op
from src.encryption import get_field_cipher

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The same literal the mapper declares. Deliberately duplicated rather than
#: imported from `src.models.user`: this migration must keep producing the
#: bytes that were correct in September 2026 even if the model's context label
#: is ever deliberately changed, and importing the model would silently
#: re-point historical data at whatever the label says today.
CONTEXT = "users.notification_webhook_url"

TABLE = "users"
COLUMN = "notification_webhook_url"
TEMPORARY_COLUMN = "notification_webhook_url_converted"

#: Rows per UPDATE statement. It bounds the size of one parameter set, not the
#: memory this migration holds: the SELECT below is fully buffered by the
#: driver before the first conversion happens. A table large enough for that to
#: matter wants the out-of-band backfill described in docs/field-encryption.md
#: rather than a bigger number here.
BATCH_ROWS = 500


def _batched(rows: Sequence[Any], size: int) -> "list[Sequence[Any]]":
    return [rows[start : start + size] for start in range(0, len(rows), size)]


def upgrade() -> None:
    # Add, backfill, drop, rename rather than `ALTER TABLE ... USING`: the
    # conversion is not expressible in SQL, because it needs a key this
    # database does not have. The new bytes are computed here and sent back.
    #
    # The add, the drop and the rename each take an ACCESS EXCLUSIVE lock on
    # `users`, and all three are catalog-only — the add has no default to
    # materialise, and the drop merely marks the attribute dead. The backfill
    # between them takes no table-level lock at all. On a table large enough
    # for its row locks to matter, run the backfill as a separate resumable job
    # between two deploys and keep the migration to the schema changes; see
    # docs/field-encryption.md.
    op.add_column(TABLE, sa.Column(TEMPORARY_COLUMN, sa.LargeBinary(), nullable=True))

    bind = op.get_bind()
    cipher = get_field_cipher()

    update = sa.text(
        f"UPDATE {TABLE} SET {TEMPORARY_COLUMN} = :value WHERE id = :id"
    ).bindparams(sa.bindparam("value", type_=sa.LargeBinary()))

    rows = (
        bind.execute(
            sa.text(
                f"SELECT id, {COLUMN} AS value FROM {TABLE} WHERE {COLUMN} IS NOT NULL"
            )
        )
        .mappings()
        .all()
    )
    for batch in _batched(rows, BATCH_ROWS):
        bind.execute(
            update,
            [
                {
                    "id": row["id"],
                    "value": cipher.encrypt(
                        str(row["value"]).encode("utf-8"), context=CONTEXT
                    ),
                }
                for row in batch
            ],
        )

    op.drop_column(TABLE, COLUMN)
    op.alter_column(TABLE, TEMPORARY_COLUMN, new_column_name=COLUMN)


def downgrade() -> None:
    # Needs every key any surviving row was sealed under, not just the active
    # one, so a downgrade run after a key has been retired from ENCRYPTION_KEYS
    # fails with UnknownKeyError before it has dropped anything. That is the
    # right outcome: the alternative is a `users` table with a NULL where a
    # webhook URL used to be.
    op.add_column(
        TABLE, sa.Column(TEMPORARY_COLUMN, sa.String(length=2048), nullable=True)
    )

    bind = op.get_bind()
    cipher = get_field_cipher()

    update = sa.text(f"UPDATE {TABLE} SET {TEMPORARY_COLUMN} = :value WHERE id = :id")

    rows = (
        bind.execute(
            sa.text(
                f"SELECT id, {COLUMN} AS value FROM {TABLE} WHERE {COLUMN} IS NOT NULL"
            )
        )
        .mappings()
        .all()
    )
    for batch in _batched(rows, BATCH_ROWS):
        bind.execute(
            update,
            [
                {
                    "id": row["id"],
                    "value": cipher.decrypt(
                        bytes(row["value"]), context=CONTEXT
                    ).decode("utf-8"),
                }
                for row in batch
            ],
        )

    op.drop_column(TABLE, COLUMN)
    op.alter_column(TABLE, TEMPORARY_COLUMN, new_column_name=COLUMN)
