"""add optimistic-concurrency version counter to users

Revision ID: 0004
Revises: 0003
Create Date: 2026-08-14

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # NOT NULL on a table that may already have rows needs a value for them,
    # which is what server_default supplies. It is kept afterwards rather than
    # dropped: the ORM populates the counter itself on every INSERT it emits,
    # but a data-fix INSERT run from psql does not, and a NULL version would
    # make the next ORM update of that row fail rather than conflict.
    #
    # Starting existing rows at 1 is safe even though some of them have been
    # updated many times. The counter is not a history — it only has to change
    # when the row changes from here on, and no client can be holding an ETag
    # for a column that did not exist a moment ago.
    op.add_column(
        "users",
        sa.Column("version", sa.Integer(), nullable=False, server_default="1"),
    )


def downgrade() -> None:
    op.drop_column("users", "version")
