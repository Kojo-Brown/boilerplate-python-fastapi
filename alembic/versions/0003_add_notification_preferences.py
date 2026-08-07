"""add notification preferences to users

Revision ID: 0003
Revises: 0002
Create Date: 2026-08-06

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # server_default is required, not cosmetic: the column is NOT NULL and the
    # table may already have rows, so Postgres needs a value for them. It is
    # kept afterwards so an INSERT from outside the ORM still gets a channel.
    op.add_column(
        "users",
        sa.Column(
            "notification_channel",
            sa.String(20),
            nullable=False,
            server_default="email",
        ),
    )
    op.add_column(
        "users",
        sa.Column("notification_webhook_url", sa.String(2048), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("users", "notification_webhook_url")
    op.drop_column("users", "notification_channel")
