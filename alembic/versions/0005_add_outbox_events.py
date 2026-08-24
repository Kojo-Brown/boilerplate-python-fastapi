"""add the transactional outbox table

Revision ID: 0005
Revises: 0004
Create Date: 2026-08-24

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_id", sa.String(length=64), nullable=False),
        sa.Column("event_name", sa.String(length=255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        # server_default rather than a Python default: the relay compares this
        # against the database's clock, so the database has to be what sets it.
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_error", sa.String(length=1000), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    # Serves the relay's only query — the ready-rows filter and its ordering —
    # in one index scan. See src/models/outbox.py.
    op.create_index(
        "ix_outbox_events_available_at_id",
        "outbox_events",
        ["available_at", "id"],
    )


def downgrade() -> None:
    # Dropping this table discards events that committed but had not yet been
    # relayed. That is a data loss, not a schema change: drain the outbox
    # (stop the writers, let the relay empty the table) before downgrading.
    op.drop_index("ix_outbox_events_available_at_id", table_name="outbox_events")
    op.drop_table("outbox_events")
