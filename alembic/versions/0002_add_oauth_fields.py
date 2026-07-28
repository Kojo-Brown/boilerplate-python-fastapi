"""add oauth fields to users

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-08

"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("users", sa.Column("oauth_provider", sa.String(50), nullable=True))
    op.add_column("users", sa.Column("oauth_sub", sa.String(255), nullable=True))
    op.create_index("ix_users_oauth_sub", "users", ["oauth_sub"])


def downgrade() -> None:
    op.drop_index("ix_users_oauth_sub", table_name="users")
    op.drop_column("users", "oauth_sub")
    op.drop_column("users", "oauth_provider")
