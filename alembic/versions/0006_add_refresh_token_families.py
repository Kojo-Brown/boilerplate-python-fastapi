"""add family and revocation provenance to refresh tokens

Revision ID: 0006
Revises: 0005
Create Date: 2026-09-21

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # `family_id` arrives nullable, is backfilled, and only then becomes NOT
    # NULL. The one-step alternative — a `server_default` of `gen_random_uuid()`
    # — is wrong here rather than merely heavier: a column default is evaluated
    # per row, so every existing token would get a *different* family, which is
    # the same answer as the backfill below but obtained in a way that then has
    # to be kept for new INSERTs too. The application assigns families (a login
    # opens one, a rotation inherits it), and a database default that silently
    # supplies one would turn a rotation that forgot to copy its parent's
    # family into a token that quietly belongs to nothing.
    op.add_column(
        "refresh_tokens",
        sa.Column("family_id", postgresql.UUID(as_uuid=True), nullable=True),
    )

    # Every token that predates rotation-with-families is its own family. That
    # is the conservative reading: these rows have no recorded ancestry, so
    # grouping any two of them would revoke a session that was never implicated,
    # and grouping none of them costs only that a *pre-existing* token replayed
    # after this deploy revokes itself rather than a chain. Each such token is
    # single-use from its next refresh onward anyway, and the successor it
    # returns is a member of a real family.
    op.execute("UPDATE refresh_tokens SET family_id = id WHERE family_id IS NULL")

    op.alter_column("refresh_tokens", "family_id", nullable=False)
    op.create_index(
        "ix_refresh_tokens_family_id", "refresh_tokens", ["family_id"], unique=False
    )

    # Left NULL for rows already revoked: the reason is not recoverable — a
    # revoked row from before this migration may have been a logout or a
    # rotation, and there is no column that distinguishes them. Guessing would
    # put fabricated provenance in the table an incident is read from.
    op.add_column(
        "refresh_tokens",
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "refresh_tokens",
        sa.Column("revoked_reason", sa.String(length=32), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("refresh_tokens", "revoked_reason")
    op.drop_column("refresh_tokens", "revoked_at")
    op.drop_index("ix_refresh_tokens_family_id", table_name="refresh_tokens")
    op.drop_column("refresh_tokens", "family_id")
