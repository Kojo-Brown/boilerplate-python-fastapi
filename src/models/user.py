import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    UUID,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.database import Base
from src.encryption import EncryptedString

# Imported for its side effect: `tenant_id` below declares a foreign key onto
# `tenants.id`, and SQLAlchemy resolves that by name against the shared
# `Base.metadata` at mapper-configuration time. Without this, a process that
# imports `src.models.user` and nothing else gets `NoReferencedTableError` on
# the first query — and `Base.metadata.create_all` silently builds a schema
# with no `tenants` table for the key to point at.
from src.models.tenant import Tenant  # noqa: F401

if TYPE_CHECKING:
    from src.models.refresh_token import RefreshToken


class User(Base):
    __tablename__ = "users"

    #: Email is unique *within a tenant*, not globally, and the difference is
    #: not a nicety. A global unique index means the first customer to sign up
    #: as `ops@acme.example` makes that address unusable for every other
    #: customer — and worse, it says so: the insert fails with a unique
    #: violation naming a row the caller cannot see under the policy, which
    #: turns the index into an oracle for "does this person have an account
    #: with one of your other customers". Scoping the constraint removes both.
    #:
    #: It replaces the single-column index rather than sitting beside it.
    #: Every read of this table runs under the policy, which contributes
    #: `tenant_id = app_current_tenant_id()` to the query as an ordinary qual,
    #: so a lookup by email has the leading column of this index available and
    #: a second index on `email` alone would be maintained on every write and
    #: chosen by nothing.
    __table_args__ = (
        UniqueConstraint("tenant_id", "email", name="uq_users_tenant_email"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    #: The tenant this user belongs to. The column the policy on this table
    #: tests, which makes it the one column in the schema that is load-bearing
    #: for isolation rather than for behaviour.
    #:
    #: `server_default` rather than a Python default, and the choice is what
    #: lets the rest of the codebase stay unaware of tenancy: an `INSERT` that
    #: never mentions `tenant_id` gets the connection's bound tenant, so
    #: `AuthService.register` did not have to change. The `WITH CHECK` half of
    #: the policy then guarantees the value is the bound tenant whether the
    #: default supplied it or a caller did, so this is a convenience and not
    #: the enforcement.
    #:
    #: `NOT NULL` with that default has a consequence worth knowing before you
    #: meet it: an insert on an *unbound* connection fails with a not-null
    #: violation rather than writing an orphan row. That is the intended
    #: behaviour — see `docs/multi-tenancy.md` — but it is also why every
    #: fixture in `tests/` runs inside `tenant_scope`.
    #:
    #: `ON DELETE CASCADE`: deleting a tenant deletes its users, which is what
    #: "offboard this customer" has to mean. Note that the cascade is a
    #: *database* delete and does not run SQLAlchemy's ORM cascade, so the
    #: refresh tokens go with them by their own `ON DELETE CASCADE` rather
    #: than by the relationship.
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        server_default=text("app_current_tenant_id()"),
        index=True,
    )
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    hashed_password: Mapped[str | None] = mapped_column(String(255), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_verified: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    role: Mapped[str] = mapped_column(String(50), nullable=False, default="user")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    oauth_provider: Mapped[str | None] = mapped_column(String(50), nullable=True)
    oauth_sub: Mapped[str | None] = mapped_column(
        String(255), nullable=True, index=True
    )

    # Which notification strategy reaches this user. Deliberately a plain string
    # rather than a native enum: adding a channel is a registration call in
    # src/notifications/registry.py, and an enum type would turn that into a
    # migration and a deploy-order problem. src.notifications resolves an
    # unknown value to UnknownNotificationChannelError rather than guessing.
    notification_channel: Mapped[str] = mapped_column(
        String(20), nullable=False, default="email", server_default="email"
    )
    # Encrypted at rest (see src/encryption/, docs/field-encryption.md). A
    # user-supplied webhook URL is a credential in practice — Slack, Discord
    # and most incident tools put the shared secret in the path — so a database
    # dump, a stray replica or a restored backup hands over the ability to post
    # into somebody's channel. That is the one column in this table whose value
    # is directly usable by whoever reads it, and it is never filtered or
    # sorted on: `src/notifications/recipients.py` loads the row and reads the
    # attribute, which is the access pattern encryption costs nothing on.
    #
    # `String(2048)` is gone with the plaintext, so the length ceiling is now
    # enforced only by `ProfileUpdateRequest` at the edge. That is where it was
    # doing the work anyway — the database limit produced a 500 rather than a
    # 422 — but it does mean a writer that bypasses the schema has no backstop.
    #
    # The literal below is the column's durable identity, bound into every
    # value's authentication tag. It must not be changed to follow a rename:
    # see the module docstring in src/encryption/types.py.
    notification_webhook_url: Mapped[str | None] = mapped_column(
        EncryptedString("users.notification_webhook_url"), nullable=True
    )

    # Optimistic concurrency. SQLAlchemy owns this counter: it sets it to 1 on
    # INSERT, appends `AND version = :current` to every UPDATE and DELETE the
    # ORM emits for this row, and raises `StaleDataError` when that matches no
    # rows — the case where someone else got there first.
    #
    # This is what makes the `If-Match` check on `/api/v1/users/me` more than
    # decorative. Comparing the client's tag against the row we just read
    # leaves a window between the read and the write; the version in the WHERE
    # clause closes it, because the database, not the application, decides who
    # wins. See `docs/optimistic-concurrency.md`.
    #
    # `server_default` is for rows that predate the column and for writes that
    # bypass the ORM; the ORM never reaches it, since versioning populates the
    # value itself. There is deliberately no Python-side `default=`: it would
    # be dead code that reads as though it were the source of the counter.
    version: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")

    refresh_tokens: Mapped[list["RefreshToken"]] = relationship(
        back_populates="user", cascade="all, delete-orphan"
    )

    __mapper_args__ = {"version_id_col": version}
