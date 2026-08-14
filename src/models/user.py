import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import UUID, Boolean, DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.database import Base

if TYPE_CHECKING:
    from src.models.refresh_token import RefreshToken


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    email: Mapped[str] = mapped_column(
        String(255), unique=True, index=True, nullable=False
    )
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
    notification_webhook_url: Mapped[str | None] = mapped_column(
        String(2048), nullable=True
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
