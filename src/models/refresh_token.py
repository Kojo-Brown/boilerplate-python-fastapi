import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Final, Literal, get_args

from sqlalchemy import UUID, Boolean, DateTime, ForeignKey, String, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.database import Base

if TYPE_CHECKING:
    from src.models.user import User

#: Why a refresh token stopped being usable.
#
# The column exists because "this token is revoked" is not one fact but three
# with very different meanings, and the difference is the whole of the reuse
# check in `AuthService.refresh`. A `"rotated"` token is the *expected* end
# state of every successful refresh — the chain moved on — so a client holding
# one is either replaying or is not the only holder. A `"logout"` token was
# ended by the account owner. A `"reuse_detected"` token was killed as
# collateral when a sibling was replayed, which is the row an operator reading
# an incident wants to be able to find by predicate rather than by timestamp
# arithmetic.
#
# A `Literal` rather than an `Enum` so the values stay plain strings in the
# database, the event payload and the logs: the outbox codec only carries JSON
# scalars (`src/outbox/codec.py`), and a `StrEnum` would encode as a string and
# decode as one, making the published event unequal to the one that was sent.
RevocationReason = Literal["rotated", "logout", "reuse_detected"]

#: The same values at runtime, for the length check and for tests that want to
#: assert the column can hold every reason rather than restate the list.
REVOCATION_REASONS: Final[tuple[str, ...]] = get_args(RevocationReason)


class RefreshToken(Base):
    """One refresh token, and its place in the chain it was issued from.

    Rotation means a refresh token is single-use: presenting one revokes it and
    returns its successor. That leaves a chain per authorization grant — a
    login — of which exactly one link is live at a time, and it is the chain,
    not the individual token, that a theft has to be answered at. Hence
    `family_id`: shared by every token descended from one login, it is the
    handle `AuthService` revokes when a used token comes back (RFC 9700
    §4.14.2). Without it the only reachable response to a replay is refusing
    that one request, which refuses the *victim* — the attacker has already
    rotated away and holds a token nothing links to the one that was replayed.
    """

    __tablename__ = "refresh_tokens"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    token: Mapped[str] = mapped_column(
        String(1024), unique=True, index=True, nullable=False
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    #: The authorization grant this token descends from. Set to a fresh UUID by
    #: the login that opened the session and copied unchanged by every rotation
    #: after it.
    #:
    #: Indexed because the one query that reads it — revoke the family — runs
    #: on the reuse path, where a sequential scan of every refresh token ever
    #: issued is the response to an attacker's request.
    #:
    #: Not a self-referencing foreign key to the first token of the chain, even
    #: though that is what it identifies: `delete_expired` hard-deletes rows,
    #: and a family whose root has expired and been swept is still a family.
    family_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), index=True, nullable=False, default=uuid.uuid4
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    revoked: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    #: When `revoked` became true, and why. Both are NULL while a token is
    #: live, and `revoked_reason` is also NULL for rows revoked before this
    #: column existed — the migration cannot invent a reason it was never told.
    #: Read them as evidence, never as the check: `revoked` is the flag that
    #: decides whether a token works.
    revoked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    revoked_reason: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    user: Mapped["User"] = relationship(back_populates="refresh_tokens")
