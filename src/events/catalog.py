"""The events this application publishes.

Keeping them in one module is what lets a reader answer "what can I subscribe
to?" without grepping for `publish`. Each is named in the past tense because
that is what an event is: a record that something happened, not a request that
something should.

The fields are the ones a subscriber can act on without a database — an id to
correlate with, an address to reach. Anything heavier is deliberately absent:
by the time subscribers run the transaction has committed and the session is
closed, so a handler that needs the full row should load it itself, in its own
session, and accept that the row may have moved on since.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Final

from src.events.base import DomainEvent


@dataclass(frozen=True, kw_only=True)
class UserEvent(DomainEvent):
    """Base for anything that happened to a user account.

    It exists so that a subscriber can observe *all* user activity — an audit
    trail, a metrics tap — with one registration that keeps working when a new
    user event is added.
    """

    event_name: ClassVar[str] = "user.event"

    user_id: str
    email: str


@dataclass(frozen=True, kw_only=True)
class UserRegistered(UserEvent):
    """A new account exists and its row is committed."""

    event_name: ClassVar[str] = "user.registered"

    #: How the account came into being: `"password"` for a normal
    #: registration, `"oauth"` for one created from a provider callback. A
    #: welcome email is worth sending either way; an "confirm your address"
    #: email is not, since OAuth already verified it.
    via: str = "password"


@dataclass(frozen=True, kw_only=True)
class UserLoggedIn(UserEvent):
    """Credentials were accepted and tokens were issued.

    Published for the initial authentication only, not for a refresh: a token
    rotation is the same session continuing, and treating it as a login would
    make "last seen" mean "last polled".
    """

    event_name: ClassVar[str] = "user.logged_in"

    #: `"password"` or `"oauth"`. Kept as a plain string rather than an enum
    #: for the same reason `users.notification_channel` is: adding a method
    #: should not become a migration.
    method: str = "password"


@dataclass(frozen=True, kw_only=True)
class RefreshTokenReuseDetected(DomainEvent):
    """An already-used refresh token was presented, and its family was killed.

    Rotation makes a refresh token single-use, so a used one coming back means
    two parties hold the same credential and the server cannot tell which of
    them is the account owner — see `docs/refresh-token-reuse.md`. Every live
    session in the family is ended before the request is refused, and this is
    the record of that having happened.

    Published *before* the 401 rather than after, because there is no after:
    the request ends in an exception, and a subscriber that needs to reach the
    account owner ("you have been signed out everywhere, and here is why")
    would otherwise be reacting to a response nobody returns. It rides the
    outbox with the revocation, so the alert and the revocation commit together
    or not at all.

    **Not a `UserEvent`, deliberately**, though it names a user. That base
    requires `email`, and `AuthService._handle_reuse` does not have one: it has
    a token row. Inheriting would mean loading the account to fill a field —
    a database round trip added to the one path whose rate an attacker chooses,
    for a value this module's own rule says does not belong in an event
    ("a handler that needs the full row should load it itself"). It is also not
    the same kind of fact: the other user events record what the user did, and
    this one records what the server did to them.
    """

    event_name: ClassVar[str] = "user.refresh_token_reuse_detected"

    #: The account whose sessions were ended. A string for the same reason
    #: every id here is one: outbox payloads carry JSON scalars only
    #: (`src/outbox/codec.py`), and a `uuid.UUID` is refused at publish time.
    user_id: str

    #: The authorization grant that was revoked — `family_id` on the rows, and
    #: the value to search the table by when reading the incident back.
    family_id: str

    #: How many *live* sessions this revocation ended. Usually 1 — rotation
    #: leaves one live link per chain — so a larger number is itself worth
    #: alerting on: it means tokens were being issued in parallel on one grant.
    #: Zero is the ordinary case for a replay against a family that was already
    #: revoked, and distinguishes "we just cut someone off" from "we refused a
    #: request against a session that had already ended".
    sessions_revoked: int


#: The event types that can be read back out of the transactional outbox.
#
# An outbox row stores `event_name` and its fields; turning that back into an
# event needs a name-to-class map, and this is it. Concrete types only —
# `UserEvent` is a base nobody publishes, and registering it would offer the
# relay a class it can never be asked for.
#
# Adding an event means adding it here. Forgetting is not a subtle bug for
# long: `tests/test_outbox_codec.py` walks this module and fails on any
# publishable event that is missing, because the alternative is discovering it
# as rows accumulating in production behind an "unknown event type" error.
EVENT_TYPES: Final[tuple[type[DomainEvent], ...]] = (
    UserRegistered,
    UserLoggedIn,
    RefreshTokenReuseDetected,
)
