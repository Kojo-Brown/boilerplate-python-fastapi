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
)
