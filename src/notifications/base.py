"""Channel-agnostic notification contract.

Nothing here imports SQLAlchemy, httpx or the settings object, so a strategy
can be written against this module without inheriting any of the three. The
concrete strategies live in `email.py`, `webhook.py` and `null.py`;
`registry.py` chooses between them from a recipient's stored preference.

The recipient is a `Recipient` value object rather than the `User` model on
purpose: a strategy that took the ORM row would drag a database session into
every delivery path and make "send this to that address" untestable without a
table. `recipients.py` maps one to the other.
"""

from __future__ import annotations

import ipaddress
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Final, Protocol, runtime_checkable
from urllib.parse import urlsplit

from src.exceptions import AppException, BadRequestError, UnprocessableEntityError

MAX_SUBJECT_LENGTH: Final[int] = 200

MAX_WEBHOOK_URL_LENGTH: Final[int] = 2048

# Hostnames that name the machine the app runs on rather than a third party.
# Delivering a user-supplied webhook to one of these is the classic SSRF shape,
# so they are refused unless the deployment opts in (see `allow_private_hosts`).
_LOOPBACK_HOSTNAMES: Final[frozenset[str]] = frozenset(
    {"localhost", "localhost.localdomain", "ip6-localhost", "ip6-loopback"}
)


class NotificationError(AppException):
    status_code = 500
    error_code = "NOTIFICATION_ERROR"

    def __init__(
        self, message: str = "Notification failed", details: object = None
    ) -> None:
        super().__init__(message, details)


class UnknownNotificationChannelError(NotificationError):
    """Raised when a recipient's channel has no registered strategy."""

    error_code = "UNKNOWN_NOTIFICATION_CHANNEL"

    def __init__(self, channel: str, available: tuple[str, ...]) -> None:
        super().__init__(
            f"Unknown notification channel '{channel}'.",
            details={"requested": channel, "available": list(available)},
        )


class RecipientNotReachableError(UnprocessableEntityError):
    """Raised when the recipient lacks the address their channel requires.

    A 422 rather than a 500: the request was understood and the strategy is
    working correctly — this particular user simply has no address on the
    channel they chose, which is a data problem, not a fault.
    """

    error_code = "RECIPIENT_NOT_REACHABLE"

    def __init__(self, channel: str, reason: str) -> None:
        super().__init__(
            f"Recipient is not reachable on channel '{channel}': {reason}",
            details={"channel": channel, "reason": reason},
        )


class NotificationDeliveryError(NotificationError):
    """Raised when the channel was usable but delivery failed anyway.

    502 rather than 500 because the failure is downstream — an SMTP host that
    timed out, a webhook endpoint that returned 503 — not a defect in this
    process. Retries, where a strategy performs them, are already exhausted by
    the time this is raised.
    """

    status_code = 502
    error_code = "NOTIFICATION_DELIVERY_FAILED"

    def __init__(self, channel: str, reason: str, attempts: int = 1) -> None:
        super().__init__(
            f"Delivery over '{channel}' failed after {attempts} attempt(s): {reason}",
            details={"channel": channel, "reason": reason, "attempts": attempts},
        )


@dataclass(frozen=True, slots=True)
class Notification:
    """What is being sent, independent of how it will be sent.

    Deliberately free of channel-specific fields. `html_body` is an *optional
    richer rendering* of `body`, not an alternative to it — a strategy that
    cannot render HTML (a webhook, an SMS gateway) still has something to send.
    """

    subject: str
    body: str
    category: str = "general"
    html_body: str | None = None
    metadata: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.subject.strip():
            raise BadRequestError("Notification subject must not be empty.")
        if len(self.subject) > MAX_SUBJECT_LENGTH:
            raise BadRequestError(
                f"Notification subject must be at most {MAX_SUBJECT_LENGTH} "
                "characters.",
                details={"length": len(self.subject)},
            )
        if not self.body.strip():
            raise BadRequestError("Notification body must not be empty.")
        if not self.category.strip():
            raise BadRequestError("Notification category must not be empty.")


@dataclass(frozen=True, slots=True)
class Recipient:
    """Who is being notified, and where they have asked to be reached.

    `channel` is the stored preference. The addresses are all optional because
    a user who has chosen one channel has no obligation to carry an address for
    the others — the strategy for the chosen channel is the only one that gets
    to insist, and it does so by raising `RecipientNotReachableError`.
    """

    id: str
    channel: str
    email: str | None = None
    webhook_url: str | None = None


@dataclass(frozen=True, slots=True)
class NotificationResult:
    """The outcome of one delivery attempt sequence.

    `delivered=False` is a *successful* outcome, not an error: it is how a
    strategy reports that it correctly decided not to send — an opted-out user
    being the reason that matters here. Failures raise instead.
    """

    channel: str
    delivered: bool
    detail: str


@runtime_checkable
class NotificationStrategy(Protocol):
    """The operations every channel supports.

    Narrow on purpose. There is no `send_bulk`, no template rendering and no
    scheduling: those belong to whatever calls this, and putting them here
    would force three implementations to fake capabilities only one of them
    has — the same reason presigned URLs are absent from `StorageBackend`.
    """

    @property
    def name(self) -> str:
        """Channel identifier, e.g. `"email"`. Matches the stored preference."""
        ...

    def supports(self, recipient: Recipient) -> bool:
        """Whether this strategy could reach `recipient` without raising.

        Lets a caller filter or fall back without provoking an exception.
        `send` re-checks; this is a query, not a guard the caller must honour.
        """
        ...

    async def send(
        self, recipient: Recipient, notification: Notification
    ) -> NotificationResult:
        """Deliver `notification`, or raise.

        Raises:
            RecipientNotReachableError: `recipient` has no address for this
                channel.
            NotificationDeliveryError: the channel was usable but delivery
                failed, with any retries already exhausted.
        """
        ...


def validate_webhook_url(url: str, *, allow_private_hosts: bool = False) -> str:
    """Return `url` unchanged if it is safe to POST to, else raise.

    A webhook target is attacker-influenced data — the user supplies it — and
    this process can reach things the user cannot, so the checks are about
    where the request would land rather than whether the URL parses:

    - HTTPS only. A notification body carries whatever the caller put in it,
      and cleartext would put that on the wire.
    - No credentials in the URL; they would end up in logs and in the
      `Location` of any redirect.
    - No loopback, private, link-local, multicast or otherwise reserved
      address literal — the metadata endpoint and internal services live there.

    Known gap, deliberately not papered over: a hostname that *resolves* to a
    private address passes this check, and re-resolving here would not close
    the hole anyway because the socket does its own lookup afterwards (DNS
    rebinding). Blocking that belongs to egress policy — an allow-list proxy or
    a network rule — not to a string check. `allow_private_hosts` exists so
    development and tests can point at a local listener; it defaults to off and
    should stay off in production.
    """
    if not url:
        raise BadRequestError("Webhook URL must not be empty.")

    if len(url) > MAX_WEBHOOK_URL_LENGTH:
        raise BadRequestError(
            f"Webhook URL must be at most {MAX_WEBHOOK_URL_LENGTH} characters.",
            details={"length": len(url)},
        )

    try:
        parts = urlsplit(url)
    except ValueError as exc:
        raise BadRequestError(
            "Webhook URL could not be parsed.", details={"url": url}
        ) from exc

    allowed_schemes = {"https", "http"} if allow_private_hosts else {"https"}
    if parts.scheme not in allowed_schemes:
        raise BadRequestError(
            "Webhook URL must use https.",
            details={"scheme": parts.scheme, "allowed": sorted(allowed_schemes)},
        )

    if parts.username or parts.password:
        raise BadRequestError("Webhook URL must not embed credentials.")

    # `urlsplit` above already rejects the malformed-authority cases that make
    # `.hostname` raise (an unclosed IPv6 bracket, for one), so by here it only
    # ever returns a string or None.
    hostname = parts.hostname
    if not hostname:
        raise BadRequestError("Webhook URL must include a host.")

    if allow_private_hosts:
        return url

    if hostname.lower() in _LOOPBACK_HOSTNAMES:
        raise BadRequestError(
            "Webhook URL must not point at the local machine.",
            details={"host": hostname},
        )

    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        # A name, not a literal. Resolution is left to the socket; see the
        # docstring for why re-resolving here would not help.
        return url

    if not address.is_global or address.is_multicast:
        raise BadRequestError(
            "Webhook URL must not point at a private or reserved address.",
            details={"host": hostname},
        )

    return url
