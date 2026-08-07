"""Webhook notification strategy.

POSTs a JSON document to a URL the user has registered. Everything awkward
about this channel — that the target is user-supplied, that the receiver has to
be able to tell a real delivery from a forged one, that a third-party endpoint
fails differently from a bug here — is handled in this module rather than left
to the caller.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import time
from collections.abc import Awaitable, Callable
from typing import Any, Final

import httpx
import structlog

from src.notifications.base import (
    Notification,
    NotificationDeliveryError,
    NotificationResult,
    Recipient,
    RecipientNotReachableError,
    validate_webhook_url,
)

logger = structlog.get_logger(__name__)

SIGNATURE_HEADER: Final[str] = "X-Notification-Signature"
TIMESTAMP_HEADER: Final[str] = "X-Notification-Timestamp"
CATEGORY_HEADER: Final[str] = "X-Notification-Category"

# 429 asks for a retry outright; 5xx says the receiver is broken right now, not
# that the request was wrong. Every other 4xx is a durable rejection — a bad
# path, a revoked endpoint — and repeating it just burns attempts.
_RETRYABLE_STATUSES: Final[frozenset[int]] = frozenset({429})

Sleeper = Callable[[float], Awaitable[None]]
Clock = Callable[[], float]


def sign_payload(secret: str, timestamp: int, body: bytes) -> str:
    """Return the value for `X-Notification-Signature`.

    The timestamp is inside the signed material, not merely alongside it, so a
    captured delivery cannot be replayed later with the header rewritten. The
    receiver recomputes `HMAC(secret, f"{t}.{body}")`, compares with
    `hmac.compare_digest`, and rejects anything older than its tolerance.
    """
    signed = f"{timestamp}.".encode() + body
    digest = hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={digest}"


class WebhookNotificationStrategy:
    """Delivers a notification as a signed JSON POST.

    `client`, `sleep` and `clock` are injectable so the retry schedule and the
    signature can be asserted on exactly, without a listening socket and
    without a test that actually waits out the back-off.
    """

    def __init__(
        self,
        *,
        secret: str = "",
        timeout: float = 10.0,
        max_attempts: int = 3,
        backoff: float = 0.5,
        allow_private_hosts: bool = False,
        client: httpx.AsyncClient | None = None,
        sleep: Sleeper | None = None,
        clock: Clock | None = None,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1.")

        self._secret = secret
        self._timeout = timeout
        self._max_attempts = max_attempts
        self._backoff = backoff
        self._allow_private_hosts = allow_private_hosts
        self._client = client
        # Only a client this object created is a client this object may close.
        self._owns_client = client is None
        self._sleep: Sleeper = sleep if sleep is not None else asyncio.sleep
        self._clock: Clock = clock if clock is not None else time.time

    @property
    def name(self) -> str:
        return "webhook"

    def supports(self, recipient: Recipient) -> bool:
        if not recipient.webhook_url:
            return False
        try:
            validate_webhook_url(
                recipient.webhook_url, allow_private_hosts=self._allow_private_hosts
            )
        except Exception:
            return False
        return True

    def _get_client(self) -> httpx.AsyncClient:
        """Return the shared client, building it on first use.

        Built lazily rather than in `__init__` because constructing an
        `AsyncClient` outside a running loop binds it to the wrong one, and the
        registry builds strategies wherever it happens to be called.
        """
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self._timeout,
                follow_redirects=False,  # a redirect would defeat the URL check
            )
        return self._client

    async def aclose(self) -> None:
        """Close the client if this strategy owns it. Safe to call twice."""
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None

    def build_payload(self, recipient: Recipient, notification: Notification) -> bytes:
        """Serialise the delivery body.

        Sorted keys and no whitespace, because the bytes produced here are the
        bytes that get signed *and* the bytes that go on the wire — a receiver
        that re-serialises before verifying would compute a different digest.
        """
        document: dict[str, Any] = {
            "recipient_id": recipient.id,
            "category": notification.category,
            "subject": notification.subject,
            "body": notification.body,
            "html_body": notification.html_body,
            "metadata": dict(notification.metadata),
        }
        return json.dumps(document, sort_keys=True, separators=(",", ":")).encode()

    def build_headers(self, notification: Notification, body: bytes) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            CATEGORY_HEADER: notification.category,
        }

        # No secret configured means no signature header at all, rather than a
        # signature over an empty key — which would look authentic to a
        # receiver doing a naive comparison.
        if self._secret:
            timestamp = int(self._clock())
            headers[TIMESTAMP_HEADER] = str(timestamp)
            headers[SIGNATURE_HEADER] = sign_payload(self._secret, timestamp, body)

        return headers

    async def send(
        self, recipient: Recipient, notification: Notification
    ) -> NotificationResult:
        if not recipient.webhook_url:
            raise RecipientNotReachableError("webhook", "no webhook URL on record")

        url = validate_webhook_url(
            recipient.webhook_url, allow_private_hosts=self._allow_private_hosts
        )
        body = self.build_payload(recipient, notification)
        client = self._get_client()

        last_reason = "no attempt was made"

        for attempt in range(1, self._max_attempts + 1):
            headers = self.build_headers(notification, body)

            try:
                response = await client.post(url, content=body, headers=headers)
            except httpx.TransportError as exc:
                last_reason = f"{type(exc).__name__}: {exc}"
            else:
                if response.is_success:
                    logger.info(
                        "notification.sent",
                        channel=self.name,
                        recipient_id=recipient.id,
                        category=notification.category,
                        status=response.status_code,
                        attempts=attempt,
                    )
                    return NotificationResult(
                        channel=self.name,
                        delivered=True,
                        detail=f"Webhook accepted with {response.status_code}.",
                    )

                retryable = (
                    response.status_code >= 500
                    or response.status_code in _RETRYABLE_STATUSES
                )
                last_reason = f"HTTP {response.status_code}"

                if not retryable:
                    logger.error(
                        "notification.failed",
                        channel=self.name,
                        recipient_id=recipient.id,
                        category=notification.category,
                        status=response.status_code,
                        attempts=attempt,
                    )
                    raise NotificationDeliveryError(self.name, last_reason, attempt)

            if attempt == self._max_attempts:
                break

            delay = self._backoff * (2 ** (attempt - 1))
            logger.warning(
                "notification.retrying",
                channel=self.name,
                recipient_id=recipient.id,
                attempt=attempt,
                next_attempt_in=delay,
                reason=last_reason,
            )
            await self._sleep(delay)

        logger.error(
            "notification.failed",
            channel=self.name,
            recipient_id=recipient.id,
            category=notification.category,
            attempts=self._max_attempts,
            reason=last_reason,
        )
        raise NotificationDeliveryError(self.name, last_reason, self._max_attempts)
