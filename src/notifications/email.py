"""Email notification strategy.

Wraps the existing email task rather than reimplementing delivery: retry,
back-off and per-attempt timeout already live in `src.tasks.email`, and having
two retry loops for the same channel is how they drift apart.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import structlog

from src.notifications.base import (
    Notification,
    NotificationDeliveryError,
    NotificationResult,
    Recipient,
    RecipientNotReachableError,
)
from src.tasks.email import EmailMessage, send_email_with_retry

logger = structlog.get_logger(__name__)

EmailDelivery = Callable[[EmailMessage], Awaitable[None]]


class EmailNotificationStrategy:
    """Sends a notification as an email.

    `delivery` is injectable so a test can assert on the `EmailMessage` that
    was built without a mail server, and so a deployment can substitute SES or
    SendGrid without touching this class.
    """

    def __init__(self, *, delivery: EmailDelivery | None = None) -> None:
        self._delivery: EmailDelivery = (
            delivery if delivery is not None else send_email_with_retry
        )

    @property
    def name(self) -> str:
        return "email"

    def supports(self, recipient: Recipient) -> bool:
        return bool(recipient.email)

    def build_message(
        self, recipient: Recipient, notification: Notification
    ) -> EmailMessage:
        """Render `notification` as an `EmailMessage`.

        Separate from `send` so the rendering can be asserted on directly, and
        so a caller that wants to queue the message through Celery instead can
        reuse the same rendering.
        """
        if not recipient.email:
            raise RecipientNotReachableError("email", "no email address on record")

        headers = {"X-Notification-Category": notification.category}
        headers.update(
            {
                f"X-Notification-{key}": value
                for key, value in notification.metadata.items()
            }
        )

        return EmailMessage(
            to=recipient.email,
            subject=notification.subject,
            body=notification.body,
            html_body=notification.html_body,
            headers=headers,
        )

    async def send(
        self, recipient: Recipient, notification: Notification
    ) -> NotificationResult:
        message = self.build_message(recipient, notification)

        try:
            await self._delivery(message)
        except Exception as exc:
            logger.error(
                "notification.failed",
                channel=self.name,
                recipient_id=recipient.id,
                category=notification.category,
                error=str(exc),
            )
            raise NotificationDeliveryError(self.name, str(exc)) from exc

        logger.info(
            "notification.sent",
            channel=self.name,
            recipient_id=recipient.id,
            category=notification.category,
        )
        return NotificationResult(
            channel=self.name,
            delivered=True,
            detail=f"Email queued for {recipient.email}.",
        )
