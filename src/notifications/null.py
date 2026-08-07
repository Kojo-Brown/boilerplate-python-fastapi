"""Opt-out notification strategy.

The Null Object of the set. A user who has turned notifications off is a
*supported* preference, not a missing one, so it gets a strategy like any other
channel instead of a `None` that every call site would have to remember to
check. `registry.py` resolves `"none"` here, and callers keep one code path.
"""

from __future__ import annotations

import structlog

from src.notifications.base import (
    Notification,
    NotificationResult,
    Recipient,
)

logger = structlog.get_logger(__name__)


class NullNotificationStrategy:
    """Accepts every notification and delivers none of them."""

    @property
    def name(self) -> str:
        return "none"

    def supports(self, recipient: Recipient) -> bool:
        """Always true: reaching nobody needs no address."""
        return True

    async def send(
        self, recipient: Recipient, notification: Notification
    ) -> NotificationResult:
        # Logged rather than dropped silently: "the user never got it" and "the
        # code never tried" look identical in an incident otherwise.
        logger.info(
            "notification.suppressed",
            channel=self.name,
            recipient_id=recipient.id,
            category=notification.category,
        )
        return NotificationResult(
            channel=self.name,
            delivered=False,
            detail="Recipient has opted out of notifications.",
        )
