"""Email strategy: message rendering and failure translation."""

from __future__ import annotations

import pytest

from src.notifications.base import (
    Notification,
    NotificationDeliveryError,
    Recipient,
    RecipientNotReachableError,
)
from src.notifications.email import EmailNotificationStrategy
from src.tasks.email import EmailMessage

RECIPIENT = Recipient(id="u1", channel="email", email="user@example.com")

NOTIFICATION = Notification(
    subject="Your export is ready",
    body="The archive is available for 24 hours.",
    category="exports",
    html_body="<p>The archive is available for 24 hours.</p>",
    metadata={"Request-Id": "abc123"},
)


class Outbox:
    def __init__(self) -> None:
        self.messages: list[EmailMessage] = []

    async def __call__(self, message: EmailMessage) -> None:
        self.messages.append(message)


async def test_notification_is_rendered_onto_the_email_message() -> None:
    outbox = Outbox()

    result = await EmailNotificationStrategy(delivery=outbox).send(
        RECIPIENT, NOTIFICATION
    )

    (message,) = outbox.messages
    assert message.to == "user@example.com"
    assert message.subject == "Your export is ready"
    assert message.body == "The archive is available for 24 hours."
    assert message.html_body == "<p>The archive is available for 24 hours.</p>"
    assert result.delivered is True
    assert result.channel == "email"


async def test_category_and_metadata_become_headers() -> None:
    outbox = Outbox()

    await EmailNotificationStrategy(delivery=outbox).send(RECIPIENT, NOTIFICATION)

    assert outbox.messages[0].headers == {
        "X-Notification-Category": "exports",
        "X-Notification-Request-Id": "abc123",
    }


async def test_a_plain_notification_carries_no_html_body() -> None:
    outbox = Outbox()

    await EmailNotificationStrategy(delivery=outbox).send(
        RECIPIENT, Notification(subject="Plain", body="Text only.")
    )

    assert outbox.messages[0].html_body is None


def test_build_message_is_usable_without_sending() -> None:
    """Exposed so a caller can queue the message through Celery instead."""
    message = EmailNotificationStrategy().build_message(RECIPIENT, NOTIFICATION)

    assert isinstance(message, EmailMessage)
    assert message.to == "user@example.com"


def test_build_message_refuses_a_recipient_with_no_address() -> None:
    with pytest.raises(RecipientNotReachableError):
        EmailNotificationStrategy().build_message(
            Recipient(id="u2", channel="email"), NOTIFICATION
        )


async def test_a_delivery_failure_becomes_a_delivery_error() -> None:
    """The SMTP library's exception must not escape as-is.

    A caller handling notifications should not have to catch whatever the
    configured transport happens to raise — that is precisely the coupling the
    strategy boundary removes.
    """

    async def _explode(_message: EmailMessage) -> None:
        raise TimeoutError("smtp host did not respond")

    with pytest.raises(NotificationDeliveryError) as excinfo:
        await EmailNotificationStrategy(delivery=_explode).send(RECIPIENT, NOTIFICATION)

    assert excinfo.value.status_code == 502
    assert excinfo.value.details == {
        "channel": "email",
        "reason": "smtp host did not respond",
        "attempts": 1,
    }


async def test_the_default_delivery_is_the_retrying_email_task() -> None:
    """Retry lives in `src.tasks.email`; this strategy must not grow a second."""
    from src.tasks.email import send_email_with_retry

    assert EmailNotificationStrategy()._delivery is send_email_with_retry
