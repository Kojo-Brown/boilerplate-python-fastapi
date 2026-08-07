"""Value objects and the webhook URL guard."""

from __future__ import annotations

import pytest

from src.exceptions import BadRequestError
from src.notifications.base import (
    MAX_SUBJECT_LENGTH,
    MAX_WEBHOOK_URL_LENGTH,
    Notification,
    NotificationDeliveryError,
    Recipient,
    UnknownNotificationChannelError,
    validate_webhook_url,
)

# --- Notification ---


def test_notification_defaults() -> None:
    notification = Notification(subject="Hi", body="There")

    assert notification.category == "general"
    assert notification.html_body is None
    assert notification.metadata == {}


def test_notification_is_frozen() -> None:
    notification = Notification(subject="Hi", body="There")

    with pytest.raises(AttributeError):
        notification.subject = "Changed"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("subject", "body"),
    [("", "body"), ("   ", "body"), ("subject", ""), ("subject", "  \n ")],
    ids=["empty-subject", "blank-subject", "empty-body", "blank-body"],
)
def test_blank_subject_or_body_is_rejected(subject: str, body: str) -> None:
    with pytest.raises(BadRequestError):
        Notification(subject=subject, body=body)


def test_overlong_subject_is_rejected() -> None:
    with pytest.raises(BadRequestError, match="at most"):
        Notification(subject="x" * (MAX_SUBJECT_LENGTH + 1), body="body")


def test_subject_at_the_limit_is_accepted() -> None:
    notification = Notification(subject="x" * MAX_SUBJECT_LENGTH, body="body")

    assert len(notification.subject) == MAX_SUBJECT_LENGTH


def test_blank_category_is_rejected() -> None:
    with pytest.raises(BadRequestError, match="category"):
        Notification(subject="Hi", body="There", category=" ")


# --- Recipient ---


def test_recipient_addresses_are_optional() -> None:
    recipient = Recipient(id="u1", channel="none")

    assert recipient.email is None
    assert recipient.webhook_url is None


def test_recipient_is_frozen() -> None:
    recipient = Recipient(id="u1", channel="email", email="a@example.com")

    with pytest.raises(AttributeError):
        recipient.email = "b@example.com"  # type: ignore[misc]


# --- Errors ---


def test_unknown_channel_error_lists_what_is_available() -> None:
    error = UnknownNotificationChannelError("carrier-pigeon", ("email", "none"))

    assert error.status_code == 500
    assert error.error_code == "UNKNOWN_NOTIFICATION_CHANNEL"
    assert error.details == {
        "requested": "carrier-pigeon",
        "available": ["email", "none"],
    }


def test_delivery_error_is_a_bad_gateway_and_reports_attempts() -> None:
    error = NotificationDeliveryError("webhook", "HTTP 503", attempts=3)

    assert error.status_code == 502
    assert error.error_code == "NOTIFICATION_DELIVERY_FAILED"
    assert error.details == {
        "channel": "webhook",
        "reason": "HTTP 503",
        "attempts": 3,
    }


# --- validate_webhook_url ---


@pytest.mark.parametrize(
    "url",
    [
        "https://hooks.example.com/endpoint",
        "https://hooks.example.com:8443/endpoint?token=x",
        "https://8.8.8.8/endpoint",
        "https://[2606:4700:4700::1111]/endpoint",
    ],
)
def test_public_https_urls_are_accepted(url: str) -> None:
    assert validate_webhook_url(url) == url


@pytest.mark.parametrize(
    "url",
    [
        "http://hooks.example.com/endpoint",
        "ftp://hooks.example.com/endpoint",
        "file:///etc/passwd",
        "gopher://hooks.example.com/",
    ],
)
def test_non_https_schemes_are_rejected(url: str) -> None:
    with pytest.raises(BadRequestError):
        validate_webhook_url(url)


@pytest.mark.parametrize(
    "url",
    [
        "https://127.0.0.1/endpoint",
        "https://localhost/endpoint",
        "https://[::1]/endpoint",
        "https://10.0.0.5/endpoint",
        "https://192.168.1.1/endpoint",
        "https://172.16.0.1/endpoint",
        # The cloud instance metadata endpoint — the payload of most SSRF
        # write-ups, and the reason this check exists at all.
        "https://169.254.169.254/latest/meta-data/",
        "https://224.0.0.1/endpoint",
        "https://0.0.0.0/endpoint",
    ],
)
def test_private_and_reserved_addresses_are_rejected(url: str) -> None:
    with pytest.raises(BadRequestError):
        validate_webhook_url(url)


def test_credentials_in_the_url_are_rejected() -> None:
    with pytest.raises(BadRequestError, match="credentials"):
        validate_webhook_url("https://user:secret@hooks.example.com/endpoint")


def test_missing_host_is_rejected() -> None:
    with pytest.raises(BadRequestError, match="host"):
        validate_webhook_url("https:///endpoint")


@pytest.mark.parametrize("url", ["https://[invalid", "https://a[b]c/endpoint"])
def test_an_unparseable_url_becomes_a_bad_request(url: str) -> None:
    """`urlsplit` raises on a malformed authority; it must not escape as-is."""
    with pytest.raises(BadRequestError, match="parsed"):
        validate_webhook_url(url)


def test_empty_url_is_rejected() -> None:
    with pytest.raises(BadRequestError, match="empty"):
        validate_webhook_url("")


def test_overlong_url_is_rejected() -> None:
    url = "https://hooks.example.com/" + "x" * MAX_WEBHOOK_URL_LENGTH

    with pytest.raises(BadRequestError, match="at most"):
        validate_webhook_url(url)


@pytest.mark.parametrize(
    "url",
    ["http://localhost:9000/hook", "https://127.0.0.1/hook", "http://10.0.0.5/hook"],
)
def test_allow_private_hosts_opens_the_door_for_development(url: str) -> None:
    assert validate_webhook_url(url, allow_private_hosts=True) == url


def test_a_hostname_resolving_privately_still_passes() -> None:
    """The documented gap, asserted so it cannot be mistaken for a guarantee.

    `internal.example.com` may well resolve to 10.0.0.5. This check is about
    the literal in the URL; keeping it out of the socket's reach is egress
    policy's job, not a string check's.
    """
    url = "https://internal.example.com/hook"

    assert validate_webhook_url(url) == url
