"""The outbound signer and the inbound verifier, against each other.

Both sides are tested thoroughly on their own, and both would stay green if the
format drifted — each would simply be self-consistent. This file is the only
place that fails when they disagree, which is why it exists: two implementations
of one wire format work perfectly until somebody adjusts a separator, and the
symptom is a partner whose integration stops verifying.

`src/notifications/webhook.py` now delegates to `src/webhooks/signature.py`, so
these pass by construction. That is the point — the test is what keeps it true.
"""

from __future__ import annotations

import pytest

from src.notifications.base import Notification, Recipient
from src.notifications.webhook import (
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    WebhookNotificationStrategy,
    sign_payload,
)
from src.webhooks.errors import (
    SignatureMismatchError,
    SignatureTimestampOutsideWindowError,
)
from src.webhooks.replay import InMemoryReplayGuard
from src.webhooks.secrets import parse_signing_secrets
from src.webhooks.signature import sign
from src.webhooks.verifier import WebhookVerifier

SECRET = "a-shared-secret-of-adequate-length!!"
NOW = 1_700_000_000.0
TOLERANCE = 300


def a_strategy() -> WebhookNotificationStrategy:
    return WebhookNotificationStrategy(
        secret=SECRET, allow_private_hosts=True, clock=lambda: NOW
    )


def a_verifier(*, now: float = NOW) -> WebhookVerifier:
    return WebhookVerifier(
        secrets=parse_signing_secrets(f"shared:{SECRET}"),
        tolerance_seconds=TOLERANCE,
        replay_guard=InMemoryReplayGuard(ttl_seconds=float(TOLERANCE * 2)),
        signature_header=SIGNATURE_HEADER,
        clock=lambda: now,
    )


@pytest.fixture
def recipient() -> Recipient:
    return Recipient(
        id="user-1",
        channel="webhook",
        email="nobody@example.test",
        webhook_url="http://localhost/hooks",
    )


@pytest.fixture
def notification() -> Notification:
    return Notification(
        category="account", subject="Welcome", body="Your account is ready."
    )


class TestTheTwoHalvesAgree:
    async def test_a_delivery_this_app_sends_is_one_this_app_accepts(
        self, recipient: Recipient, notification: Notification
    ) -> None:
        """The assertion that fails if either side's format moves."""
        strategy = a_strategy()
        body = strategy.build_payload(recipient, notification)
        headers = strategy.build_headers(notification, body)

        verified = await a_verifier().verify(
            signature=headers[SIGNATURE_HEADER], body=body
        )

        assert verified.key_id == "shared"
        assert verified.body == body

    async def test_the_two_signing_entry_points_produce_the_same_value(self) -> None:
        """`sign_payload` is a name kept for the notification code, not a fork."""
        assert sign_payload(SECRET, int(NOW), b"body") == sign(
            SECRET, int(NOW), b"body"
        )

    async def test_a_body_modified_in_transit_is_caught(
        self, recipient: Recipient, notification: Notification
    ) -> None:
        """A proxy that re-serialises JSON breaks this, and should."""
        strategy = a_strategy()
        body = strategy.build_payload(recipient, notification)
        headers = strategy.build_headers(notification, body)

        with pytest.raises(SignatureMismatchError):
            await a_verifier().verify(
                signature=headers[SIGNATURE_HEADER], body=body.replace(b"Welcome", b"x")
            )


class TestTheUnsignedTimestampHeaderIsReallyUnsigned:
    """The trap, demonstrated end to end with the header this app really sends.

    `build_headers` emits `X-Notification-Timestamp` next to the signature. It is
    there for a human reading a request log; a receiver that took its replay
    window from it would accept a capture of any age, and this is what that looks
    like from both sides.
    """

    async def test_the_capture_is_refused_on_age_despite_a_current_header(
        self, recipient: Recipient, notification: Notification
    ) -> None:
        """An attacker rewrites the unsigned header and gets nowhere.

        The verifier reads the age from `t=` inside the signature, so the
        rewritten header changes nothing: a year-old capture is still a year old.
        """
        strategy = WebhookNotificationStrategy(
            secret=SECRET, allow_private_hosts=True, clock=lambda: NOW
        )
        body = strategy.build_payload(recipient, notification)
        captured = strategy.build_headers(notification, body)
        # What the attacker controls: every header, including this one.
        captured[TIMESTAMP_HEADER] = str(int(NOW) + 365 * 24 * 3600)

        a_year_later = NOW + 365 * 24 * 3600
        with pytest.raises(SignatureTimestampOutsideWindowError):
            await a_verifier(now=a_year_later).verify(
                signature=captured[SIGNATURE_HEADER], body=body
            )

    async def test_the_sent_headers_do_agree_when_nobody_has_tampered(
        self, recipient: Recipient, notification: Notification
    ) -> None:
        """Stated so the test above is clearly about tampering and not a mismatch.

        The two values are equal on a genuine delivery. That is exactly what
        makes trusting the unsigned one so easy to get away with in testing.
        """
        strategy = a_strategy()
        body = strategy.build_payload(recipient, notification)
        headers = strategy.build_headers(notification, body)

        assert headers[TIMESTAMP_HEADER] == str(int(NOW))
        assert f"t={int(NOW)}," in headers[SIGNATURE_HEADER]


class TestAnUnsignedSender:
    async def test_a_strategy_with_no_secret_sends_nothing_to_verify(
        self, recipient: Recipient, notification: Notification
    ) -> None:
        """The outbound side treats an empty secret as "unsigned".

        The inbound side must not mirror that leniency, and does not: a verifier
        cannot be built without a secret at all. The two asymmetries are
        deliberate — an unsigned *delivery* is a choice a sender makes about a
        local receiver, while an unsigned *acceptance* is an open endpoint.
        """
        strategy = WebhookNotificationStrategy(secret="", allow_private_hosts=True)
        body = strategy.build_payload(recipient, notification)

        headers = strategy.build_headers(notification, body)

        assert SIGNATURE_HEADER not in headers
