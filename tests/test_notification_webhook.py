"""Webhook delivery: payload, signature, retry schedule and failure classes."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import AsyncIterator

import httpx
import pytest

from src.exceptions import BadRequestError
from src.notifications.base import (
    Notification,
    NotificationDeliveryError,
    Recipient,
    RecipientNotReachableError,
)
from src.notifications.webhook import (
    CATEGORY_HEADER,
    SIGNATURE_HEADER,
    TIMESTAMP_HEADER,
    WebhookNotificationStrategy,
    sign_payload,
)

SECRET = "test-webhook-secret-not-a-real-key"
FIXED_TIME = 1_754_400_000.0

RECIPIENT = Recipient(
    id="11111111-1111-4111-8111-111111111111",
    channel="webhook",
    webhook_url="https://hooks.example.com/u1",
)

NOTIFICATION = Notification(
    subject="Your export is ready",
    body="The archive is available for 24 hours.",
    category="exports",
    metadata={"Request-Id": "abc123"},
)


class Recorder:
    """Captures requests and replays a scripted sequence of responses."""

    def __init__(self, *statuses: int) -> None:
        self._statuses = list(statuses) or [200]
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        index = min(len(self.requests) - 1, len(self._statuses) - 1)
        return httpx.Response(self._statuses[index])


class SleepLog:
    """Stands in for `asyncio.sleep` so back-off is asserted, not waited out."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


def make_strategy(
    recorder: Recorder,
    *,
    sleeper: SleepLog | None = None,
    secret: str = SECRET,
    max_attempts: int = 3,
    backoff: float = 0.5,
) -> WebhookNotificationStrategy:
    return WebhookNotificationStrategy(
        secret=secret,
        max_attempts=max_attempts,
        backoff=backoff,
        client=httpx.AsyncClient(transport=httpx.MockTransport(recorder.handler)),
        sleep=sleeper if sleeper is not None else SleepLog(),
        clock=lambda: FIXED_TIME,
    )


@pytest.fixture
async def recorder() -> AsyncIterator[Recorder]:
    yield Recorder(200)


# --- Payload ---


async def test_payload_carries_the_notification_and_recipient(
    recorder: Recorder,
) -> None:
    await make_strategy(recorder).send(RECIPIENT, NOTIFICATION)

    body = json.loads(recorder.requests[0].content)
    assert body == {
        "recipient_id": RECIPIENT.id,
        "category": "exports",
        "subject": "Your export is ready",
        "body": "The archive is available for 24 hours.",
        "html_body": None,
        "metadata": {"Request-Id": "abc123"},
    }


async def test_payload_is_serialised_deterministically(recorder: Recorder) -> None:
    """Sorted, whitespace-free — the bytes signed are the bytes sent."""
    strategy = make_strategy(recorder)

    await strategy.send(RECIPIENT, NOTIFICATION)

    raw = recorder.requests[0].content
    assert raw == strategy.build_payload(RECIPIENT, NOTIFICATION)
    assert b", " not in raw
    keys = [key for key in json.loads(raw)]
    assert keys == sorted(keys)


async def test_content_type_and_category_headers_are_set(recorder: Recorder) -> None:
    await make_strategy(recorder).send(RECIPIENT, NOTIFICATION)

    headers = recorder.requests[0].headers
    assert headers["Content-Type"] == "application/json"
    assert headers[CATEGORY_HEADER] == "exports"


# --- Signature ---


async def test_delivery_is_signed_over_timestamp_and_body(recorder: Recorder) -> None:
    strategy = make_strategy(recorder)

    await strategy.send(RECIPIENT, NOTIFICATION)

    request = recorder.requests[0]
    timestamp = int(FIXED_TIME)
    expected = hmac.new(
        SECRET.encode(),
        f"{timestamp}.".encode() + request.content,
        hashlib.sha256,
    ).hexdigest()

    assert request.headers[TIMESTAMP_HEADER] == str(timestamp)
    assert request.headers[SIGNATURE_HEADER] == f"t={timestamp},v1={expected}"


def test_signature_changes_with_the_timestamp() -> None:
    """Replay protection: the same body signed a second later differs.

    If the timestamp were only a header alongside the digest, a captured
    delivery could be replayed indefinitely by rewriting it.
    """
    body = b'{"a":1}'

    assert sign_payload(SECRET, 1000, body) != sign_payload(SECRET, 1001, body)


def test_signature_changes_with_the_body() -> None:
    assert sign_payload(SECRET, 1000, b'{"a":1}') != sign_payload(
        SECRET, 1000, b'{"a":2}'
    )


async def test_no_secret_means_no_signature_header(recorder: Recorder) -> None:
    """Better an absent header than a digest keyed on the empty string."""
    await make_strategy(recorder, secret="").send(RECIPIENT, NOTIFICATION)

    headers = recorder.requests[0].headers
    assert SIGNATURE_HEADER not in headers
    assert TIMESTAMP_HEADER not in headers


# --- Success ---


@pytest.mark.parametrize("status", [200, 201, 202, 204])
async def test_any_2xx_counts_as_delivered(status: int) -> None:
    recorder = Recorder(status)

    result = await make_strategy(recorder).send(RECIPIENT, NOTIFICATION)

    assert result.delivered is True
    assert result.channel == "webhook"
    assert str(status) in result.detail
    assert len(recorder.requests) == 1


# --- Retry ---


async def test_a_5xx_is_retried_then_succeeds() -> None:
    recorder = Recorder(503, 200)
    sleeper = SleepLog()

    result = await make_strategy(recorder, sleeper=sleeper).send(
        RECIPIENT, NOTIFICATION
    )

    assert result.delivered is True
    assert len(recorder.requests) == 2
    assert sleeper.delays == [0.5]


async def test_backoff_doubles_and_stops_after_the_last_attempt() -> None:
    recorder = Recorder(500)
    sleeper = SleepLog()

    with pytest.raises(NotificationDeliveryError) as excinfo:
        await make_strategy(recorder, sleeper=sleeper).send(RECIPIENT, NOTIFICATION)

    assert len(recorder.requests) == 3
    # Two sleeps for three attempts — never one after the final failure.
    assert sleeper.delays == [0.5, 1.0]
    assert excinfo.value.details == {
        "channel": "webhook",
        "reason": "HTTP 500",
        "attempts": 3,
    }


async def test_429_is_retried() -> None:
    recorder = Recorder(429, 200)

    result = await make_strategy(recorder).send(RECIPIENT, NOTIFICATION)

    assert result.delivered is True
    assert len(recorder.requests) == 2


@pytest.mark.parametrize("status", [400, 401, 403, 404, 410, 422])
async def test_other_4xx_fails_immediately_without_retrying(status: int) -> None:
    """A rejected request stays rejected; repeating it only burns attempts."""
    recorder = Recorder(status)
    sleeper = SleepLog()

    with pytest.raises(NotificationDeliveryError) as excinfo:
        await make_strategy(recorder, sleeper=sleeper).send(RECIPIENT, NOTIFICATION)

    assert len(recorder.requests) == 1
    assert sleeper.delays == []
    assert excinfo.value.details == {
        "channel": "webhook",
        "reason": f"HTTP {status}",
        "attempts": 1,
    }


async def test_transport_errors_are_retried_then_reported() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectTimeout("timed out", request=request)

    sleeper = SleepLog()
    strategy = WebhookNotificationStrategy(
        secret=SECRET,
        max_attempts=2,
        backoff=0.25,
        client=httpx.AsyncClient(transport=httpx.MockTransport(handler)),
        sleep=sleeper,
        clock=lambda: FIXED_TIME,
    )

    with pytest.raises(NotificationDeliveryError, match="ConnectTimeout"):
        await strategy.send(RECIPIENT, NOTIFICATION)

    assert attempts == 2
    assert sleeper.delays == [0.25]


async def test_max_attempts_of_one_never_sleeps() -> None:
    recorder = Recorder(500)
    sleeper = SleepLog()

    with pytest.raises(NotificationDeliveryError):
        await make_strategy(recorder, sleeper=sleeper, max_attempts=1).send(
            RECIPIENT, NOTIFICATION
        )

    assert len(recorder.requests) == 1
    assert sleeper.delays == []


def test_max_attempts_below_one_is_refused() -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        WebhookNotificationStrategy(max_attempts=0)


# --- Recipient and URL handling ---


async def test_missing_webhook_url_raises_not_reachable(recorder: Recorder) -> None:
    recipient = Recipient(id="u2", channel="webhook", webhook_url=None)

    with pytest.raises(RecipientNotReachableError):
        await make_strategy(recorder).send(recipient, NOTIFICATION)

    assert recorder.requests == []


async def test_an_unsafe_url_is_refused_before_any_request(
    recorder: Recorder,
) -> None:
    """The SSRF check runs before the socket does, not after a failed attempt."""
    recipient = Recipient(
        id="u3",
        channel="webhook",
        webhook_url="https://169.254.169.254/latest/meta-data/",
    )

    with pytest.raises(BadRequestError):
        await make_strategy(recorder).send(recipient, NOTIFICATION)

    assert recorder.requests == []


async def test_supports_reflects_url_safety(recorder: Recorder) -> None:
    strategy = make_strategy(recorder)

    assert strategy.supports(RECIPIENT) is True
    assert strategy.supports(Recipient(id="u", channel="webhook")) is False
    assert (
        strategy.supports(
            Recipient(id="u", channel="webhook", webhook_url="https://127.0.0.1/hook")
        )
        is False
    )


async def test_redirects_are_not_followed() -> None:
    """A 302 would let a vetted URL hand the delivery to an unvetted one."""
    recorder = Recorder(302)

    with pytest.raises(NotificationDeliveryError, match="302"):
        await make_strategy(recorder).send(RECIPIENT, NOTIFICATION)

    assert len(recorder.requests) == 1


# --- Client lifecycle ---


async def test_an_injected_client_is_not_closed_by_aclose() -> None:
    client = httpx.AsyncClient(transport=httpx.MockTransport(Recorder(200).handler))
    strategy = WebhookNotificationStrategy(client=client)

    await strategy.aclose()

    assert client.is_closed is False
    await client.aclose()


async def test_an_owned_client_is_built_lazily_and_closed() -> None:
    strategy = WebhookNotificationStrategy(allow_private_hosts=True)

    client = strategy._get_client()
    assert client.is_closed is False

    await strategy.aclose()
    assert client.is_closed is True
    # Idempotent: a second close on a strategy that no longer holds a client.
    await strategy.aclose()
