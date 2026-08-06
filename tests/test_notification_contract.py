"""One suite, run against every notification strategy.

A strategy is only pluggable if the things the registry returns are
interchangeable, so the behaviour callers rely on is asserted once and
parametrised over the implementations rather than written three times with
three sets of assumptions. `WebhookNotificationStrategy` participates through
an `httpx.MockTransport`, which exercises the real request-building and
response-handling code without a listening socket.
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx
import pytest

from src.notifications.base import (
    Notification,
    NotificationResult,
    NotificationStrategy,
    Recipient,
    RecipientNotReachableError,
)
from src.notifications.email import EmailNotificationStrategy
from src.notifications.null import NullNotificationStrategy
from src.notifications.webhook import WebhookNotificationStrategy
from src.tasks.email import EmailMessage

NOTIFICATION = Notification(
    subject="Your export is ready",
    body="The archive you requested is available for the next 24 hours.",
    category="exports",
)


@dataclass
class Case:
    """A strategy plus the two recipients the contract needs to talk about."""

    strategy: NotificationStrategy
    reachable: Recipient
    # None where the channel has no notion of an unreachable recipient.
    unreachable: Recipient | None
    # False for the opt-out strategy, whose whole job is not to deliver.
    delivers: bool


async def _accept(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(200)


@pytest.fixture(params=["email", "webhook", "none"])
def case(request: pytest.FixtureRequest) -> Case:
    if request.param == "email":

        async def _deliver(_message: EmailMessage) -> None:
            return None

        return Case(
            strategy=EmailNotificationStrategy(delivery=_deliver),
            reachable=Recipient(id="u1", channel="email", email="user@example.com"),
            unreachable=Recipient(id="u2", channel="email", email=None),
            delivers=True,
        )

    if request.param == "webhook":
        client = httpx.AsyncClient(transport=httpx.MockTransport(_accept))
        return Case(
            strategy=WebhookNotificationStrategy(client=client),
            reachable=Recipient(
                id="u1",
                channel="webhook",
                webhook_url="https://hooks.example.com/u1",
            ),
            unreachable=Recipient(id="u2", channel="webhook", webhook_url=None),
            delivers=True,
        )

    return Case(
        strategy=NullNotificationStrategy(),
        reachable=Recipient(id="u1", channel="none"),
        unreachable=None,
        delivers=False,
    )


@pytest.fixture
def strategy(case: Case) -> NotificationStrategy:
    return case.strategy


# --- The contract ---


def test_strategy_satisfies_the_protocol(strategy: NotificationStrategy) -> None:
    assert isinstance(strategy, NotificationStrategy)
    assert strategy.name in {"email", "webhook", "none"}


async def test_send_returns_a_result_naming_its_own_channel(case: Case) -> None:
    result = await case.strategy.send(case.reachable, NOTIFICATION)

    assert isinstance(result, NotificationResult)
    assert result.channel == case.strategy.name
    assert result.delivered is case.delivers
    assert result.detail


async def test_supports_is_true_for_a_reachable_recipient(case: Case) -> None:
    assert case.strategy.supports(case.reachable) is True


async def test_supports_is_false_for_an_unreachable_recipient(case: Case) -> None:
    if case.unreachable is None:
        pytest.skip(f"every recipient is reachable on '{case.strategy.name}'")

    assert case.strategy.supports(case.unreachable) is False


async def test_unreachable_recipient_raises_rather_than_reporting_success(
    case: Case,
) -> None:
    """The failure mode `supports` predicts is the one `send` produces.

    A strategy that returned `delivered=False` here would be indistinguishable
    from the opt-out strategy, and a missing address would look like a
    preference.
    """
    if case.unreachable is None:
        pytest.skip(f"every recipient is reachable on '{case.strategy.name}'")

    with pytest.raises(RecipientNotReachableError) as excinfo:
        await case.strategy.send(case.unreachable, NOTIFICATION)

    assert excinfo.value.status_code == 422
    assert excinfo.value.error_code == "RECIPIENT_NOT_REACHABLE"
    assert excinfo.value.details == {
        "channel": case.strategy.name,
        "reason": excinfo.value.details["reason"],  # type: ignore[index]
    }


async def test_send_is_repeatable(case: Case) -> None:
    """No strategy holds one-shot state that a second send would trip over."""
    first = await case.strategy.send(case.reachable, NOTIFICATION)
    second = await case.strategy.send(case.reachable, NOTIFICATION)

    assert first.channel == second.channel
    assert first.delivered == second.delivered


@pytest.mark.parametrize(
    "notification",
    [
        Notification(subject="Plain", body="Text only."),
        Notification(subject="Rich", body="Text.", html_body="<p>Text.</p>"),
        Notification(
            subject="Tagged",
            body="Text.",
            category="security",
            metadata={"Request-Id": "abc123"},
        ),
    ],
    ids=["plain", "html", "metadata"],
)
async def test_every_strategy_accepts_every_notification_shape(
    case: Case, notification: Notification
) -> None:
    """`html_body` and `metadata` are optional enrichments, not requirements.

    A strategy that cannot render HTML still has to accept a notification that
    carries it — otherwise the caller would have to know the channel, which is
    the coupling the pattern exists to remove.
    """
    result = await case.strategy.send(case.reachable, notification)

    assert result.channel == case.strategy.name
