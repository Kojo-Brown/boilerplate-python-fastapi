"""The built-in subscribers, and how they get attached to a bus."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
import structlog

from src.events.bus import EventBus
from src.events.catalog import UserEvent, UserLoggedIn, UserRegistered
from src.events.subscribers import (
    DEFAULT_SUBSCRIBERS,
    record_user_activity,
    register_default_subscribers,
    send_welcome_email_on_registration,
)


@pytest.fixture
def bus() -> EventBus:
    return EventBus()


@pytest.fixture
def queued(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Stand in for the Celery task so nothing reaches a broker."""
    task = MagicMock()
    monkeypatch.setattr(
        "src.events.subscribers.send_welcome_email_task", task, raising=True
    )
    return task


# --- the audit subscriber ---


async def test_user_activity_is_logged_with_the_fields_needed_to_join_it() -> None:
    event = UserRegistered(
        event_id="evt_1",
        occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
        user_id="usr_1",
        email="alice@example.com",
    )

    with structlog.testing.capture_logs() as logs:
        await record_user_activity(event)

    assert logs == [
        {
            "event": "audit.user_activity",
            "log_level": "info",
            "event_name": "user.registered",
            "event_id": "evt_1",
            "occurred_at": "2026-01-01T00:00:00+00:00",
            "user_id": "usr_1",
        }
    ]


async def test_the_audit_line_carries_no_address() -> None:
    """The event has the email because the mailer needs it; the audit trail
    does not, and logs outlive the account."""
    event = UserLoggedIn(user_id="usr_1", email="alice@example.com")

    with structlog.testing.capture_logs() as logs:
        await record_user_activity(event)

    assert "alice@example.com" not in str(logs)


async def test_audit_covers_every_user_event(bus: EventBus) -> None:
    register_default_subscribers(bus)
    names = {s.name for s in bus.subscribers_for(UserLoggedIn)}

    assert "audit.user_activity" in names


# --- the welcome-email subscriber ---


async def test_welcome_email_is_queued_not_sent(queued: MagicMock) -> None:
    await send_welcome_email_on_registration(
        UserRegistered(user_id="usr_1", email="alice@example.com")
    )

    queued.delay.assert_called_once_with(to="alice@example.com")


async def test_a_broken_broker_does_not_break_the_publish(
    bus: EventBus, queued: MagicMock
) -> None:
    """The registration has already committed by the time this runs; a queue
    that is down must show up as a failed subscriber, not a failed signup."""
    queued.delay.side_effect = ConnectionError("broker unreachable")
    register_default_subscribers(bus)

    result = await bus.publish(UserRegistered(user_id="usr_1", email="a@example.com"))

    assert [f.subscriber for f in result.failures] == ["email.welcome"]
    # Named rather than counted: what matters is that every *other* subscriber
    # still ran, and a bare number says that only until the next one is added.
    assert {o.subscriber for o in result.outcomes if o.ok} == {
        "audit.user_activity",
        "sse.user_streams",
    }


async def test_only_registrations_get_a_welcome_email(
    bus: EventBus, queued: MagicMock
) -> None:
    register_default_subscribers(bus)

    await bus.publish(UserLoggedIn(user_id="usr_1", email="alice@example.com"))

    queued.delay.assert_not_called()


# --- registration ---


def test_register_default_subscribers_attaches_every_spec(bus: EventBus) -> None:
    registered = register_default_subscribers(bus)

    assert len(registered) == len(DEFAULT_SUBSCRIBERS)
    assert {s.name for s in registered} == {
        "audit.user_activity",
        "email.welcome",
        "sse.user_streams",
    }


def test_registering_twice_does_not_double_the_subscribers(bus: EventBus) -> None:
    """A lifespan that runs twice would otherwise send two welcome emails."""
    register_default_subscribers(bus)
    second = register_default_subscribers(bus)

    assert second == ()
    assert len(bus.subscribers_for(UserRegistered)) == len(DEFAULT_SUBSCRIBERS)


def test_specs_that_talk_to_another_process_carry_a_timeout(bus: EventBus) -> None:
    registered = {s.name: s for s in register_default_subscribers(bus)}

    assert registered["email.welcome"].timeout == 5.0


def test_register_defaults_to_the_process_wide_bus(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fresh = EventBus()
    monkeypatch.setattr("src.events.subscribers.event_bus", fresh, raising=True)

    register_default_subscribers()

    assert len(fresh.subscribers_for(UserRegistered)) == len(DEFAULT_SUBSCRIBERS)


def test_every_default_spec_observes_a_user_event() -> None:
    """A spec pointing at an event nothing publishes is dead configuration."""
    for spec in DEFAULT_SUBSCRIBERS:
        assert issubclass(spec.event_type, UserEvent)


async def test_defaults_are_registered_by_the_app_lifespan() -> None:
    from src.events.bus import event_bus
    from src.main import app, lifespan

    event_bus.clear()
    async with lifespan(app):
        during: set[str] = {s.name for s in event_bus.subscribers_for(UserRegistered)}

    assert {"audit.user_activity", "email.welcome"} <= during
    # Shutdown drops them again, so a second app in the same process starts clean.
    assert event_bus.subscribers_for(UserRegistered) == ()


def test_subscriber_spec_is_immutable() -> None:
    spec: Any = DEFAULT_SUBSCRIBERS[0]

    with pytest.raises(AttributeError):
        spec.name = "renamed"
