"""Domain event → frame: routing, naming, and what stays off the wire."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from src.events.bus import EventBus
from src.events.catalog import UserEvent, UserLoggedIn, UserRegistered
from src.events.subscribers import DEFAULT_SUBSCRIBERS, register_default_subscribers
from src.sse.bridge import (
    SUBSCRIBER_NAME,
    publish_user_event_to_streams,
    to_server_sent_event,
)
from src.sse.hub import EventStreamHub, user_topic


@pytest.fixture
def hub() -> EventStreamHub:
    """A hub of this test's own, rather than the process-wide one."""
    return EventStreamHub()


def registered() -> UserRegistered:
    return UserRegistered(user_id="user-1", email="nobody@example.test")


class TestRendering:
    def test_the_event_name_matches_the_domain_event(self) -> None:
        """So `addEventListener` in a browser uses the name from the catalog."""
        assert to_server_sent_event(registered()).event == "user.registered"

    def test_a_subclass_keeps_its_own_name(self) -> None:
        event = UserLoggedIn(user_id="user-1", email="nobody@example.test")

        assert to_server_sent_event(event).event == "user.logged_in"

    def test_the_payload_carries_identity_and_timing(self) -> None:
        event = registered()

        payload = json.loads(to_server_sent_event(event).data)

        assert payload == {
            "event_name": "user.registered",
            "event_id": event.event_id,
            "occurred_at": event.occurred_at.isoformat(),
        }

    def test_the_email_address_stays_off_the_stream(self) -> None:
        """It is on the event and is not needed to act on the notification."""
        frame = to_server_sent_event(registered())

        assert "nobody@example.test" not in frame.data

    def test_no_id_is_sent(self) -> None:
        """An id promises resumption, and nothing here can replay."""
        assert to_server_sent_event(registered()).id is None

    def test_the_payload_survives_a_round_trip_through_the_wire(self) -> None:
        event = registered()

        frame = to_server_sent_event(event).encode().decode("utf-8")
        data = "".join(
            line.removeprefix("data: ")
            for line in frame.split("\n")
            if line.startswith("data: ")
        )

        assert json.loads(data)["event_id"] == event.event_id

    def test_an_occurred_at_with_an_offset_is_serialised_in_full(self) -> None:
        event = UserRegistered(
            user_id="user-1",
            email="nobody@example.test",
            occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
        )

        assert "2026-01-01T00:00:00+00:00" in to_server_sent_event(event).data


class TestFanOut:
    async def test_the_event_reaches_the_account_that_owns_it(
        self, hub: EventStreamHub
    ) -> None:
        async with hub.subscribe(user_topic("user-1")) as events:
            await publish_user_event_to_streams(registered(), hub)
            hub.close()

            received = [e async for e in events]

        assert [e.event for e in received] == ["user.registered"]

    async def test_another_account_receives_nothing(self, hub: EventStreamHub) -> None:
        """The routing key is the event's own user id, not a filter downstream."""
        async with hub.subscribe(user_topic("user-2")) as events:
            await publish_user_event_to_streams(registered(), hub)
            hub.close()

            assert [e async for e in events] == []

    async def test_publishing_with_nobody_connected_is_harmless(
        self, hub: EventStreamHub
    ) -> None:
        await publish_user_event_to_streams(registered(), hub)


class TestRegistration:
    async def test_the_bridge_is_a_default_subscriber(self) -> None:
        bus = EventBus()

        names = {s.name for s in register_default_subscribers(bus)}

        assert SUBSCRIBER_NAME in names

    async def test_it_observes_the_base_event_so_new_events_are_covered(self) -> None:
        """A user event added tomorrow reaches connected clients on day one."""
        (spec,) = [s for s in DEFAULT_SUBSCRIBERS if s.name == SUBSCRIBER_NAME]

        assert spec.event_type is UserEvent

    async def test_publishing_through_the_bus_reaches_a_stream(
        self, hub: EventStreamHub, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """End to end over the real bus, with the global hub swapped out."""
        monkeypatch.setattr("src.sse.bridge.event_stream_hub", hub)
        bus = EventBus()
        register_default_subscribers(bus)

        async with hub.subscribe(user_topic("user-1")) as events:
            result = await bus.publish(registered())
            hub.close()

            received = [e async for e in events]

        assert result.ok
        assert [e.event for e in received] == ["user.registered"]

    async def test_a_stream_that_overflows_does_not_fail_the_publish(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A slow browser must not fail the request that caused the event."""
        hub = EventStreamHub(buffer=1)
        monkeypatch.setattr("src.sse.bridge.event_stream_hub", hub)
        bus = EventBus()
        register_default_subscribers(bus)

        async with hub.subscribe(user_topic("user-1")):
            first = await bus.publish(registered())
            second = await bus.publish(registered())

        assert first.ok
        assert second.ok
