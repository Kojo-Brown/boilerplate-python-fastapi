"""Fan-out: who receives an event, and what a slow client costs everyone else."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from src.sse.event import ServerSentEvent
from src.sse.hub import (
    DEFAULT_BUFFER_EVENTS,
    OVERFLOW_EVENT,
    EventStreamHub,
    _Subscriber,  # the reserved-slot guards; see below
    event_stream_hub,
    user_topic,
)


def event(n: int) -> ServerSentEvent:
    return ServerSentEvent(data=f"event-{n}")


async def drain(events: AsyncIterator[ServerSentEvent]) -> list[ServerSentEvent]:
    """Read a terminated stream to its end."""
    return [e async for e in events]


class TestDelivery:
    async def test_a_subscriber_receives_what_is_published_to_its_topic(self) -> None:
        hub = EventStreamHub()

        async with hub.subscribe("t") as events:
            hub.publish("t", event(1))
            hub.close("t")

            assert await drain(events) == [event(1)]

    async def test_every_subscriber_on_a_topic_receives_the_event(self) -> None:
        hub = EventStreamHub()

        async with hub.subscribe("t") as first, hub.subscribe("t") as second:
            assert hub.publish("t", event(1)) == 2
            hub.close("t")

            assert await drain(first) == [event(1)]
            assert await drain(second) == [event(1)]

    async def test_another_topic_receives_nothing(self) -> None:
        """The topic is the authorisation boundary, not a filter after the fact."""
        hub = EventStreamHub()

        async with hub.subscribe("mine") as mine:
            assert hub.publish("theirs", event(1)) == 0
            hub.close("mine")

            assert await drain(mine) == []

    async def test_publishing_to_nobody_is_not_an_error(self) -> None:
        assert EventStreamHub().publish("empty", event(1)) == 0

    async def test_order_is_preserved(self) -> None:
        hub = EventStreamHub()

        async with hub.subscribe("t") as events:
            for i in range(10):
                hub.publish("t", event(i))
            hub.close("t")

            assert await drain(events) == [event(i) for i in range(10)]

    async def test_events_published_before_subscribing_are_not_replayed(self) -> None:
        """There is no history to join; the endpoint's `ready` event says so."""
        hub = EventStreamHub()
        hub.publish("t", event(1))

        async with hub.subscribe("t") as events:
            hub.close("t")

            assert await drain(events) == []

    async def test_a_waiting_subscriber_is_woken_by_a_publish(self) -> None:
        """The consumer blocks in `get`, which is where an SSE stream idles."""
        hub = EventStreamHub()

        async with hub.subscribe("t") as events:
            reader = asyncio.ensure_future(anext(events))
            await asyncio.sleep(0)
            assert not reader.done()

            hub.publish("t", event(1))

            assert await asyncio.wait_for(reader, timeout=1.0) == event(1)


class TestRegistration:
    async def test_subscribing_registers_and_leaving_deregisters(self) -> None:
        hub = EventStreamHub()

        async with hub.subscribe("t"):
            assert hub.subscriber_count("t") == 1
        assert hub.subscriber_count("t") == 0

    async def test_an_emptied_topic_is_forgotten(self) -> None:
        """Otherwise a per-user topic leaks an entry per account, forever."""
        hub = EventStreamHub()

        async with hub.subscribe("t"):
            assert hub.topics == ("t",)
        assert not hub.topics

    async def test_deregistration_survives_an_exception_in_the_block(self) -> None:
        hub = EventStreamHub()

        with pytest.raises(RuntimeError):
            async with hub.subscribe("t"):
                raise RuntimeError("handler blew up")

        assert hub.subscriber_count("t") == 0

    async def test_one_subscriber_leaving_does_not_disturb_the_others(self) -> None:
        hub = EventStreamHub()

        async with hub.subscribe("t") as kept:
            async with hub.subscribe("t"):
                pass
            assert hub.subscriber_count("t") == 1

            hub.publish("t", event(1))
            hub.close("t")

            assert await drain(kept) == [event(1)]


class TestTheSlowClient:
    async def test_a_subscriber_that_falls_behind_is_closed_with_an_overflow(
        self,
    ) -> None:
        hub = EventStreamHub(buffer=2)

        async with hub.subscribe("t") as events:
            for i in range(5):
                hub.publish("t", event(i))

            received = await drain(events)

        assert [e.data for e in received[:2]] == ["event-0", "event-1"]
        assert received[-1].event == OVERFLOW_EVENT
        assert len(received) == 3

    async def test_the_overflow_event_says_to_refetch(self) -> None:
        """A drop policy would leave the client silently working from stale state."""
        hub = EventStreamHub(buffer=1)

        async with hub.subscribe("t") as events:
            hub.publish("t", event(1))
            hub.publish("t", event(2))

            received = await drain(events)

        assert "Reconnect" in received[-1].data

    async def test_an_overflowed_subscriber_is_dropped_from_the_registry(self) -> None:
        """Fanning out to a closed stream is work for a client that is gone."""
        hub = EventStreamHub(buffer=1)

        async with hub.subscribe("t"):
            hub.publish("t", event(1))
            hub.publish("t", event(2))

            assert hub.subscriber_count("t") == 0

    async def test_a_slow_subscriber_does_not_stop_a_fast_one(self) -> None:
        """The invariant the whole module is arranged around."""
        hub = EventStreamHub(buffer=2)

        async with hub.subscribe("t") as slow, hub.subscribe("t") as fast:
            # `fast` keeps up: one read per publish. `slow` never reads.
            for i in range(5):
                hub.publish("t", event(i))
                if i < 2:
                    assert await anext(fast) == event(i)

            assert [e.data for e in await drain(slow)][:2] == ["event-0", "event-1"]
            assert (await anext(fast)).data == "event-2"

    async def test_publishing_never_blocks_or_raises(self) -> None:
        """`publish` is synchronous: there is nothing in it that can suspend."""
        hub = EventStreamHub(buffer=1)

        async with hub.subscribe("t"):
            for i in range(1000):
                hub.publish("t", event(i))

    async def test_a_buffer_below_one_is_refused(self) -> None:
        with pytest.raises(ValueError, match="at least 1"):
            EventStreamHub(buffer=0)

    def test_the_default_buffer_is_bounded(self) -> None:
        """Worst-case memory is this times the connection limit."""
        assert 0 < DEFAULT_BUFFER_EVENTS <= 1024


class TestClosing:
    async def test_closing_a_topic_ends_its_streams(self) -> None:
        hub = EventStreamHub()

        async with hub.subscribe("t") as events:
            assert hub.close("t") == 1

            assert await drain(events) == []

    async def test_closing_delivers_what_was_already_buffered(self) -> None:
        """A shutdown should not throw away events the client can still have."""
        hub = EventStreamHub()

        async with hub.subscribe("t") as events:
            hub.publish("t", event(1))
            hub.close("t")

            assert await drain(events) == [event(1)]

    async def test_closing_everything_closes_every_topic(self) -> None:
        hub = EventStreamHub()

        async with hub.subscribe("a") as first, hub.subscribe("b") as second:
            assert hub.close() == 2

            assert await drain(first) == []
            assert await drain(second) == []

    async def test_closing_twice_is_harmless(self) -> None:
        """The lifespan may run more than once under a test client."""
        hub = EventStreamHub()

        async with hub.subscribe("t") as events:
            hub.close("t")
            assert hub.close("t") == 0

            assert await drain(events) == []

    async def test_closing_an_unknown_topic_is_harmless(self) -> None:
        assert EventStreamHub().close("nobody") == 0

    async def test_a_closed_stream_receives_no_further_events(self) -> None:
        hub = EventStreamHub()

        async with hub.subscribe("t") as events:
            hub.close("t")
            assert hub.publish("t", event(1)) == 0

            assert await drain(events) == []


class TestTheReservedSlot:
    """The guards that make "the terminal envelope always fits" true.

    A subscriber's queue holds `buffer + 1`, and the spare slot exists so that
    an overflow can be *reported* at the moment there is no room left. Spending
    it twice would raise `QueueFull` out of `publish`, which is documented as
    total and is called from inside somebody's HTTP request.

    Reached directly because the public surface cannot get there: `publish` and
    `close` both drop a subscriber the instant it is terminated, and neither
    method awaits, so nothing interleaves between the two. That is exactly why
    these are asserted here rather than left to a caller that does not exist
    yet.
    """

    def test_offering_to_a_terminated_subscriber_is_refused(self) -> None:
        subscriber = _Subscriber("t", 1)
        assert subscriber.offer(event(1)) is True
        assert subscriber.offer(event(2)) is False

        assert subscriber.offer(event(3)) is False

    def test_closing_a_terminated_subscriber_does_nothing(self) -> None:
        subscriber = _Subscriber("t", 1)
        subscriber.offer(event(1))
        subscriber.offer(event(2))  # overflows, spending the reserved slot

        subscriber.close()  # would be a second terminal envelope

        assert subscriber.pending == 2

    def test_closing_twice_spends_one_slot(self) -> None:
        subscriber = _Subscriber("t", 1)

        subscriber.close()
        subscriber.close()

        assert subscriber.pending == 1


class TestTopics:
    def test_a_user_topic_is_namespaced(self) -> None:
        """So that a topic keyed on something else cannot collide with a user id."""
        assert user_topic("abc") == "user:abc"

    def test_different_users_get_different_topics(self) -> None:
        assert user_topic("a") != user_topic("b")

    def test_the_process_wide_hub_exists(self) -> None:
        """Publisher and subscriber have to find the same registry."""
        assert isinstance(event_stream_hub, EventStreamHub)
