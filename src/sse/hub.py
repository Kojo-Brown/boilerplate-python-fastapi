"""Fan-out to connected streams, with the slow client made someone else's problem.

An SSE endpoint needs something to stream. That something is almost always a
process-wide source — a domain event, a job status change — and every connected
client wants a filtered view of it. The hub is the join: publishers write to a
*topic*, and each open stream holds a subscription to one.

## The invariant that shapes the whole module

**Publishing never blocks and never fails.** `publish` is a plain `def` with no
awaits in it, because there is nothing here it could correctly wait for. Its
callers are an event-bus subscriber running inside somebody's registration
request, and eventually a relay draining the outbox; a fan-out that could
suspend on a client's TCP window would put a phone on a train into the critical
path of an HTTP request that has nothing to do with it, and one that could
raise would fail that request because a browser tab three timezones away is
slow.

So each subscriber has its own **bounded** queue, `publish` uses `put_nowait`,
and a queue with no room is a condition to be resolved rather than waited out.

## What happens to the slow client, and why not the obvious thing

The obvious policies are both worse than they look:

**Drop the oldest event.** The stream stays open and the client never learns it
is now working from an incomplete picture. For a UI that renders a list from a
stream of deltas, that is a view which is silently wrong until a reload it has
no reason to perform.

**Drop the newest event.** Same, plus the client is now permanently behind
rather than transiently.

This hub does neither: a subscriber that cannot keep up is **closed**, after
being told why. The last thing its stream carries is an `overflow` event naming
how many it missed, and an `EventSource` reconnects on its own within
`retry` milliseconds — arriving as a new subscriber with an empty queue and,
for any application that seeds a stream with current state, a fresh snapshot.
"Your connection was too slow, start again" is a recoverable and *visible*
outcome, which neither drop policy is.

The buffer therefore sizes the transient a client may fall behind by, not a
tolerance for a permanently slow one. `SSE_CLIENT_BUFFER_EVENTS` events is
per open stream, so the memory this can consume is that times the connection
limit, times an event — all three of which are numbers somebody chose.

## Registration

`subscribe` is an `async with`, not a method returning an iterator, because the
deregistration is the part that matters: a subscription left in the registry
after its client is gone is fanned out to on every publish for the lifetime of
the process. Tying it to a context manager means the release happens on the way
out of the same block that took it, including when the stream is closed from
outside by a client disconnect.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Final

import structlog

from src.config import settings
from src.sse.event import ServerSentEvent

logger = structlog.get_logger(__name__)

#: Events one stream may fall behind by before it is closed. Two orders of
#: magnitude above the burst a single request can cause, and small enough that
#: the worst case across a full connection limit is still a number of megabytes
#: rather than a page of arithmetic.
DEFAULT_BUFFER_EVENTS: Final[int] = 64

#: The `event:` name a client sees when it was too slow. Named rather than a
#: comment: this one has to be visible to application code, because it is the
#: signal to refetch rather than to carry on applying deltas.
OVERFLOW_EVENT: Final[str] = "overflow"


@dataclass(frozen=True, slots=True)
class _Overflowed:
    """Terminal envelope: this stream fell behind and events were not buffered.

    Carries no count on purpose. The number that could be recorded here is the
    number missed *at the moment the queue filled*, and events keep being
    published to a topic after that — so any figure put on the wire would be
    the first of an unknown total, which is worse than no figure at all. What
    the client needs is the fact, and it is exact.
    """


@dataclass(frozen=True, slots=True)
class _Closed:
    """Terminal envelope: the hub is shutting the stream down deliberately."""


type _Envelope = ServerSentEvent | _Overflowed | _Closed


class _Subscriber:
    """One open stream's slot in the registry: a bounded queue and its state.

    The queue is created one slot larger than `buffer`. That reserved slot is
    never used for an event, only for a terminal envelope, which is what makes
    "tell the client it overflowed" possible at the exact moment there is no
    room left — the alternative being to drop the notification about dropping
    things.
    """

    __slots__ = ("_buffer", "_queue", "_terminated", "topic")

    def __init__(self, topic: str, buffer: int) -> None:
        self.topic = topic
        self._buffer = buffer
        self._queue: asyncio.Queue[_Envelope] = asyncio.Queue(maxsize=buffer + 1)
        self._terminated = False

    @property
    def pending(self) -> int:
        """Events buffered but not yet handed to the client."""
        return self._queue.qsize()

    def offer(self, event: ServerSentEvent) -> bool:
        """Buffer `event`, or terminate this subscriber if there is no room.

        Returns `True` if the event was buffered. `False` means the subscriber
        has been closed and should be dropped from the registry; it is never a
        transient condition to retry.
        """
        if self._terminated:
            # Already ended, and not yet dropped from the registry — a second
            # publish in the same loop, or a close racing an overflow.
            return False
        if self._queue.qsize() >= self._buffer:
            self._terminated = True
            self._queue.put_nowait(_Overflowed())
            return False
        self._queue.put_nowait(event)
        return True

    def close(self) -> None:
        """End the stream after whatever it has already buffered."""
        if self._terminated:
            return
        # Fits by construction: the reserved slot is spent at most once, and
        # `_terminated` is what records that it has been.
        self._terminated = True
        self._queue.put_nowait(_Closed())

    async def events(self) -> AsyncGenerator[ServerSentEvent, None]:
        """Yield buffered events until the stream is terminated."""
        while True:
            envelope = await self._queue.get()
            if isinstance(envelope, _Overflowed):
                logger.warning(
                    "sse.subscriber_overflowed",
                    topic=self.topic,
                    buffer=self._buffer,
                )
                yield ServerSentEvent(
                    event=OVERFLOW_EVENT,
                    data=(
                        "This stream fell too far behind and was closed. "
                        "Reconnect and refetch current state; events since "
                        "the last one delivered were not buffered."
                    ),
                )
                return
            if isinstance(envelope, _Closed):
                return
            yield envelope


class EventStreamHub:
    """Routes events to the open streams subscribed to their topic.

    One process-wide instance (`event_stream_hub`) is the normal case, and its
    reach is exactly one process: a second replica has its own hub and its own
    connections, so an event published on this one is delivered to the clients
    connected *here*. Making that cross-process is a broker's job — see the
    Redis and Kafka items in `SPEC.md` — and the seam for it is `publish`,
    which a relay can call on the receiving side without any stream knowing.

    Construct another instance to give a test its own, rather than publishing
    into the global one and relying on teardown.
    """

    def __init__(self, *, buffer: int = DEFAULT_BUFFER_EVENTS) -> None:
        """
        Args:
            buffer: Events a single stream may fall behind by before it is
                closed with an `overflow` event. Must be at least 1.

        Raises:
            ValueError: `buffer` is below 1.
        """
        if buffer < 1:
            raise ValueError(f"buffer must be at least 1, got {buffer}.")
        self._buffer = buffer
        self._topics: dict[str, list[_Subscriber]] = {}

    def subscriber_count(self, topic: str) -> int:
        """Open streams currently subscribed to `topic`."""
        return len(self._topics.get(topic, ()))

    @property
    def topics(self) -> tuple[str, ...]:
        """Topics with at least one open stream."""
        return tuple(self._topics)

    @asynccontextmanager
    async def subscribe(
        self, topic: str
    ) -> AsyncGenerator[AsyncIterator[ServerSentEvent], None]:
        """Register a stream on `topic` and yield the events published to it.

        Only events published *after* this returns are delivered: there is no
        replay buffer, so an endpoint whose client needs current state should
        send it as the first frame of the stream rather than expecting the hub
        to have kept it.
        """
        subscriber = _Subscriber(topic, self._buffer)
        self._topics.setdefault(topic, []).append(subscriber)
        logger.debug(
            "sse.subscribed", topic=topic, subscribers=self.subscriber_count(topic)
        )
        try:
            yield subscriber.events()
        finally:
            self._drop(subscriber)
            logger.debug(
                "sse.unsubscribed",
                topic=topic,
                subscribers=self.subscriber_count(topic),
                pending=subscriber.pending,
            )

    def publish(self, topic: str, event: ServerSentEvent) -> int:
        """Deliver `event` to every stream on `topic`. Returns how many took it.

        Synchronous and total: no awaits, no exceptions. A count below
        `subscriber_count(topic)` means the difference were closed for falling
        behind, which is already logged — callers are not expected to react.
        """
        subscribers = self._topics.get(topic)
        if not subscribers:
            return 0
        delivered = 0
        # Over a copy: `offer` returning False drops the subscriber from the
        # live list, and mutating what you are iterating skips the next one.
        for subscriber in tuple(subscribers):
            if subscriber.offer(event):
                delivered += 1
            else:
                self._drop(subscriber)
        return delivered

    def close(self, topic: str | None = None) -> int:
        """End every stream on `topic`, or on all topics. Returns how many.

        For shutdown: an ended stream is a clean end-of-body the client
        reconnects from, which is a better last impression than the reset it
        gets when the process exits underneath it.
        """
        targets = (
            tuple(self._topics.get(topic, ()))
            if topic is not None
            else tuple(s for subs in self._topics.values() for s in subs)
        )
        for subscriber in targets:
            subscriber.close()
            self._drop(subscriber)
        if targets:
            logger.info("sse.streams_closed", topic=topic, closed=len(targets))
        return len(targets)

    def _drop(self, subscriber: _Subscriber) -> None:
        """Remove `subscriber` from the registry, tidying an emptied topic.

        Idempotent: a subscriber closed for overflow is dropped by the
        publisher and again by its own `subscribe` block on the way out.
        """
        subscribers = self._topics.get(subscriber.topic)
        if subscribers is None:
            return
        if subscriber in subscribers:
            subscribers.remove(subscriber)
        # An empty list left behind is a slow leak on a per-user topic: one
        # entry per account that ever opened a stream, for the life of the
        # process.
        if not subscribers:
            del self._topics[subscriber.topic]


def user_topic(user_id: str) -> str:
    """The topic carrying one account's events.

    Namespaced rather than the bare id so that a future topic keyed by
    something else — an order, a job — cannot collide with a user id, and so
    that a topic name in a log says what it names.
    """
    return f"user:{user_id}"


#: The process-wide hub the application's endpoints and subscribers use.
event_stream_hub: Final[EventStreamHub] = EventStreamHub(
    buffer=settings.SSE_CLIENT_BUFFER_EVENTS
)


__all__ = [
    "DEFAULT_BUFFER_EVENTS",
    "OVERFLOW_EVENT",
    "EventStreamHub",
    "event_stream_hub",
    "user_topic",
]
