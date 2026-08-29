"""Domain events → open streams. The one place the two vocabularies meet.

`src/events` publishes `DomainEvent`s to in-process subscribers; `src/sse/hub.py`
fans `ServerSentEvent`s out to connected clients. This module is the subscriber
that turns the first into the second, and keeping it here rather than in
`src/events/subscribers.py` is what lets the SSE feature be added or removed
without touching the event system.

## Three decisions about what goes on the wire

**A user's events go to that user's topic and nowhere else.** The routing key
is `user_topic(event.user_id)`, and the endpoint subscribes to the topic of the
*authenticated* caller — so the authorisation is the subscription, not a filter
applied after fan-out that a later refactor could drop.

**The `event:` name is the domain event's own name** (`user.registered`), so a
client can `addEventListener("user.registered")` and the names in the browser
match the names in `src/events/catalog.py` and in the audit log.

**No `id:` field.** Setting it would give the client a last-event-id it sends
back as `Last-Event-ID` after a reconnect, and this application has nothing to
replay from: the hub buffers per connection, and a connection that dropped took
its buffer with it. An id here would be a promise of resumption that is not
kept — worse than none, because a client written against it would stop treating
gaps as possible.

## What is on the wire, and what is not

The payload is the event's identity and timing, not its contents: `event_name`,
`event_id` and `occurred_at`. `email` is on the event and stays off the stream
— it is not needed to act on the notification, and an event stream is the
easiest thing in an application to accidentally widen the audience of.
`event_id` is the join key to the audit line the same event produced.
"""

from __future__ import annotations

import json
from typing import Final

import structlog

from src.events.catalog import UserEvent
from src.sse.event import ServerSentEvent
from src.sse.hub import EventStreamHub, event_stream_hub, user_topic

logger = structlog.get_logger(__name__)

#: Registration name for the bus subscriber below.
SUBSCRIBER_NAME: Final[str] = "sse.user_streams"


def to_server_sent_event(event: UserEvent) -> ServerSentEvent:
    """Render `event` as the frame its owner's streams will receive."""
    return ServerSentEvent(
        event=type(event).event_name,
        data=json.dumps(
            {
                "event_name": type(event).event_name,
                "event_id": event.event_id,
                "occurred_at": event.occurred_at.isoformat(),
            },
            # The same refusal `src/streaming/ndjson.py` and
            # `src/outbox/codec.py` make: Python emits bare `NaN` and
            # `Infinity`, which no other language's JSON parser accepts. None
            # of the fields above can be a float today, and this is what keeps
            # that true when one is added.
            allow_nan=False,
            separators=(",", ":"),
        ),
    )


async def publish_user_event_to_streams(
    event: UserEvent, hub: EventStreamHub | None = None
) -> None:
    """Fan `event` out to the account's open streams.

    Registered against `UserEvent`, so a new user event reaches subscribers the
    day it is added — the same reason `record_user_activity` is registered
    against the base class.

    Nothing is awaited: `EventStreamHub.publish` is synchronous precisely so
    that this subscriber cannot make the request that published the event wait
    on a client's socket, or fail because of one. It is `async` because the bus
    requires it, and rightly — a synchronous subscriber that *did* block would
    block the loop rather than one request.
    """
    target = hub if hub is not None else event_stream_hub
    delivered = target.publish(user_topic(event.user_id), to_server_sent_event(event))
    if delivered:
        logger.debug(
            "sse.event_fanned_out",
            event_name=type(event).event_name,
            event_id=event.event_id,
            streams=delivered,
        )


__all__ = [
    "SUBSCRIBER_NAME",
    "publish_user_event_to_streams",
    "to_server_sent_event",
]
