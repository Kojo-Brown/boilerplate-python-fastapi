"""`/api/v1/events` — a live feed of the caller's own account activity.

Authorisation here is the *subscription*, not a filter. The topic is derived
from the authenticated user inside the handler and cannot be influenced by the
request, so there is no parameter naming whose events to stream and therefore
nothing to get wrong: an endpoint that took a `user_id` and checked it would be
one refactor away from a stream of somebody else's activity.

`CurrentUserDep` resolves before the response starts, which is the ordering
everything about a streaming route depends on. A 401 is a JSON error envelope
with a 401 status; once the first frame is out, the status line is spent and an
error can only be expressed as an event in the body.

## Why the subscription lives in the generator

`hub.subscribe()` is entered inside `_stream`, not in the handler. A context
manager entered in the handler would have exited by the time the response was
returned — the registration would be gone before the first frame — and the
release has to happen when the *body* ends, which is the only thing that knows
the client has gone.

The `ready` event is sent after the subscription is registered and not before.
That ordering is the endpoint's one guarantee to a client: anything published
after `ready` arrives is buffered for this stream, so a client that refetches
its state on `ready` cannot have missed an event in between. There is no replay
buffer to close that window any other way — see `src/sse/bridge.py` on why no
`id:` is sent.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

from fastapi import APIRouter

from src.config import settings
from src.dependencies import CurrentUserDep, EventStreamHubDep
from src.sse.event import SSE_MEDIA_TYPE, ServerSentEvent
from src.sse.hub import EventStreamHub, user_topic
from src.sse.response import EventSourceResponse
from src.sse.stream import sse_stream

router = APIRouter(prefix="/events", tags=["events"])

_STREAM_NAME = "user-events"

#: Sent once, as soon as the stream is registered.
READY_EVENT = "ready"

_STREAM_DESCRIPTION = """
An open `text/event-stream` carrying the authenticated user's account events
as they happen. Consume it with `EventSource` (or any SSE client); the browser
reconnects on its own when the connection drops.

Frames you can receive:

* `event: ready` — the stream is registered. Everything published after this
  reaches you; anything before it was missed, so this is the point at which to
  refetch state.
* `event: user.*` — a domain event for this account, named as in the audit
  log, with `event_id` and `occurred_at` in a JSON payload.
* `event: overflow` — this connection fell too far behind and is being closed.
  Reconnect and refetch; the events in between were not buffered.
* Lines beginning with `:` — keepalive comments. Not dispatched to listeners.

There is **no replay**: `Last-Event-ID` is not honoured, and no `id:` field is
sent, because a reconnecting client cannot be given the events it missed. The
stream also ends on its own after `SSE_MAX_STREAM_SECONDS`, which is an
ordinary end of body and not an error — the client reconnects.
"""


async def _stream(
    hub: EventStreamHub, topic: str
) -> AsyncGenerator[ServerSentEvent, None]:
    """Register on `topic` and yield `ready`, then everything published to it.

    The subscription is released by this generator's `finally` — which
    `EventSourceResponse` runs by closing it — so an abandoned stream stops
    being fanned out to as soon as the disconnect is discovered.
    """
    async with hub.subscribe(topic) as events:
        yield ServerSentEvent(
            event=READY_EVENT,
            data="subscribed",
        )
        async for event in events:
            yield event


@router.get(
    "/stream",
    response_class=EventSourceResponse,
    summary="Stream the authenticated user's events",
    description=_STREAM_DESCRIPTION,
    responses={
        200: {
            "content": {SSE_MEDIA_TYPE: {}},
            "description": "An open event stream. Ends only on disconnect, "
            "shutdown, or the configured lifetime.",
        },
        401: {"description": "The caller is not authenticated."},
    },
)
async def stream_events(
    current_user: CurrentUserDep,
    hub: EventStreamHubDep,
) -> EventSourceResponse:
    """Open the caller's event stream."""
    return EventSourceResponse(
        sse_stream(
            _stream(hub, user_topic(str(current_user.id))),
            name=_STREAM_NAME,
            heartbeat=settings.SSE_HEARTBEAT_SECONDS,
            retry=settings.SSE_RETRY_MS,
            max_seconds=settings.SSE_MAX_STREAM_SECONDS,
        )
    )
