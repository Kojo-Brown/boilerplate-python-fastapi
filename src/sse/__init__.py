"""Server-sent events: a one-way stream a browser reconnects to by itself.

SSE is a long-lived `text/event-stream` response over ordinary HTTP. That is
the whole protocol, and it is why it is worth reaching for before a WebSocket
whenever the traffic is server-to-client: it needs no upgrade, no subprotocol
and no framing library, it carries the request's cookies and `Authorization`
header like any other GET, and the client's reconnect logic is the browser's
rather than yours.

What it costs is that the response never ends, and every module here exists
because something ordinary stops being true once that is the case.

**The status line is spent.** Everything that can go wrong after the first
frame has to be expressed *in* the body — see the `overflow` event — because
the 200 is already on the wire.

**A quiet connection is indistinguishable from a dead one.** Nothing on the
server polls for a disconnect; it is discovered by a write that fails. So the
keepalive in `heartbeat.py` is not decoration, it is the detector, and its
interval is the ceiling on how long an abandoned stream holds its resources.

**Every connection is a buffer somebody has to bound.** `hub.py` gives each
stream a fixed queue and closes the ones that fall behind, so that one slow
client costs one connection rather than the publisher's throughput or the
process's memory.

**Every connection is also a lifetime.** `stream.py` ends a stream after a
configured ceiling, cleanly, because a connection held open indefinitely
outlives the token that opened it and the deploy that is trying to drain it.

Module by module:

| module         | question it answers                                    |
| -------------- | ------------------------------------------------------ |
| `event.py`     | what a frame looks like on the wire, and what corrupts it |
| `heartbeat.py` | when to write into silence, and how the client's absence is noticed |
| `hub.py`       | who receives a published event, and what happens to a slow one |
| `stream.py`    | the body as a whole: preamble, lifetime, and one log line |
| `response.py`  | the headers, and closing the generator when the client goes |
| `bridge.py`    | how a domain event becomes a frame                     |

`GET /api/v1/events/stream` is the endpoint they assemble into.

## What is not here

**Replay.** No `id:` is sent and `Last-Event-ID` is not honoured, because
nothing stores past events to replay from: the buffer is per connection and
dies with it. A client's contract is that it may have missed events while
disconnected, which is why the stream opens with a `ready` event to refetch
from. Durable replay is the outbox's and a broker's job — see the Redis
Streams and Kafka items in `SPEC.md`.

**Cross-process fan-out.** The hub reaches the clients connected to *this*
process. With more than one replica, an event published on one reaches only
the streams it holds; the seam for fixing that is `EventStreamHub.publish`,
which a broker subscriber can call on each replica without any stream
knowing.

**Client-to-server messages.** SSE has no upstream channel by design. That is
the next item in `SPEC.md` and it is a WebSocket.

See `docs/server-sent-events.md`.
"""

from src.sse.bridge import publish_user_event_to_streams, to_server_sent_event
from src.sse.event import (
    SSE_MEDIA_TYPE,
    ServerSentEvent,
    comment_frame,
    retry_frame,
)
from src.sse.heartbeat import (
    DEFAULT_COMMENT,
    DEFAULT_HEARTBEAT_SECONDS,
    with_heartbeat,
)
from src.sse.hub import (
    DEFAULT_BUFFER_EVENTS,
    OVERFLOW_EVENT,
    EventStreamHub,
    event_stream_hub,
    user_topic,
)
from src.sse.response import EventSourceResponse
from src.sse.stream import LIFETIME_COMMENT, sse_stream

__all__ = [
    "DEFAULT_BUFFER_EVENTS",
    "DEFAULT_COMMENT",
    "DEFAULT_HEARTBEAT_SECONDS",
    "LIFETIME_COMMENT",
    "OVERFLOW_EVENT",
    "SSE_MEDIA_TYPE",
    "EventSourceResponse",
    "EventStreamHub",
    "ServerSentEvent",
    "comment_frame",
    "event_stream_hub",
    "publish_user_event_to_streams",
    "retry_frame",
    "sse_stream",
    "to_server_sent_event",
    "user_topic",
    "with_heartbeat",
]
