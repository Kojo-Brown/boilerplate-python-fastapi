# Server-sent events

`GET /api/v1/events/stream` holds a connection open and writes the caller's
account events to it as they happen. This document is about the four things
that stop being true once a response never ends, and the one of them that is
usually discovered in production.

- [The shape](#the-shape)
- [The heartbeat is the disconnect detector](#the-heartbeat-is-the-disconnect-detector)
- [The slow client](#the-slow-client)
- [The lifetime](#the-lifetime)
- [The wire format](#the-wire-format)
- [Configuration](#configuration)
- [Consuming the stream](#consuming-the-stream)
- [Adding a stream](#adding-a-stream)
- [What is deliberately not here](#what-is-deliberately-not-here)

## The shape

```
src/sse/event.py       what a frame is on the wire, and what corrupts one
src/sse/heartbeat.py   writing into silence, and noticing the client is gone
src/sse/hub.py         who receives a published event; the slow-client policy
src/sse/stream.py      the body as a whole: preamble, lifetime, one log line
src/sse/response.py    headers, and closing the generator when the client goes
src/sse/bridge.py      the event-bus subscriber that turns a domain event into a frame
src/api/v1/events.py   the route
```

Data flows the other way round:

```
UserEvent ─bridge─▶ hub ─per-connection queue─▶ encode ─heartbeat─▶ socket
                                                └── one open stream ──┘
```

## The heartbeat is the disconnect detector

This is the part that is easy to get wrong, because getting it wrong looks
exactly like getting it right until the connection count stops going down.

Under ASGI, a server reports a vanished client by putting `http.disconnect` on
the receive channel. Starlette's `StreamingResponse` reads that channel **only
on servers advertising ASGI spec_version below 2.4**; on anything newer it
streams the body and relies on `send()` raising `OSError`, which it converts
to `ClientDisconnect`. So:

* nothing polls for a disconnect — it is discovered by a **write that fails**;
* an SSE stream with nothing to say makes no writes.

A stream with no keepalive over an idle topic is therefore not merely quiet, it
is *undetectable*. The client closes the tab, the socket goes away, and the
body generator stays suspended holding a hub subscription and a connection slot
until something is published on that topic — which for a per-user topic may be
never, and which is also the thing that repairs it, so the leak is invisible to
any test that disconnects and then publishes.

Writing a comment frame every `SSE_HEARTBEAT_SECONDS` bounds it:

```
: keep-alive        ← a comment. Dispatches no event; the client never sees it.
```

The first keepalive after the client goes away raises in `send`, the response
unwinds, `EventSourceResponse` closes the body generator, and its `finally`
deregisters the subscription. **The keepalive interval is the ceiling on how
long an abandoned stream holds its resources** — it is the number to reach for
when idle connections accumulate, not the buffer sizes.

`tests/test_sse_endpoint.py` asserts this against a real socket: a client is
dropped without a close handshake and the hub registration is gone within a
few multiples of the interval.

The better-known reason for a keepalive is still true — nginx, ELB and most
CDNs close a connection idle for 60 seconds, and the browser's automatic
reconnect makes that look like flaky Wi-Fi rather than a timeout — but it is
the less important one.

## The slow client

Every open stream is a buffer somebody has to bound. `EventStreamHub.publish`
is a plain `def` with no awaits: its callers are an event-bus subscriber
running inside somebody's HTTP request, and a fan-out that could suspend on a
client's TCP window would put a phone on a train into the critical path of a
registration that has nothing to do with it.

So each subscriber has its own bounded queue and `publish` uses `put_nowait`.
A queue with no room is resolved rather than waited out, and the two obvious
resolutions are both worse than they look:

| policy | what the client sees |
| --- | --- |
| drop oldest | a view that is silently wrong until a reload it has no reason to do |
| drop newest | the same, and permanently behind rather than transiently |
| **close it** | an `overflow` event, then a reconnect and a refetch |

This hub closes it. The last frame such a stream carries is:

```
event: overflow
data: This stream fell too far behind and was closed. Reconnect and refetch…
```

The buffer therefore sizes the transient a client may fall behind by, not a
tolerance for a permanently slow one. Worst-case memory is
`SSE_CLIENT_BUFFER_EVENTS` × the connection limit × an event, all three of
which are numbers somebody chose.

## The lifetime

`SSE_MAX_STREAM_SECONDS` ends a stream by *finishing* it — a last comment and a
clean end of body — rather than by raising. A completed stream and a cancelled
one are identical to an `EventSource`, which reconnects either way, so the
difference is entirely server-side: a clean end releases the subscription
through the ordinary path, while an exception crossing a body that is already
streaming a 200 is a mid-response connection reset with a traceback for
something that is not an error.

An unbounded connection accumulates whatever the process cannot reclaim
underneath it: a rotated token stays live, and a stream stays pinned to a
replica a deploy is trying to drain. A ceiling turns all of that into a
recurring, ordinary event.

It is checked between frames rather than with `asyncio.timeout`, for the reason
`src/streaming/backpressure.py` gives: a timer armed inside a generator frame
fires wherever the *consuming task* happens to be, which for a generator
suspended at a `yield` is somewhere in the response machinery. The keepalive
guarantees the generator is resumed at least every interval, so a
between-frames check lands within one interval of the deadline.

## The wire format

SSE has no length prefix and no escaping: fields are terminated by line breaks,
so any line break inside a value is a frame boundary rather than a character.
`src/sse/event.py` refuses or normalises the four ways that goes wrong, each of
which is silent on the server:

| input | what a client would do without the guard |
| --- | --- |
| `\n` in `data` | ends the field; a blank line ends the *event* and dispatches the rest as a second one |
| bare `\r` in `data` | splits there — Python's `str.split("\n")` does not |
| empty `data` | the frame is never dispatched: an event with only a name fires no listener |
| NUL in `id` | the field is *ignored*, leaving the client's last-event-id stale after a reconnect |

Note the direction of the third and fourth: both are cases where the protocol
does something reasonable and silent, and the code turns it into an exception
with a traceback.

## Configuration

| setting | default | what it bounds |
| --- | --- | --- |
| `SSE_HEARTBEAT_SECONDS` | 15.0 | proxy idle timeouts, **and** how long an abandoned stream goes unnoticed |
| `SSE_RETRY_MS` | 3000 | how long a client waits before reconnecting |
| `SSE_CLIENT_BUFFER_EVENTS` | 64 | how far one stream may fall behind before it is closed |
| `SSE_MAX_STREAM_SECONDS` | 3600.0 | how long any one connection lives |

## Consuming the stream

```js
const stream = new EventSource("/api/v1/events/stream", { withCredentials: true });

stream.addEventListener("ready", () => refetchState());
stream.addEventListener("user.registered", (e) => apply(JSON.parse(e.data)));
stream.addEventListener("overflow", () => refetchState());
```

Three things a client has to know:

**Refetch on `ready`.** It is sent after the subscription is registered, so
anything published after it arrives is buffered for that connection. There is
no replay, so that is the only point at which a client's state is known to be
current.

**A reconnect is not a resumption.** The browser reconnects automatically, and
events published while it was away are gone. Treat every `ready` as a resync
rather than as a continuation.

**Keepalive comments are invisible.** They never reach a listener; that is what
makes them safe to send at any interval.

`curl -N` shows the raw stream, keepalives included, which is the fastest way
to check that a proxy in front is not buffering the response.

## Adding a stream

1. Publish to a topic. `EventStreamHub.publish(topic, event)` is synchronous
   and total — call it from an event-bus subscriber (`src/sse/bridge.py` is the
   worked example) or from anything else that knows something happened.
2. Subscribe in the route's body generator, not in the handler. The
   subscription has to be released when the *body* ends, since that is what
   knows the client has gone; a context manager entered in the handler has
   already exited by the time the response is returned.
3. Derive the topic from the authenticated caller. The subscription is the
   authorisation; an endpoint that took a topic parameter and checked it would
   be one refactor away from streaming somebody else's activity.
4. Return `EventSourceResponse(sse_stream(...))` so the frames get the headers
   and the generator gets closed.

## What is deliberately not here

**Replay.** No `id:` field is sent and `Last-Event-ID` is not honoured, because
nothing stores past events to replay from — the buffer is per connection and
dies with it. Sending an id would promise a resumption that is not kept, which
is worse than sending none: a client written against it would stop treating
gaps as possible. Durable replay is the outbox's and a broker's job.

**Cross-process fan-out.** The hub reaches the clients connected to *this*
process. With more than one replica, an event published on one reaches only the
streams that replica holds. The seam is `EventStreamHub.publish`, which a
broker subscriber can call on each replica without any stream knowing.

**Client-to-server messages.** SSE has no upstream channel by design. That is
what a WebSocket is for, and it is the next item in `SPEC.md`.
