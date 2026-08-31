# WebSockets

`GET /api/v1/ws` — an authenticated, bidirectional connection with rooms and a
per-connection rate limit. Implementation in `src/ws/`.

**Reach for this only when messages travel upward.** `src/sse` already carries
server-to-client events over ordinary HTTP, with the browser's own reconnect
logic and no framing library, and `docs/server-sent-events.md` makes that case.
A WebSocket earns its cost when a client needs to *send*, and when one client's
message has to reach others: chat, collaborative editing, presence. If the
traffic only goes down, use the event stream.

What the upgrade costs is that four things HTTP was doing for you stop working,
and each is a section below.

## The shape

```
authenticate ──▶ accept(subprotocol) ──▶ Connection.run() ──▶ close
   (before)         (echo the tag)      reader + writer + rooms
```

| module                | question it answers                                          |
| --------------------- | ------------------------------------------------------------ |
| `src/ws/auth.py`      | how a browser presents a credential, and when it is checked   |
| `src/ws/protocol.py`  | what a frame carries, and what a close frame may say          |
| `src/ws/ratelimit.py` | what one connection may send, and what happens when it is over |
| `src/ws/rooms.py`     | who receives a broadcast, and what a slow member costs        |
| `src/ws/connection.py`| one socket's lifetime: one reader, one writer, three endings  |

## Authentication has nowhere to live

```js
const ws = new WebSocket(url, protocols)   // that is the entire API
```

There is no third argument. A browser's `WebSocket` constructor **cannot set a
request header**, so `Authorization: Bearer …` — the scheme every other route
here uses — is unavailable to exactly the clients this endpoint is for. There
are three ways around that and this endpoint takes the third.

**A token in the query string** (`wss://host/ws?token=eyJ…`) works everywhere
and writes a live credential into the one part of a request that everything
records: the access log, every proxy in between, browser history, and the
`Referer` of any page that links onward. **Not accepted here**, and
`tests/test_ws_auth.py` pins the refusal.

**A first message after `accept()`** inverts the property that makes
authentication useful: the connection exists before anyone has proven anything,
so an unauthenticated peer holds a socket and a task for the whole grace
period. It also discards the handshake's own failure channel — before
`accept()` the server can still answer with an HTTP status; after it, only
close codes.

**`Sec-WebSocket-Protocol`**, which is what this endpoint uses. The
constructor's second argument is sent as a request *header*, and the browser
will send whatever strings it is given:

```js
const ws = new WebSocket(url, ["bearer.auth.v1", accessToken])
```

The server **must** select one of the offered protocols or the handshake fails,
and it selects the *tag* — never the token, which would echo the credential back
in a response header. A JWT is a legal subprotocol token (base64url plus `.`),
which is the only reason this works without further encoding.

Non-browser clients can set headers, so `Authorization` is accepted too and
wins when both are present.

### What a rejected handshake looks like

Two facts, measured against uvicorn 0.51 and starlette 1.3 rather than assumed:

* **The close code is discarded.** `await websocket.close(code=4401)` before
  `accept()` delivers 4401 to nobody; the handshake fails with **HTTP 403**
  whatever code was passed.
* **The denial-response extension does not survive the round trip.** ASGI 2.4's
  `websocket.http.response` is advertised and starlette implements
  `send_denial_response`, which would allow the same 401 JSON envelope the rest
  of this API returns. The bytes do reach a plain HTTP client — a raw `httpx`
  request sees the 401 and the envelope — but uvicorn also logs `ASGI callable
  returned without completing handshake`, and the reference Python client fails
  with `InvalidMessage: did not receive a valid HTTP response` instead of
  reporting the status. An error channel real clients cannot read is not an
  error channel.

So a refusal is a bare 403, and the reason is written to the server's log
(`ws.handshake_rejected`) because that is the only place it survives.

## A credential now outlives its use

A request finishes long before its token expires. A connection routinely does
not — the default `ACCESS_TOKEN_EXPIRE_MINUTES` is 30 and a socket may be held
for hours. **An endpoint that verifies `exp` only at the handshake grants an
access that never ends**: a role changed or a session revoked an hour ago is
still live on a connection opened before it.

So a connection carries its token's expiry as a deadline and closes with
`4401 TOKEN_EXPIRED` when it passes, which is a distinct code precisely because
the remedy is distinct — refresh, then reconnect. `ready` carries `expires_at`
so a client can schedule that rather than discover it.

`WS_MAX_CONNECTION_SECONDS` is a second, longer ceiling, for the reason the SSE
stream has one: a connection that never ends pins a replica a deploy is trying
to drain. The nearer deadline wins.

## There is no request to rate limit

The whole connection is one request, so the per-address limiter in
`src/limiter.py` counts it once and never again, however many messages travel
down it. `src/ws/ratelimit.py` is a per-connection budget in its place. Three
things about it matter more than the bucket arithmetic:

**It never sleeps.** `await asyncio.sleep(retry_after)` in the receive loop
reads like backpressure and is its opposite — the frames are already in the
server's buffers, so sleeping does not slow the sender down, it stops draining
what the sender is still filling.

**It never queues the overflow.** Buffering rejected messages to replay later
turns a rate limit into a memory limit and delivers a burst of stale messages at
a time nobody asked for. A rejected message is rejected; the client is told, with
the seconds that would have helped, and decides whether it is still worth
sending.

**Two dimensions.** A limit on messages per second is defeated by
one-megabyte messages; a limit on bytes per second is defeated by a flood of
`{}`. Both are bounded and a message must fit both.

One rejection is a client that misjudged its rate. `WS_MAX_RATE_VIOLATIONS`
*consecutive* rejections — an accepted message resets the count — is a client
that is not reading its errors, and it is closed with `4429`.

## Every peer is now a publisher

A broadcast runs inside *some other client's* receive loop. If delivering to a
member could suspend on that member's socket, one participant on hotel wifi
would pace the whole room, and every other member's inbound handling with it.

`RoomRegistry.broadcast` is therefore a plain `def` with no awaits and no
exceptions — the same invariant `src/sse/hub.py` holds, for a sharper reason.
Delivery is an offer into the recipient's own bounded queue, and a member with
no room left is removed from every room and closed with `4430 OVERFLOW` rather
than being waited for or silently skipped. A client that silently missed three
messages renders a view that is wrong until a reload it has no reason to
perform; a closed connection is a visible event its reconnect handler already
knows how to answer.

One connection has one outbound queue and **one writer task**, and nothing else
sends. Two coroutines writing to one WebSocket is not a lost message — ASGI
`websocket.send` is not re-entrant and interleaved calls corrupt the frame
stream, which the peer sees as a protocol error.

## The wire format

Client to server: a JSON **object** in a **text** frame, with a `type`.

```json
{"type": "join",    "room": "lobby"}
{"type": "leave",   "room": "lobby"}
{"type": "publish", "room": "lobby", "data": {"body": "hello"}}
{"type": "ping"}
```

A binary frame closes the connection with `4400`: `send(str)` and `send(blob)`
are different opcodes, and a client reaching this endpoint over the wrong one
has a bug a lenient server would hide.

`ping` is not redundant with the protocol's own ping. **WebSocket ping/pong is
handled by the ASGI server and has no ASGI message type at all**, so an
application cannot observe a pong — or send a ping. A client that wants to prove
*this endpoint* is answering, rather than the load balancer in front of it, has
to ask in band.

Server to client:

```json
{"type": "ready",  "connection_id": "…", "user_id": "…", "expires_at": "…"}
{"type": "joined", "room": "lobby", "members": 3}
{"type": "left",   "room": "lobby"}
{"type": "message","room": "lobby", "from": "…", "sent_at": "…", "data": …}
{"type": "pong"}
{"type": "error",  "code": "rate_limited", "message": "…", "retry_after": 1.2}
```

`error` is an ordinary message, not a close: one bad frame is a bug in one of
the client's code paths, not grounds to tear down the rest of its work. The
codes are a closed vocabulary (`ErrorCode`) so a client can branch on them
without matching on prose, and `retry_after` appears only when waiting is
actually the remedy.

**A publisher is not echoed to and there is no delivery receipt.** The sender
already has the payload, and what a receipt could report is how many queues
accepted the message — which a client would read as how many people saw it.

One decoder detail that is not stylistic: `json.loads` accepts `NaN`,
`Infinity` and `-Infinity`, and `json.dumps(allow_nan=False)` refuses them. Left
alone that asymmetry is a live fault — the value parses at the sender and fails
at the *broadcast*, which is one client's payload costing a whole room their
connections. They are refused at the decoder, where the error still belongs to
the client that sent it.

### Close codes

| code | meaning | what the client should do |
| ---- | ------- | ------------------------- |
| 1001 | shutdown, or `WS_MAX_CONNECTION_SECONDS` | reconnect immediately |
| 1011 | unexpected server failure | reconnect with backoff |
| 4400 | binary frame or undecodable text | fix the client |
| 4401 | the access token expired | **refresh**, then reconnect |
| 4408 | idle past `WS_IDLE_TIMEOUT_SECONDS` | reconnect when there is something to say |
| 4429 | ignored the rate limit | pause, fix the send rate |
| 4430 | fell too far behind | reconnect and refetch state |

1008 (policy violation) is deliberately unused: it is one code for every reason,
so a client could not tell "you flooded us" from "your token expired" — and
those want opposite reconnect behaviour.

A close frame's payload is at most 125 bytes, two of which are the code. An
over-long reason is not truncated, it makes the frame invalid, so `close_reason`
enforces the 123-byte budget on a character boundary.

## Configuration

See `.env.example`. Three of these are easy to set wrongly:

* `WS_MAX_MESSAGE_BYTES` is **not** the real ceiling on an inbound frame. The
  ASGI server reads and buffers a message before application code sees it, so
  uvicorn's `--ws-max-size` (16 MiB by default) is what stops a large frame.
  Lower both, together.
* `WS_BYTE_BURST` must be at least `WS_MAX_MESSAGE_BYTES`, or a maximum-size
  message is permanently unaffordable and the client retries it until the
  violation budget disconnects it. `Connection` refuses to be constructed
  otherwise rather than discovering this under load.
* `WS_IDLE_TIMEOUT_SECONDS` is not a liveness check — see `ping` above.

## Consuming it from a browser

```js
const ws = new WebSocket("wss://host/api/v1/ws", ["bearer.auth.v1", accessToken])

ws.onmessage = (event) => {
  const frame = JSON.parse(event.data)
  switch (frame.type) {
    case "ready":   refetchState(); scheduleRefresh(frame.expires_at); break
    case "message": apply(frame.room, frame.data);                     break
    case "error":   console.warn(frame.code, frame.message);           break
  }
}

ws.onopen  = () => ws.send(JSON.stringify({type: "join", room: "lobby"}))
ws.onclose = (event) => {
  if (event.code === 4401) refreshTokenThenReconnect()
  else if (event.code !== 4400) reconnectWithBackoff()
}
```

Unlike an `EventSource`, a `WebSocket` does **not** reconnect on its own. The
close-code table above is what the reconnect logic you have to write should
branch on. Re-joining rooms after a reconnect is safe: `join` is idempotent, and
deliberately so, precisely because that is what a reconnect handler wants to do.

## What is deliberately not here

**Cross-process fan-out.** The registry reaches the connections held by *this*
process. With more than one replica, a message published on one reaches only
the members connected there. The seam is `RoomRegistry.broadcast`, which a
broker subscriber can call on each replica without any connection knowing —
the same seam, and the same open item, as `EventStreamHub.publish`.

**History and replay.** A client that reconnects has missed whatever was
published while it was away, and nothing stores it. `ready` carries no cursor
for the reason `src/sse` sends no `id:`: an identifier the server cannot honour
later is a promise of resumption that loses messages rather than admitting to a
gap. Durable replay is a broker's job — see the Redis Streams and Kafka items in
`SPEC.md`.

**Room authorisation beyond authentication.** Any authenticated caller may join
any well-formed room name. A deployment where that is not true needs a policy
keyed on what its rooms actually mean — an organisation id, a document's ACL —
which is a question about the domain, not about fan-out. The seam is the
endpoint, between `validate_room_name` and `join`.

**Presence.** Nobody is told that somebody joined or left; `joined` carries a
member count to the joiner alone. Presence is a broadcast per membership change
plus a state to reconcile after every reconnect, which is a feature rather than
a line.

**A connection limit.** Nothing bounds how many sockets one account may open.
Each costs a task, a queue and a share of every broadcast to its rooms, all of
which are bounded per connection — but the count is not.
