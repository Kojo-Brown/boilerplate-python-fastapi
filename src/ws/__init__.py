"""WebSockets: the transport you reach for only when messages travel upward.

`src/sse` already carries server-to-client events over ordinary HTTP, with the
browser's own reconnect logic and no framing library. Everything a WebSocket
adds over that costs something, so the case for one is narrow and specific: a
client that needs to **send**, and a fan-out where one client's message reaches
others. Chat, collaborative editing, presence. If the traffic only goes down,
`GET /api/v1/events/stream` is the better endpoint and `src/sse/__init__.py`
argues why.

What the upgrade costs is that four things HTTP was doing for you stop:

**Authentication has nowhere to live.** A browser's `WebSocket` constructor
cannot set a header, so `Authorization: Bearer …` is unavailable to the clients
this is for. `auth.py` is that problem and the three answers to it, one of
which — a token in the query string — this endpoint refuses.

**A credential now outlives its use.** A request finishes long before its token
expires; a connection routinely does not. Verifying `exp` only at the handshake
grants an access that never ends, so a connection carries its token's expiry as
a deadline. See `connection.py`.

**There is no request to rate limit.** The whole connection is one request, so
the per-address middleware in `src/limiter.py` counts it once and never again,
however many messages travel down it. `ratelimit.py` is a per-connection budget
in its place — and, more to the point, the three tempting ways of applying one
that make the problem worse.

**Every peer is now a publisher.** A broadcast runs inside *some other client's*
receive loop, so anything that could block or fail while delivering to one
member is a way for one participant to pace or break the room. `rooms.py`
holds the same "publishing never blocks and never fails" invariant as
`src/sse/hub.py`, for a sharper reason.

Module by module:

| module         | question it answers                                          |
| -------------- | ------------------------------------------------------------ |
| `auth.py`      | how a browser presents a credential, and when it is checked   |
| `protocol.py`  | what a frame carries, and what a close frame may say          |
| `ratelimit.py` | what one connection may send, and what happens when it is over |
| `rooms.py`     | who receives a broadcast, and what a slow member costs        |
| `connection.py`| one socket's lifetime: one reader, one writer, three endings  |

`/api/v1/ws` is the endpoint they assemble into.

## What is not here

**Cross-process fan-out.** The registry reaches the connections held by *this*
process. With more than one replica, a message published on one reaches only
the members connected there. The seam is `RoomRegistry.broadcast`, which a
broker subscriber can call on each replica without any connection knowing —
the same seam, and the same open item, as `EventStreamHub.publish`.

**History and replay.** A client that reconnects has missed whatever was
published while it was away, and nothing here stores it. `ready` carries no
cursor for the same reason `src/sse` sends no `id:`: an identifier the server
cannot honour later is a promise of resumption that loses messages instead of
admitting to a gap.

**Room authorisation beyond authentication.** Any authenticated caller may join
any well-formed room name. `rooms.py` says where a policy goes and why it is
not guessed at here.

**Presence.** Nobody is told that somebody joined or left; `joined` carries a
member count to the joiner alone. Presence is a broadcast per membership change
and a state to reconcile after every reconnect, which is a feature rather than
a line.

See `docs/websockets.md`.
"""

from src.ws.auth import (
    AUTH_SUBPROTOCOL,
    AuthenticatedClient,
    UserLookup,
    WebSocketAuthError,
    authenticate,
    extract_credential,
)
from src.ws.connection import Connection
from src.ws.protocol import (
    ClientMessage,
    ClientMessageType,
    CloseCode,
    ErrorCode,
    MalformedMessage,
    ServerMessageType,
    close_reason,
    decode_client_message,
    encode_server_message,
    error_message,
)
from src.ws.ratelimit import Decision, RateLimiter, TokenBucket
from src.ws.rooms import (
    InvalidRoomName,
    RoomMember,
    RoomRegistry,
    room_registry,
    validate_room_name,
)

__all__ = [
    "AUTH_SUBPROTOCOL",
    "AuthenticatedClient",
    "ClientMessage",
    "ClientMessageType",
    "CloseCode",
    "Connection",
    "Decision",
    "ErrorCode",
    "InvalidRoomName",
    "MalformedMessage",
    "RateLimiter",
    "RoomMember",
    "RoomRegistry",
    "ServerMessageType",
    "TokenBucket",
    "UserLookup",
    "WebSocketAuthError",
    "authenticate",
    "close_reason",
    "decode_client_message",
    "encode_server_message",
    "error_message",
    "extract_credential",
    "room_registry",
    "validate_room_name",
]
