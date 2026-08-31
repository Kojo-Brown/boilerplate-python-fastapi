"""`/api/v1/ws` — the bidirectional endpoint, and the order its four steps run in.

    authenticate ──▶ accept(subprotocol) ──▶ Connection.run() ──▶ close
       (before)         (echo the tag)         (reader + writer)

Every one of those boundaries is somewhere a WebSocket endpoint is commonly got
wrong, and three of them are ordering rather than logic.

## Authenticate before `accept`, echo after

`accept()` is not "start the handshake", it is "the handshake succeeded". Once
it has been called the transport has no status codes left, only close frames —
and the peer has a socket, a task and a slot in whatever connection limit the
process has, which is exactly what an unauthenticated caller should not get.
So the credential is resolved first, and the subprotocol to echo comes out of
*that* resolution: `accept(subprotocol=...)` has to name something the client
offered, and which one it is depends on how the token arrived. See
`src/ws/auth.py`, which also records what a rejected handshake actually looks
like at the client end.

## A connection must not hold a database session

The obvious dependency is `db: DbSession`, and FastAPI would resolve it happily
— then hold that session, and the pooled connection under it, open for as long
as the WebSocket lives. At the default `WS_MAX_CONNECTION_SECONDS` that is an
hour per connection, so a few hundred idle chat tabs exhaust a pool sized for
requests that take milliseconds, and the symptom is timeouts in unrelated
endpoints.

The database is needed exactly once, to turn a verified `sub` into a row. So
the endpoint takes a *lookup* — a callable that opens a session, reads, and
closes it — and by the time the socket is serving traffic no session is held at
all. This is the one place in the codebase where `Depends(get_db)` would be
wrong, and it is wrong for a reason that has nothing to do with the query.

## Nothing raises out of the handler

`AppException` reaches a JSON envelope because `src/exception_handlers.py` is
registered for HTTP. There is no equivalent for a WebSocket — an exception out
of the handler is a 500 that no client can read and a stack trace attributed to
the ASGI server — so the failure paths here end in a close frame, and the
unexpected one is caught and turned into `CloseCode.INTERNAL_ERROR` with a
fixed message rather than a quoted traceback.

## What this endpoint is for, and what SSE is still better at

If the traffic is server-to-client only, `GET /api/v1/events/stream` is the
better tool and `src/sse/__init__.py` says why. This endpoint exists for the
half SSE has no answer to: messages travelling *up*, from many clients, fanned
out to a room of others.
"""

from __future__ import annotations

import uuid
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, WebSocket
from starlette.websockets import WebSocketDisconnect

from src.config import Settings, settings
from src.database import AsyncSessionLocal
from src.models.user import User
from src.repositories.user import UserRepository
from src.ws.auth import UserLookup, WebSocketAuthError, authenticate
from src.ws.connection import Connection
from src.ws.protocol import CloseCode, close_reason
from src.ws.ratelimit import RateLimiter
from src.ws.rooms import RoomRegistry, room_registry

logger = structlog.get_logger(__name__)

router = APIRouter(tags=["websocket"])

ENDPOINT_PATH = "/ws"


async def lookup_user(user_id: uuid.UUID) -> User | None:
    """Read one user in a session of its own, opened and closed here.

    Deliberately not `Depends(get_db)` — see the module docstring. The session
    is the narrowest thing that will do the job and it is gone before the
    connection starts serving.
    """
    async with AsyncSessionLocal() as session:
        return await UserRepository(session).get(user_id)


def get_user_lookup() -> UserLookup:
    """The lookup the endpoint authenticates with.

    A provider so a test can substitute an in-memory store without a database,
    the same seam `get_user_store` gives the HTTP routes.
    """
    return lookup_user


def get_room_registry() -> RoomRegistry:
    """The process-wide room registry.

    Returns the module singleton rather than building one: two registries in a
    process is a room whose members cannot hear each other, and the symptom —
    messages that arrive for some clients and not others, depending on which
    connection happened to construct which — is far from the cause.
    """
    return room_registry


def get_ws_settings() -> Settings:
    """The settings the endpoint reads its bounds from.

    Provided rather than imported so a test can hand over a `Settings` built
    with a two-message budget or a 50ms idle timeout. The global is frozen
    on purpose (see its docstring), so constructing another is the seam.
    """
    return settings


UserLookupDep = Annotated[UserLookup, Depends(get_user_lookup)]
RoomRegistryDep = Annotated[RoomRegistry, Depends(get_room_registry)]
WsSettingsDep = Annotated[Settings, Depends(get_ws_settings)]


def build_rate_limiter(config: Settings) -> RateLimiter:
    """This connection's inbound budget, from configuration.

    Called per connection, because a `RateLimiter` holds that connection's
    remaining allowance. One shared instance would let a single client's flood
    throttle everybody — the failure the limiter exists to prevent,
    reintroduced by the object meant to prevent it.
    """
    return RateLimiter(
        messages_per_second=config.WS_MESSAGES_PER_SECOND,
        message_burst=config.WS_MESSAGE_BURST,
        bytes_per_second=config.WS_BYTES_PER_SECOND,
        byte_burst=config.WS_BYTE_BURST,
        max_consecutive_violations=config.WS_MAX_RATE_VIOLATIONS,
    )


@router.websocket(ENDPOINT_PATH)
async def websocket_endpoint(
    websocket: WebSocket,
    lookup: UserLookupDep,
    rooms: RoomRegistryDep,
    config: WsSettingsDep,
) -> None:
    """Serve one authenticated WebSocket until it ends."""
    try:
        client = await authenticate(websocket, lookup)
    except WebSocketAuthError as exc:
        # Logged rather than sent: a refusal before `accept` reaches the client
        # as an HTTP 403 with no body, and the close code passed here is
        # discarded by the server. This log line is the only place the reason
        # survives.
        logger.info(
            "ws.handshake_rejected",
            reason=exc.reason,
            path=websocket.url.path,
            client=websocket.client.host if websocket.client else None,
        )
        await websocket.close(code=int(CloseCode.PROTOCOL_ERROR))
        return

    await websocket.accept(subprotocol=client.subprotocol)
    logger.info(
        "ws.connected",
        user=str(client.user.id),
        subprotocol=client.subprotocol,
        expires_at=client.expires_at.isoformat(),
    )

    connection = Connection(
        websocket,
        client,
        rooms,
        build_rate_limiter(config),
        max_rooms=config.WS_MAX_ROOMS_PER_CONNECTION,
        max_message_bytes=config.WS_MAX_MESSAGE_BYTES,
        outbound_buffer=config.WS_OUTBOUND_BUFFER_MESSAGES,
        idle_timeout=config.WS_IDLE_TIMEOUT_SECONDS,
        max_seconds=config.WS_MAX_CONNECTION_SECONDS,
    )
    try:
        await connection.run()
    except WebSocketDisconnect:
        # The peer vanished between `run`'s own handling and here. Ordinary.
        logger.debug("ws.disconnected_during_close", connection=connection.id)
    except Exception:
        # There is no exception handler for this transport, so an escape here
        # is an unlogged 500 the client sees as a reset. A fixed message, not
        # the exception's own: the connection is already authenticated, but a
        # traceback still says more about the process than a peer should learn.
        logger.exception("ws.connection_failed", connection=connection.id)
        with_note = close_reason("internal error")
        try:
            await websocket.close(code=int(CloseCode.INTERNAL_ERROR), reason=with_note)
        except (RuntimeError, WebSocketDisconnect, OSError):
            pass


__all__ = [
    "ENDPOINT_PATH",
    "build_rate_limiter",
    "get_room_registry",
    "get_user_lookup",
    "get_ws_settings",
    "lookup_user",
    "router",
]
