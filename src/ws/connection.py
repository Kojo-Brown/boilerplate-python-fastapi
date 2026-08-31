"""One connection's lifetime: one reader, one writer, and three ways it ends.

    receive loop ──dispatch──▶ RoomRegistry.broadcast ──offer──▶ other connections
         │                                                             │
         └──▶ own outbound queue ──▶ writer task ──▶ socket ◀──────────┘

## Exactly one task ever calls `send`

Two coroutines writing to the same WebSocket is not a race that shows up as a
lost message; ASGI `websocket.send` is not re-entrant and interleaved calls
corrupt the frame stream, which the peer sees as a protocol error and closes.
It is easy to arrive at by accident: a broadcast delivering straight to another
connection's socket would have *that* connection's frames written by whichever
sender happened to publish.

So a connection has one outbound queue and one writer draining it, and nothing
else sends. `offer` — the method rooms deliver through — is a `put_nowait` and
belongs to whoever is publishing; the socket belongs to the writer alone. The
one exception is the close frame, sent by `run` *after* the writer has stopped,
which is why the writer is a child of a scope that exits first.

## The queue is bounded, and a full one is terminal

`WS_OUTBOUND_BUFFER_MESSAGES` is what this connection may fall behind by. Past
that it is closed with `CloseCode.OVERFLOW` rather than having messages dropped
under it, for the reason `src/sse/hub.py` sets out: a client that silently
missed three messages is rendering a view that is wrong until a reload it has
no reason to perform, while a closed connection is a visible, recoverable event
its reconnect handler already knows how to answer.

The queue is created one slot larger than the buffer, and that slot is reserved
for the terminal marker — otherwise the notification that the queue is full
would be the thing there is no room for.

## Three endings, three close codes, and why a timeout is not a `timeout`

A connection ends when the client leaves, when it goes quiet, or when its time
is up — and the last is two different deadlines that a single `asyncio.timeout`
around the read serves:

**The token's own expiry.** An access token is good for
`ACCESS_TOKEN_EXPIRE_MINUTES`; a connection is good for as long as it stays
open. Checking `exp` only at the handshake grants an access that never ends —
a session revoked, a role changed or a password rotated an hour ago is still
live on a socket opened before it. This is the failure mode a WebSocket has and
a request does not, and `CloseCode.TOKEN_EXPIRED` is distinct precisely so a
client knows to refresh rather than redial.

**A ceiling on the connection itself** (`WS_MAX_CONNECTION_SECONDS`), for the
reason the SSE stream has one: a connection that never ends pins a replica a
deploy is trying to drain.

Both are compared against the same monotonic clock and the nearer one wins.
The idle timeout shares the mechanism and is a different question — "is anyone
there?" rather than "may they still be?" — so it gets its own close code.

The timer is `asyncio.timeout` around `receive()` *in the task that awaits it*,
which is the one place that construction is straightforwardly correct: the
cancellation lands on the read and nowhere else. `src/sse/stream.py` explains
at length why the same tool is wrong one layer inside a generator; the
difference is not the tool.

## Rate limiting happens before parsing

`RateLimiter.offer` is consulted with the frame's byte length before
`json.loads` sees it. Parsing is the work the budget exists to protect, and a
limiter applied to the decoded message has already paid for the attack it was
meant to stop.
"""

from __future__ import annotations

import asyncio
import uuid
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any, Final

import structlog
from starlette.types import Message
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

from src.decorators.base import DEFAULT_CLOCK, Clock
from src.structured.scope import TaskScope, WhenScopeExits
from src.ws.auth import AuthenticatedClient
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
from src.ws.ratelimit import RateLimiter
from src.ws.rooms import InvalidRoomName, RoomRegistry, validate_room_name

logger = structlog.get_logger(__name__)

#: Put on the outbound queue to stop the writer. A sentinel object rather than
#: `None`, so that no encodable payload could ever be mistaken for it.
_STOP: Final[object] = object()

#: How long a closing connection waits for the writer to drain what is already
#: queued. Short: the frames that matter here are the handful explaining why the
#: connection is ending, and a client too slow to take those in a second is one
#: the outbound buffer has already made a decision about.
_FLUSH_SECONDS: Final[float] = 1.0


class Connection:
    """A single authenticated WebSocket, from `accept()` to the close frame.

    Satisfies `src/ws/rooms.RoomMember`: rooms deliver to it through `offer`,
    which is synchronous and total, and know nothing else about it.

    Not reusable. One instance per socket, and `run()` is called once.
    """

    def __init__(
        self,
        websocket: WebSocket,
        client: AuthenticatedClient,
        rooms: RoomRegistry,
        limiter: RateLimiter,
        *,
        max_rooms: int,
        max_message_bytes: int,
        outbound_buffer: int,
        idle_timeout: float,
        max_seconds: float,
        clock: Clock = DEFAULT_CLOCK,
    ) -> None:
        """
        Args:
            websocket: Already accepted. Accepting is the endpoint's job
                because the subprotocol to echo is an authentication decision.
            client: Who is on the other end, and until when.
            rooms: The registry this connection joins rooms in.
            limiter: This connection's own inbound budget. One per connection —
                a shared limiter would let one client's flood throttle
                everybody.
            max_rooms: Rooms this connection may be in at once. Bounded because
                each membership is an entry in the registry and a share of
                every broadcast; unbounded, one client can make itself the
                recipient of all traffic in the process.
            max_message_bytes: Inbound frame ceiling. Must not exceed the ASGI
                server's own `--ws-max-size`, which is the one that actually
                stops a large frame being read.
            outbound_buffer: Messages this connection may fall behind by.
            idle_timeout: Seconds without an inbound frame before closing.
            max_seconds: Ceiling on the connection's lifetime.
            clock: Monotonic seconds; a controllable one in tests.

        Raises:
            ValueError: a bound is not positive, or `max_message_bytes` exceeds
                what the limiter's byte burst could ever admit — which would
                make a legal message permanently unaffordable and leave the
                client retrying it until the violation budget disconnected it.
        """
        if max_rooms < 1:
            raise ValueError(f"max_rooms must be at least 1, got {max_rooms}.")
        if max_message_bytes < 1:
            raise ValueError(
                f"max_message_bytes must be at least 1, got {max_message_bytes}."
            )
        if outbound_buffer < 1:
            raise ValueError(
                f"outbound_buffer must be at least 1, got {outbound_buffer}."
            )
        if idle_timeout <= 0:
            raise ValueError(f"idle_timeout must be positive, got {idle_timeout}.")
        if max_seconds <= 0:
            raise ValueError(f"max_seconds must be positive, got {max_seconds}.")
        if max_message_bytes > limiter.byte_capacity:
            raise ValueError(
                f"max_message_bytes ({max_message_bytes}) exceeds the limiter's "
                f"byte burst ({limiter.byte_capacity}): a maximum-size message "
                "could never be admitted."
            )

        self._websocket = websocket
        self._client = client
        self._rooms = rooms
        self._limiter = limiter
        self._max_rooms = max_rooms
        self._max_message_bytes = max_message_bytes
        self._idle_timeout = idle_timeout
        self._clock = clock

        self._id = uuid.uuid4().hex
        self._outbound: asyncio.Queue[str | object] = asyncio.Queue(
            maxsize=outbound_buffer + 1
        )
        self._buffer = outbound_buffer
        self._terminated = False
        #: Set when `offer` ends the connection. The receive loop waits on it
        #: alongside the socket, because an overflow is raised by whichever
        #: *other* connection published into a full queue — not by anything
        #: this connection is about to hear from its own client.
        self._ended = asyncio.Event()
        self._sent = 0
        self._received = 0

        started = clock()
        self._started = started
        #: When the idle timer last restarted. The handshake counts as
        #: activity, so a client that connects and says nothing is closed one
        #: idle interval later rather than immediately.
        self._last_message = started
        lifetime_deadline = started + max_seconds
        # The token's expiry is wall-clock and everything else here is
        # monotonic, so it is converted once, at construction, rather than
        # compared against `datetime.now()` on every pass of the loop — which
        # would make the connection's lifetime follow an NTP correction.
        token_seconds = (client.expires_at - datetime.now(UTC)).total_seconds()
        token_deadline = started + token_seconds
        if token_deadline <= lifetime_deadline:
            self._deadline = token_deadline
            self._deadline_code = CloseCode.TOKEN_EXPIRED
            self._deadline_reason = "access token expired; reconnect with a fresh one"
        else:
            self._deadline = lifetime_deadline
            self._deadline_code = CloseCode.GOING_AWAY
            self._deadline_reason = "connection lifetime reached; reconnect"

        self._close_code = CloseCode.GOING_AWAY
        self._close_note = "server closing"

    @property
    def id(self) -> str:
        """This connection's identifier. Per *connection*, not per user.

        Two tabs are two connections and two ids, which is what makes a log
        line about one of them useful.
        """
        return self._id

    @property
    def user_id(self) -> str:
        return str(self._client.user.id)

    @property
    def pending(self) -> int:
        """Messages queued but not yet written to the socket."""
        return self._outbound.qsize()

    def offer(self, payload: str) -> bool:
        """Queue an already-encoded frame. Never blocks, never raises.

        Returns `False` when this connection has no room left and must be
        dropped from every room; it is terminal, not a condition to retry.

        The payload arrives encoded because a broadcast serialises once for the
        whole room rather than once per member — and, more importantly, because
        a value that cannot be encoded then fails in the *sender's* receive
        loop, where it is that client's error, instead of in each recipient's
        writer, where it would take down every member of the room.
        """
        if self._terminated:
            return False
        if self._outbound.qsize() >= self._buffer:
            self._terminated = True
            # Fits by construction: the reserved slot is spent at most once,
            # and `_terminated` is the record that it has been.
            self._outbound.put_nowait(_STOP)
            self._close_code = CloseCode.OVERFLOW
            self._close_note = "too far behind; reconnect and refetch state"
            self._ended.set()
            return False
        self._outbound.put_nowait(payload)
        return True

    def send_soon(self, message: dict[str, Any]) -> bool:
        """Encode `message` and queue it for this connection alone.

        For frames this connection is the only recipient of — `ready`, `error`,
        acknowledgements. A room's traffic goes through `offer` instead, having
        been encoded once for everybody.
        """
        return self.offer(encode_server_message(message))

    async def run(self) -> None:
        """Serve the connection until it ends, then close it.

        The writer is a child of a scope that exits before the close frame is
        sent, which is what keeps "one task sends" true right to the end: by
        the time `close()` is called, nothing else can be mid-`send`.

        Room membership is released in `finally` rather than after the loop,
        because the ways this returns include the ones that are not returns —
        a client disconnect surfaces as an exception from `receive`, and a
        member left in a room after that is broadcast to forever.
        """
        try:
            async with TaskScope(
                f"ws-connection:{self._id}", on_exit=WhenScopeExits.CANCEL
            ) as scope:
                writer = scope.start_soon(self._write_outbound, name="writer")
                self.send_soon(
                    {
                        "type": ServerMessageType.READY.value,
                        "connection_id": self._id,
                        "user_id": self.user_id,
                        # The client is told when it will be disconnected, so a
                        # refresh can be scheduled instead of discovered.
                        "expires_at": self._client.expires_at.isoformat(),
                    }
                )
                try:
                    await self._receive()
                finally:
                    await self._flush(writer)
        except WebSocketDisconnect:
            # The client left. Not an error, and the only ending that arrives
            # as an exception.
            self._close_code = CloseCode.GOING_AWAY
            self._close_note = "client disconnected"
        finally:
            left = self._rooms.leave_all(self)
            logger.info(
                "ws.connection_closed",
                connection=self._id,
                user=self.user_id,
                code=int(self._close_code),
                rooms=sorted(left),
                received=self._received,
                sent=self._sent,
                seconds=round(self._clock() - self._started, 3),
            )
            await self._close()

    async def _flush(self, writer: asyncio.Task[Any]) -> None:
        """Let the writer finish what is already queued, then stop it.

        Without this the scope's `CANCEL` would end the writer with messages
        still in the queue, and the ones lost would be exactly the last ones
        written: the `error` frame explaining a rate-limit disconnect, or the
        `left` acknowledgement a client is waiting on. Those are the frames a
        closing connection most needs to deliver.

        Bounded, because "drain the queue" against a client that has stopped
        reading is a wait with no end. When the peer is gone the first `send`
        raises and the writer returns immediately, so the timeout is reached
        only by a client that is present and slow — which is the case the
        buffer already has an answer for.
        """
        if not self._terminated:
            # Room is guaranteed: `offer` marks the connection terminated at
            # `qsize() >= buffer`, and the queue holds one more than that.
            self._terminated = True
            self._outbound.put_nowait(_STOP)
        # `asyncio.wait` rather than `wait_for`: it reports a timeout by
        # returning an empty set instead of raising, and — the part that
        # matters — it does not cancel the task it was waiting on. `wait_for`
        # would, which for a writer mid-`send` is a half-written frame on a
        # socket that is about to carry a close. A cancellation of *this* task
        # still propagates, which is correct: whoever cancelled the connection
        # is not waiting for its flush.
        done, _ = await asyncio.wait({writer}, timeout=_FLUSH_SECONDS)
        if not done:
            # The scope cancels it next.
            logger.debug("ws.flush_incomplete", connection=self._id)

    async def _close(self) -> None:
        """Send the close frame, if the socket is still there to send it on."""
        if self._websocket.client_state is WebSocketState.DISCONNECTED:
            return
        if self._websocket.application_state is WebSocketState.DISCONNECTED:
            return
        try:
            await self._websocket.close(
                code=int(self._close_code), reason=close_reason(self._close_note)
            )
        except (RuntimeError, WebSocketDisconnect, OSError):
            # The peer is already gone, or starlette has recorded the close
            # this connection is only now getting to. Nothing left to say.
            logger.debug("ws.close_frame_undeliverable", connection=self._id)

    async def _write_outbound(self) -> None:
        """Drain the outbound queue to the socket. The only sender.

        Never raises: a failed write means the peer is gone, which the receive
        side is about to discover on its own, and letting it out here would
        take down the task scope with an exception for an ordinary disconnect.
        """
        while True:
            item = await self._outbound.get()
            if not isinstance(item, str):
                return
            try:
                await self._websocket.send_text(item)
            except (WebSocketDisconnect, RuntimeError, OSError) as exc:
                logger.debug(
                    "ws.write_failed", connection=self._id, error=type(exc).__name__
                )
                return
            self._sent += 1

    async def _receive(self) -> None:
        """Read and dispatch until a deadline, the client, or a policy ends it.

        Three things end this loop and only one of them is a frame arriving,
        which is why the read is raced rather than simply awaited:

        * a message or a disconnect, from `receive()`;
        * a deadline, from the timeout on the wait;
        * **an overflow, raised by another task entirely.** A broadcast
          published by some *other* connection is what fills this one's queue,
          and the connection it terminates may be a read-only participant that
          will never send anything again. Waiting for its next frame to notice
          would leave it open until the idle timeout — holding a socket the
          server has already decided to close, which on a busy room is one
          abandoned connection per slow client rather than none.

        The pending read is cancelled on the way out. Unlike the equivalent in
        `src/sse/heartbeat.py` — where cancelling a pending `__anext__` is how
        an event goes missing — nothing here is lost by it: the loop unwinds
        only when the connection is ending, and a frame in flight at that point
        is one the client gets no answer to either way.
        """
        ended = asyncio.ensure_future(self._ended.wait())
        reading: asyncio.Task[Message] | None = None
        try:
            while True:
                budget, code, note = self._next_deadline()
                if budget <= 0:
                    self._close_code, self._close_note = code, note
                    return

                if reading is None:
                    reading = asyncio.ensure_future(self._websocket.receive())
                done, _ = await asyncio.wait(
                    {reading, ended},
                    timeout=budget,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if not done:
                    # Recomputed rather than assumed: the deadline that fired is
                    # whichever is now in the past, and between arming the timer
                    # and it firing the *other* one may have become the nearer.
                    self._close_code, self._close_note = self._next_deadline()[1:]
                    return
                if ended in done:
                    # `offer` terminated this connection while it was waiting.
                    # The close code it chose is already recorded.
                    return

                # Raises `WebSocketDisconnect` for a peer that vanished without
                # the courtesy of a close frame, which `run` handles.
                message = reading.result()
                reading = None
                if not self._on_message(message):
                    return
        finally:
            for task in (reading, ended):
                if task is not None and not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task

    def _on_message(self, message: Message) -> bool:
        """Handle one ASGI receive message. `False` to end the connection."""
        if message["type"] == "websocket.disconnect":
            self._close_code = CloseCode.GOING_AWAY
            self._close_note = "client disconnected"
            return False

        text = message.get("text")
        if text is None:
            self._close_code = CloseCode.PROTOCOL_ERROR
            self._close_note = "binary frames are not accepted; send JSON text"
            return False

        self._received += 1
        self._last_message = self._clock()
        if not self._admit(text):
            return False
        # An overflow caused by handling this very message — the writer has
        # already been stopped by the sentinel, so there is nothing left to
        # send and no point reading more.
        return not self._terminated

    def _next_deadline(self) -> tuple[float, CloseCode, str]:
        """Seconds until the next thing that ends this connection, and which."""
        now = self._clock()
        idle_at = self._last_message + self._idle_timeout
        if idle_at <= self._deadline:
            return (
                idle_at - now,
                CloseCode.IDLE_TIMEOUT,
                f"no message for {self._idle_timeout:g}s",
            )
        return self._deadline - now, self._deadline_code, self._deadline_reason

    def _admit(self, text: str) -> bool:
        """Charge, decode and dispatch one frame. `False` to end the connection."""
        size = len(text.encode("utf-8"))
        decision = self._limiter.offer(size)
        if not decision.allowed:
            if decision.disconnect:
                logger.warning(
                    "ws.rate_limit_disconnect",
                    connection=self._id,
                    user=self.user_id,
                    violations=self._limiter.violations,
                )
                self._close_code = CloseCode.RATE_LIMITED
                self._close_note = "too many messages; slow down before reconnecting"
                return False
            self.send_soon(
                error_message(
                    ErrorCode.RATE_LIMITED,
                    "Message rate exceeded; this message was not processed.",
                    retry_after=decision.retry_after,
                )
            )
            return True

        try:
            message = decode_client_message(text, max_bytes=self._max_message_bytes)
        except MalformedMessage as exc:
            self.send_soon(error_message(exc.code, exc.message))
            return True

        self._dispatch(message)
        return True

    def _dispatch(self, message: ClientMessage) -> None:
        """Act on one decoded message. Synchronous — nothing here waits.

        Every handler is a queue operation or a dictionary lookup, and keeping
        them that way is what makes a broadcast cost the sender's loop nothing
        beyond the fan-out itself.
        """
        if message.type is ClientMessageType.PING:
            self.send_soon({"type": ServerMessageType.PONG.value})
            return

        # Every remaining type names a room; the decoder guaranteed a string.
        assert message.room is not None
        try:
            room = validate_room_name(message.room)
        except InvalidRoomName as exc:
            self.send_soon(error_message(ErrorCode.INVALID_ROOM, str(exc)))
            return

        match message.type:
            case ClientMessageType.JOIN:
                self._join(room)
            case ClientMessageType.LEAVE:
                self._leave(room)
            case ClientMessageType.PUBLISH:
                self._publish(room, message.data)
            case _:  # pragma: no cover - ClientMessageType is exhausted above
                raise AssertionError(f"unhandled message type {message.type!r}")

    def _join(self, room: str) -> None:
        current = self._rooms.rooms_of(self)
        if room not in current and len(current) >= self._max_rooms:
            self.send_soon(
                error_message(
                    ErrorCode.ROOM_LIMIT_REACHED,
                    f"This connection may be in at most {self._max_rooms} rooms.",
                )
            )
            return
        members = self._rooms.join(room, self)
        self.send_soon(
            {
                "type": ServerMessageType.JOINED.value,
                "room": room,
                "members": members,
            }
        )

    def _leave(self, room: str) -> None:
        if not self._rooms.leave(room, self):
            self.send_soon(
                error_message(
                    ErrorCode.NOT_IN_ROOM, f"This connection is not in {room!r}."
                )
            )
            return
        self.send_soon({"type": ServerMessageType.LEFT.value, "room": room})

    def _publish(self, room: str, data: Any) -> None:
        """Fan `data` out to the room's other members.

        Membership is the authorisation: a connection may publish only to a
        room it has joined, so there is no separate permission to keep in step
        with the one `join` already applied.

        The sender is excluded. It has the payload, and an echo would double
        the traffic of the busiest case — a room where everyone is typing — to
        tell each client something it just said. There is deliberately **no
        delivery receipt**: what this could report is how many queues accepted
        the message, and a client would read that as how many people saw it.
        """
        if not self._rooms.contains(room, self):
            self.send_soon(
                error_message(
                    ErrorCode.NOT_IN_ROOM,
                    f"Join {room!r} before publishing to it.",
                )
            )
            return

        body = {
            "type": ServerMessageType.MESSAGE.value,
            "room": room,
            "from": self.user_id,
            "sent_at": datetime.now(UTC).isoformat(),
            "data": data,
        }
        try:
            # Encoded here, in the sender's loop, and once for the whole room.
            # `json.loads` accepts `NaN` and `Infinity` where `json.dumps`
            # refuses them, so a payload that arrived legally can still be
            # unencodable — and the decoder rejects those precisely so this
            # cannot happen. Kept as a guard rather than an assumption because
            # the alternative failure is every member of the room losing their
            # connection over one sender's payload.
            payload = encode_server_message(body)
        except ValueError as exc:  # pragma: no cover - decoder rejects these first
            self.send_soon(
                error_message(
                    ErrorCode.INVALID_FIELD, f"Payload cannot be serialised: {exc}"
                )
            )
            return

        delivered = self._rooms.broadcast(room, payload, exclude=self)
        logger.debug(
            "ws.published",
            connection=self._id,
            room=room,
            bytes=len(payload),
            delivered=delivered,
        )


__all__ = ["Connection"]
