"""One connection's behaviour, over a socket that is a queue and a list.

The endpoint suite (`tests/test_ws_endpoint.py`) runs a real uvicorn and proves
the wiring. This one substitutes the socket, because the questions here are
about *decisions* — which close code, which error, what happens to a room when
a member overflows — and each of them is a fixture setup rather than a race to
provoke over TCP.

The clock is the real monotonic one. `asyncio.timeout` measures loop time, so a
manual clock would disagree with the timer the code under test actually arms;
the deadline tests use durations of a few tens of milliseconds instead.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from starlette.websockets import WebSocketDisconnect, WebSocketState

from src.auth.utils import AccessTokenClaims
from src.models.user import User
from src.ws.auth import AuthenticatedClient
from src.ws.connection import Connection
from src.ws.protocol import CloseCode, ErrorCode, ServerMessageType
from src.ws.ratelimit import RateLimiter
from src.ws.rooms import RoomRegistry

#: Short enough that a deadline test finishes quickly, long enough that it is
#: not reached by a test doing several round trips first.
QUICK = 0.08

#: Generous against QUICK: the assertion is *that* the deadline ends the
#: connection, not the millisecond it does so.
PATIENCE = 5.0


class ScriptedSocket:
    """A `WebSocket` with a queue for inbound frames and a list for outbound.

    `receive()` blocks on an empty queue exactly as the real one does, which is
    what lets the idle and lifetime deadlines actually fire rather than racing
    an immediate `StopIteration`.
    """

    def __init__(self) -> None:
        self.inbound: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.sent: list[str] = []
        self.closed: tuple[int, str] | None = None
        self.client_state = WebSocketState.CONNECTED
        self.application_state = WebSocketState.CONNECTED
        #: Set to hold the writer mid-flight, so a test can watch the outbound
        #: queue fill without the writer draining it.
        self.writer_gate: asyncio.Event | None = None

    async def receive(self) -> dict[str, Any]:
        return await self.inbound.get()

    async def send_text(self, text: str) -> None:
        if self.writer_gate is not None:
            await self.writer_gate.wait()
        self.sent.append(text)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.closed = (code, reason)
        self.application_state = WebSocketState.DISCONNECTED

    # --- driving it -------------------------------------------------------

    def client_sends(self, **message: Any) -> None:
        self.inbound.put_nowait(
            {"type": "websocket.receive", "text": json.dumps(message)}
        )

    def client_sends_raw(self, text: str) -> None:
        self.inbound.put_nowait({"type": "websocket.receive", "text": text})

    def client_sends_binary(self, data: bytes = b"\x01") -> None:
        self.inbound.put_nowait({"type": "websocket.receive", "bytes": data})

    def client_disconnects(self) -> None:
        self.inbound.put_nowait({"type": "websocket.disconnect", "code": 1000})

    # --- reading it -------------------------------------------------------

    @property
    def frames(self) -> list[dict[str, Any]]:
        return [json.loads(text) for text in self.sent]

    def of_type(self, kind: ServerMessageType) -> list[dict[str, Any]]:
        return [frame for frame in self.frames if frame["type"] == kind.value]


def authenticated(user: User, *, expires_in: float = 3600.0) -> AuthenticatedClient:
    return AuthenticatedClient(
        user=user,
        claims=AccessTokenClaims(
            subject=uuid.UUID(str(user.id)),
            email=user.email,
            role=user.role,
            expires_at=datetime.now(UTC) + timedelta(seconds=expires_in),
        ),
        subprotocol=None,
    )


def limiter(**overrides: float) -> RateLimiter:
    values: dict[str, float] = {
        "messages_per_second": 1000,
        "message_burst": 1000,
        "bytes_per_second": 1_000_000,
        "byte_burst": 1_000_000,
        "max_consecutive_violations": 5,
    }
    values.update(overrides)
    return RateLimiter(
        messages_per_second=values["messages_per_second"],
        message_burst=int(values["message_burst"]),
        bytes_per_second=values["bytes_per_second"],
        byte_burst=int(values["byte_burst"]),
        max_consecutive_violations=int(values["max_consecutive_violations"]),
    )


@pytest.fixture
def rooms() -> RoomRegistry:
    return RoomRegistry()


@pytest.fixture
def socket() -> ScriptedSocket:
    return ScriptedSocket()


def build(
    socket: ScriptedSocket,
    user: User,
    rooms: RoomRegistry,
    *,
    expires_in: float = 3600.0,
    limits: RateLimiter | None = None,
    **overrides: Any,
) -> Connection:
    settings: dict[str, Any] = {
        "max_rooms": 4,
        "max_message_bytes": 4096,
        "outbound_buffer": 32,
        "idle_timeout": 30.0,
        "max_seconds": 3600.0,
    }
    settings.update(overrides)
    return Connection(
        socket,  # type: ignore[arg-type]
        authenticated(user, expires_in=expires_in),
        rooms,
        limits if limits is not None else limiter(),
        **settings,
    )


async def serve(connection: Connection) -> None:
    """Run a connection whose inbound frames are already queued."""
    await asyncio.wait_for(connection.run(), timeout=PATIENCE)


class TestConstruction:
    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("max_rooms", 0),
            ("max_message_bytes", 0),
            ("outbound_buffer", 0),
            ("idle_timeout", 0.0),
            ("max_seconds", 0.0),
        ],
    )
    def test_a_bound_that_is_not_positive_is_refused(
        self,
        socket: ScriptedSocket,
        mock_user: User,
        rooms: RoomRegistry,
        field: str,
        value: float,
    ) -> None:
        with pytest.raises(ValueError, match=field):
            build(socket, mock_user, rooms, **{field: value})  # type: ignore[arg-type]

    def test_a_message_ceiling_above_the_byte_burst_is_refused(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """Otherwise a legal message is permanently unaffordable.

        The client would retry it, be refused every time for a reason no wait
        fixes, and be disconnected by the violation budget — for sending
        exactly what it was told it could send.
        """
        with pytest.raises(ValueError, match="could never be admitted"):
            build(
                socket,
                mock_user,
                rooms,
                max_message_bytes=5000,
                limits=limiter(byte_burst=4096),
            )


class TestTheOpeningFrame:
    async def test_ready_is_the_first_thing_sent(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms))

        assert socket.frames[0]["type"] == ServerMessageType.READY.value

    async def test_ready_carries_the_deadline_the_client_has_to_plan_around(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """So a refresh can be scheduled rather than discovered by a close."""
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms, expires_in=60))

        ready = socket.frames[0]
        assert ready["user_id"] == str(mock_user.id)
        assert ready["connection_id"]
        expires = datetime.fromisoformat(ready["expires_at"])
        assert 0 < (expires - datetime.now(UTC)).total_seconds() <= 60


class TestPing:
    async def test_a_ping_is_answered_in_band(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """The protocol's own ping/pong has no ASGI message type at all.

        An application cannot observe a pong, which is exactly why a client
        that wants to prove *this endpoint* is answering has to ask here.
        """
        socket.client_sends(type="ping")
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms))

        assert socket.of_type(ServerMessageType.PONG)


class TestRooms:
    async def test_joining_is_acknowledged_with_the_member_count(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        socket.client_sends(type="join", room="lobby")
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms))

        assert socket.of_type(ServerMessageType.JOINED) == [
            {"type": "joined", "room": "lobby", "members": 1}
        ]

    async def test_an_invalid_room_name_is_an_error_not_a_close(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        socket.client_sends(type="join", room="Lobby Room")
        socket.client_sends(type="ping")
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms))

        assert socket.of_type(ServerMessageType.ERROR)[0]["code"] == (
            ErrorCode.INVALID_ROOM.value
        )
        # Still serving: one bad frame is a bug in one code path.
        assert socket.of_type(ServerMessageType.PONG)

    async def test_the_room_limit_is_enforced(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        for i in range(3):
            socket.client_sends(type="join", room=f"room{i}")
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms, max_rooms=2))

        assert len(socket.of_type(ServerMessageType.JOINED)) == 2
        assert socket.of_type(ServerMessageType.ERROR)[0]["code"] == (
            ErrorCode.ROOM_LIMIT_REACHED.value
        )

    async def test_rejoining_a_room_does_not_count_against_the_limit(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """A client re-joining after a reconnect must not be refused for it."""
        for _ in range(4):
            socket.client_sends(type="join", room="lobby")
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms, max_rooms=1))

        assert len(socket.of_type(ServerMessageType.JOINED)) == 4
        assert socket.of_type(ServerMessageType.ERROR) == []

    async def test_leaving_is_acknowledged(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        socket.client_sends(type="join", room="lobby")
        socket.client_sends(type="leave", room="lobby")
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms))

        assert socket.of_type(ServerMessageType.LEFT) == [
            {"type": "left", "room": "lobby"}
        ]

    async def test_leaving_a_room_never_joined_is_an_error(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        socket.client_sends(type="leave", room="lobby")
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms))

        assert socket.of_type(ServerMessageType.ERROR)[0]["code"] == (
            ErrorCode.NOT_IN_ROOM.value
        )

    async def test_disconnecting_releases_every_membership(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """A member left behind is broadcast to for the life of the process."""
        socket.client_sends(type="join", room="a")
        socket.client_sends(type="join", room="b")
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms))

        assert rooms.rooms == ()


class TestPublishing:
    async def test_membership_is_the_authorisation(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """No second permission to keep in step with the one `join` applied."""
        socket.client_sends(type="publish", room="lobby", data={"body": "hi"})
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms))

        assert socket.of_type(ServerMessageType.ERROR)[0]["code"] == (
            ErrorCode.NOT_IN_ROOM.value
        )

    async def test_a_published_message_reaches_the_other_members(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        listener = _RecordingMember()
        rooms.join("lobby", listener)
        socket.client_sends(type="join", room="lobby")
        socket.client_sends(type="publish", room="lobby", data={"body": "hi"})
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms))

        (received,) = [json.loads(p) for p in listener.received]
        assert received["room"] == "lobby"
        assert received["from"] == str(mock_user.id)
        assert received["data"] == {"body": "hi"}
        assert datetime.fromisoformat(received["sent_at"])

    async def test_the_sender_is_not_echoed_to(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """It already has the payload; an echo doubles the busiest case."""
        rooms.join("lobby", _RecordingMember())
        socket.client_sends(type="join", room="lobby")
        socket.client_sends(type="publish", room="lobby", data="hi")
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms))

        assert socket.of_type(ServerMessageType.MESSAGE) == []

    async def test_the_payload_is_encoded_once_for_the_whole_room(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """Every member is offered the identical string, not its own copy."""
        members = [_RecordingMember() for _ in range(3)]
        for member in members:
            rooms.join("lobby", member)
        socket.client_sends(type="join", room="lobby")
        socket.client_sends(type="publish", room="lobby", data="hi")
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms))

        payloads = {member.received[0] for member in members}
        assert len(payloads) == 1


class TestMalformedInput:
    async def test_a_frame_that_is_not_json_is_reported_and_survived(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        socket.client_sends_raw("{nope")
        socket.client_sends(type="ping")
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms))

        assert socket.of_type(ServerMessageType.ERROR)[0]["code"] == (
            ErrorCode.MALFORMED_MESSAGE.value
        )
        assert socket.of_type(ServerMessageType.PONG)

    async def test_an_oversized_frame_is_reported_and_survived(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        socket.client_sends(type="publish", room="lobby", data="x" * 500)
        socket.client_sends(type="ping")
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms, max_message_bytes=200))

        assert socket.of_type(ServerMessageType.ERROR)[0]["code"] == (
            ErrorCode.MESSAGE_TOO_LARGE.value
        )
        assert socket.of_type(ServerMessageType.PONG)

    async def test_a_binary_frame_ends_the_connection(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """Not a bad message — a client sending on the wrong opcode entirely."""
        socket.client_sends_binary()

        await serve(build(socket, mock_user, rooms))

        assert socket.closed is not None
        assert socket.closed[0] == int(CloseCode.PROTOCOL_ERROR)


class TestRateLimiting:
    async def test_an_over_budget_message_is_refused_with_a_wait(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """Refused, not queued: a rejected message is the client's to resend."""
        for _ in range(4):
            socket.client_sends(type="ping")
        socket.client_disconnects()

        await serve(
            build(
                socket,
                mock_user,
                rooms,
                limits=limiter(message_burst=2, messages_per_second=0.5),
            )
        )

        errors = socket.of_type(ServerMessageType.ERROR)
        assert len(socket.of_type(ServerMessageType.PONG)) == 2
        assert errors[0]["code"] == ErrorCode.RATE_LIMITED.value
        assert errors[0]["retry_after"] > 0

    async def test_the_byte_budget_catches_what_the_message_budget_misses(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """A generous message allowance is defeated by large messages."""
        socket.client_sends(type="publish", room="lobby", data="x" * 300)
        socket.client_sends(type="publish", room="lobby", data="x" * 300)
        socket.client_disconnects()

        await serve(
            build(
                socket,
                mock_user,
                rooms,
                max_message_bytes=400,
                limits=limiter(byte_burst=400, bytes_per_second=1),
            )
        )

        codes = [f["code"] for f in socket.of_type(ServerMessageType.ERROR)]
        assert ErrorCode.RATE_LIMITED.value in codes

    async def test_persistent_flooding_ends_the_connection(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """A client that is not reading its own errors is not slowing down."""
        for _ in range(20):
            socket.client_sends(type="ping")

        await serve(
            build(
                socket,
                mock_user,
                rooms,
                limits=limiter(
                    message_burst=1,
                    messages_per_second=0.01,
                    max_consecutive_violations=3,
                ),
            )
        )

        assert socket.closed is not None
        assert socket.closed[0] == int(CloseCode.RATE_LIMITED)

    async def test_the_final_error_frame_still_reaches_the_client(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """The frames a closing connection most needs to deliver are the last.

        Without the bounded flush before the writer is cancelled, the errors
        explaining the disconnect are exactly the ones dropped.
        """
        for _ in range(6):
            socket.client_sends(type="ping")

        await serve(
            build(
                socket,
                mock_user,
                rooms,
                limits=limiter(
                    message_burst=1,
                    messages_per_second=0.01,
                    max_consecutive_violations=3,
                ),
            )
        )

        errors = socket.of_type(ServerMessageType.ERROR)
        assert len(errors) == 2
        assert all(e["code"] == ErrorCode.RATE_LIMITED.value for e in errors)


class TestTheSlowClient:
    async def test_a_connection_that_falls_behind_is_closed_rather_than_trimmed(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """Held messages are not dropped under a client that cannot tell.

        The writer is gated so nothing drains, the room is published into past
        the buffer, and the connection ends with a code that means "refetch".
        """
        socket.writer_gate = asyncio.Event()
        connection = build(socket, mock_user, rooms, outbound_buffer=2)
        serving = asyncio.ensure_future(connection.run())
        await asyncio.sleep(0)
        rooms.join("lobby", connection)

        for i in range(10):
            rooms.broadcast("lobby", f'{{"n":{i}}}')

        socket.writer_gate.set()
        await asyncio.wait_for(serving, timeout=PATIENCE)

        assert socket.closed is not None
        assert socket.closed[0] == int(CloseCode.OVERFLOW)

    async def test_an_overflowed_connection_leaves_every_room(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """Otherwise every later broadcast fills a queue nothing is draining."""
        socket.writer_gate = asyncio.Event()
        connection = build(socket, mock_user, rooms, outbound_buffer=1)
        serving = asyncio.ensure_future(connection.run())
        await asyncio.sleep(0)
        rooms.join("lobby", connection)

        for i in range(5):
            rooms.broadcast("lobby", f'{{"n":{i}}}')

        assert rooms.rooms == ()
        socket.writer_gate.set()
        await asyncio.wait_for(serving, timeout=PATIENCE)


class TestDeadlines:
    async def test_an_idle_connection_is_closed(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """Nothing is queued, so `receive` blocks and the timer is what fires."""
        await serve(build(socket, mock_user, rooms, idle_timeout=QUICK))

        assert socket.closed is not None
        assert socket.closed[0] == int(CloseCode.IDLE_TIMEOUT)

    async def test_activity_restarts_the_idle_timer(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """Otherwise a busy connection is closed on a fixed schedule."""
        connection = build(socket, mock_user, rooms, idle_timeout=QUICK * 4)
        serving = asyncio.ensure_future(connection.run())
        for _ in range(3):
            await asyncio.sleep(QUICK)
            socket.client_sends(type="ping")

        await asyncio.wait_for(serving, timeout=PATIENCE)

        # Outlived a fixed QUICK*4 window because each ping restarted it.
        assert len(socket.of_type(ServerMessageType.PONG)) == 3

    async def test_an_expiring_token_ends_the_connection_with_its_own_code(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """The failure a WebSocket has and a request does not.

        Checking `exp` only at the handshake grants an access that never ends:
        a role changed or a session revoked an hour ago is still live on a
        socket opened before it.
        """
        await serve(
            build(socket, mock_user, rooms, expires_in=QUICK, idle_timeout=30.0)
        )

        assert socket.closed is not None
        assert socket.closed[0] == int(CloseCode.TOKEN_EXPIRED)

    async def test_the_lifetime_ceiling_ends_it_with_going_away(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """A different remedy: reconnect, no refresh needed."""
        await serve(
            build(
                socket,
                mock_user,
                rooms,
                expires_in=3600,
                max_seconds=QUICK,
                idle_timeout=30.0,
            )
        )

        assert socket.closed is not None
        assert socket.closed[0] == int(CloseCode.GOING_AWAY)

    async def test_the_nearer_deadline_wins(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """A long-lived token under a short ceiling is not a token problem."""
        await serve(
            build(
                socket,
                mock_user,
                rooms,
                expires_in=3600,
                max_seconds=QUICK,
                idle_timeout=QUICK * 50,
            )
        )

        assert socket.closed is not None
        assert socket.closed[0] == int(CloseCode.GOING_AWAY)


class _RecordingMember:
    """A room member that is a list. Enough to satisfy `RoomMember`."""

    def __init__(self, *, accepts: bool = True) -> None:
        self.received: list[str] = []
        self._accepts = accepts

    @property
    def id(self) -> str:
        return "recording"

    def offer(self, payload: str) -> bool:
        if not self._accepts:
            return False
        self.received.append(payload)
        return True


class TestWhenTheSocketItselfFails:
    """The paths a healthy integration test never reaches.

    Each one is an ordinary consequence of the peer going away at an awkward
    moment, and each one is a place where raising instead of unwinding would
    turn a disconnect into a 500 with a traceback.
    """

    async def test_a_disconnect_raised_from_receive_is_not_an_error(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """Starlette raises rather than returning when the peer vanishes."""

        async def _vanish() -> dict[str, Any]:
            raise WebSocketDisconnect(code=1006)

        socket.receive = _vanish  # type: ignore[method-assign]
        connection = build(socket, mock_user, rooms)
        rooms.join("lobby", connection)

        await serve(connection)

        assert rooms.rooms == ()

    async def test_a_failed_write_stops_the_writer_rather_than_the_scope(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """The peer is gone; the receive side is about to find out for itself.

        Letting this out would take the task scope down with an exception for
        what is an ordinary disconnect.
        """

        async def _fail(_: str) -> None:
            raise OSError("peer reset")

        socket.send_text = _fail  # type: ignore[method-assign, assignment]
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms))

        assert socket.sent == []

    async def test_an_undeliverable_close_frame_is_not_an_error(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """There is nothing left to say and nowhere to say it."""

        async def _fail(code: int = 1000, reason: str = "") -> None:
            raise RuntimeError("already closed")

        socket.close = _fail  # type: ignore[method-assign]
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms))

    @pytest.mark.parametrize("side", ["client_state", "application_state"])
    async def test_no_close_is_attempted_on_an_already_closed_socket(
        self,
        socket: ScriptedSocket,
        mock_user: User,
        rooms: RoomRegistry,
        side: str,
    ) -> None:
        """Either end having recorded the close means there is no frame to send.

        `client_state` is the peer having gone; `application_state` is this
        side having already sent one. Checking only one of them leaves the
        other as a `RuntimeError` raised out of a `finally`.
        """
        setattr(socket, side, WebSocketState.DISCONNECTED)
        socket.client_disconnects()

        await serve(build(socket, mock_user, rooms))

        assert socket.closed is None

    async def test_a_client_too_slow_for_the_flush_is_not_waited_on_forever(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """The flush is bounded: "drain the queue" against a stalled reader
        is a wait with no end, and the connection is already closing."""
        socket.writer_gate = asyncio.Event()  # never set
        connection = build(socket, mock_user, rooms, idle_timeout=QUICK)

        await asyncio.wait_for(connection.run(), timeout=PATIENCE)

        assert socket.sent == []


class TestTheOutboundQueue:
    async def test_pending_reports_what_the_writer_has_not_taken(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        socket.writer_gate = asyncio.Event()
        connection = build(socket, mock_user, rooms, outbound_buffer=8)
        serving = asyncio.ensure_future(connection.run())
        await asyncio.sleep(0)
        rooms.join("lobby", connection)

        rooms.broadcast("lobby", '{"n":1}')

        assert connection.pending >= 1
        socket.writer_gate.set()
        socket.client_disconnects()
        await asyncio.wait_for(serving, timeout=PATIENCE)

    def test_offering_to_a_terminated_connection_is_refused(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """Terminal, not transient: a second publish in the same loop, or a
        close racing an overflow, must not queue behind the sentinel."""
        connection = build(socket, mock_user, rooms, outbound_buffer=1)
        connection.offer("first")

        assert connection.offer("second") is False
        assert connection.offer("third") is False


class TestADeadlineAlreadyPassed:
    async def test_a_token_expiring_during_the_handshake_closes_at_once(
        self, socket: ScriptedSocket, mock_user: User, rooms: RoomRegistry
    ) -> None:
        """Nothing is read: the credential was spent before the loop began."""
        await serve(build(socket, mock_user, rooms, expires_in=-1.0))

        assert socket.closed is not None
        assert socket.closed[0] == int(CloseCode.TOKEN_EXPIRED)
