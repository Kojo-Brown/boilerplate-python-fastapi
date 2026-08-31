"""`/api/v1/ws`, over a real socket, with a real handshake.

## Why this suite starts a server

`ASGITransport` speaks HTTP and has no WebSocket support at all, so there is no
in-process option here of the kind the rest of the suite uses. That turns out
to be the right constraint rather than an inconvenience: the questions this
file exists to answer are all about the *handshake* — what a rejected upgrade
looks like to a client, whether the negotiated subprotocol comes back, what
close code actually arrives — and every one of them is a property of the bytes
on the wire rather than of the handler.

`tests/test_ws_connection.py` covers the decisions behind those bytes against a
scripted socket. This file covers the wiring, and the two facts about uvicorn
that `src/ws/auth.py` documents and nothing else would catch.

The lifespan is disabled: these tests need routing and dependencies, not a
process pool, an outbox relay or a Redis connection.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncGenerator
from types import SimpleNamespace
from typing import Any

import pytest
import uvicorn
import websockets
from starlette.websockets import WebSocketDisconnect
from websockets.asyncio.client import ClientConnection

from src.api.v1.ws import (
    ENDPOINT_PATH,
    get_room_registry,
    get_user_lookup,
    get_ws_settings,
    lookup_user,
)
from src.auth.utils import create_access_token
from src.config import settings
from src.database import get_db
from src.main import app
from src.models.user import User
from src.ws.auth import AUTH_SUBPROTOCOL, REJECTED_QUERY_PARAM
from src.ws.protocol import CloseCode, ErrorCode, ServerMessageType
from src.ws.rooms import RoomRegistry, room_registry

#: Long enough that a frame the application has produced is read; short enough
#: that a frame it should never produce does not stall the suite.
READ_TIMEOUT = 5.0

URL_PATH = f"/api/v1{ENDPOINT_PATH}"


@pytest.fixture
def rooms() -> RoomRegistry:
    """A registry of this test's own, rather than the process-wide one."""
    return RoomRegistry()


@pytest.fixture
async def server(rooms: RoomRegistry, mock_user: User) -> AsyncGenerator[str, None]:
    """A uvicorn serving the application on an ephemeral port. Yields its origin.

    `get_db` is overridden with something that *fails*, which is the assertion
    rather than a convenience: this endpoint must not hold a request-scoped
    session for the life of a connection, so a connection that opens at all is
    proof it never asked for one. See the module docstring in `src/api/v1/ws.py`
    for what holding one would cost.
    """

    async def _no_session() -> AsyncGenerator[None, None]:
        raise AssertionError("the websocket endpoint must not hold a db session")
        yield  # pragma: no cover - unreachable, satisfies the generator protocol

    async def _lookup(user_id: uuid.UUID) -> User | None:
        return mock_user if user_id == mock_user.id else None

    app.dependency_overrides[get_db] = _no_session
    app.dependency_overrides[get_user_lookup] = lambda: _lookup
    app.dependency_overrides[get_room_registry] = lambda: rooms

    config = uvicorn.Config(
        app, host="127.0.0.1", port=0, log_level="warning", lifespan="off"
    )
    instance = uvicorn.Server(config)
    serving = asyncio.ensure_future(instance.serve())
    try:
        while not instance.started:
            await asyncio.sleep(0.01)
        port = instance.servers[0].sockets[0].getsockname()[1]
        yield f"ws://127.0.0.1:{port}"
    finally:
        instance.should_exit = True
        await asyncio.wait_for(serving, timeout=READ_TIMEOUT)
        app.dependency_overrides.clear()


@pytest.fixture
def token(mock_user: User) -> str:
    return create_access_token(str(mock_user.id), mock_user.email, mock_user.role)


class Client:
    """One open connection, with the small helpers every test here wants."""

    def __init__(self, socket: ClientConnection) -> None:
        self.socket = socket

    async def receive(self) -> dict[str, Any]:
        raw = await asyncio.wait_for(self.socket.recv(), timeout=READ_TIMEOUT)
        assert isinstance(raw, str), "this endpoint only ever sends text frames"
        parsed: dict[str, Any] = json.loads(raw)
        return parsed

    async def send(self, **message: Any) -> None:
        await self.socket.send(json.dumps(message))

    async def opened(self) -> dict[str, Any]:
        """Consume the `ready` frame and return it."""
        ready = await self.receive()
        assert ready["type"] == ServerMessageType.READY.value
        return ready

    async def expect(self, kind: ServerMessageType) -> dict[str, Any]:
        """Read frames until one of `kind` arrives, and return it."""
        while True:
            frame = await self.receive()
            if frame["type"] == kind.value:
                return frame

    async def closed(self) -> int:
        """Wait for the close frame and return its code."""
        with pytest.raises(websockets.ConnectionClosed):
            while True:
                await self.receive()
        return self.socket.close_code or 0


def connect(server: str, token: str) -> Any:
    """Open an authenticated connection using the header carrier."""
    return websockets.connect(
        f"{server}{URL_PATH}", additional_headers={"Authorization": f"Bearer {token}"}
    )


class TestTheHandshake:
    async def test_an_unauthenticated_upgrade_is_refused(self, server: str) -> None:
        """Refused *before* accept, so it never becomes a connection at all."""
        with pytest.raises(websockets.InvalidStatus) as caught:
            async with websockets.connect(f"{server}{URL_PATH}"):
                pass

        assert caught.value.response.status_code == 403

    async def test_a_rejection_is_an_http_status_and_not_a_close_code(
        self, server: str
    ) -> None:
        """The fact `src/ws/auth.py` documents, pinned rather than assumed.

        `websocket.close(code=...)` before `accept()` does not deliver that
        code: uvicorn answers the upgrade with HTTP 403 and the code is
        discarded. Any error handling that tried to distinguish causes at this
        stage would be relying on something the transport does not carry.
        """
        with pytest.raises(websockets.InvalidStatus) as caught:
            async with websockets.connect(
                f"{server}{URL_PATH}",
                additional_headers={"Authorization": "Bearer nonsense"},
            ):
                pass

        assert caught.value.response.status_code == 403

    async def test_a_token_in_the_query_string_does_not_authenticate(
        self, server: str, token: str
    ) -> None:
        """The design this endpoint refuses, over a real URL."""
        with pytest.raises(websockets.InvalidStatus):
            async with websockets.connect(
                f"{server}{URL_PATH}?{REJECTED_QUERY_PARAM}={token}"
            ):
                pass

    async def test_the_header_carrier_opens_a_connection(
        self, server: str, token: str
    ) -> None:
        async with connect(server, token) as socket:
            ready = await Client(socket).opened()

        assert ready["user_id"]
        assert socket.subprotocol is None

    async def test_the_subprotocol_carrier_opens_a_connection(
        self, server: str, token: str
    ) -> None:
        """What a browser has to do, since it cannot set a header."""
        async with websockets.connect(
            f"{server}{URL_PATH}",
            subprotocols=[AUTH_SUBPROTOCOL, token],  # type: ignore[list-item]
        ) as socket:
            await Client(socket).opened()

            # The tag is echoed and the credential is not. Selecting nothing
            # would fail the handshake at the client end; selecting the token
            # would put it in a response header.
            assert socket.subprotocol == AUTH_SUBPROTOCOL


class TestMessaging:
    async def test_a_ping_is_answered(self, server: str, token: str) -> None:
        async with connect(server, token) as socket:
            client = Client(socket)
            await client.opened()

            await client.send(type="ping")

            assert (await client.receive())["type"] == ServerMessageType.PONG.value

    async def test_joining_a_room_is_acknowledged(
        self, server: str, token: str
    ) -> None:
        async with connect(server, token) as socket:
            client = Client(socket)
            await client.opened()

            await client.send(type="join", room="lobby")
            joined = await client.expect(ServerMessageType.JOINED)

        assert joined["room"] == "lobby"

    async def test_a_message_reaches_the_other_member_of_the_room(
        self, server: str, token: str
    ) -> None:
        """The feature, end to end and over two real sockets."""
        async with connect(server, token) as first, connect(server, token) as second:
            sender, listener = Client(first), Client(second)
            await sender.opened()
            await listener.opened()
            await sender.send(type="join", room="lobby")
            await sender.expect(ServerMessageType.JOINED)
            await listener.send(type="join", room="lobby")
            await listener.expect(ServerMessageType.JOINED)

            await sender.send(type="publish", room="lobby", data={"body": "hello"})
            received = await listener.expect(ServerMessageType.MESSAGE)

        assert received["data"] == {"body": "hello"}
        assert received["room"] == "lobby"

    async def test_a_member_of_another_room_does_not_receive_it(
        self, server: str, token: str
    ) -> None:
        async with connect(server, token) as first, connect(server, token) as second:
            sender, outsider = Client(first), Client(second)
            await sender.opened()
            await outsider.opened()
            await sender.send(type="join", room="a")
            await sender.expect(ServerMessageType.JOINED)
            await outsider.send(type="join", room="b")
            await outsider.expect(ServerMessageType.JOINED)

            await sender.send(type="publish", room="a", data="theirs")
            # The round trip that proves the negative: `outsider` answers its
            # own ping, which cannot arrive before a message published first.
            await outsider.send(type="ping")
            answer = await outsider.receive()

        assert answer["type"] == ServerMessageType.PONG.value

    async def test_publishing_without_joining_is_refused(
        self, server: str, token: str
    ) -> None:
        """Membership is the authorisation; there is no second check to drift."""
        async with connect(server, token) as socket:
            client = Client(socket)
            await client.opened()

            await client.send(type="publish", room="lobby", data="hi")
            error = await client.expect(ServerMessageType.ERROR)

        assert error["code"] == ErrorCode.NOT_IN_ROOM.value

    async def test_a_malformed_frame_does_not_end_the_connection(
        self, server: str, token: str
    ) -> None:
        async with connect(server, token) as socket:
            client = Client(socket)
            await client.opened()

            await socket.send("{not json")
            error = await client.expect(ServerMessageType.ERROR)
            await client.send(type="ping")
            answer = await client.expect(ServerMessageType.PONG)

        assert error["code"] == ErrorCode.MALFORMED_MESSAGE.value
        assert answer


class TestClosing:
    async def test_a_binary_frame_closes_with_a_protocol_error(
        self, server: str, token: str
    ) -> None:
        """A close code, unlike a rejected handshake, does reach the client."""
        async with connect(server, token) as socket:
            client = Client(socket)
            await client.opened()

            await socket.send(b"\x00\x01")

            assert await client.closed() == int(CloseCode.PROTOCOL_ERROR)

    async def test_a_disconnect_releases_every_room_membership(
        self, server: str, token: str, rooms: RoomRegistry
    ) -> None:
        """A member left behind is broadcast to for the life of the process."""
        async with connect(server, token) as socket:
            client = Client(socket)
            await client.opened()
            await client.send(type="join", room="lobby")
            await client.expect(ServerMessageType.JOINED)
            assert rooms.member_count("lobby") == 1

        await eventually(lambda: rooms.rooms == ())

    async def test_an_abandoned_connection_is_released_too(
        self, server: str, token: str, rooms: RoomRegistry
    ) -> None:
        """No close handshake — the connection is dropped underneath the server.

        What a closed laptop lid does. The registration has to go anyway.
        """
        socket = await connect(server, token)
        client = Client(socket)
        await client.opened()
        await client.send(type="join", room="lobby")
        await client.expect(ServerMessageType.JOINED)

        # `abort()` on the transport rather than `close()` on the connection:
        # the latter sends a close frame, which is the orderly case the test
        # above already covers. This one kills the TCP connection with no
        # goodbye at all, so the server discovers it from a read that ends
        # rather than from anything the client said.
        socket.transport.abort()

        await eventually(lambda: rooms.rooms == ())


class TestBounds:
    async def test_a_connection_ends_when_its_deadline_passes(
        self, server: str, token: str
    ) -> None:
        """Over a real socket, with the close code that says which deadline."""
        app.dependency_overrides[get_ws_settings] = lambda: settings.model_copy(
            update={"WS_MAX_CONNECTION_SECONDS": 0.15, "WS_IDLE_TIMEOUT_SECONDS": 30.0}
        )

        async with connect(server, token) as socket:
            client = Client(socket)
            await client.opened()

            assert await client.closed() == int(CloseCode.GOING_AWAY)

    async def test_an_idle_connection_is_closed(self, server: str, token: str) -> None:
        """The ASGI server's own ping/pong is invisible here, so this is the
        only thing bounding how long a silent socket is held."""
        app.dependency_overrides[get_ws_settings] = lambda: settings.model_copy(
            update={"WS_IDLE_TIMEOUT_SECONDS": 0.15}
        )

        async with connect(server, token) as socket:
            client = Client(socket)
            await client.opened()

            assert await client.closed() == int(CloseCode.IDLE_TIMEOUT)

    async def test_a_flooding_client_is_disconnected(
        self, server: str, token: str
    ) -> None:
        app.dependency_overrides[get_ws_settings] = lambda: settings.model_copy(
            update={
                "WS_MESSAGE_BURST": 1,
                "WS_MESSAGES_PER_SECOND": 0.01,
                "WS_MAX_RATE_VIOLATIONS": 2,
            }
        )

        async with connect(server, token) as socket:
            client = Client(socket)
            await client.opened()
            for _ in range(6):
                await client.send(type="ping")

            assert await client.closed() == int(CloseCode.RATE_LIMITED)

    async def test_an_oversized_message_is_refused_without_closing(
        self, server: str, token: str
    ) -> None:
        app.dependency_overrides[get_ws_settings] = lambda: settings.model_copy(
            update={"WS_MAX_MESSAGE_BYTES": 256}
        )

        async with connect(server, token) as socket:
            client = Client(socket)
            await client.opened()

            await client.send(type="publish", room="lobby", data="x" * 1000)
            error = await client.expect(ServerMessageType.ERROR)

        assert error["code"] == ErrorCode.MESSAGE_TOO_LARGE.value


async def eventually(predicate: Any, *, timeout: float = READ_TIMEOUT) -> None:
    """Wait for `predicate()` to hold, rather than sleeping a guessed interval."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition was still false at the deadline")


class TestTheProviders:
    def test_the_lookup_provider_returns_the_real_one(self) -> None:
        assert get_user_lookup() is lookup_user

    def test_the_registry_provider_returns_the_process_wide_one(self) -> None:
        """Two registries in a process is a room whose members cannot hear
        each other, and the symptom is far from the cause."""
        assert get_room_registry() is room_registry

    def test_the_settings_provider_returns_the_frozen_global(self) -> None:
        assert get_ws_settings() is settings

    async def test_the_lookup_closes_its_session_before_returning(
        self, monkeypatch: pytest.MonkeyPatch, mock_user: User
    ) -> None:
        """The claim the whole endpoint rests on, asserted rather than argued.

        A connection lives for up to `WS_MAX_CONNECTION_SECONDS`; a pooled
        session held for that long is a pool exhausted by idle chat tabs. So
        the session has to be gone by the time the socket starts serving, and
        this is where that is checked.
        """
        opened: list[str] = []
        closed: list[str] = []

        class _Session:
            async def __aenter__(self) -> _Session:
                opened.append("in")
                return self

            async def __aexit__(self, *_: object) -> None:
                closed.append("out")

        monkeypatch.setattr("src.api.v1.ws.AsyncSessionLocal", lambda: _Session())
        monkeypatch.setattr(
            "src.api.v1.ws.UserRepository",
            lambda _session: SimpleNamespace(get=_returning(mock_user)),
        )

        assert await lookup_user(mock_user.id) is mock_user
        assert opened and closed


class TestAnUnexpectedFailure:
    async def test_it_becomes_a_close_frame_rather_than_a_reset(
        self, server: str, token: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """There is no exception handler registered for this transport.

        An escape from the handler is an unlogged 500 that the client sees as
        a connection reset with no explanation, so the endpoint catches it and
        says goodbye properly — with a fixed message, not the exception's own.
        """

        async def _explode(self: Any) -> None:
            raise RuntimeError("something in the connection went wrong")

        monkeypatch.setattr("src.ws.connection.Connection.run", _explode)

        async with connect(server, token) as socket:
            assert await Client(socket).closed() == int(CloseCode.INTERNAL_ERROR)


def _returning(user: User) -> Any:
    async def _get(_: uuid.UUID) -> User:
        return user

    return _get


class TestTheLastResortPaths:
    async def test_a_peer_that_vanishes_during_close_is_ordinary(
        self, server: str, token: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`run` handles its own disconnect; this covers one arriving later."""

        monkeypatch.setattr(
            "src.ws.connection.Connection.run", _raising(WebSocketDisconnect(1006))
        )

        async with connect(server, token) as socket:
            await Client(socket).closed()

    async def test_an_undeliverable_close_after_a_failure_is_swallowed(
        self, server: str, token: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The very last thing that can go wrong, and there is nothing to do.

        Raising here would replace a logged internal error with an unlogged
        one from inside the exception handler for it.
        """
        monkeypatch.setattr(
            "src.ws.connection.Connection.run", _raising(RuntimeError("boom"))
        )

        async def _cannot_close(*_: object, **__: object) -> None:
            raise RuntimeError("socket already gone")

        monkeypatch.setattr("starlette.websockets.WebSocket.close", _cannot_close)

        async with connect(server, token) as socket:
            await Client(socket).closed()


def _raising(error: BaseException) -> Any:
    async def _run(self: Any) -> None:
        raise error

    return _run
