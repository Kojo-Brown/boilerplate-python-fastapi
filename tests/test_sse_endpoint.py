"""`GET /api/v1/events/stream`, over a real socket.

## Why this suite starts a server

Every other route in this repository is tested through `ASGITransport`, which
runs the application in-process and is the right tool for a request that ends.
It cannot be the tool here: `handle_async_request` awaits the whole application
call and accumulates the body before returning a response, so a stream that
never ends never returns — and its `receive` only reports `http.disconnect`
*after* the response completes, which for an SSE endpoint is never.

Both of those are symptoms of the same thing, and it is the thing under test:
this response outlives the request that made it. So these tests run a real
uvicorn on an ephemeral port and talk to it over TCP, which is what makes
`test_a_dropped_connection_releases_the_subscription` possible — the client
vanishes, the next keepalive fails to write, and the subscription is released.
That path cannot be provoked in-process, and it is the whole feature.

The lifespan is disabled: these tests need routing and dependencies, not a
process pool, an outbox relay or a Redis connection.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock

import pytest
import uvicorn
from httpx import AsyncClient, Response

from src.api.v1.events import READY_EVENT
from src.auth.dependencies import get_current_user
from src.config import settings
from src.database import get_db
from src.dependencies import get_event_stream_hub
from src.main import app
from src.models.user import User
from src.sse.event import SSE_MEDIA_TYPE, ServerSentEvent
from src.sse.hub import OVERFLOW_EVENT, EventStreamHub, user_topic

ENDPOINT = "/api/v1/events/stream"

#: Long enough that a frame the application has produced is read; short enough
#: that a frame it should never produce does not stall the suite.
READ_TIMEOUT = 5.0

#: Keepalive interval for these tests. Also the window in which an abandoned
#: stream is noticed, which is why the disconnect test can be quick.
FAST_HEARTBEAT = 0.05


@pytest.fixture
def hub() -> EventStreamHub:
    """A hub of this test's own, rather than the process-wide one."""
    return EventStreamHub()


@pytest.fixture(autouse=True)
def fast_heartbeat(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keepalives every 50ms rather than every 15 seconds.

    A *different* `Settings` rather than a mutated one: the global is frozen on
    purpose (see its docstring), and the seam the codebase offers is to build
    another and hand it over. Here that means replacing the reference the route
    module reads, which is undone by `monkeypatch` after each test.
    """
    monkeypatch.setattr(
        "src.api.v1.events.settings",
        settings.model_copy(update={"SSE_HEARTBEAT_SECONDS": FAST_HEARTBEAT}),
    )


@pytest.fixture
async def server(
    hub: EventStreamHub,
    mock_db: AsyncMock,
) -> AsyncGenerator[str, None]:
    """A uvicorn serving the application on an ephemeral port. Yields its origin.

    Authentication is deliberately *not* overridden here — that is `as_user`
    below. An override installed for every test would also apply to the
    unauthenticated one, which would then open a stream instead of being
    rejected and would never return.
    """

    async def _override_db() -> AsyncGenerator[AsyncMock, None]:
        yield mock_db

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_event_stream_hub] = lambda: hub

    config = uvicorn.Config(
        app, host="127.0.0.1", port=0, log_level="warning", lifespan="off"
    )
    instance = uvicorn.Server(config)
    serving = asyncio.ensure_future(instance.serve())
    try:
        while not instance.started:
            await asyncio.sleep(0.01)
        port = instance.servers[0].sockets[0].getsockname()[1]
        yield f"http://127.0.0.1:{port}"
    finally:
        instance.should_exit = True
        await asyncio.wait_for(serving, timeout=READ_TIMEOUT)
        app.dependency_overrides.clear()


@pytest.fixture
def as_user(mock_user: User) -> None:
    """Resolve `get_current_user` to `mock_user` for the tests that ask for it."""

    async def _override_current_user() -> User:
        return mock_user

    app.dependency_overrides[get_current_user] = _override_current_user


@pytest.fixture
async def client(
    server: str, as_user: None, auth_headers: dict[str, str]
) -> AsyncGenerator[AsyncClient, None]:
    """An authenticated client talking to the live server."""
    async with AsyncClient(base_url=server, headers=auth_headers) as http:
        yield http


class FrameReader:
    """Reads SSE frames off one open response.

    One reader per response, and one iterator per reader: httpx allows a
    response body to be iterated exactly once, and the frames are separated by
    a blank line rather than aligned to chunk boundaries, so the buffer has to
    live across reads.
    """

    def __init__(self, response: Response) -> None:
        self._chunks = response.aiter_bytes()
        self._buffer = ""

    async def _next_frame(self) -> str:
        while "\n\n" not in self._buffer:
            self._buffer += (await anext(self._chunks)).decode("utf-8")
        frame, _, self._buffer = self._buffer.partition("\n\n")
        return frame

    async def frames(self, count: int) -> list[str]:
        """Read exactly `count` frames, keepalive comments included."""
        return [
            await asyncio.wait_for(self._next_frame(), timeout=READ_TIMEOUT)
            for _ in range(count)
        ]

    async def events(self, count: int) -> list[str]:
        """Read `count` frames that are *events*, discarding keepalives."""
        collected: list[str] = []
        while len(collected) < count:
            (frame,) = await self.frames(1)
            if not frame.startswith(":"):
                collected.append(frame)
        return collected

    async def opened(self) -> None:
        """Consume the preamble and the `ready` event."""
        preamble, ready = await self.events(2)
        assert field(preamble, "retry") is not None
        assert field(ready, "event") == READY_EVENT

    async def ended(self) -> None:
        """Wait for the body to end, which is what a closed stream looks like."""

        async def drain() -> None:
            async for _ in self._chunks:
                pass

        await asyncio.wait_for(drain(), timeout=READ_TIMEOUT)


def field(frame: str, name: str) -> str | None:
    """The first value of `name` in a frame, or `None` if it has none."""
    for line in frame.split("\n"):
        key, _, value = line.partition(":")
        if key == name:
            return value.removeprefix(" ")
    return None


async def eventually(predicate: Any, *, timeout: float = READ_TIMEOUT) -> None:
    """Wait for `predicate()` to hold, rather than sleeping a guessed interval."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition was still false at the deadline")


class TestAuthorisation:
    async def test_an_unauthenticated_caller_gets_a_401_envelope(
        self, server: str
    ) -> None:
        """Resolved before the response starts, so it is still an ordinary error."""
        async with AsyncClient(base_url=server) as anonymous:
            response = await anonymous.get(ENDPOINT)

        assert response.status_code == 401
        assert response.headers["content-type"].startswith("application/json")

    async def test_a_caller_only_ever_subscribes_to_their_own_topic(
        self, client: AsyncClient, hub: EventStreamHub, mock_user: User
    ) -> None:
        """There is no parameter naming whose events to stream."""
        async with client.stream("GET", ENDPOINT) as response:
            reader = FrameReader(response)
            await reader.opened()

            assert hub.topics == (user_topic(str(mock_user.id)),)


class TestStreamShape:
    async def test_the_response_is_an_event_stream(self, client: AsyncClient) -> None:
        async with client.stream("GET", ENDPOINT) as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith(SSE_MEDIA_TYPE)
            assert response.headers["cache-control"] == "no-store"
            assert response.headers["x-accel-buffering"] == "no"

    async def test_the_body_is_chunked_rather_than_length_delimited(
        self, client: AsyncClient
    ) -> None:
        """A length would have to be known before the first byte."""
        async with client.stream("GET", ENDPOINT) as response:
            assert "content-length" not in response.headers

    async def test_the_stream_opens_with_a_retry_directive_and_a_ready_event(
        self, client: AsyncClient
    ) -> None:
        async with client.stream("GET", ENDPOINT) as response:
            preamble, ready = await FrameReader(response).events(2)

        assert field(preamble, "retry") == str(settings.SSE_RETRY_MS)
        assert field(ready, "event") == READY_EVENT

    async def test_ready_arrives_after_the_subscription_is_registered(
        self, client: AsyncClient, hub: EventStreamHub, mock_user: User
    ) -> None:
        """The endpoint's one guarantee: publish after `ready`, receive it."""
        async with client.stream("GET", ENDPOINT) as response:
            reader = FrameReader(response)
            await reader.opened()

            assert hub.subscriber_count(user_topic(str(mock_user.id))) == 1

    async def test_a_quiet_stream_is_kept_alive_with_comments(
        self, client: AsyncClient
    ) -> None:
        """Without these the connection is closed by the first proxy in the path."""
        async with client.stream("GET", ENDPOINT) as response:
            reader = FrameReader(response)
            await reader.opened()
            frames = await reader.frames(3)

        assert all(frame.startswith(":") for frame in frames)


class TestDelivery:
    async def test_an_event_published_after_ready_reaches_the_client(
        self, client: AsyncClient, hub: EventStreamHub, mock_user: User
    ) -> None:
        async with client.stream("GET", ENDPOINT) as response:
            reader = FrameReader(response)
            await reader.opened()

            hub.publish(
                user_topic(str(mock_user.id)),
                ServerSentEvent(event="user.registered", data='{"n":1}'),
            )
            (frame,) = await reader.events(1)

        assert field(frame, "event") == "user.registered"
        assert field(frame, "data") == '{"n":1}'

    async def test_events_arrive_in_order(
        self, client: AsyncClient, hub: EventStreamHub, mock_user: User
    ) -> None:
        async with client.stream("GET", ENDPOINT) as response:
            reader = FrameReader(response)
            await reader.opened()

            for i in range(5):
                hub.publish(user_topic(str(mock_user.id)), ServerSentEvent(data=f"{i}"))
            frames = await reader.events(5)

        assert [field(f, "data") for f in frames] == ["0", "1", "2", "3", "4"]

    async def test_another_users_events_do_not_arrive(
        self, client: AsyncClient, hub: EventStreamHub, mock_user: User
    ) -> None:
        async with client.stream("GET", ENDPOINT) as response:
            reader = FrameReader(response)
            await reader.opened()

            hub.publish(user_topic("somebody-else"), ServerSentEvent(data="theirs"))
            hub.publish(user_topic(str(mock_user.id)), ServerSentEvent(data="mine"))
            (frame,) = await reader.events(1)

        assert field(frame, "data") == "mine"

    async def test_a_multiline_payload_survives_the_round_trip(
        self, client: AsyncClient, hub: EventStreamHub, mock_user: User
    ) -> None:
        """Over a real socket, where a mis-framed event would end early."""
        async with client.stream("GET", ENDPOINT) as response:
            reader = FrameReader(response)
            await reader.opened()

            hub.publish(
                user_topic(str(mock_user.id)), ServerSentEvent(data="one\ntwo\nthree")
            )
            (frame,) = await reader.events(1)

        assert [line for line in frame.split("\n") if line.startswith("data")] == [
            "data: one",
            "data: two",
            "data: three",
        ]


class TestTheSlowClient:
    async def test_a_client_that_falls_behind_is_told_and_closed(
        self, client: AsyncClient, mock_user: User
    ) -> None:
        """The stream ends with a signal to refetch, not with silent gaps."""
        hub = EventStreamHub(buffer=1)
        app.dependency_overrides[get_event_stream_hub] = lambda: hub

        async with client.stream("GET", ENDPOINT) as response:
            reader = FrameReader(response)
            await reader.opened()

            topic = user_topic(str(mock_user.id))
            for i in range(10):
                hub.publish(topic, ServerSentEvent(data=f"{i}"))
            frames = await reader.events(2)

        assert field(frames[-1], "event") == OVERFLOW_EVENT
        assert "Reconnect" in (field(frames[-1], "data") or "")


class TestDisconnect:
    async def test_a_dropped_connection_releases_the_subscription(
        self, client: AsyncClient, hub: EventStreamHub, mock_user: User
    ) -> None:
        """The feature, end to end and over a real socket.

        The client goes away without saying so. Nothing polls for that: the
        next keepalive write fails, the response unwinds, the body generator is
        closed, and its `finally` deregisters the subscription. Every one of
        those links has to hold, and a break anywhere leaves a registration
        that is fanned out to for the life of the process.
        """
        topic = user_topic(str(mock_user.id))
        response = await client.send(client.build_request("GET", ENDPOINT), stream=True)
        reader = FrameReader(response)
        await reader.opened()
        assert hub.subscriber_count(topic) == 1

        # No `aclose()` handshake — the connection is dropped underneath the
        # server, which is what a closed laptop lid does.
        await client.aclose()

        await eventually(lambda: hub.subscriber_count(topic) == 0)

    async def test_the_release_happens_within_a_heartbeat_interval(
        self, client: AsyncClient, hub: EventStreamHub, mock_user: User
    ) -> None:
        """The keepalive interval is the ceiling on how long a leak persists."""
        topic = user_topic(str(mock_user.id))
        response = await client.send(client.build_request("GET", ENDPOINT), stream=True)
        reader = FrameReader(response)
        await reader.opened()

        loop = asyncio.get_running_loop()
        dropped = loop.time()
        await client.aclose()
        await eventually(lambda: hub.subscriber_count(topic) == 0)

        # Generous against the 50ms interval — the assertion is that detection
        # is bounded by the keepalive rather than by a garbage collection.
        assert loop.time() - dropped < FAST_HEARTBEAT * 20

    async def test_closing_the_response_deregisters_the_subscription(
        self, client: AsyncClient, hub: EventStreamHub, mock_user: User
    ) -> None:
        """The orderly version of the same thing."""
        topic = user_topic(str(mock_user.id))
        async with client.stream("GET", ENDPOINT) as response:
            reader = FrameReader(response)
            await reader.opened()
            assert hub.subscriber_count(topic) == 1

        await eventually(lambda: hub.topics == ())

    async def test_a_reconnecting_client_starts_clean(
        self, client: AsyncClient, hub: EventStreamHub, mock_user: User
    ) -> None:
        """What an `EventSource` does by itself after any of the above."""
        topic = user_topic(str(mock_user.id))
        for _ in range(2):
            async with client.stream("GET", ENDPOINT) as response:
                reader = FrameReader(response)
                await reader.opened()
                assert hub.subscriber_count(topic) == 1
            await eventually(lambda: hub.subscriber_count(topic) == 0)

    async def test_closing_the_hub_ends_an_open_stream(
        self, client: AsyncClient, hub: EventStreamHub
    ) -> None:
        """Shutdown ends the body cleanly rather than resetting the socket."""
        async with client.stream("GET", ENDPOINT) as response:
            reader = FrameReader(response)
            await reader.opened()

            hub.close()

            # The body ends rather than stalling: an `EventSource` treats that
            # as a dropped connection and reconnects, which is the point.
            await reader.ended()
