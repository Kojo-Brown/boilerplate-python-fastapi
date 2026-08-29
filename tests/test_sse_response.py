"""Headers, and the guarantee that the body generator is closed.

Driven through the ASGI interface directly rather than a test client, because
the behaviour under test is what happens when `send` *fails* — which is how a
real client disconnect reaches the application on ASGI 2.4 and later, and which
no client that stays connected can provoke.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, MutableMapping
from typing import Any

import pytest
from starlette.requests import ClientDisconnect

from src.sse.event import SSE_MEDIA_TYPE
from src.sse.response import EventSourceResponse


async def frames(*items: bytes) -> AsyncGenerator[bytes, None]:
    for item in items:
        yield item


async def call(
    response: EventSourceResponse,
    *,
    fail_on_chunk: int | None = None,
    spec_version: str = "2.4",
) -> list[MutableMapping[str, Any]]:
    """Run the response as an ASGI app, collecting what it sends.

    `fail_on_chunk` makes the nth body message raise `OSError`, which is what
    an ASGI server raises once the client's socket is gone.
    """
    sent: list[MutableMapping[str, Any]] = []
    chunks = 0

    async def receive() -> dict[str, Any]:
        # Never resolves. On spec_version 2.4 nothing reads this channel; the
        # disconnect arrives as a failing `send` instead.
        await asyncio.Event().wait()
        raise AssertionError("unreachable")  # pragma: no cover

    async def send(message: MutableMapping[str, Any]) -> None:
        nonlocal chunks
        if message["type"] == "http.response.body":
            chunks += 1
            if fail_on_chunk is not None and chunks == fail_on_chunk:
                raise OSError("client went away")
        sent.append(message)

    scope: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": spec_version},
        "method": "GET",
        "path": "/",
        "headers": [],
    }
    await response(scope, receive, send)
    return sent


def headers_of(sent: list[MutableMapping[str, Any]]) -> dict[str, str]:
    start = next(m for m in sent if m["type"] == "http.response.start")
    return {k.decode().lower(): v.decode() for k, v in start["headers"]}


class TestHeaders:
    async def test_the_media_type_is_an_event_stream(self) -> None:
        sent = await call(EventSourceResponse(frames(b"data: a\n\n")))

        assert headers_of(sent)["content-type"].startswith(SSE_MEDIA_TYPE)

    async def test_the_charset_is_declared(self) -> None:
        """SSE is UTF-8 by definition; saying so costs nothing and helps proxies."""
        sent = await call(EventSourceResponse(frames(b"data: a\n\n")))

        assert "charset=utf-8" in headers_of(sent)["content-type"]

    async def test_proxy_buffering_is_disabled(self) -> None:
        """Without this an nginx in front delivers nothing until the stream ends."""
        sent = await call(EventSourceResponse(frames(b"data: a\n\n")))

        assert headers_of(sent)["x-accel-buffering"] == "no"

    async def test_the_stream_is_not_cached(self) -> None:
        sent = await call(EventSourceResponse(frames(b"data: a\n\n")))

        assert headers_of(sent)["cache-control"] == "no-store"

    async def test_there_is_no_content_length(self) -> None:
        """It cannot be known; the server frames the body as chunked instead."""
        sent = await call(EventSourceResponse(frames(b"data: a\n\n")))

        assert "content-length" not in headers_of(sent)

    async def test_no_hop_by_hop_connection_header_is_set(self) -> None:
        """ASGI forbids it, and HTTP/2 has no such field."""
        sent = await call(EventSourceResponse(frames(b"data: a\n\n")))

        assert "connection" not in headers_of(sent)

    async def test_extra_headers_are_applied_over_the_defaults(self) -> None:
        response = EventSourceResponse(
            frames(b"data: a\n\n"), headers={"X-Trace": "abc"}
        )

        sent = await call(response)

        assert headers_of(sent)["x-trace"] == "abc"


class TestBody:
    async def test_every_frame_reaches_the_client(self) -> None:
        sent = await call(EventSourceResponse(frames(b"a", b"b")))

        bodies = [m["body"] for m in sent if m["type"] == "http.response.body"]
        assert bodies == [b"a", b"b", b""]

    async def test_frames_are_sent_as_they_are_produced(self) -> None:
        """One ASGI message per frame: the point of streaming at all."""
        sent = await call(EventSourceResponse(frames(b"a", b"b", b"c")))

        assert len([m for m in sent if m["type"] == "http.response.body"]) == 4


class TestClientDisconnect:
    async def test_the_generator_is_closed_when_the_response_ends(self) -> None:
        closed = asyncio.Event()

        async def body() -> AsyncGenerator[bytes, None]:
            try:
                yield b"a"
            finally:
                closed.set()

        await call(EventSourceResponse(body()))

        assert closed.is_set()

    async def test_the_generator_is_closed_when_the_client_disappears(self) -> None:
        """The case that matters: an SSE stream is *designed* to be abandoned.

        Without this the body is left suspended holding a hub subscription
        until the garbage collector finalises it.
        """
        closed = asyncio.Event()

        async def body() -> AsyncGenerator[bytes, None]:
            try:
                yield b"a"
                yield b"b"
                yield b"c"
            finally:
                closed.set()

        # Starlette turns the failing `send` into this, which is the only
        # signal an application gets that the client has gone.
        with pytest.raises(ClientDisconnect):
            await call(EventSourceResponse(body()), fail_on_chunk=2)

        assert closed.is_set()

    async def test_the_generator_is_closed_on_older_servers_too(self) -> None:
        """Below ASGI 2.4 Starlette cancels the stream instead of raising."""
        closed = asyncio.Event()

        async def body() -> AsyncGenerator[bytes, None]:
            try:
                yield b"a"
                await asyncio.Event().wait()
                yield b"b"  # pragma: no cover - never reached
            finally:
                closed.set()

        response = EventSourceResponse(body())
        task = asyncio.ensure_future(call(response, spec_version="2.3"))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert closed.is_set()
