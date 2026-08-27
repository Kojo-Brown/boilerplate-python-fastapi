"""`NDJSONStreamingResponse`: the headers, and closing the body iterator.

The disconnect tests drive the response as an ASGI app directly rather than
through a client, because what has to be observed is what `send` raising does
to the generator — and no HTTP client can arrange that from the outside.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from starlette.requests import ClientDisconnect
from starlette.responses import StreamingResponse

from src.streaming.response import NDJSON_MEDIA_TYPE, NDJSONStreamingResponse


def _scope() -> dict[str, Any]:
    """A minimal ASGI HTTP scope, on the spec version Starlette branches on."""
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "method": "GET",
        "path": "/api/v1/exports/users",
        "headers": [],
    }


async def _receive() -> dict[str, Any]:  # pragma: no cover - never awaited
    await asyncio.Event().wait()
    raise AssertionError("unreachable")


class _Recorder:
    """Collects ASGI messages, optionally failing at the nth body chunk."""

    def __init__(self, fail_after: int | None = None) -> None:
        self.messages: list[dict[str, Any]] = []
        self._fail_after = fail_after
        self._bodies = 0

    async def __call__(self, message: dict[str, Any]) -> None:
        if message["type"] == "http.response.body":
            self._bodies += 1
            if self._fail_after is not None and self._bodies > self._fail_after:
                # How uvicorn surfaces a client that has gone away, and what
                # Starlette turns into `ClientDisconnect` on ASGI 2.4+.
                raise OSError("client disconnected")
        self.messages.append(message)

    @property
    def body(self) -> bytes:
        return b"".join(
            message.get("body", b"")
            for message in self.messages
            if message["type"] == "http.response.body"
        )


async def _chunks(closed: asyncio.Event | None = None) -> AsyncGenerator[bytes, None]:
    try:
        for i in range(5):
            yield f"line {i}\n".encode()
    finally:
        if closed is not None:
            closed.set()


class TestHeaders:
    def test_it_is_an_ndjson_attachment(self) -> None:
        response = NDJSONStreamingResponse(_chunks(), filename="users.ndjson")

        assert response.headers["content-type"].startswith(NDJSON_MEDIA_TYPE)
        assert (
            response.headers["content-disposition"]
            == 'attachment; filename="users.ndjson"'
        )

    def test_nothing_may_store_it_and_no_proxy_may_buffer_it(self) -> None:
        response = NDJSONStreamingResponse(_chunks(), filename="users.ndjson")

        assert response.headers["cache-control"] == "no-store"
        assert response.headers["x-accel-buffering"] == "no"

    def test_there_is_no_content_length(self) -> None:
        """It cannot be known before the last row, so the framing is chunked."""
        response = NDJSONStreamingResponse(_chunks(), filename="users.ndjson")

        assert "content-length" not in response.headers

    def test_extra_headers_win_over_the_defaults(self) -> None:
        response = NDJSONStreamingResponse(
            _chunks(),
            filename="users.ndjson",
            headers={"Cache-Control": "private, no-store"},
        )

        assert response.headers["cache-control"] == "private, no-store"

    @pytest.mark.parametrize(
        "filename",
        ["", "../etc/passwd", 'users".ndjson', "users\r\nX-Evil: 1", "a" * 129],
    )
    def test_an_unsafe_filename_is_refused(self, filename: str) -> None:
        """A header built by interpolation is a response-splitting bug."""
        with pytest.raises(ValueError, match="must match"):
            NDJSONStreamingResponse(_chunks(), filename=filename)


class TestSendingTheBody:
    async def test_every_chunk_reaches_the_client(self) -> None:
        recorder = _Recorder()

        await NDJSONStreamingResponse(_chunks(), filename="users.ndjson")(
            _scope(), _receive, recorder
        )

        assert recorder.body == b"".join(f"line {i}\n".encode() for i in range(5))

    async def test_the_generator_is_closed_when_the_stream_ends(self) -> None:
        closed = asyncio.Event()

        await NDJSONStreamingResponse(_chunks(closed), filename="users.ndjson")(
            _scope(), _receive, _Recorder()
        )

        assert closed.is_set()

    async def test_the_generator_is_closed_when_the_client_disconnects(self) -> None:
        """The release that has to happen when a download is abandoned."""
        closed = asyncio.Event()
        response = NDJSONStreamingResponse(_chunks(closed), filename="users.ndjson")

        with pytest.raises(ClientDisconnect):
            await response(_scope(), _receive, _Recorder(fail_after=2))

        assert closed.is_set()


class TestTheProblemBeingSolved:
    async def test_the_base_class_leaves_an_abandoned_generator_open(self) -> None:
        """Why `stream_response` is overridden at all.

        Starlette iterates the body with `async for` and never calls
        `aclose()`, so a generator abandoned because the client went away keeps
        its frame — and here, its database cursor — until the garbage collector
        finalizes it.
        """
        closed = asyncio.Event()
        body = _chunks(closed)
        response = StreamingResponse(body, media_type=NDJSON_MEDIA_TYPE)

        with pytest.raises(ClientDisconnect):
            await response(_scope(), _receive, _Recorder(fail_after=2))

        assert not closed.is_set(), (
            "StreamingResponse now closes its body iterator on disconnect; "
            "NDJSONStreamingResponse.stream_response may be redundant"
        )

        await body.aclose()
