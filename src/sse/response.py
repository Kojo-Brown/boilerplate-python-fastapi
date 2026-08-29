"""The response class an SSE route returns.

`StreamingResponse` with a media type would be most of the way there. The rest
is four headers and one lifecycle guarantee, each of which is the difference
between a stream that works and one that works on a developer's laptop.

## The generator is closed when the response ends

Starlette iterates the body with `async for` and never calls `aclose()`. On a
normal end that costs nothing — the generator is exhausted and its `finally`
has already run. On a client disconnect it costs everything: the body
generator is left suspended at a `yield`, holding a hub subscription and a
pending read, until the garbage collector finalises it. For a stream, "until
the GC gets to it" is not a schedule a connection limit can be sized against —
and unlike an export, an SSE endpoint is *designed* to be abandoned by clients,
so this is the common path rather than the exceptional one.

`aclosing` around the body makes the release happen at the end of the response,
in the same await that discovered the client had gone. This is the same
reasoning as `src/streaming/response.py`, and it applies harder here.

## `X-Accel-Buffering: no`

nginx buffers proxied responses by default. For an export that means the
download arrives in one lump instead of progressively; for an event stream it
means nothing arrives *at all* until the buffer fills or the connection ends,
which for a stream that never ends is never. SSE behind a default nginx without
this header is the single most common way the feature is reported broken, and
the failure cannot be reproduced by any test that talks to the application
directly.

## `Cache-Control: no-store`

These bodies are somebody's account activity, and an intermediary that decided
to cache an infinite response would be holding it open on the origin as well.

## Two headers that are deliberately absent

**`Connection: keep-alive`.** A hop-by-hop header that the ASGI spec forbids an
application from setting: HTTP/2 and HTTP/3 have no such field, and uvicorn
manages HTTP/1.1's own. Setting it is a no-op at best.

**`Content-Length`.** There is no length; the body ends when the stream does.
The ASGI server frames it with chunked transfer encoding, which is what allows
a body to be written indefinitely — and is why `text/event-stream` needs no
terminal record of the kind `src/streaming/ndjson.py` has to construct. A
truncated event stream is not a wrong answer to a question, it is a
subscription that ended, and the client's job on seeing one is to reconnect,
which it does without being asked.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Mapping
from contextlib import aclosing

from starlette.background import BackgroundTask
from starlette.responses import StreamingResponse
from starlette.types import Send

from src.sse.event import SSE_MEDIA_TYPE


class EventSourceResponse(StreamingResponse):
    """A `text/event-stream` body whose generator is closed when it ends.

    Args:
        content: The frame generator. Typed as an `AsyncGenerator` rather than
            the `AsyncIterable` the base class accepts, because closing it is
            the point and a bare iterable has nothing to close.
        headers: Extra headers, applied over the defaults above.
    """

    media_type = SSE_MEDIA_TYPE

    def __init__(
        self,
        content: AsyncGenerator[bytes, None],
        *,
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
        background: BackgroundTask | None = None,
    ) -> None:
        merged: dict[str, str] = {
            "Cache-Control": "no-store",
            "X-Accel-Buffering": "no",
        }
        merged.update(headers or {})
        # Kept as its own reference: the base class stores this on
        # `body_iterator`, typed as an `AsyncContentStream` that need not have
        # `aclose`, and narrowing it back with a cast would be asserting
        # something the constructor already knows.
        self._frames = content
        super().__init__(
            content,
            status_code=status_code,
            headers=merged,
            media_type=self.media_type,
            background=background,
        )

    async def stream_response(self, send: Send) -> None:
        """Send the body, closing the generator however this returns.

        Covers the normal end, an exception from `send` — which is how
        Starlette surfaces a client disconnect on ASGI 2.4 and later, and
        therefore how an abandoned stream is discovered at all — and the
        cancellation it uses on older servers.
        """
        async with aclosing(self._frames):
            await super().stream_response(send)


__all__ = ["EventSourceResponse"]
