"""The response class an NDJSON export is returned as.

Two things this adds to `StreamingResponse`, both of which are bugs rather than
polish if they are left out.

## The generator is closed when the response ends

Starlette iterates the body with `async for` and never calls `aclose()`. When
the loop ends normally the generator is exhausted and its `finally` has already
run, so nothing is wrong — but when the loop ends because the *client*
disconnected, the generator is left suspended at a `yield` with its cleanup
pending, and Python finalizes it whenever the garbage collector next gets to
it. For this application that generator owns a `TaskScope` holding a producer
task, which holds a server-side cursor, which holds a pooled database
connection. Waiting for a collection cycle to release those is how a handful of
abandoned downloads exhausts the pool.

`aclosing` around the body makes the release happen where it belongs: at the
end of the response, on the way out of the same await that noticed the client
had gone.

## Buffering is turned off at the proxy, not just here

An application can stream perfectly and still deliver the whole export in one
lump, because nginx buffers proxied responses by default and will happily
accumulate the entire body before forwarding a byte. `X-Accel-Buffering: no`
turns that off for nginx and its derivatives, and is ignored by everything
else. Without it, streaming is a property of this process rather than of what
the client experiences — and the failure is invisible in every test that talks
to the application directly.

`Cache-Control: no-store` is here for the reason `/api/v1/users/me` has it:
these bodies are somebody's user table.

There is deliberately no `Content-Length`. It cannot be known before the last
row is read, and computing it would mean doing the export twice or buffering
it, which is what streaming is for. The ASGI server therefore frames the
response with chunked transfer encoding — which is also why the terminal record
in `src/streaming/ndjson.py` exists, since chunked framing terminates cleanly
whether or not the application finished.
"""

from __future__ import annotations

import re
from collections.abc import AsyncGenerator, Mapping
from contextlib import aclosing
from typing import Final

from starlette.background import BackgroundTask
from starlette.responses import StreamingResponse
from starlette.types import Send

#: RFC 6266 §4.1 allows a bare `filename` only for a token-ish value. Anything
#: outside this set would need the `filename*` form and percent-encoding, and a
#: header assembled by naive interpolation is a response-splitting bug waiting
#: for a filename with a newline in it. Export filenames are chosen by this
#: codebase and not by clients, so refusing the rest is free.
_SAFE_FILENAME: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9._-]{1,128}$")

NDJSON_MEDIA_TYPE: Final[str] = "application/x-ndjson"


class NDJSONStreamingResponse(StreamingResponse):
    """A chunked `application/x-ndjson` download whose body iterator is closed.

    Args:
        content: The chunk generator. Typed as an `AsyncGenerator` rather than
            the `AsyncIterable` the base class accepts, because closing it is
            the point and a bare iterable has nothing to close.
        filename: The download name, offered as `Content-Disposition:
            attachment`. Must match `[A-Za-z0-9._-]{1,128}`.
        headers: Extra headers, applied over the defaults below.

    Raises:
        ValueError: `filename` is not a safe token.
    """

    media_type = NDJSON_MEDIA_TYPE

    def __init__(
        self,
        content: AsyncGenerator[bytes, None],
        *,
        filename: str,
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
        background: BackgroundTask | None = None,
    ) -> None:
        if not _SAFE_FILENAME.match(filename):
            raise ValueError(
                f"filename {filename!r} must match {_SAFE_FILENAME.pattern}."
            )
        merged: dict[str, str] = {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
            "X-Accel-Buffering": "no",
        }
        merged.update(headers or {})
        # Kept as its own reference: the base class stores this on
        # `body_iterator`, typed as an `AsyncContentStream` that need not have
        # `aclose`, and narrowing it back with a cast would be asserting
        # something the constructor already knows.
        self._chunks = content
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
        Starlette surfaces a client disconnect on ASGI 2.4 and later — and the
        cancellation it uses on older servers.
        """
        async with aclosing(self._chunks):
            await super().stream_response(send)


__all__ = ["NDJSON_MEDIA_TYPE", "NDJSONStreamingResponse"]
