"""The pipeline an export route hands to `StreamingResponse`.

    records ──encode+coalesce──▶ chunks ──bounded queue──▶ socket
             (producer task, one deadline)   (readahead)    (response task)

Three stages, each fixing something the stage on its own would get wrong:

**Encode and coalesce** (`src/streaming/ndjson.py`) so the wire carries whole
lines in useful quantities rather than one syscall per row.

**Read ahead by a bounded amount** (`src/streaming/backpressure.py`) so
producing and sending overlap without the queue between them becoming a copy of
the export in memory.

**Append a terminal record**, here, because the other two cannot: this is the
only stage that knows both how many records went out and whether the producer
stopped on purpose.

## The rule that shapes all of this

Once the first chunk is sent the status line is spent. `app.add_exception_handler`
is installed inside the middleware stack and cannot help — Starlette's own
`ServerErrorMiddleware` re-raises rather than rendering when the response has
started, because there is nothing else it could do. So a failure after the
first byte has exactly two possible outcomes: the connection is cut and the
client sees a truncated file with a 200 status, or the *body* admits to it.

`stream_ndjson_export` makes it the second one. Everything the producer can
raise is turned into a terminal `failed` record, so the client's parse of the
last line is the answer to "did I get all of this", and no HTTP status has to
be reinterpreted after the fact.

Failures *before* the first byte are a different matter and stay ordinary. The
route resolves its dependencies, authorises the caller, and validates its query
parameters before this generator is ever iterated, so a 401, 403 or 422 is
still a JSON error envelope with the right status — which is the reason the
first chunk is produced lazily rather than eagerly primed.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator, Callable, Mapping
from contextlib import aclosing

import structlog

from src.streaming.backpressure import DEFAULT_READAHEAD, with_readahead
from src.streaming.ndjson import (
    DEFAULT_CHUNK_BYTES,
    RecordCount,
    chunk_records,
    completion_record,
    encode_line,
    failure_record,
)

logger = structlog.get_logger(__name__)

#: Builds the records to export. Called once, inside the producer task.
type RecordSource = Callable[[], AsyncIterator[Mapping[str, object]]]


async def stream_ndjson_export(
    records: RecordSource,
    *,
    name: str,
    chunk_bytes: int = DEFAULT_CHUNK_BYTES,
    readahead: int = DEFAULT_READAHEAD,
    budget: float | None = None,
) -> AsyncGenerator[bytes, None]:
    """Stream `records` as NDJSON, ending with a record that says how it went.

    Args:
        records: Builds the record iterator. Anything it raises becomes a
            terminal `failed` record rather than propagating, since by then
            the response is already a 200.
        name: Identifies the stream in logs, task names and deadline errors.
        chunk_bytes: Flush threshold for the coalescing buffer.
        readahead: Chunks the producer may run ahead by. Peak memory for the
            stream is about `readahead * chunk_bytes`.
        budget: Seconds the producer may take in total, or `None` for no
            ceiling. Bounds how long a pooled connection and a server-side
            cursor are held, including time spent blocked on a client that
            has stopped reading.

    Yields:
        UTF-8 chunks, each a whole number of NDJSON lines.
    """
    counter = RecordCount()

    def produce() -> AsyncIterator[bytes]:
        return chunk_records(records(), chunk_bytes=chunk_bytes, counter=counter)

    try:
        # `aclosing`, not a bare `async for`: breaking out of a loop over a
        # generator leaves it suspended, and this generator is closed from
        # outside whenever a client disconnects. Without it the read-ahead
        # scope — and the producer task, and its cursor — would be released by
        # the garbage collector rather than here.
        async with aclosing(
            with_readahead(
                produce,
                readahead=readahead,
                name=name,
                budget=budget,
            )
        ) as chunks:
            async for chunk in chunks:
                yield chunk
    except Exception as exc:
        # Not re-raised, and that is the whole point of this function. Letting
        # it propagate here would cut the connection with a 200 already on the
        # wire and leave the client holding a file that looks complete.
        logger.warning(
            "streaming.export_failed",
            stream=name,
            records=counter.emitted,
            error=type(exc).__name__,
        )
        yield encode_line(failure_record(counter.emitted, exc))
        return

    logger.info(
        "streaming.export_complete",
        stream=name,
        records=counter.emitted,
    )
    yield encode_line(completion_record(counter.emitted))


__all__ = ["RecordSource", "stream_ndjson_export"]
