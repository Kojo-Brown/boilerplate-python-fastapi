"""NDJSON on the wire: one record per line, and a last line that says so.

## Why the format is newline-delimited and not a JSON array

A JSON array has to be closed. Streaming one means writing `[`, streaming
records, and writing `]` — and if the export dies at the four-millionth row the
client receives a document that no parser will accept, which is at least
honest, but it also means no client can process anything until the last byte
has arrived. NDJSON is parseable a line at a time, which is what makes a large
export usable by a consumer that streams it into a database rather than into
memory.

## The problem NDJSON creates, and the terminal record that fixes it

Truncation is invisible. A stream cut off at row 400,000 is a *valid* NDJSON
document of 400,000 records, and every framing HTTP has to say otherwise is
unavailable by the time it matters: `Content-Length` cannot be known before the
last row, chunked encoding's terminator is written by the ASGI server whether
or not the application finished, and the status line went out with the first
chunk — the response is a 200 by the time anything can go wrong. So a consumer
that does the obvious thing gets a short file and no error, and a nightly job
quietly syncs a fraction of the table.

Every stream therefore ends with a **terminal record**, and its absence is the
signal:

    {"id": "...", "email": "..."}                       ← a user
    {"id": "...", "email": "..."}                       ← a user
    {"_export": "complete", "records": 2}               ← the stream ended here

    {"_export": "failed", "records": 41000,
     "error": "DEADLINE_EXCEEDED", "message": "..."}    ← and here, badly

The rule for clients is one line long: **a stream whose last record is not an
`_export` record is truncated, whatever its HTTP status said.** `records` is
the count in the body above, so a consumer can check what it parsed against
what the server claims it sent, and `error` is an application error code from
`src/exceptions.py` — the same vocabulary the JSON error envelope uses, so
failures are classified identically whether they happen before or after the
first byte.

`_export` cannot collide with a data record: export row schemas are frozen
Pydantic models with declared fields, and `tests/test_streaming_ndjson.py`
asserts that none of them declares this key.

## Two encoding details that are not stylistic

**`allow_nan=False`.** Python's `json` emits bare `NaN` and `Infinity` for
non-finite floats, which no other language's JSON parser accepts. The default
turns an unrepresentable value into a line that breaks the consumer's parser
somewhere in the middle of a large file. Refusing it makes the same problem an
exception on the server, where it has a traceback. `src/outbox/codec.py`
refuses non-finite floats for a related reason.

**U+2028 and U+2029 are escaped even though `ensure_ascii=False`.** Exports are
large and ASCII-escaping every non-Latin name inflates them for nothing, so
this writes UTF-8 directly — but Python's own `str.splitlines()` treats
U+2028/U+2029 as line breaks, and so does the JavaScript spec for source text.
A display name containing one would split a record in half for a client
splitting the way its standard library suggests. Escaping the two of them is
valid JSON, decodes back to the same string, and costs a scan that finds
nothing on essentially every record.
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from typing import Final

from src.exceptions import AppException
from src.streaming.closing import closing_iterator

#: The key that marks the terminal record. Reserved: no export row may use it.
TERMINAL_KEY: Final[str] = "_export"

#: 64 KiB. Comfortably above a TCP segment and below the point where the
#: coalescing buffer is itself worth worrying about; the read-ahead depth
#: multiplies it, so this and `readahead` together are the memory bound.
DEFAULT_CHUNK_BYTES: Final[int] = 65536

#: Escaped despite `ensure_ascii=False`; see the module docstring.
_LINE_SEPARATORS: Final[tuple[tuple[str, str], ...]] = (
    ("\u2028", "\\u2028"),
    ("\u2029", "\\u2029"),
)


class RecordCount:
    """How many records have been written into chunks handed downstream.

    Mutable, and a plain class rather than a frozen dataclass, because it is
    *state* and not a value: the producer writes it as it goes and the terminal
    record reads it once the producer has stopped. The queue in
    `src/streaming/backpressure.py` is what orders those two — the count is
    read only after the envelope that ends the stream has been received, which
    the producer put there after its last write.

    It counts records that reached a yielded chunk, not records the socket
    accepted. Those differ only when the client disconnects mid-stream, and in
    that case no terminal record is emitted at all, so the number is never
    reported for a body that did not contain it.
    """

    __slots__ = ("emitted",)

    def __init__(self) -> None:
        self.emitted = 0


def encode_line(record: Mapping[str, object]) -> bytes:
    """Serialise one record as a UTF-8 NDJSON line, newline included.

    Raises:
        ValueError: the record contains a non-finite float, or a value `json`
            cannot serialise.
    """
    text = json.dumps(
        record,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    for raw, escaped in _LINE_SEPARATORS:
        text = text.replace(raw, escaped)
    return f"{text}\n".encode()


def completion_record(emitted: int) -> dict[str, object]:
    """The terminal record for a stream that delivered every record."""
    return {TERMINAL_KEY: "complete", "records": emitted}


def failure_record(emitted: int, error: BaseException) -> dict[str, object]:
    """The terminal record for a stream that stopped early.

    An `AppException` contributes its own `error_code` and message, so a client
    classifies a mid-stream failure exactly as it would the JSON envelope for
    the same failure before the first byte. Anything else is reported as
    `INTERNAL_ERROR` with a fixed message: the response is already a 200 and
    the body is already public, so this is no place to start quoting exception
    text at whoever asked for the export.
    """
    if isinstance(error, AppException):
        return {
            TERMINAL_KEY: "failed",
            "records": emitted,
            "error": error.error_code,
            "message": error.message,
        }
    return {
        TERMINAL_KEY: "failed",
        "records": emitted,
        "error": "INTERNAL_ERROR",
        "message": "The export stopped before it finished.",
    }


async def chunk_records(
    records: AsyncIterator[Mapping[str, object]],
    *,
    chunk_bytes: int = DEFAULT_CHUNK_BYTES,
    counter: RecordCount,
) -> AsyncGenerator[bytes, None]:
    """Encode `records` and coalesce the lines into chunks of ~`chunk_bytes`.

    Coalescing is not an optimisation to be measured later. Yielding one line
    per record means one ASGI `send` per record, which is one `write` syscall
    and one HTTP chunk header — around a dozen bytes of framing on a
    hundred-byte row — for every user in the table. The buffer is flushed on
    the first record that takes it past the threshold, so a chunk may exceed
    `chunk_bytes` by up to one record and never splits a line.

    A failing source flushes what is buffered before the error is allowed
    through. Those records were read successfully and encoded successfully;
    dropping them would silently discard up to a chunk's worth of work, and —
    worse — the terminal record would then report a smaller number than the
    export had actually managed. Yielding from the `except` block hands the
    partial chunk over, and the `raise` runs when the consumer asks for the
    next one.

    Args:
        records: The records to encode. Iterated inside `closing_iterator`, so
            a generator holding a cursor is finalized when this stops early.
        chunk_bytes: Flush threshold in bytes. Must be positive.
        counter: Updated as chunks are yielded, for the terminal record.

    Raises:
        ValueError: `chunk_bytes` is not positive, or a record cannot be
            encoded (see `encode_line`).
    """
    if chunk_bytes < 1:
        raise ValueError(f"chunk_bytes must be at least 1, got {chunk_bytes}.")

    buffer = bytearray()
    pending = 0

    def _take() -> bytes:
        nonlocal pending
        counter.emitted += pending
        pending = 0
        chunk = bytes(buffer)
        buffer.clear()
        return chunk

    async with closing_iterator(records) as source:
        try:
            async for record in source:
                buffer += encode_line(record)
                pending += 1
                if len(buffer) >= chunk_bytes:
                    yield _take()
        except Exception:
            if buffer:
                yield _take()
            raise
    if buffer:
        yield _take()


__all__ = [
    "DEFAULT_CHUNK_BYTES",
    "TERMINAL_KEY",
    "RecordCount",
    "chunk_records",
    "completion_record",
    "encode_line",
    "failure_record",
]
