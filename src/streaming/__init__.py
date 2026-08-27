"""Large responses that never exist in memory all at once.

A row count nobody bounded is the shape of most of this application's
worst-case memory use, and an export endpoint is where it shows up first:
`select(User)` into a list into a `JSONResponse` is fine on a seed database and
is an OOM kill on a real one, with no line of code in between to blame.

Three modules, each answering one question that has to be answered before that
is fixed rather than moved:

**How do the bytes get produced without materialising the answer?**
`ndjson.py` — one record per line, encoded and coalesced into chunks big enough
that framing is not most of the traffic.

**What stops the producer running away from the client?** `backpressure.py` —
a single owned producer task behind a bounded queue, so read-ahead is a number
you choose rather than however many rows the database can return before the
client's TCP window closes.

**What happens when it fails halfway?** `export.py` — nothing HTTP can do,
because the 200 is already sent, so the body ends with a record that says
whether it is complete. `response.py` makes sure the generator behind it is
closed when the client hangs up rather than whenever the collector notices.

## What is not here

**CSV.** It is the format everyone asks for and it cannot express the one thing
this design turns on: a truncated CSV is a valid CSV, with no place to put a
terminal marker that a spreadsheet would not render as a final row of data. An
export people load into Excel and silently lose 30% of is worse than one they
have to convert, so the conversion is the client's — the terminal record is
what makes it safe to do.

**A background export job with a download link.** That is the right answer past
some size, and it is a different feature: a job table, object storage, a
notification, an expiry policy. This is the synchronous half, bounded by
`EXPORT_DEADLINE_SECONDS` so that the point at which it stops being the right
answer is a configured number rather than a pager.

See `docs/streaming.md`.
"""

from src.streaming.backpressure import DEFAULT_READAHEAD, with_readahead
from src.streaming.export import RecordSource, stream_ndjson_export
from src.streaming.ndjson import (
    DEFAULT_CHUNK_BYTES,
    TERMINAL_KEY,
    RecordCount,
    chunk_records,
    completion_record,
    encode_line,
    failure_record,
)
from src.streaming.response import NDJSON_MEDIA_TYPE, NDJSONStreamingResponse

__all__ = [
    "DEFAULT_CHUNK_BYTES",
    "DEFAULT_READAHEAD",
    "NDJSON_MEDIA_TYPE",
    "NDJSONStreamingResponse",
    "RecordCount",
    "RecordSource",
    "TERMINAL_KEY",
    "chunk_records",
    "completion_record",
    "encode_line",
    "failure_record",
    "stream_ndjson_export",
    "with_readahead",
]
