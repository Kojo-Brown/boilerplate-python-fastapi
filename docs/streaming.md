# Streaming exports

`GET /api/v1/exports/users` returns every account in the system without the
application ever holding more than about 128 KiB of it. This document is about
the four things that have to be true for that sentence to mean anything, and
the one that is usually skipped.

- [The shape](#the-shape)
- [Backpressure](#backpressure)
- [Failing after the first byte](#failing-after-the-first-byte)
- [Holding the cursor](#holding-the-cursor)
- [Configuration](#configuration)
- [Consuming an export](#consuming-an-export)
- [Adding an export](#adding-an-export)
- [What is deliberately not here](#what-is-deliberately-not-here)

## The shape

```
src/users/export.py         what a record is, and the port that produces one
src/repositories/user.py    the adapter: one server-side cursor over `users`
src/streaming/ndjson.py     encode a record, coalesce lines into chunks
src/streaming/backpressure.py  one owned producer task behind a bounded queue
src/streaming/export.py     the pipeline, plus the terminal record
src/streaming/response.py   the response class, and closing the generator
src/api/v1/exports.py       the route
```

Data flows the other way round:

```
users ─cursor─▶ records ─encode─▶ chunks ─bounded queue─▶ socket
                └──────── producer task, one deadline ────┘
```

## Backpressure

Hand `StreamingResponse` a plain async generator and producing and sending are
the same coroutine: every database read waits for the previous socket write and
every write waits for the next read. That is correct, and it is why a client on
a slow link keeps a database cursor idle for the whole download.

The obvious fix is a task and a queue, and the obvious queue is unbounded — at
which point a producer that reads faster than the client accepts reads *the
whole table* into memory, which is the thing streaming was for. Nothing raises;
the pod is OOM-killed under the traffic that caused it.

`with_readahead` is the middle. One producer task, one queue with a maximum
size. The producer runs ahead by at most `readahead` chunks and then blocks in
`Queue.put`, so the client's read rate reaches the database instead of the
other way round, and peak memory is:

```
EXPORT_READAHEAD_CHUNKS × EXPORT_CHUNK_BYTES     # 2 × 64 KiB = 128 KiB
```

per in-flight export, which is a number you can multiply by the concurrent
downloads you expect and put in a memory request.

`readahead=1` is the smallest useful value and is not the same as no queue: one
chunk is prepared while the previous one is in flight, which is the overlap the
whole mechanism exists for.

The producer is a child of a `TaskScope` (see
[structured concurrency](./structured-concurrency.md)), with
`WhenScopeExits.CANCEL`. A bare `asyncio.create_task` here would be every
problem that module documents at once: a client that disconnects mid-export
would leave a task the loop holds only weakly, still reading rows, with its
exception retrieved by nobody.

## Failing after the first byte

This is the part that is usually skipped, and it is the reason the body has a
format rather than just rows in it.

Once the first chunk is sent, the status line is spent. The exception handlers
in `src/exception_handlers.py` are installed inside the middleware stack and
cannot help; Starlette's own `ServerErrorMiddleware` re-raises rather than
rendering when the response has started, because there is nothing else it could
do. So a failure at row 400,000 has two possible outcomes: the connection is
cut and the client keeps a truncated file that arrived with a `200 OK`, or the
body admits to it.

Nothing in HTTP rescues this. `Content-Length` cannot be known before the last
row; chunked transfer encoding's terminator is written by the ASGI server
whether or not the application finished; and NDJSON truncated at any line is
still valid NDJSON. A consumer doing the obvious thing gets a short file and no
error, and a nightly job quietly syncs a fraction of the table.

So **every stream ends with a terminal record**:

```json
{"id":"…","email":"…","role":"user", …}
{"id":"…","email":"…","role":"user", …}
{"_export":"complete","records":2}
```

and when it goes wrong:

```json
{"id":"…","email":"…","role":"user", …}
{"_export":"failed","records":41000,"error":"DEADLINE_EXCEEDED","message":"…"}
```

`error` is an application error code from `src/exceptions.py`, so a mid-stream
failure is classified with exactly the vocabulary the JSON error envelope uses
before the first byte. An unexpected exception reports `INTERNAL_ERROR` and a
fixed message: the response is already a 200 and the body is already on its way
to whoever asked, which is no place to quote a traceback.

Records already encoded into the coalescing buffer are flushed before the
failure record, so `records` describes the body above it exactly, and nothing
that was successfully read is thrown away.

Failures *before* the first byte stay ordinary. Authorisation, query-parameter
validation and dependency resolution all happen before the generator is
iterated, so a 401, 403 or 422 is a normal JSON envelope with a normal status.

## Holding the cursor

An export is one `SELECT` with `yield_per`, which under asyncpg is a
server-side cursor. Two consequences:

**It is consistent.** Postgres evaluates the cursor against the snapshot the
statement started with, so a five-minute export is a coherent picture of the
table at the moment it began — rows written after it started are not in it, and
rows deleted after it started still are.

**It holds a pooled connection for as long as the client reads.** That is the
real cost of a synchronous export, and it is bounded by
`EXPORT_DEADLINE_SECONDS` rather than left to the client. The deadline is armed
around the producer *task*, so it covers time spent blocked on a client that
has stopped reading as well as time spent querying — a stalled download holds
the cursor exactly as effectively as a slow query does. When it expires, the
stream ends with a `failed` terminal record instead of a truncation nobody can
detect.

Two things release that cursor early, and both had to be built:

- `NDJSONStreamingResponse` closes its body generator. Starlette iterates the
  body with `async for` and never calls `aclose()`, so a generator abandoned
  because the client went away keeps its frame — and the `TaskScope`, producer
  task and cursor inside it — until the garbage collector finalizes it. That is
  not a schedule a connection pool can be sized against.
- `stream_ndjson_export` closes the read-ahead generator for the same reason,
  and `with_readahead` closes the source. Each layer of the pipeline closes the
  one below it, because breaking out of an `async for` closes nothing.

The query selects the eight published columns rather than `select(User)`. That
is not about memory — SQLAlchemy's identity map holds weak references, so
streamed entities are collected as they go. It is about the field list:
`select(User)` emits `SELECT users.hashed_password, …`, so an export that must
not contain a password hash would be fetching one per row and discarding it in
the application. `tests/test_streaming_db.py` captures the SQL and asserts the
hash is never named — and asserts that the entity query it replaces *does* name
it, so the reason cannot quietly stop being true.

## Configuration

| Setting | Default | What it trades |
| --- | --- | --- |
| `EXPORT_CHUNK_BYTES` | 65536 | Framing overhead against buffer size. Below ~4 KiB the HTTP chunk headers and syscalls start to be a real fraction of the traffic. |
| `EXPORT_READAHEAD_CHUNKS` | 2 | Overlap against memory. Multiplied by the above, this is the peak per export. |
| `EXPORT_BATCH_ROWS` | 500 | Round trips against latency to the first chunk. Not a memory knob — the read-ahead bounds memory whatever the cursor fetches. |
| `EXPORT_DEADLINE_SECONDS` | 300.0 | How long one export may hold a connection. Must sit above the slowest legitimate export; when it cannot, the answer is an asynchronous export job, not a bigger number. |

## Consuming an export

```python
import httpx, json

with httpx.stream(
    "GET",
    "https://api.example.com/api/v1/exports/users",
    headers={"Authorization": f"Bearer {token}"},
) as response:
    response.raise_for_status()
    terminal = None
    for line in response.iter_lines():
        record = json.loads(line)
        if "_export" in record:
            terminal = record
            break
        handle(record)

if terminal is None:
    raise RuntimeError("export truncated: no terminal record")
if terminal["_export"] != "complete":
    raise RuntimeError(f"export failed after {terminal['records']} records")
```

The rule is one line long: **a stream whose last record is not an `_export`
record is truncated, whatever its HTTP status said.** Check it. `records` is
there so a consumer can compare what it parsed against what the server claims
it sent.

Do not use `str.splitlines()` on the decoded body to frame records: it breaks
on U+2028 and U+2029 as well as `\n`. This encoder escapes both for exactly
that reason, so it is safe here — but the habit is not.

## Adding an export

1. Declare the record as a frozen Pydantic model, listing the fields
   explicitly. A column added to the table later is then absent from the export
   until somebody decides otherwise, which is the direction that fails safe.
2. Declare a port for it — a `Protocol` with one method returning an
   `AsyncIterator` of that record. Write it as a plain `def`, not `async def`:
   that is what an async generator function's type is, and `async def` in the
   protocol would make every implementation fail to match.
3. Implement the adapter with `session.stream(...)` and
   `execution_options(yield_per=...)`, naming the columns you publish and
   closing the result in a `finally`.
4. Wire the provider into `src/dependencies.py` with `Depends(get_db)`, so the
   cursor lives on the request's session.
5. In the route, build a *factory* for the records and hand it to
   `stream_ndjson_export`, wrapped in an `NDJSONStreamingResponse`. A factory,
   because the iterator has to be created inside the producer task: one built
   in the handler and never started warns from the garbage collector, and one
   started in another task opens its cursor outside the scope that owns it.

## What is deliberately not here

**CSV.** It is the format everyone asks for, and it cannot express the thing
this design turns on. A truncated CSV is a valid CSV, and there is nowhere to
put a terminal marker that a spreadsheet would not render as one more row of
data. An export that people open in Excel and silently lose 30% of is worse
than one they have to convert, so converting is the client's job — after it has
checked the terminal record.

**A background export job with a download link.** Past some size that is the
right answer, and it is a different feature: a job table, object storage, a
notification, an expiry policy. This is the synchronous half, and
`EXPORT_DEADLINE_SECONDS` is where the boundary between them is written down.

**Resumption.** There is no cursor parameter and no `Range` support, so a
consumer whose stream failed at record 41,000 starts again. Making that
resumable means a stable snapshot that outlives one request, which is the
background-job feature above rather than a parameter on this one.

**Rate limiting or admission control on exports.** Each one holds a connection
for its duration, so enough concurrent exports will exhaust the pool. The
deadline bounds each of them; nothing yet bounds how many run at once. See
`src/limiter.py` for the per-route mechanism this will use.
