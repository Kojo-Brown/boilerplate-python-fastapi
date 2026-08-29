"""The body an SSE route returns: frames, keepalives, a lifetime, and a log line.

    events ──encode──▶ frames ──heartbeat──▶ bytes ──▶ socket
            (per event)         (on silence)   (this generator, one lifetime)

Composition only. What an event *is* belongs to the endpoint, how it is
serialised to `src/sse/event.py`, when a keepalive is due to
`src/sse/heartbeat.py`; what is left here is the three things that need a view
of the stream as a whole.

## The `retry:` preamble

Sent before anything else, because it is advice the client needs *before* the
connection it applies to fails. An `EventSource` that has not been told
otherwise reconnects on a delay of its own choosing, and every client picking
its own delay is what turns a rolling deploy into a synchronised reconnect
storm from every browser tab that was connected.

## The lifetime, and why it is not a timeout

`max_seconds` ends a stream by *finishing* it — a last comment frame and a
clean end of body — rather than by raising. A cancelled stream and a completed
one look identical to an `EventSource`, which reconnects either way, so the
difference is entirely on this side: a clean end releases the hub
subscription through the ordinary path, while a `DeadlineExceeded` crossing a
generator that is already streaming a 200 becomes a mid-body connection reset
with a traceback in the logs for something that is not an error.

It is checked between frames rather than imposed with `asyncio.timeout`, and
that is deliberate for the reason `src/streaming/backpressure.py` gives:
`asyncio.timeout` cancels the *task*, and a timer armed inside a generator
frame fires wherever the consuming task happens to be — which for a generator
suspended at a `yield` is somewhere in the response machinery, not here. The
heartbeat guarantees this generator is resumed at least every `interval`
seconds, so a between-frames check lands within one interval of the deadline;
for a ceiling measured in minutes or hours that is precision nobody is
counting on, bought without introducing cancellation into a body that is
mid-response.

## The log line

One per stream, on the way out, saying which way it went:

* `exhausted` — the source ended. The endpoint decided to stop.
* `lifetime` — `max_seconds` was reached. Expect the client back immediately.
* `aborted` — the generator was closed from outside before either. **This is
  the client disconnect**, and it is the number to watch: it arriving one
  heartbeat interval after a client vanishes is the mechanism in
  `src/sse/heartbeat.py` working, and it never arriving at all is that
  mechanism switched off.

Nothing is awaited in that `finally`. It runs during `GeneratorExit` on the
abort path, where a suspension would raise `RuntimeError` — structlog's call is
synchronous, which is what makes the disconnect observable at all.
"""

from __future__ import annotations

import time
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import aclosing
from typing import Final

import structlog

from src.sse.event import ServerSentEvent, comment_frame, retry_frame
from src.sse.heartbeat import DEFAULT_HEARTBEAT_SECONDS, with_heartbeat
from src.streaming.closing import closing_iterator

logger = structlog.get_logger(__name__)

#: Comment sent as the last frame of a stream that reached `max_seconds`.
#: Visible in a `curl` session, invisible to `EventSource` listeners.
LIFETIME_COMMENT: Final[str] = "stream lifetime reached; reconnect"


class _Delivered:
    """How many events — not keepalives — have been written into the body.

    Mutable state rather than a return value: the count is written by the
    encoding stage and read by the log line in `finally`, and on the abort path
    there is no return for it to travel through.
    """

    __slots__ = ("count",)

    def __init__(self) -> None:
        self.count = 0


async def _encode(
    events: AsyncIterator[ServerSentEvent],
    delivered: _Delivered,
) -> AsyncGenerator[bytes, None]:
    """Serialise each event, counting the ones that reach the body.

    `closing_iterator`, not a bare `async for`. Closing *this* generator —
    which is what a client disconnect eventually does — throws `GeneratorExit`
    at its own `yield` and leaves `events` suspended at its, closed by the
    garbage collector whenever it gets there. `events` is the hub subscription,
    so that is the difference between a disconnect releasing the registration
    and a dead stream still being fanned out to.
    """
    async with closing_iterator(events) as source:
        async for event in source:
            delivered.count += 1
            yield event.encode()


async def sse_stream(
    events: AsyncIterator[ServerSentEvent],
    *,
    name: str,
    heartbeat: float = DEFAULT_HEARTBEAT_SECONDS,
    retry: int | None = None,
    max_seconds: float | None = None,
) -> AsyncGenerator[bytes, None]:
    """Stream `events` as `text/event-stream` bytes.

    Args:
        events: The events to send. Closed on the way out if it is a
            generator, which is what releases a hub subscription when the
            client disconnects.
        name: Identifies the stream in logs.
        heartbeat: Seconds of silence before a keepalive comment. Also the
            ceiling on how long an abandoned stream goes unnoticed.
        retry: Reconnection delay in milliseconds to advertise, or `None` to
            leave the client's default alone.
        max_seconds: Ceiling on the stream's lifetime, or `None` for none.
            Ends the body cleanly; the client reconnects.

    Yields:
        UTF-8 frames, each a complete SSE event or comment.
    """
    delivered = _Delivered()
    started = time.monotonic()
    outcome = "aborted"
    try:
        if retry is not None:
            yield retry_frame(retry)
        # `aclosing`, not a bare `async for`: this generator is closed from
        # outside on a client disconnect, which leaves the loop without running
        # the heartbeat generator's `finally` — and that `finally` is what
        # cancels the pending read and closes the source behind it.
        async with aclosing(
            with_heartbeat(_encode(events, delivered), interval=heartbeat)
        ) as frames:
            async for frame in frames:
                yield frame
                if max_seconds is None:
                    continue
                if time.monotonic() - started >= max_seconds:
                    yield comment_frame(LIFETIME_COMMENT)
                    outcome = "lifetime"
                    return
        outcome = "exhausted"
    finally:
        logger.info(
            "sse.stream_closed",
            stream=name,
            outcome=outcome,
            events=delivered.count,
            seconds=round(time.monotonic() - started, 3),
        )


__all__ = ["LIFETIME_COMMENT", "sse_stream"]
