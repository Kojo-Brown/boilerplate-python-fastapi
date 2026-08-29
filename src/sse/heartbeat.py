"""The keepalive, which is also the only thing that notices the client left.

## A heartbeat is not politeness, it is the disconnect detector

Under ASGI, a server tells the application a client has gone by putting
`http.disconnect` on the receive channel. Starlette's `StreamingResponse` reads
that channel **only on servers advertising ASGI spec_version below 2.4**; on
anything newer — which is what this application runs on — `__call__` streams
the body and relies on `send()` raising `OSError`, which it turns into
`ClientDisconnect`. Both halves of that are worth stating plainly:

* Nothing polls. The disconnect is discovered by a **write that fails**.
* An SSE stream that has nothing to say makes no writes.

So a stream with no keepalive and an idle source is not merely quiet: it is
*undetectable*. The client closes the tab, the socket goes away, and the
generator stays suspended at its `yield` holding a hub subscription, a task and
a connection slot, until something happens to publish an event on that topic —
which for a per-user topic may be never. The leak is invisible in every test
that disconnects and then publishes, because publishing is what repairs it.

Writing a comment frame every `interval` seconds is what converts that into a
bounded detection time: the first heartbeat after the client goes away raises
in `send`, the response unwinds, and `EventSourceResponse` closes this
generator, running the `finally` that releases the subscription. **The
heartbeat interval is therefore the ceiling on how long an abandoned stream
holds its resources**, and it is the number to reach for when idle connections
accumulate — not the buffer sizes.

The keepalive is a *comment* (`: keep-alive`) rather than a named event with no
data, because a frame with an empty data buffer is never dispatched by the
client and a named event with a payload would show up in application code that
has to learn to ignore it. A comment is invisible above the parser by
construction.

## Proxies, which is the reason everybody else adds a heartbeat

nginx, ELB and most CDNs close a connection that has carried no bytes for their
idle timeout — 60 seconds is a common default, and the client's automatic
reconnect makes the resulting churn look like flaky Wi-Fi rather than a
misconfiguration. Any `interval` comfortably under that timeout fixes it, which
is the well-known half of this module's job and the less important one.

## Why the wait is structured the way it is

`asyncio.wait` with a timeout, over a task that survives the timeout — not
`asyncio.timeout` around `anext`, and not a queue.

The pending `__anext__` is *kept* when the timer fires: the same task is waited
on again after the heartbeat goes out. Cancelling it instead would abandon a
retrieval that may already have removed an item from the source's own buffer,
so every idle tick would be a chance to lose exactly one event. Since the
source here is a hub subscription whose queue is drained by that call, "lost on
a timer, rarely, under load" is the shape of bug this construction is chosen to
make impossible — `tests/test_sse_heartbeat.py` pins it with an event delivered
across several heartbeats.

There is deliberately no queue in front of the source, unlike
`src/streaming/backpressure.py`. Read-ahead is what an export wants, because
its producer is a database cursor that should run while bytes are in flight. A
live event stream has no such producer: `src/sse/hub.py` already owns a bounded
buffer per subscriber, and a second one here would only add a place for events
to sit getting staler while the client waits for them.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import suppress
from typing import Final

from src.sse.event import comment_frame
from src.streaming.closing import closing_iterator

#: Comfortably under the 60-second idle timeout that nginx, ELB and most CDNs
#: default to, and therefore also the default ceiling on how long an abandoned
#: stream goes unnoticed.
DEFAULT_HEARTBEAT_SECONDS: Final[float] = 15.0

#: Body of the keepalive comment. Not empty: a bare `:` is legal and some
#: intermediaries have historically dropped zero-length lines, and a word here
#: makes a raw `curl` of the endpoint self-explanatory.
DEFAULT_COMMENT: Final[str] = "keep-alive"


class _End:
    """The source iterator is exhausted.

    A sentinel instance rather than `None`, so that a stream of optional values
    could never be confused with the end of one.
    """

    __slots__ = ()


_END: Final[_End] = _End()


async def _next_frame(frames: AsyncIterator[bytes]) -> bytes | _End:
    """Await one frame, reporting exhaustion as a value rather than an exception.

    `StopAsyncIteration` cannot cross a task boundary usefully — it is caught
    by whatever awaits the task and re-raised as itself, in a frame that is not
    an `async for` — so the end of the source is carried back as `_END`.
    """
    try:
        return await anext(frames)
    except StopAsyncIteration:
        return _END


async def with_heartbeat(
    frames: AsyncIterator[bytes],
    *,
    interval: float = DEFAULT_HEARTBEAT_SECONDS,
    comment: str = DEFAULT_COMMENT,
) -> AsyncGenerator[bytes, None]:
    """Yield `frames`, injecting a comment frame after every `interval` of silence.

    The timer measures silence, not wall-clock position: it restarts whenever
    anything is yielded, so a busy stream emits no keepalives at all and one
    that goes quiet emits its first exactly `interval` after the last frame.

    Args:
        frames: Encoded SSE frames. Closed on the way out if it is a generator,
            including when the consumer stops early — which is how a client
            disconnect releases whatever the source holds.
        interval: Seconds of silence before a keepalive. Must be positive.
        comment: Body of the keepalive comment.

    Raises:
        ValueError: `interval` is not positive, or `comment` contains a line
            break.
    """
    if interval <= 0:
        raise ValueError(f"interval must be positive, got {interval}.")
    beat = comment_frame(comment)

    async with closing_iterator(frames) as source:
        pending: asyncio.Task[bytes | _End] | None = None
        try:
            while True:
                if pending is None:
                    pending = asyncio.ensure_future(_next_frame(source))
                # The task is *not* cancelled when this returns empty-handed;
                # it is waited on again next time round. See the module
                # docstring — cancelling it is how an event goes missing.
                done, _ = await asyncio.wait({pending}, timeout=interval)
                if not done:
                    yield beat
                    continue
                frame = pending.result()
                pending = None
                if isinstance(frame, _End):
                    return
                yield frame
        finally:
            # Before `closing_iterator` gets its turn, and that ordering is
            # load-bearing: `aclose()` on a generator with an `__anext__` still
            # in flight raises RuntimeError rather than closing it.
            if pending is not None:
                pending.cancel()
                with suppress(asyncio.CancelledError):
                    await pending


__all__ = [
    "DEFAULT_COMMENT",
    "DEFAULT_HEARTBEAT_SECONDS",
    "with_heartbeat",
]
