"""Read-ahead with a ceiling: the producer runs ahead, but only so far.

## What "backpressure-aware" has to mean here

Hand `StreamingResponse` a plain async generator and producing and sending are
*the same coroutine*. Every database read waits for the previous chunk's socket
write, and every socket write waits for the next read. On a fast client that
costs a round trip of latency per chunk; on a slow one the database cursor sits
idle for the whole download while its connection stays checked out of the pool.

The obvious fix — a task filling a queue — is worse, because the obvious queue
is unbounded. A producer that reads rows faster than a phone on a train can
accept them will read *the entire table* into that queue, which is the export
materialised in memory: exactly the thing streaming was for. Nothing raises;
the pod is OOM-killed instead, and only under the traffic that made it happen.

`with_readahead` is the middle: one producer task, one **bounded** queue. The
producer runs ahead by at most `readahead` items and then blocks in
`Queue.put`, which is the backpressure — the client's read rate reaches the
database through the queue instead of the other way round. Peak memory is
`readahead` items and cannot become anything else, so with byte chunks as the
item type it is a number you can multiply out before deploying.

`readahead=1` is not the same as no queue: the producer may prepare one chunk
while the previous one is in flight, which is the overlap the whole thing is
for. It is the smallest useful setting rather than a disabled one.

## Who owns the producer

`TaskScope` from `src/structured`, with `WhenScopeExits.CANCEL`. A bare
`asyncio.create_task` here would be the textbook version of every problem that
module documents: the task is held weakly by the loop, so a client that
disconnects mid-export leaves it running with nobody to notice, and its
exception surfaces from a `__del__` if it surfaces at all. With a scope, the
producer has ended — cancelled and unwound, its cursor closed — by the time
this generator's `finally` returns, whether the stream ended, failed, or the
client hung up at the third chunk.

The source is iterated inside `closing_iterator` for the second half of that.
Cancelling the producer task raises `CancelledError` at whichever await is
pending, typically `Queue.put`, which leaves the source generator suspended at
its own `yield` and finalized whenever the garbage collector next runs. For a
generator holding a server-side cursor that is not soon enough, and closing it
here is what turns "eventually" into "before this block exits".

## Failure, and the one thing it cannot do

A producer exception is put on the queue *behind* the items already in it and
re-raised in the consumer once those have been delivered. Order is preserved,
so a caller that has already written 40,000 rows to the client sees the failure
after them rather than instead of them.

That is deliberately all it does. Once the response has started, HTTP has no
way to retract the 200 already sent, so the caller — not this module — has to
decide how the *body* admits to being incomplete. `src/streaming/export.py`
does it with a terminal record.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Callable
from dataclasses import dataclass
from typing import Final

import structlog

from src.streaming.closing import closing_iterator
from src.structured.deadline import deadline
from src.structured.scope import TaskScope, WhenScopeExits

logger = structlog.get_logger(__name__)

#: A zero-argument callable producing the iterator to read ahead of.
#:
#: A factory rather than an iterator for the reason `TaskScope.start_soon` and
#: `src/parallel/io.py` take factories: the object has to be created inside the
#: producer task, so that a scope which never starts it never leaves an
#: un-awaited generator for the garbage collector to complain about.
type IteratorFactory[T] = Callable[[], AsyncIterator[T]]

#: One chunk in flight while the next is prepared. The smallest value that
#: still overlaps producing with sending; see the module docstring.
DEFAULT_READAHEAD: Final[int] = 2


@dataclass(frozen=True, slots=True)
class _Item[T]:
    """One value on its way from the producer to the consumer."""

    value: T


@dataclass(frozen=True, slots=True)
class _Finished:
    """The producer stopped. `error` is `None` if it ran out of items."""

    error: Exception | None


async def _pump[T](
    source: IteratorFactory[T],
    queue: asyncio.Queue[_Item[T] | _Finished],
) -> None:
    """Move every item from `source()` into `queue`, closing it on the way out."""
    async with closing_iterator(source()) as items:
        async for item in items:
            await queue.put(_Item(item))


async def with_readahead[T](
    source: IteratorFactory[T],
    *,
    readahead: int = DEFAULT_READAHEAD,
    name: str,
    budget: float | None = None,
) -> AsyncGenerator[T, None]:
    """Yield `source()`'s items, produced at most `readahead` ahead of the caller.

    Args:
        source: Builds the iterator to read. Called once, inside the producer
            task.
        readahead: How many items may sit ready before the producer blocks.
            Peak memory is this many items, and nothing else.
        name: Names the scope and the deadline in logs and errors, so a hung
            export is identifiable in `asyncio.all_tasks()`.
        budget: Seconds the producer may take in total, or `None` for no
            ceiling. The clock covers the time it spends blocked on a full
            queue as well as the time it spends producing, which is the point:
            what has to be bounded is how long the underlying resource — a
            server-side cursor, a pooled connection — is held, and a client
            that has stopped reading holds it just as effectively as a slow
            query does.

    Raises:
        ValueError: `readahead` is below 1.
        DeadlineExceeded: `budget` ran out. Raised in the consumer, after every
            item the producer had already handed over.
        Exception: whatever `source()` raised, re-raised here once the items
            ahead of it have been yielded.
    """
    if readahead < 1:
        raise ValueError(f"readahead must be at least 1, got {readahead}.")

    queue: asyncio.Queue[_Item[T] | _Finished] = asyncio.Queue(maxsize=readahead)

    async def produce() -> None:
        try:
            if budget is None:
                await _pump(source, queue)
            else:
                # Inside the producer *task*, never inside the source
                # generator. `asyncio.timeout` cancels the task it was entered
                # in, and the cancellation lands wherever that task is
                # suspended — usually `Queue.put`, which is a frame the
                # generator's own `async with` would never see. Entered here,
                # the conversion to `DeadlineExceeded` happens in the same
                # frame that armed the timer, whichever await was cut.
                async with deadline(budget, name=name):
                    await _pump(source, queue)
        except Exception as exc:
            # Forwarded rather than raised, so it arrives behind the items
            # already queued instead of overtaking them. `Exception` and not
            # `BaseException`: a cancellation is the scope telling this task to
            # stop, and there is nobody left to deliver it to.
            await queue.put(_Finished(exc))
        else:
            await queue.put(_Finished(None))

    async with TaskScope(f"readahead:{name}", on_exit=WhenScopeExits.CANCEL) as scope:
        scope.start_soon(produce, name="produce")
        while True:
            envelope = await queue.get()
            if isinstance(envelope, _Finished):
                if envelope.error is not None:
                    logger.warning(
                        "streaming.producer_failed",
                        stream=name,
                        error=type(envelope.error).__name__,
                    )
                    raise envelope.error
                return
            yield envelope.value


__all__ = [
    "DEFAULT_READAHEAD",
    "IteratorFactory",
    "with_readahead",
]
