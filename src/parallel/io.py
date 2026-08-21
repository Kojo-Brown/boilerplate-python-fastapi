"""Fanning out IO-bound work with a bound on how much runs at once.

## Why not `asyncio.gather`

`gather` is the obvious tool and it is wrong in two ways that both stay hidden
until production.

**It is unbounded.** `gather(*(fetch(u) for u in urls))` with ten thousand URLs
opens ten thousand sockets at once. The upstream sees a thundering herd, the
process runs out of file descriptors, and the connection pool underneath —
`httpx`'s, SQLAlchemy's — silently serialises everything behind its own limit
while ten thousand tasks sit in the loop's ready queue making the scheduler
slower. The fix is a semaphore, and the semaphore has to be acquired *inside*
each task, not around the `gather`.

**It leaks siblings on failure.** This is the one that surprises people:
`gather(..., return_exceptions=False)` propagates the first exception to the
awaiting coroutine *and leaves the other tasks running*. It does not cancel
them. So a handler that fans out five calls, has one fail, and returns a 502
has four requests still in flight against the upstream, writing their results
nowhere, keeping connections checked out, and logging exceptions from a request
that finished minutes ago. Under load that is a slow leak with no obvious
cause, and `gather`'s own documentation is easy to read as promising the
opposite.

`gather_bounded` fixes both: concurrency is capped, and on failure the
remaining tasks are cancelled *and awaited* before the exception leaves, so
nothing outlives the call.

## Why factories, not coroutines

`gather_bounded` takes callables that *return* awaitables, not awaitables:

    await gather_bounded((partial(fetch, url) for url in urls), limit=8)

Two reasons, both practical. A coroutine object starts existing the moment you
write `fetch(url)`, so passing coroutines means constructing all ten thousand
up front — the memory this function exists to bound. And a coroutine that is
never awaited (because a fail-fast run cancelled the batch before reaching it)
emits `RuntimeWarning: coroutine ... was never awaited` from the garbage
collector, at a point in the log with nothing to do with the failure. Factories
are constructed lazily, one per slot, so neither happens.

## Order, and what comes back

Results are returned in the order of the inputs, never completion order. That
is what makes the function safe to zip back against whatever produced the
inputs, and it is worth stating because "bounded concurrency" helpers often
return completion order and produce a bug that only appears when one call is
slower than the others.

## What this is not

It is not a rate limiter. A semaphore bounds *concurrency* — how many are in
flight — not *rate*, how many start per second. Eight concurrent calls that each
take 10ms is 800 requests per second at an upstream that may only allow 100.
When the upstream publishes a rate, that needs a token bucket as well; the
semaphore only keeps this process from opening more sockets than it can afford.

It is also not a retry loop. `src/decorators/retry.py` wraps a single call, and
the composition that usually works is retry on the inside — `partial(retry(...)
(fetch), url)` — so an attempt that fails on its own is retried within its slot
rather than failing the whole batch.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable
from enum import StrEnum
from typing import Literal, overload

import structlog

logger = structlog.get_logger(__name__)

#: A zero-argument callable producing the awaitable to run. `functools.partial`
#: is the usual way to make one; a lambda works too, since nothing here crosses
#: a process boundary — unlike `src/parallel/cpu.py`, where it would not.
type AwaitableFactory[T] = Callable[[], Awaitable[T]]


class WhenOneFails(StrEnum):
    """What a batch does with the rest of its work when one item raises.

    Named for the decision rather than for a mechanism, because the two options
    are a genuine product choice and the wrong one is silent. `CANCEL_REST`
    suits a request handler assembling one response: if a part is missing the
    response cannot be built, so continuing to spend on the other parts buys
    nothing. `RUN_ALL` suits work whose items are independently useful — a
    fan-out of notifications, a batch import — where one bad row must not
    discard the ninety-nine good ones.
    """

    #: Cancel the outstanding items, wait for them to unwind, re-raise the
    #: first exception. Nothing is still running when the call returns.
    CANCEL_REST = "cancel_rest"

    #: Let every item finish. Exceptions are returned in place of results, so
    #: the caller decides per item.
    RUN_ALL = "run_all"


@overload
async def gather_bounded[T](
    factories: Iterable[AwaitableFactory[T]],
    *,
    limit: int | None = ...,
    semaphore: asyncio.Semaphore | None = ...,
    when_one_fails: Literal[WhenOneFails.CANCEL_REST] = ...,
) -> list[T]: ...


@overload
async def gather_bounded[T](
    factories: Iterable[AwaitableFactory[T]],
    *,
    limit: int | None = ...,
    semaphore: asyncio.Semaphore | None = ...,
    when_one_fails: Literal[WhenOneFails.RUN_ALL],
) -> list[T | BaseException]: ...


async def gather_bounded[T](
    factories: Iterable[AwaitableFactory[T]],
    *,
    limit: int | None = None,
    semaphore: asyncio.Semaphore | None = None,
    when_one_fails: WhenOneFails = WhenOneFails.CANCEL_REST,
) -> list[T] | list[T | BaseException]:
    """Run every factory's awaitable, at most `limit` at a time, in input order.

    Pass `limit` for a bound private to this call, or `semaphore` for one shared
    with other callers. Sharing is the right choice when the thing being
    protected is a *resource* rather than this batch — one semaphore held next
    to an upstream client bounds every fan-out that talks to it, where a
    per-call limit of 8 in six concurrent requests is 48 connections.

    The return type follows the mode, through two overloads rather than one
    union: `CANCEL_REST` gives `list[T]` — anything that failed raised instead
    of being returned — while `RUN_ALL` gives `list[T | BaseException]`, so a
    caller who does not narrow it gets a type error rather than a runtime
    surprise. `partition_results` is the narrowing. A single union return would
    have pushed that burden onto the common path, where it does not belong.

    Cancellation from outside propagates: if the caller is cancelled, every
    outstanding item is cancelled and awaited before `CancelledError` leaves,
    so a disconnected client never leaves work running behind it.
    """
    if limit is None and semaphore is None:
        raise ValueError("Pass either `limit` or `semaphore`.")
    if limit is not None and semaphore is not None:
        raise ValueError("Pass `limit` or `semaphore`, not both.")
    if limit is not None and limit < 1:
        raise ValueError(f"limit must be at least 1, got {limit}.")

    # Built inside the coroutine, so the semaphore belongs to the loop that is
    # about to use it. A module-level default would be shared across every
    # loop the process ever runs — which in a test suite means one test's
    # leftover waiters throttling the next.
    gate = semaphore if semaphore is not None else asyncio.Semaphore(limit or 1)

    factory_list = list(factories)
    if not factory_list:
        return []

    async def run_one(factory: AwaitableFactory[T]) -> T:
        # Acquired inside the task rather than around the batch: the point is
        # that only `limit` awaitables exist and are in flight at once, and a
        # semaphore held outside would let every one of them be constructed and
        # scheduled before the first acquire.
        async with gate:
            return await factory()

    tasks = [asyncio.ensure_future(run_one(factory)) for factory in factory_list]

    try:
        if when_one_fails is WhenOneFails.RUN_ALL:
            return await asyncio.gather(*tasks, return_exceptions=True)

        # `gather(return_exceptions=False)` would raise here and leave the
        # siblings running, which is the leak this module exists to close. Wait
        # for the first exception instead, then unwind deliberately.
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        failure = next(
            (task.exception() for task in done if task.exception() is not None), None
        )
        if failure is not None:
            await _cancel_and_drain(pending)
            raise failure
        return [task.result() for task in tasks]
    except asyncio.CancelledError:
        # The caller was cancelled — a client disconnect, an enclosing
        # `wait_for`. Nothing outlives this function: the items are cancelled
        # and awaited before the cancellation is allowed to continue upwards.
        await _cancel_and_drain(tasks)
        raise


async def _cancel_and_drain[T](tasks: Iterable[asyncio.Future[T]]) -> None:
    """Cancel `tasks` and wait for every one of them to actually finish.

    The waiting is the part that matters. `task.cancel()` only *requests*
    cancellation: it schedules a `CancelledError` into the coroutine, which then
    has to be resumed for the exception to be delivered and for its `finally`
    blocks — the ones releasing connections and closing files — to run.
    Returning without awaiting would leave that unfinished, which is the same
    leak in a different costume.

    `return_exceptions=True` because these tasks are expected to end in
    `CancelledError` and a few may have already failed on their own; neither is
    news at this point, and letting either propagate would replace the original
    exception with an artefact of the cleanup.
    """
    outstanding = [task for task in tasks if not task.done()]
    for task in outstanding:
        task.cancel()
    if outstanding:
        await asyncio.gather(*outstanding, return_exceptions=True)


@overload
async def map_bounded[T, R](
    fn: Callable[[T], Awaitable[R]],
    items: Iterable[T],
    *,
    limit: int | None = ...,
    semaphore: asyncio.Semaphore | None = ...,
    when_one_fails: Literal[WhenOneFails.CANCEL_REST] = ...,
) -> list[R]: ...


@overload
async def map_bounded[T, R](
    fn: Callable[[T], Awaitable[R]],
    items: Iterable[T],
    *,
    limit: int | None = ...,
    semaphore: asyncio.Semaphore | None = ...,
    when_one_fails: Literal[WhenOneFails.RUN_ALL],
) -> list[R | BaseException]: ...


async def map_bounded[T, R](
    fn: Callable[[T], Awaitable[R]],
    items: Iterable[T],
    *,
    limit: int | None = None,
    semaphore: asyncio.Semaphore | None = None,
    when_one_fails: WhenOneFails = WhenOneFails.CANCEL_REST,
) -> list[R] | list[R | BaseException]:
    """`gather_bounded` for the common case of one async function over a list.

    Exists because building the factories by hand is where the mistake gets
    made: `(fn(item) for item in items)` looks like a generator of factories
    and is a generator of *coroutines*, which reintroduces both problems the
    factory form avoids. Spelling it once here means no call site has to get
    the `partial` right.

    The late-binding closure trap is why this takes `fn` and `items` separately
    rather than accepting `lambda: fn(item)` from the caller: written that way
    in a loop, every lambda would capture the same `item`.
    """
    factories = [_bind(fn, item) for item in items]
    # Dispatched rather than forwarded, because `gather_bounded`'s overloads are
    # keyed on the *literal* mode: forwarding the variable would match neither
    # of them and collapse the return type. The branch is the price of the
    # caller keeping a precise one.
    if when_one_fails is WhenOneFails.RUN_ALL:
        return await gather_bounded(
            factories,
            limit=limit,
            semaphore=semaphore,
            when_one_fails=WhenOneFails.RUN_ALL,
        )
    return await gather_bounded(
        factories,
        limit=limit,
        semaphore=semaphore,
        when_one_fails=WhenOneFails.CANCEL_REST,
    )


def _bind[T, R](fn: Callable[[T], Awaitable[R]], item: T) -> AwaitableFactory[R]:
    """Bind `item` now, so the factory does not read a loop variable later."""

    def factory() -> Awaitable[R]:
        return fn(item)

    return factory


def partition_results[T](
    results: list[T | BaseException],
) -> tuple[list[tuple[int, T]], list[tuple[int, BaseException]]]:
    """Split a `RUN_ALL` result list into successes and failures, with indices.

    The indices are the point. A caller that filtered the exceptions out with a
    comprehension would keep the successes but lose which input each one came
    from, and re-deriving that by position no longer works once the list has
    holes in it — which is exactly when the caller needs to report *which*
    items failed.
    """
    successes: list[tuple[int, T]] = []
    failures: list[tuple[int, BaseException]] = []
    for index, result in enumerate(results):
        if isinstance(result, BaseException):
            failures.append((index, result))
        else:
            successes.append((index, result))
    return successes, failures
