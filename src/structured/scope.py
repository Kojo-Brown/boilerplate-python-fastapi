"""Background tasks that cannot outlive the block that started them.

## The problem with `asyncio.create_task`

    asyncio.create_task(send_the_thing())

Three things are wrong with that line and none of them raise:

**Nobody owns the task.** The loop holds only a weak reference, so a task with
no strong reference anywhere can be garbage-collected mid-await and simply stop
— documented behaviour, and the reason `asyncio.create_task`'s own
documentation tells you to keep the return value. What the caller sees is work
that silently did not happen, at a rate that depends on when the collector ran.

**Nobody reads the exception.** A task that raises and is never awaited logs
`Task exception was never retrieved` from a `__del__`, at collection time,
detached from any request context — if it logs at all, since the loop's default
handler can be replaced. The failure is not raised, not counted, and not
attributable.

**It outlives its context.** The task borrows the `ContextVar`s of whoever
created it and keeps running after that request has answered, so its logs carry
a stale request id, its database session may already be closed, and shutdown
does not wait for it.

`TaskScope` fixes all three by making the lifetime lexical: a task started in a
scope has ended by the time the block does, and whichever way it ended is
visible to the code that opened the scope.

## Why not just `asyncio.TaskGroup`

Because `TaskGroup` waits, and half of what a server runs in the background
never finishes. Put the outbox relay's `while True:` loop in a plain
`TaskGroup` and `__aexit__` blocks forever: shutdown hangs, SIGTERM escalates
to SIGKILL, and the in-flight batch is truncated rather than rolled back. A
daemon needs the opposite exit rule — cancel, then wait for the unwinding — and
that is `WhenScopeExits.CANCEL`.

The rest of `TaskGroup` is exactly right and is used as-is underneath, because
the parts that look simple are the parts that are not: cancelling siblings when
one child fails, collecting the results into a `BaseExceptionGroup`, and the
`uncancel()` bookkeeping that keeps an enclosing `asyncio.timeout` able to tell
its own expiry from someone else's cancellation. Reimplementing that to add two
features would be a downgrade.

What this adds on top:

- **Factories, not coroutines**, for the reason `src/parallel/io.py` takes
  them: a coroutine object exists from the moment it is written, so one that
  the scope never gets to start emits `RuntimeWarning: coroutine ... was never
  awaited` from the garbage collector, somewhere unrelated in the log.
- **`CANCEL` on exit**, above.
- **Names on every child**, prefixed with the scope's. In a process that is
  hung, `asyncio.all_tasks()` is the only evidence available, and a list of
  `<Task pending coro=<run() running at relay.py:280>>` is not evidence.
- **A real error for starting a task in a closed scope**, rather than
  `TaskGroup`'s message, which names neither the scope nor the task.

## What still propagates as an `ExceptionGroup`

A child that raises takes the scope down, and the exception arrives wrapped by
`TaskGroup` itself: `ExceptionGroup("unhandled errors in a TaskGroup",
[OSError(...)])`, caught with `except*`. Unwrapping single-child groups was
considered and rejected — it
makes the common case prettier and the type of the exception depend on how many
children happened to fail, so the handler that works in testing is the one that
breaks the day two of them fail together.

The one thing that is unwrapped is the *body's* own exception. `TaskGroup`
appends it to the group alongside its children's, so `raise ValueError` inside
the block leaves it as an `ExceptionGroup` — which means every caller who
writes an ordinary `raise` inside a scope has to write `except*` outside it,
for an error with nothing to do with concurrency. A `with` block should hand
back the exception you raised in it. That rule is narrow enough not to conflict
with the paragraph above: the group's single member has to *be* the object the
body raised, so a child's failure is never unwrapped, and a body exception that
arrives alongside a child failure stays grouped with it.

That a crashing daemon takes the scope down is the intended behaviour and not a
side effect. The alternative is a process that keeps serving with its relay
dead, which is the failure mode the outbox exists to prevent.

See `docs/structured-concurrency.md`.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from enum import StrEnum
from types import TracebackType
from typing import Any

import structlog

from src.structured.errors import TaskScopeClosedError

logger = structlog.get_logger(__name__)

#: A zero-argument callable producing the coroutine to run as a child.
#:
#: Narrower than `AwaitableFactory` in `src/parallel/io.py`, which accepts any
#: `Awaitable`, and narrower because `TaskGroup.create_task` is: a `Future` or
#: an object with `__await__` cannot be turned into a task, and accepting one
#: here would move that `TypeError` from the type checker to the first request
#: that used it. An async function satisfies this; so does
#: `functools.partial` of one.
type CoroutineFactory = Callable[[], Coroutine[Any, Any, Any]]


class WhenScopeExits(StrEnum):
    """What leaving the block does to children that are still running.

    Named for the decision rather than the mechanism, matching `WhenOneFails`
    in `src/parallel/io.py`, because picking the wrong one does not fail
    loudly: `WAIT` around a daemon hangs shutdown, and `CANCEL` around work
    that was supposed to finish drops it a moment before it would have.
    """

    #: Wait for every child to finish on its own. `asyncio.TaskGroup`'s rule,
    #: and the right one for work the block exists to complete — a fan-out
    #: whose results the caller is about to use.
    WAIT = "wait"

    #: Cancel every child, then wait for it to unwind. The right one for
    #: anything that runs until told to stop: a poller, a lease renewer, a
    #: relay. The waiting is not politeness — `cancel()` only schedules the
    #: `CancelledError`, and the coroutine has to be resumed for its `finally`
    #: to roll a transaction back or release a lock.
    CANCEL = "cancel"


class TaskScope:
    """A block whose background tasks have ended by the time it exits.

        async with TaskScope("relay", on_exit=WhenScopeExits.CANCEL) as scope:
            scope.start_soon(relay.run, name="drain")
            yield                       # serve requests
        # the drain task is cancelled, unwound, and finished here

    Not reusable and not reentrant: a closed scope raises rather than opening
    again, because the second use would share the first use's name and the
    confusion that causes in a log outlives whatever the reuse saved.
    """

    def __init__(
        self,
        name: str,
        *,
        on_exit: WhenScopeExits = WhenScopeExits.WAIT,
    ) -> None:
        self._name = name
        self._on_exit = on_exit
        self._group: asyncio.TaskGroup | None = None
        self._closed = False
        self._children: list[asyncio.Task[Any]] = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def on_exit(self) -> WhenScopeExits:
        return self._on_exit

    @property
    def open(self) -> bool:
        """Whether `start_soon` would be accepted right now."""
        return self._group is not None

    @property
    def children(self) -> tuple[asyncio.Task[Any], ...]:
        """Every child started in this scope, finished ones included.

        Finished ones included on purpose: after the block exits this is the
        record of what ran, which is what a shutdown log and a test both want.
        """
        return tuple(self._children)

    async def __aenter__(self) -> TaskScope:
        if self._closed or self._group is not None:
            raise TaskScopeClosedError(
                f"Task scope {self._name!r} cannot be entered twice."
            )
        group = asyncio.TaskGroup()
        await group.__aenter__()
        self._group = group
        return self

    def start_soon(
        self,
        factory: CoroutineFactory,
        *,
        name: str,
    ) -> asyncio.Task[Any]:
        """Start `factory()` as a child of this scope.

        Synchronous, like `TaskGroup.create_task` and unlike everything else
        here: the task is scheduled and the caller carries on. The returned
        task is for inspection — its name, whether it is done — and awaiting it
        outside the scope defeats the point of having one.

        Raises:
            TaskScopeClosedError: the scope is not open.
        """
        group = self._group
        if group is None:
            raise TaskScopeClosedError(
                f"Cannot start {name!r}: task scope {self._name!r} is not open."
            )
        task: asyncio.Task[Any] = group.create_task(
            factory(), name=f"{self._name}:{name}"
        )
        self._children.append(task)
        logger.debug("structured.child_started", scope=self._name, child=name)
        return task

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        group = self._group
        if group is None:  # pragma: no cover - unreachable via `async with`
            return None
        self._group = None
        self._closed = True

        if self._on_exit is WhenScopeExits.CANCEL:
            # Before delegating, and only here: `TaskGroup.__aexit__` waits for
            # children it has no reason to cancel, which for a `while True:`
            # loop is forever. Cancelling first turns that wait into the drain
            # it needs to be — and it stays a real drain, because the group
            # still awaits every child afterwards.
            #
            # A child cancelled this way contributes no error: `TaskGroup`
            # discards cancelled children rather than collecting them, so a
            # clean shutdown does not arrive as an `ExceptionGroup` of eight
            # `CancelledError`s. A child that had already *failed* on its own
            # still does, which is the asymmetry to want.
            for child in self._children:
                if not child.done():
                    child.cancel()

        try:
            try:
                # `TaskGroup.__aexit__` returns `None` — it never suppresses —
                # so this one does too, and the `bool | None` in the signature
                # is the protocol's shape rather than a claim that anything is
                # swallowed.
                await group.__aexit__(exc_type, exc, traceback)
            except BaseExceptionGroup as grouped:
                # `TaskGroup` appends the *body's* exception to the group as
                # well as its children's, so `raise ValueError` inside the
                # block comes back out as an `ExceptionGroup` wrapping it. That
                # is a surprise a context manager has no business springing:
                # the exception you raised inside a `with` should be the one
                # that leaves it, or every caller has to write `except*` for
                # errors that have nothing to do with concurrency.
                #
                # The unwrapping rule is deliberately narrow — the group's one
                # member must *be* the object the body raised — so it can never
                # unwrap a child's failure. Single-child groups from children
                # stay grouped, because otherwise the type of the exception
                # would depend on how many children happened to fail, and the
                # handler that worked in testing would break the day two of
                # them failed together.
                if (
                    exc is not None
                    and len(grouped.exceptions) == 1
                    and grouped.exceptions[0] is exc
                ):
                    raise exc from None
                raise
            return None
        finally:
            logger.debug(
                "structured.scope_closed",
                scope=self._name,
                on_exit=str(self._on_exit),
                children=len(self._children),
                failed=sum(
                    1
                    for child in self._children
                    if child.done()
                    and not child.cancelled()
                    and child.exception() is not None
                ),
            )


__all__ = [
    "CoroutineFactory",
    "TaskScope",
    "WhenScopeExits",
]
