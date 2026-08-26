"""Finishing the cleanup after the caller has already been cancelled.

## The situation

A client disconnects. Starlette cancels the handler task, and the
`CancelledError` surfaces at whatever the handler was awaiting. That is correct
and desirable — nobody is waiting for the answer any more — right up to the
point where the unwinding reaches a `finally:` that has to release an
idempotency reservation, roll a transaction back, or release a distributed
lock. Those awaits are on the cancelled task too, so the cleanup is cancelled
in the middle, and what is left behind is a reservation answering every retry
with 409 for its whole TTL, or a lock nobody holds.

The failure needs two cancellations to appear, which is why it survives review:
the first is delivered and caught, the cleanup starts, and it takes a *second*
one — a shutdown cancelling outstanding tasks, an enclosing `TaskGroup`
aborting, a `gather_bounded` draining its siblings — to cut the cleanup short.
So it never happens in development and happens under load.

## Why `asyncio.shield` is not the answer

`shield` is the obvious tool and it solves the other half of the problem:

    await asyncio.shield(release())

The inner coroutine is protected from the cancellation. The *await* is not — it
raises `CancelledError` immediately, the caller carries on unwinding, and
`release()` keeps running with nobody holding it. Which is precisely the
unowned task from `src/structured/scope.py`: it may be collected mid-flight, its
exception is never retrieved, and shutdown does not wait for it. `shield`
converts "cleanup that got cancelled" into "cleanup that may or may not
happen", and the second one is harder to see.

What is actually needed is to keep waiting: absorb the cancellations aimed at
us, let the cleanup finish, and only then honour them. That is `protect` and
`finalize`.

## Which of the two

They differ in one decision — what to do with a cancellation that arrived while
the cleanup was running — and it is the decision that depends on where the call
sits.

`protect` re-raises it. Use it where the protected call is the work: the
caller was cancelled, the work is done, and the cancellation still has to reach
whoever asked for it.

`finalize` re-arms it on the current task instead of raising, and swallows the
cleanup's own failures. Use it in an `except:` or `finally:` block, where
raising would replace the exception being unwound with an artefact of the
cleanup — turning a 500 anyone could debug into a bare `CancelledError`, and
losing the original entirely. Re-arming means the cancellation is delivered at
the caller's next `await` and the task still ends cancelled, so nothing is
dropped; it is deferred by exactly the length of the cleanup.

Neither swallows a cancellation. `_drain` catches `CancelledError` and does not
re-raise it there, which is what `tests/test_cancellation_gate.py` exists to
forbid, so it is on that gate's exemption table with this paragraph's reason.

See `docs/structured-concurrency.md`.
"""

from __future__ import annotations

import asyncio

import structlog

from src.parallel.io import AwaitableFactory
from src.structured.errors import DeadlineExceeded

logger = structlog.get_logger(__name__)


async def _drain[T](
    task: asyncio.Task[T],
    *,
    name: str,
    timeout: float | None,
) -> tuple[asyncio.CancelledError | None, bool]:
    """Wait for `task`, absorbing cancellations aimed at us.

    Returns the first absorbed cancellation (or `None`) and whether the
    protection budget ran out. The task is always finished when this returns.

    `asyncio.wait` rather than `asyncio.shield`, and the difference is the
    whole mechanism: `wait` does not cancel the futures it is waiting on, so a
    cancellation delivered here leaves `task` running and we can go straight
    back to waiting for it. `shield` would deliver the same cancellation and
    lose the reference at the same time.

    `uncancel()` on each absorbed cancellation is bookkeeping that looks
    optional and is not. An enclosing `asyncio.timeout` decides whether the
    cancellation it sees is its own expiry by comparing the task's cancelling
    count against the one it recorded on entry; leaving the count high means
    the enclosing scope stops converting its own expiry into `TimeoutError`
    and re-raises a `CancelledError` nobody requested.
    """
    loop = asyncio.get_running_loop()
    expires_at = None if timeout is None else loop.time() + timeout
    deferred: asyncio.CancelledError | None = None
    current = asyncio.current_task()

    while not task.done():
        wait_for = None if expires_at is None else max(0.0, expires_at - loop.time())
        try:
            await asyncio.wait({task}, timeout=wait_for)
        except asyncio.CancelledError as exc:
            if deferred is None:
                deferred = exc
            if current is not None:
                current.uncancel()
            logger.warning("structured.cancellation_deferred", scope=name)
            continue

        if not task.done():
            # The budget ran out. The protection is over either way, so the
            # only question is whether the work keeps running unowned — and it
            # must not, which is what the cancel-and-await below is for.
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            return deferred, True

    return deferred, False


async def protect[T](
    factory: AwaitableFactory[T],
    *,
    name: str,
    timeout: float | None = None,
) -> T:
    """Run `factory()` to completion even if the caller is cancelled meanwhile.

    Takes a factory rather than an awaitable for the reason `gather_bounded`
    does: a coroutine that is never awaited — because an argument raised before
    this was reached — warns from the garbage collector rather than at the call
    site.

    `timeout` bounds the protection, not the work's usefulness. Without one, a
    cleanup that hangs holds shutdown open until the supervisor sends SIGKILL,
    which truncates every *other* shutdown step that had not run yet. With one,
    the work is cancelled and awaited when the budget runs out. It is a
    separate number from `deadline()` on purpose: this call is already past the
    point where the request's budget stopped meaning anything.

    Raises:
        asyncio.CancelledError: a cancellation arrived while waiting. Re-raised
            after the work has finished, never before, and it outranks both of
            the following — the caller is going away, and what the work
            returned or raised is no longer interesting.
        DeadlineExceeded: `timeout` elapsed. The work was cancelled and awaited
            before this was raised, so nothing is still running.
        Exception: whatever the work itself raised.
    """
    task = asyncio.ensure_future(factory())
    task.set_name(f"protected:{name}")

    deferred, timed_out = await _drain(task, name=name, timeout=timeout)
    if deferred is not None:
        raise deferred
    if timed_out:
        # `timeout` cannot be None here: `_drain` only reports a budget it was
        # given one to measure.
        raise DeadlineExceeded(name, timeout or 0.0)
    return task.result()


async def finalize[T](
    factory: AwaitableFactory[T],
    *,
    name: str,
    timeout: float | None = None,
) -> T | None:
    """`protect` for an `except:` or `finally:` block: never raises.

    Returns the result, or `None` if the cleanup failed, was cancelled, or ran
    past `timeout`. Every one of those is logged with `name`, because a cleanup
    that silently did not happen is the failure this module exists to prevent
    and swapping one silence for another would be no improvement.

    A cancellation absorbed while the cleanup ran is re-armed on the current
    task rather than raised, so it is delivered at the caller's next `await`
    and the task still ends cancelled — deferred by the length of the cleanup,
    not discarded. That is what lets the caller's own `raise` re-raise the
    exception it was already unwinding instead of this one.

    Not for the happy path. A cleanup whose failure genuinely has to fail the
    request is `protect`; this one is for the cases where the response has
    already gone out or a more important exception is in flight.
    """
    task = asyncio.ensure_future(factory())
    task.set_name(f"finalize:{name}")

    deferred, timed_out = await _drain(task, name=name, timeout=timeout)

    if deferred is not None:
        current = asyncio.current_task()
        if current is not None:
            # Re-armed, not raised. Delivered at the caller's next await; if
            # there is none, `Task.__step` applies it when the coroutine ends,
            # so the task is still reported as cancelled either way.
            current.cancel()
        logger.warning("structured.cancellation_rearmed", scope=name)

    if timed_out:
        logger.error("structured.finalize_timed_out", scope=name, timeout=timeout)
        return None
    if task.cancelled():
        logger.warning("structured.finalize_cancelled", scope=name)
        return None

    failure = task.exception()
    if failure is not None:
        logger.error(
            "structured.finalize_failed",
            scope=name,
            error=f"{type(failure).__name__}: {failure}",
        )
        return None
    return task.result()


__all__ = [
    "finalize",
    "protect",
]
