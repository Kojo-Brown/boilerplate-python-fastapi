"""Re-running a transaction that lost a deadlock.

A deadlock is not a bug to be fixed at the call site; with concurrent writers
and more than one row it is a scheduling outcome, and Postgres has already
resolved it by the time anyone hears about it — one transaction was chosen and
killed, the other committed. The victim's correct response is to do the whole
thing again.

## Why `@retry` from `src/decorators` cannot do this

It is the same shape and it would be wrong, for a reason worth stating in full:
**a Postgres transaction is dead after any error, and re-running a statement
inside it does not fail with the original error but with SQLSTATE 25P02**,
"current transaction is aborted, commands ignored until end of transaction
block". So the second attempt would not retry the deadlock — it would raise
something new, unrelated and confusing, and the third attempt would do it
again. The retry is only sound with a `ROLLBACK` in between, which is a fact
about database sessions that a general-purpose call retrier has no business
knowing. Hence a second, narrower loop here. The backoff policy itself is not
duplicated: both call `backoff_delay` in `src/decorators/base.py`.

## What the work must look like

Two obligations, and neither can be checked from here:

**Re-read everything, every attempt.** The rollback undoes the reads as well as
the writes, so a row loaded before the first attempt describes a world that no
longer exists — and, more to the point, was the stale copy that lost. Take
identifiers as arguments and load rows inside the callable.

**Own the commit.** A serialisation failure under `REPEATABLE READ` or
`SERIALIZABLE` is raised by `COMMIT` itself, not by the statement that caused
it, so a caller that commits after this function returns has put the failure it
wanted retried outside the loop.

## What it deliberately does not do

It does not roll back after the final failure. On exhaustion the exception
propagates and the session is left exactly as an unwrapped call would have left
it — aborted, for the caller's existing error handling to deal with. Cleaning
up on the way out would mean a rollback on a path where a *second* failure has
nowhere to go but on top of the first, and would make the wrapper's failure
mode differ from the unwrapped one for no gain: nothing was committed either
way.

It also does not retry inside a savepoint. `ROLLBACK TO SAVEPOINT` would
recover a subtransaction, but it keeps every lock the outer transaction already
holds — including, quite possibly, the one the other party is waiting on. So
this owns the transaction it retries. Do not call it inside a wider transaction
whose work you expect to survive.
"""

from __future__ import annotations

import asyncio
import functools
import random
from collections.abc import Awaitable, Callable
from typing import Concatenate, ParamSpec, TypeVar

import structlog
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.decorators.base import (
    DEFAULT_RNG,
    AsyncSleeper,
    backoff_delay,
    default_event_name,
)
from src.locking.errors import RETRYABLE_SQLSTATES, is_retryable_conflict, sqlstate

logger = structlog.get_logger(__name__)

P = ParamSpec("P")
R = TypeVar("R")

#: A unit of work run inside one transaction. It is handed the session rather
#: than closing over one so that the same callable can be re-run against a
#: session the loop has just rewound, and so the dependency is visible in the
#: signature instead of hidden in a closure.
TransactionalWork = Callable[Concatenate[AsyncSession, P], Awaitable[R]]


async def run_with_deadlock_retry[**Q, T](
    session: AsyncSession,
    work: Callable[Concatenate[AsyncSession, Q], Awaitable[T]],
    /,
    *args: Q.args,
    **kwargs: Q.kwargs,
) -> T:
    """Run `work(session, ...)`, retrying it if it loses a deadlock.

    Rolls the session back between attempts — see the module docstring for why
    that is load-bearing rather than tidy — waits with full-jitter backoff, and
    tries again. Anything that is not a retryable conflict propagates on the
    first attempt.

    Policy is configured by decorating with `retry_on_deadlock` instead; this
    function takes the defaults so that the common call reads as the work it
    does. `*args`/`**kwargs` are forwarded to `work` unchanged and are typed
    against its signature, so a wrong argument is a mypy error rather than a
    `TypeError` on the first deadlock in production.

    Example:
        >>> async def settle(session: AsyncSession, invoice_id: uuid.UUID) -> None:
        ...     invoice = await lock_row(session, Invoice, invoice_id)
        ...     ...
        ...     await session.commit()
        >>> await run_with_deadlock_retry(session, settle, invoice_id)
    """
    return await _DEFAULT_POLICY.run(session, work, *args, **kwargs)


class DeadlockRetryPolicy:
    """How many times to re-run a losing transaction, and how long to wait.

    Returned by `retry_on_deadlock()`; usable directly through `run()` when the
    work is a closure rather than a named function.
    """

    def __init__(
        self,
        *,
        attempts: int,
        codes: frozenset[str],
        base_delay: float,
        max_delay: float,
        jitter: bool,
        rng: random.Random,
        asleep: AsyncSleeper,
        event: str | None,
    ) -> None:
        if attempts < 1:
            raise ValueError("attempts must be at least 1.")
        if base_delay < 0:
            raise ValueError("base_delay must not be negative.")
        if max_delay < base_delay:
            raise ValueError("max_delay must be at least base_delay.")
        if not codes:
            raise ValueError(
                "codes must name at least one SQLSTATE; an empty set is a "
                "retry policy that never retries, which is a bug rather than "
                "a configuration."
            )

        self._attempts = attempts
        self._codes = codes
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._jitter = jitter
        self._rng = rng
        self._asleep = asleep
        self._event = event

    def __call__(
        self, func: Callable[Concatenate[AsyncSession, P], Awaitable[R]]
    ) -> Callable[Concatenate[AsyncSession, P], Awaitable[R]]:
        """Wrap an `async def f(session, ...)` so it retries under this policy.

        The session being the first parameter is the convention that makes this
        typeable: `Concatenate[AsyncSession, P]` preserves every other
        parameter, so the wrapper has the wrapped function's signature and
        mypy still checks its call sites.
        """
        event = self._event or default_event_name(func)

        # `session` is positional-only here so the wrapper's type matches
        # `Concatenate[AsyncSession, P]`, where the prefix is positional by
        # definition. `functools.wraps` still copies `__name__`, `__doc__` and
        # `__wrapped__` across, so `inspect.signature` reports the decorated
        # function's real parameters — which is what FastAPI reads if one of
        # these ever becomes a dependency.
        @functools.wraps(func)
        async def wrapper(
            session: AsyncSession, /, *args: P.args, **kwargs: P.kwargs
        ) -> R:
            return await self._run(session, func, event, *args, **kwargs)

        return wrapper

    async def run[**Q, T](
        self,
        session: AsyncSession,
        work: Callable[Concatenate[AsyncSession, Q], Awaitable[T]],
        /,
        *args: Q.args,
        **kwargs: Q.kwargs,
    ) -> T:
        """Run `work(session, *args, **kwargs)` under this policy."""
        return await self._run(
            session, work, self._event or default_event_name(work), *args, **kwargs
        )

    async def _run[**Q, T](
        self,
        session: AsyncSession,
        work: Callable[Concatenate[AsyncSession, Q], Awaitable[T]],
        event: str,
        *args: Q.args,
        **kwargs: Q.kwargs,
    ) -> T:
        for attempt in range(1, self._attempts + 1):
            try:
                return await work(session, *args, **kwargs)
            except asyncio.CancelledError:
                # Never retried, whatever `codes` says. The caller has stopped
                # waiting — a disconnected client, a shutdown — and re-running
                # a transaction on its behalf holds locks for work nobody will
                # read. Re-raised before the retryability check so a deadlock
                # that arrives *as* a cancellation cannot be caught by it.
                raise
            except SQLAlchemyError as exc:
                if not is_retryable_conflict(exc, codes=self._codes):
                    raise
                if attempt >= self._attempts:
                    logger.warning(
                        f"{event}.deadlock_retry_exhausted",
                        attempts=self._attempts,
                        sqlstate=sqlstate(exc),
                    )
                    raise

                delay = backoff_delay(
                    attempt,
                    base_delay=self._base_delay,
                    max_delay=self._max_delay,
                    jitter=self._jitter,
                    rng=self._rng,
                )
                logger.warning(
                    f"{event}.deadlock_retry_scheduled",
                    attempt=attempt,
                    of=self._attempts,
                    delay_ms=round(delay * 1000, 3),
                    sqlstate=sqlstate(exc),
                )
                await self._discard(session, event)
                await self._asleep(delay)
        raise AssertionError("unreachable")  # pragma: no cover

    async def _discard(self, session: AsyncSession, event: str) -> None:
        """Rewind the failed transaction so the next attempt has a live session.

        A failing rollback is logged and swallowed. The alternative is to raise
        it, which replaces the deadlock the caller was told about with an error
        about cleanup; and if the rollback failed the connection is gone, so
        the next attempt will fail immediately anyway and do so with a message
        about the real problem.
        """
        try:
            await session.rollback()
        except SQLAlchemyError as exc:
            logger.warning(f"{event}.deadlock_retry_rollback_failed", error=str(exc))


def retry_on_deadlock(
    *,
    attempts: int = 3,
    codes: frozenset[str] = RETRYABLE_SQLSTATES,
    base_delay: float = 0.05,
    max_delay: float = 1.0,
    jitter: bool = True,
    rng: random.Random = DEFAULT_RNG,
    asleep: AsyncSleeper = asyncio.sleep,
    event: str | None = None,
) -> DeadlockRetryPolicy:
    """Decorate `async def f(session, ...)` to re-run it when it loses a deadlock.

    The decorated function must take an `AsyncSession` as its first parameter,
    must load everything it uses through that session on every call, and must
    commit before it returns. See the module docstring — those three are the
    whole contract, and none of them can be enforced from here.

    Args:
        attempts: Total runs, not extra ones — `attempts=3` is one try and two
            retries. Must be at least 1.
        codes: SQLSTATEs worth re-running. Defaults to deadlock (40P01) and
            serialisation failure (40001). Adding 55P03 here is possible and
            usually wrong: it is what `nowait` and `lock_timeout` raise, and
            retrying it turns a caller's explicit "do not queue" into a queue.
        base_delay: Seconds to wait after the first failure, before jitter.
            Short by default — the winning transaction has already committed,
            so the contention is over and a long wait just adds latency.
        max_delay: Ceiling on the backoff, before jitter.
        jitter: Draw each wait uniformly from `[0, ceiling]`. Leave this on
            outside tests: two transactions deadlocked against each other are
            released together, and retrying in lockstep re-creates the same
            deadlock on the same rows.
        rng: Jitter source. Pass a seeded `random.Random` to pin delays.
        asleep: Awaitable sleep, injectable so a test need not spend the wait.
        event: Log event prefix. Defaults to `module.qualname` of the wrapped
            function; events are `<prefix>.deadlock_retry_scheduled`,
            `.deadlock_retry_exhausted` and `.deadlock_retry_rollback_failed`.

    Raises:
        ValueError: The policy is unusable — fewer than one attempt, a negative
            `base_delay`, a `max_delay` below it, or an empty `codes`. Raised at
            decoration time, so a bad policy fails at import rather than on the
            first deadlock.

    Example:
        >>> @retry_on_deadlock(attempts=5)
        ... async def transfer(
        ...     session: AsyncSession, src: uuid.UUID, dst: uuid.UUID, amount: int
        ... ) -> None:
        ...     # Lock in primary-key order so two opposite transfers queue
        ...     # rather than deadlock; the retry is the net, not the plan.
        ...     for account_id in sorted((src, dst)):
        ...         await lock_row(session, Account, account_id)
        ...     ...
        ...     await session.commit()
    """
    return DeadlockRetryPolicy(
        attempts=attempts,
        codes=codes,
        base_delay=base_delay,
        max_delay=max_delay,
        jitter=jitter,
        rng=rng,
        asleep=asleep,
        event=event,
    )


_DEFAULT_POLICY = retry_on_deadlock()
