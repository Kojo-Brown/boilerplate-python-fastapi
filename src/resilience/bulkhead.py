"""One compartment per dependency, and a bounded queue in front of it.

The breaker next door limits calls to a dependency that has *failed*. It does
nothing about one that is merely slow, and slow is the more common way a third
party takes an application down. Nothing is raised, nothing is logged, every
call eventually succeeds — and while they are in flight they hold connections,
tasks, and whatever the handlers above them are still holding. A dependency
that answers in 4s instead of 40ms does not fail; it multiplies its share of
this process by a hundred, and everything unrelated to it starves.

A bulkhead is the ship-building metaphor taken literally: the hull is divided
so that a breach floods one compartment instead of the vessel. Here the hull is
this process's capacity and a compartment is one origin's share of it. When the
compartment is full, calls to *that* origin are refused; calls to everything
else are untouched, which is the whole property.

```
    ┌── https://api.stripe.com ──┐   limit 20   queue ≤ 40  ─▶ refuse
    ├── https://api.paypal.com ──┤   limit 20   queue ≤ 40  ─▶ refuse
    └── https://hooks.example ───┘   limit 20   queue ≤ 40  ─▶ refuse
```

## Why not `asyncio.Semaphore`

A semaphore is the right shape and gives two of the three things needed, which
is exactly why reaching for it is tempting. The three:

* **A count of what is in flight.** `Semaphore` has this.
* **A bound on what is *waiting*.** It does not. `Semaphore.acquire` queues
  every caller and its waiter list has no ceiling, so a dependency that stops
  answering collects an unbounded pile of tasks — a memory limit reached by a
  concurrency limit, which is the failure this module exists to prevent, one
  layer up. The waiter count is `_waiters`, private and nullable, so bounding
  it means owning the queue.
* **Independence from the event loop it was first used in.**
  `asyncio.Semaphore` binds to a loop on its first contended acquire and raises
  `RuntimeError: bound to a different event loop` afterwards. A process-wide
  registry outlives any one loop — most visibly in a test suite, where every
  test gets a fresh one — so the binding is a latent failure that appears only
  under contention. Futures created per acquisition have no such binding.

So the queue is explicit: a deque of futures, FIFO, with the slot **handed
directly** from the caller that releases it to the waiter at the head. Handing
it over rather than waking everyone and letting them race is what makes the
fairness real — a woken waiter that finds the slot taken has to queue again,
and under sustained load it can do that forever while later arrivals walk in.

## Why the fast path checks the queue

`if self._in_flight < limit and not self._waiters` — the second half is what
stops a new arrival from stepping over a call that is already waiting. Without
it a busy compartment starves its queue exactly the way an unfair lock does,
and the acquire timeout turns from a rare event into the common one.

## What this is not

It is not a rate limiter. A compartment bounds how many calls are *in flight*,
not how many start per second: twenty concurrent calls that each take 10ms is
2,000 requests per second at a dependency that may allow 100. The same
distinction `src/parallel/io.py` draws for `gather_bounded`, and for the same
reason — when an upstream publishes a rate, that needs a token bucket as well.

It is not a retry budget and it is not a deadline. It does consult
`current_deadline()` before queueing, because waiting two seconds for a slot
inside a request with 200ms left is time spent producing an answer nobody will
read — and, when the enclosing budget is the shorter of the two, it declines to
arm a timer of its own and lets that scope be the thing that expires and names
itself. See `_wait_budget`.

See `docs/bulkheads.md`.
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from typing import NoReturn

import structlog

from src.decorators.base import DEFAULT_CLOCK, Clock
from src.resilience.base import (
    BulkheadConfig,
    BulkheadFullError,
    BulkheadRejection,
)
from src.structured.deadline import current_deadline

logger = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class BulkheadStats:
    """A snapshot of one compartment, for a health endpoint or a log line."""

    origin: str
    limit: int
    in_flight: int
    queued: int
    rejected: int


class BulkheadSlot:
    """One admitted call's share of a compartment. Release it exactly once.

    Returned by `Bulkhead.acquire`. `release` is idempotent, because the
    transport releases on the error path and again when the response body is
    closed, and which of the two happens depends on how the request ended.

    Not a dataclass: `released` is a latch this object flips on itself, which
    is the opposite of the value semantics `tests/test_immutability_gate.py`
    holds dataclasses in `src/` to — the same reason `CircuitCall` is not one.
    """

    __slots__ = ("bulkhead", "released")

    def __init__(self, bulkhead: Bulkhead) -> None:
        self.bulkhead = bulkhead
        self.released = False

    def release(self) -> None:
        """Give the slot back, to the waiter at the head of the queue if any."""
        if self.released:
            return
        self.released = True
        self.bulkhead._release()


class Bulkhead:
    """Admission control for one origin: `limit` in flight, `max_queue` waiting.

    Not constructed directly in application code — `BulkheadRegistry` owns one
    per origin and hands them to the transport.

    Every method that mutates the counters is synchronous, for the reason
    `circuit.py` gives at length: a state machine with an `await` in the middle
    of a transition can be observed halfway through it. The one `await` here is
    `acquire`'s wait on its own future, which happens after the accounting is
    settled and changes nothing while it is suspended.
    """

    def __init__(
        self,
        origin: str,
        *,
        config: BulkheadConfig | None = None,
        clock: Clock = DEFAULT_CLOCK,
    ) -> None:
        self._origin = origin
        self._config = config if config is not None else BulkheadConfig()
        self._clock = clock
        self._in_flight = 0
        self._waiters: deque[asyncio.Future[None]] = deque()
        self._rejected = 0

    @property
    def origin(self) -> str:
        return self._origin

    @property
    def config(self) -> BulkheadConfig:
        return self._config

    @property
    def in_flight(self) -> int:
        """Calls holding a slot right now."""
        return self._in_flight

    @property
    def queued(self) -> int:
        """Calls waiting for one."""
        return len(self._waiters)

    @property
    def rejected(self) -> int:
        """Calls refused since this compartment was created or last reset."""
        return self._rejected

    def stats(self) -> BulkheadStats:
        return BulkheadStats(
            origin=self._origin,
            limit=self._config.limit,
            in_flight=self._in_flight,
            queued=len(self._waiters),
            rejected=self._rejected,
        )

    async def acquire(self) -> BulkheadSlot:
        """Take a slot, waiting at most `acquire_timeout` for one.

        Raises:
            BulkheadFullError: The compartment is full and either the queue is
                too, the wait expired, or the enclosing deadline had no time
                left to spend on waiting.
            DeadlineExceeded: An enclosing `deadline()` shorter than the
                acquire timeout expired while this call was queued. Deliberately
                not translated into a `BulkheadFullError` — see `_wait_budget`.
        """
        if self._in_flight < self._config.limit and not self._waiters:
            self._in_flight += 1
            return BulkheadSlot(self)

        if len(self._waiters) >= self._config.max_queue:
            self._reject(BulkheadRejection.QUEUE_FULL, waited=0.0)

        timeout = self._wait_budget()

        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append(waiter)
        started = self._clock()
        try:
            async with asyncio.timeout(timeout):
                await waiter
        except TimeoutError:
            if _was_handed_a_slot(waiter):
                # The slot arrived in the same tick the timer fired. Taking it
                # is the right way to break that tie: refusing would reject a
                # call that has capacity waiting for it and push the slot back
                # into a queue whose head has just been told there is none.
                #
                # No cover, and the reason is worth stating rather than
                # leaving as a bare pragma. Reaching this needs the hand-off
                # and the timer to land in one iteration of the loop, and
                # CPython's `_run_once` makes that impossible on purpose: the
                # wake-up `set_result` schedules is queued in the iteration
                # *before* the one that drains the timer heap, so the waiter
                # always resumes first and cancels the timer on its way out.
                # That is an ordering guarantee of one loop implementation, not
                # of asyncio — and this application runs on uvloop, whose
                # scheduling is its own. A slot lost here is one unit of
                # capacity gone from the compartment for the life of the
                # process, with nothing logged, so the branch stays.
                return BulkheadSlot(self)  # pragma: no cover
            self._reject(
                BulkheadRejection.ACQUIRE_TIMEOUT, waited=self._clock() - started
            )
        except BaseException:
            # Cancellation, or an enclosing deadline expiring. Re-raised
            # immediately below; the branch exists only so that a slot handed
            # over in the same tick is passed on rather than lost, which would
            # shrink the compartment by one for the life of the process.
            if _was_handed_a_slot(waiter):
                self._release()
            raise
        finally:
            # A waiter that was served has already been popped, so this is a
            # no-op for it and a removal for every other way out.
            self._discard(waiter)

        return BulkheadSlot(self)

    def reset(self) -> None:
        """Empty the compartment and refuse everyone waiting in it.

        For tests, and for an operator override. The waiters are failed rather
        than dropped: a future nobody completes is a caller suspended forever,
        which is a worse outcome than the rejection they were queued to risk.
        """
        while self._waiters:
            waiter = self._waiters.popleft()
            if not waiter.done():
                waiter.set_exception(
                    self._error(BulkheadRejection.QUEUE_FULL, waited=0.0)
                )
        self._in_flight = 0
        self._rejected = 0

    def _wait_budget(self) -> float | None:
        """How long this call may wait; `None` to let the enclosing scope say.

        Three answers, and the middle one is the interesting one.

        With no enclosing `deadline()` there is nothing to consult and the
        acquire timeout stands. With one that is already **spent**, there is no
        wait worth starting, and this refuses rather than queueing — using
        `BulkheadRejection.NO_BUDGET`, not `clamp_to_deadline`, whose
        `DeadlineExceeded` is an `AppException` rather than an
        `httpx.TransportError` and would therefore escape every caller that
        handles "could not reach the dependency" and surface as a 500.

        With one that is **shorter than the acquire timeout**, no timer of our
        own is armed and the wait is left to the enclosing scope. This is the
        rule `deadline()` already applies to its own nesting, for the same
        reason: two timers set to the same instant both fire, both cancel the
        same task, and which one wins the race decides the error message — for
        a distinction that matters, since "the request budget ran out" and "the
        compartment for this dependency is full" have different fixes. The
        enclosing scope owns the instant, so the enclosing scope names it.
        """
        budget = current_deadline()
        if budget is None:
            return self._config.acquire_timeout
        remaining = budget.remaining()
        if remaining <= 0.0:
            self._reject(BulkheadRejection.NO_BUDGET, waited=0.0)
        if remaining <= self._config.acquire_timeout:
            return None
        return self._config.acquire_timeout

    def _discard(self, waiter: asyncio.Future[None]) -> None:
        try:
            self._waiters.remove(waiter)
        except ValueError:
            pass

    def _release(self) -> None:
        """Hand the slot to the waiter at the head, or return it to the pool.

        Ownership is transferred rather than released and re-acquired, so
        `_in_flight` does not move when a waiter is served. Cancelled waiters
        are skipped rather than counted: one at the head that nobody is
        awaiting any more would otherwise absorb the slot silently.
        """
        while self._waiters:
            waiter = self._waiters.popleft()
            if not waiter.done():
                waiter.set_result(None)
                return
        self._in_flight = max(0, self._in_flight - 1)

    def _error(self, reason: BulkheadRejection, *, waited: float) -> BulkheadFullError:
        return BulkheadFullError(
            self._origin,
            reason,
            limit=self._config.limit,
            queued=len(self._waiters),
            waited=waited,
            retry_after=self._config.acquire_timeout,
        )

    def _reject(self, reason: BulkheadRejection, *, waited: float) -> NoReturn:
        self._rejected += 1
        logger.warning(
            "bulkhead.rejected",
            origin=self._origin,
            reason=str(reason),
            limit=self._config.limit,
            in_flight=self._in_flight,
            queued=len(self._waiters),
            waited_ms=round(waited * 1000, 3),
        )
        raise self._error(reason, waited=waited)


def _was_handed_a_slot(waiter: asyncio.Future[None]) -> bool:
    """Whether `waiter` was served before whatever ended its wait.

    A future can be done and still not carry a slot — `reset` fails it — so
    "done" alone is not the question. Checked in two places where the caller is
    unwinding, and getting it wrong in either direction is a slot permanently
    lost from the compartment or one handed out twice.
    """
    return waiter.done() and not waiter.cancelled() and waiter.exception() is None


class BulkheadRegistry:
    """One compartment per origin, created on first sight of that origin.

    Shared by every client handed the same registry, which is what makes the
    limit a limit: two `httpx.AsyncClient`s built for the same dependency have
    two connection pools and would otherwise be entitled to twice the
    concurrency, for no reason anyone chose. Growth is bounded by the number of
    distinct origins this application calls out to — as with
    `CircuitBreakerRegistry`, do not hand this a registry keyed by a URL taken
    from user input.
    """

    __slots__ = ("_bulkheads", "clock", "config")

    def __init__(
        self,
        *,
        config: BulkheadConfig | None = None,
        clock: Clock = DEFAULT_CLOCK,
    ) -> None:
        self.config = config if config is not None else BulkheadConfig()
        self.clock = clock
        self._bulkheads: dict[str, Bulkhead] = {}

    def get(self, origin: str) -> Bulkhead:
        bulkhead = self._bulkheads.get(origin)
        if bulkhead is None:
            bulkhead = Bulkhead(origin, config=self.config, clock=self.clock)
            self._bulkheads[origin] = bulkhead
        return bulkhead

    def stats(self) -> dict[str, BulkheadStats]:
        """A snapshot for a health endpoint or a log line."""
        return {origin: b.stats() for origin, b in self._bulkheads.items()}

    def reset(self) -> None:
        """Empty every compartment. For tests, and for an operator override."""
        for bulkhead in self._bulkheads.values():
            bulkhead.reset()


#: The registry the default client factory uses, so that two clients built by
#: `resilient_async_client` for the same dependency share one compartment
#: instead of each getting a full one. Pass an explicit registry to isolate.
DEFAULT_BULKHEADS: BulkheadRegistry = BulkheadRegistry()
