"""The compartment: admission, the bounded queue, fairness, and giving back.

Everything here drives `Bulkhead` directly, against an injected clock and real
tasks. Nothing sleeps for real: a call is made to occupy a slot by handing it a
future the test completes when it wants the call to finish, which is what keeps
a test about a one-second acquire timeout instant and exact.

The transport integration — the hard timeout, and the slot outliving the send —
is in `test_resilience_transport.py`, next to the retry loop it composes with.
"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from src.resilience.base import (
    BulkheadConfig,
    BulkheadFullError,
    BulkheadRejection,
    is_local_shortage,
)
from src.resilience.bulkhead import Bulkhead, BulkheadRegistry
from src.structured.deadline import deadline
from src.structured.errors import DeadlineExceeded

ORIGIN = "https://api.test"


class FakeClock:
    """A monotonic clock the test advances by hand."""

    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def build(
    *,
    limit: int = 1,
    max_queue: int = 4,
    acquire_timeout: float = 1.0,
    clock: FakeClock | None = None,
) -> Bulkhead:
    return Bulkhead(
        ORIGIN,
        config=BulkheadConfig(
            limit=limit,
            max_queue=max_queue,
            acquire_timeout=acquire_timeout,
            execution_timeout=None,
        ),
        clock=clock if clock is not None else FakeClock(),
    )


async def settle() -> None:
    """Let every task that is ready to run reach its next suspension point.

    Two passes rather than one: a hand-off wakes a waiter, and the woken waiter
    then needs its own turn to return from `acquire`.
    """
    for _ in range(2):
        await asyncio.sleep(0)


async def queued(bulkhead: Bulkhead) -> asyncio.Task[None]:
    """A task parked in `bulkhead`'s queue, guaranteed to have got there.

    It gives the slot straight back once it is served, which is what makes it
    useful for asserting that a hand-off happened at all. Use `Holder` when the
    test needs the slot to stay taken.
    """

    async def acquire_and_release() -> None:
        slot = await bulkhead.acquire()
        slot.release()

    task = asyncio.create_task(acquire_and_release())
    await settle()
    return task


class Holder:
    """A call that takes a slot and keeps it until the test hands it back.

    The stand-in for a slow dependency: no sleeping, no wall clock, and the
    moment the call "finishes" is chosen by the test rather than raced for.
    """

    def __init__(self, bulkhead: Bulkhead) -> None:
        self._finish: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.task = asyncio.create_task(self._run(bulkhead))

    async def _run(self, bulkhead: Bulkhead) -> None:
        slot = await bulkhead.acquire()
        try:
            await self._finish
        finally:
            slot.release()

    def finish(self) -> None:
        if not self._finish.done():
            self._finish.set_result(None)


# ---- admission -------------------------------------------------------------


async def test_a_call_is_admitted_while_the_compartment_has_room() -> None:
    bulkhead = build(limit=2)

    first = await bulkhead.acquire()
    second = await bulkhead.acquire()

    assert bulkhead.in_flight == 2
    assert bulkhead.queued == 0
    first.release()
    second.release()
    assert bulkhead.in_flight == 0


async def test_releasing_twice_does_not_hand_out_capacity_that_does_not_exist() -> None:
    bulkhead = build(limit=1)
    slot = await bulkhead.acquire()

    slot.release()
    slot.release()

    assert bulkhead.in_flight == 0


async def test_a_full_compartment_makes_the_next_call_wait() -> None:
    bulkhead = build(limit=1)
    held = await bulkhead.acquire()

    waiting = await queued(bulkhead)

    assert bulkhead.queued == 1
    assert not waiting.done()

    held.release()
    await settle()
    assert waiting.done()
    assert bulkhead.in_flight == 0


async def test_the_slot_is_handed_to_the_waiter_rather_than_released() -> None:
    """`in_flight` never dips on a hand-off, so nobody else can step in.

    Releasing the count and letting the woken waiter re-acquire would open a
    window in which a brand new call finds the compartment with room. This
    asserts the window does not exist: the count stays at the limit across the
    hand-off.
    """
    bulkhead = build(limit=1)
    held = await bulkhead.acquire()
    waiting = await queued(bulkhead)

    held.release()
    assert bulkhead.in_flight == 1  # transferred, not returned

    await settle()
    assert waiting.done()


# ---- fairness --------------------------------------------------------------


async def test_a_new_arrival_does_not_step_over_the_queue() -> None:
    """Arriving at a compartment with a free slot is not enough to take it.

    Without the queue check on the fast path, a busy compartment starves its
    own queue the way an unfair lock does: every newcomer walks straight in
    during the tick between a slot being freed and the waiter at the head
    resuming to claim it, and the acquire timeout turns from a rare event into
    the common one.
    """
    bulkhead = build(limit=1)
    held = await bulkhead.acquire()
    first_waiter = Holder(bulkhead)
    await settle()
    newcomer = await queued(bulkhead)
    assert bulkhead.queued == 2

    held.release()
    await settle()

    # The slot went to the waiter that arrived first, and the newcomer — which
    # was queued before that slot ever came free — is still behind it.
    assert bulkhead.in_flight == 1
    assert not newcomer.done()

    first_waiter.finish()
    await settle()
    assert newcomer.done()


async def test_waiters_are_served_in_the_order_they_arrived() -> None:
    bulkhead = build(limit=1)
    held = await bulkhead.acquire()
    order: list[int] = []

    async def wait_and_record(index: int) -> None:
        slot = await bulkhead.acquire()
        order.append(index)
        slot.release()

    tasks = []
    for index in range(3):
        tasks.append(asyncio.create_task(wait_and_record(index)))
        await settle()

    held.release()
    await asyncio.gather(*tasks)

    assert order == [0, 1, 2]


async def test_a_cancelled_waiter_is_skipped_rather_than_absorbing_the_slot() -> None:
    bulkhead = build(limit=1)
    held = await bulkhead.acquire()
    doomed = await queued(bulkhead)
    survivor = await queued(bulkhead)

    doomed.cancel()
    held.release()
    await settle()

    assert survivor.done()
    assert bulkhead.in_flight == 0


# ---- refusing --------------------------------------------------------------


async def test_the_queue_has_a_ceiling_and_past_it_calls_are_refused() -> None:
    bulkhead = build(limit=1, max_queue=2)
    held = await bulkhead.acquire()
    waiters = [await queued(bulkhead) for _ in range(2)]

    with pytest.raises(BulkheadFullError) as caught:
        await bulkhead.acquire()

    assert caught.value.reason is BulkheadRejection.QUEUE_FULL
    assert caught.value.waited == 0.0
    assert caught.value.limit == 1
    assert caught.value.queued == 2
    assert bulkhead.rejected == 1

    held.release()
    await asyncio.gather(*waiters)


async def test_a_wait_that_runs_out_is_refused_and_says_how_long_it_waited() -> None:
    clock = FakeClock()
    bulkhead = build(limit=1, acquire_timeout=0.05, clock=clock)
    held = await bulkhead.acquire()

    # The wait itself is a real 50ms so that `asyncio.timeout` genuinely
    # expires; the duration the error reports comes from the injected clock,
    # which the test advances by exactly the amount it wants to read back.
    async def refused() -> BulkheadFullError:
        with pytest.raises(BulkheadFullError) as caught:
            await bulkhead.acquire()
        return caught.value

    task = asyncio.create_task(refused())
    await settle()
    clock.advance(0.05)
    error = await task

    assert error.reason is BulkheadRejection.ACQUIRE_TIMEOUT
    assert error.waited == pytest.approx(0.05)
    assert error.retry_after == pytest.approx(0.05)
    assert bulkhead.queued == 0  # the expired waiter took itself out
    held.release()


async def test_a_refusal_is_not_a_timeout_exception() -> None:
    """It must not be mistaken for the dependency failing to answer.

    An `httpx.TimeoutException` would be counted against the breaker by the
    transport and would make `request_was_sent` treat a request that provably
    never left as possibly-delivered — which is what stops a `POST` being
    repeated. Both are wrong, and both are silent.
    """
    bulkhead = build(limit=1, max_queue=0)
    held = await bulkhead.acquire()

    with pytest.raises(BulkheadFullError) as caught:
        await bulkhead.acquire()

    assert isinstance(caught.value, httpx.TransportError)
    assert not isinstance(caught.value, httpx.TimeoutException)
    assert is_local_shortage(caught.value)
    held.release()


async def test_the_message_names_the_origin_and_the_numbers() -> None:
    bulkhead = build(limit=1, max_queue=0)
    held = await bulkhead.acquire()

    with pytest.raises(BulkheadFullError) as caught:
        await bulkhead.acquire()

    message = str(caught.value)
    assert ORIGIN in message
    assert "queue_full" in message
    assert "1 in flight" in message
    held.release()


# ---- the enclosing deadline ------------------------------------------------


async def test_a_wait_never_outlives_the_request_that_started_it() -> None:
    """A three-second acquire timeout inside a 50ms request waits 50ms.

    Spending the full acquire timeout when the enclosing scope expired long
    ago produces an answer nobody is waiting for, on a slot somebody else could
    have used. The enclosing scope is also what *names* the failure: it owns
    the instant, so it is the one that fires, and `DeadlineExceeded` rather
    than `BulkheadFullError` is the correct — and more useful — report.
    """
    bulkhead = build(limit=1, acquire_timeout=3.0)
    held = await bulkhead.acquire()

    started = asyncio.get_running_loop().time()
    with pytest.raises(DeadlineExceeded) as caught:
        async with deadline(0.05, name="request"):
            await bulkhead.acquire()
    waited = asyncio.get_running_loop().time() - started

    assert caught.value.scope == "request"
    assert waited < 1.0
    assert bulkhead.queued == 0
    held.release()


async def test_a_budget_longer_than_the_wait_leaves_the_acquire_timeout_alone() -> None:
    """The clamp only ever shortens. A generous request budget is not a licence
    to sit in a queue for longer than the compartment allows."""
    clock = FakeClock()
    bulkhead = build(limit=1, acquire_timeout=0.05, clock=clock)
    held = await bulkhead.acquire()

    async def refused() -> BulkheadFullError:
        with pytest.raises(BulkheadFullError) as caught:
            await bulkhead.acquire()
        return caught.value

    async with deadline(30.0, name="request"):
        task = asyncio.create_task(refused())
        await settle()
        clock.advance(0.05)
        error = await task

    assert error.reason is BulkheadRejection.ACQUIRE_TIMEOUT
    held.release()


async def test_a_spent_budget_refuses_without_queueing_at_all() -> None:
    bulkhead = build(limit=1, acquire_timeout=1.0)
    held = await bulkhead.acquire()

    async def acquire_once_the_budget_is_gone() -> BulkheadFullError:
        await asyncio.sleep(0.02)
        with pytest.raises(BulkheadFullError) as caught:
            await bulkhead.acquire()
        return caught.value

    # The task inherits the `Deadline` through the context but not the timer,
    # which cancels the task that opened the scope and nobody else. That is
    # what lets a spent budget be observed at all, and it is not contrived: a
    # handler that fans out and lets one branch run long looks exactly like it.
    async with deadline(0.01, name="request"):
        task = asyncio.create_task(acquire_once_the_budget_is_gone())

    error = await task

    assert error.reason is BulkheadRejection.NO_BUDGET
    assert bulkhead.queued == 0
    held.release()


# ---- cancellation ----------------------------------------------------------


async def test_a_waiter_cancelled_after_being_served_gives_the_slot_back() -> None:
    """The race that silently shrinks a compartment for the life of a process.

    A slot is handed to the waiter at the head, and the waiter is cancelled
    before it resumes to take it. Nothing else holds that slot and nothing else
    will ever release it, so getting this wrong costs one unit of capacity per
    occurrence, permanently, with no error anywhere.
    """
    bulkhead = build(limit=1)
    held = await bulkhead.acquire()
    waiting = await queued(bulkhead)

    held.release()  # hands the slot straight to `waiting`
    assert bulkhead.in_flight == 1
    waiting.cancel()
    await settle()

    assert bulkhead.in_flight == 0
    assert bulkhead.queued == 0


async def test_a_cancelled_wait_propagates_rather_than_becoming_a_refusal() -> None:
    bulkhead = build(limit=1)
    held = await bulkhead.acquire()
    waiting = await queued(bulkhead)

    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting

    assert bulkhead.queued == 0
    held.release()


# ---- reset -----------------------------------------------------------------


async def test_reset_empties_the_compartment_and_refuses_the_queue() -> None:
    """Waiters are failed rather than dropped.

    A future nobody completes is a caller suspended forever, which is a worse
    outcome than the rejection they queued knowing they might get.
    """
    bulkhead = build(limit=1)
    await bulkhead.acquire()
    waiting = await queued(bulkhead)

    bulkhead.reset()

    with pytest.raises(BulkheadFullError):
        await waiting
    assert bulkhead.in_flight == 0
    assert bulkhead.queued == 0


# ---- configuration ---------------------------------------------------------


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"limit": 0}, "limit must be at least 1."),
        ({"max_queue": -1}, "max_queue must not be negative."),
        ({"acquire_timeout": -0.5}, "acquire_timeout must not be negative."),
        ({"execution_timeout": 0.0}, "execution_timeout must be positive, or None."),
    ],
)
def test_an_unusable_compartment_fails_at_construction(
    kwargs: dict[str, float], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        BulkheadConfig(**kwargs)  # type: ignore[arg-type]


def test_the_compartment_is_the_only_thing_bounding_outbound_concurrency() -> None:
    """Clients from `resilient_async_client` have an **unbounded** pool.

    The factory passes `httpx.Limits()` when the caller names none, and a bare
    `Limits()` leaves `max_connections` as `None`, which httpx reads as no
    ceiling at all — the familiar 100 is `httpx.Client`'s own default and is
    replaced wholesale by anything passed explicitly. So nothing in this
    codebase has ever bounded how many requests it will have in flight to one
    dependency, and the compartment is now the only thing that does.

    Left that way rather than "fixed" by naming a pool limit in the factory: a
    per-dependency bound that sheds immediately with a typed error naming the
    origin is strictly better than a process-wide one that sheds after a full
    pool timeout with a `PoolTimeout`, and a cap underneath would only decide
    which of the two a caller hits first. Written down here because it is the
    assumption the default `limit` is chosen against.
    """
    assert httpx.Limits().max_connections is None
    assert BulkheadConfig().limit > 0


# ---- the registry ----------------------------------------------------------


async def test_each_origin_gets_its_own_compartment() -> None:
    """The property the whole package exists for: one flooded, one dry."""
    registry = BulkheadRegistry(config=BulkheadConfig(limit=1, max_queue=0))
    slow = registry.get("https://slow.test")
    healthy = registry.get("https://healthy.test")

    held = await slow.acquire()
    with pytest.raises(BulkheadFullError):
        await slow.acquire()

    other = await healthy.acquire()  # untouched
    assert healthy.in_flight == 1

    held.release()
    other.release()


async def test_the_same_origin_gets_the_same_compartment() -> None:
    registry = BulkheadRegistry()
    assert registry.get(ORIGIN) is registry.get(ORIGIN)
    assert registry.get(ORIGIN).origin == ORIGIN


async def test_stats_report_every_compartment_the_registry_has_seen() -> None:
    registry = BulkheadRegistry(config=BulkheadConfig(limit=2, max_queue=0))
    bulkhead = registry.get(ORIGIN)
    held = await bulkhead.acquire()
    await bulkhead.acquire()
    with pytest.raises(BulkheadFullError):
        await bulkhead.acquire()

    snapshot = registry.stats()[ORIGIN]

    assert snapshot.origin == ORIGIN
    assert snapshot.limit == 2
    assert snapshot.in_flight == 2
    assert snapshot.queued == 0
    assert snapshot.rejected == 1
    held.release()


async def test_resetting_the_registry_resets_every_compartment() -> None:
    registry = BulkheadRegistry(config=BulkheadConfig(limit=1))
    bulkhead = registry.get(ORIGIN)
    await bulkhead.acquire()

    registry.reset()

    assert bulkhead.in_flight == 0
    assert registry.stats()[ORIGIN].rejected == 0
