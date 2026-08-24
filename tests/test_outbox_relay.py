"""The relay's policy, with the database left out of it.

What is under test here is scheduling and failure handling: which events are
completed, which are rescheduled and how far out, what happens to the batch
when one event is poison, and what happens to the loop when the whole tick
fails. The claim itself — `FOR UPDATE SKIP LOCKED`, and the transaction that
makes it mean anything — cannot be faked usefully and is measured against a
real Postgres in `tests/test_outbox_db.py`.

The dispatcher is a real `EventBus` throughout. Subscriber isolation, the MRO
walk and the empty-`PublishResult`-for-no-subscribers rule are the bus's
behaviour, and a fake dispatcher would be asserting on a reimplementation of it.
"""

from __future__ import annotations

import asyncio
import random
import uuid
from collections.abc import AsyncIterator, Callable, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pytest

from src.events.base import DomainEvent
from src.events.bus import EventBus
from src.events.catalog import UserLoggedIn, UserRegistered
from src.immutable import FrozenDict
from src.outbox.base import BatchScope, OutboxBatch, PendingEvent
from src.outbox.codec import default_codec
from src.outbox.relay import TASK_NAME, DrainResult, OutboxRelay, RelayConfig

PINNED_AT = datetime(2026, 8, 24, 12, 30, tzinfo=UTC)


class StopRelay(Exception):
    """Raised by the fake sleeper to end `run()` at a known point.

    `run` only sleeps outside its `try`, so this propagates instead of being
    swallowed as a failed tick — which is what makes "how many ticks happened"
    an assertion rather than a race against a timer.
    """


@dataclass
class RecordedFailure:
    entry: PendingEvent
    error: str
    retry_in: float


@dataclass
class FakeOutbox:
    """A queue of pending rows, plus the batch scope over it.

    Deliberately not a database: there is no locking, no visibility rule and no
    `available_at`, because none of those is what this file is asking about.
    """

    pending: list[PendingEvent] = field(default_factory=list)
    completed: list[PendingEvent] = field(default_factory=list)
    failures: list[RecordedFailure] = field(default_factory=list)
    claims: list[int] = field(default_factory=list)
    commits: int = 0
    rollbacks: int = 0
    #: Set to make `claim` raise, standing in for a database that has gone away.
    claim_error: Exception | None = None

    async def claim(self, *, limit: int) -> tuple[PendingEvent, ...]:
        self.claims.append(limit)
        if self.claim_error is not None:
            raise self.claim_error
        taken, self.pending = self.pending[:limit], self.pending[limit:]
        return tuple(taken)

    async def complete(self, entry: PendingEvent) -> None:
        self.completed.append(entry)

    async def fail(self, entry: PendingEvent, *, error: str, retry_in: float) -> None:
        self.failures.append(RecordedFailure(entry, error, retry_in))

    def scope(self) -> BatchScope:
        @asynccontextmanager
        async def open_batch() -> AsyncIterator[OutboxBatch]:
            try:
                yield self
            except BaseException:
                self.rollbacks += 1
                raise
            self.commits += 1

        return open_batch


class RecordingSleeper:
    """A `Sleeper` that records what it was asked to wait and never waits.

    Stops the loop after `stop_after` calls so a test of `run` terminates at a
    point it chose rather than when a timer happens to fire.
    """

    def __init__(self, stop_after: int = 1) -> None:
        self.waits: list[float] = []
        self._stop_after = stop_after

    async def __call__(self, seconds: float) -> None:
        self.waits.append(seconds)
        if len(self.waits) >= self._stop_after:
            raise StopRelay
        await asyncio.sleep(0)


class ScriptedSleeper(RecordingSleeper):
    """A sleeper that changes the world one step before each wait.

    The loop's only pause is the sleep, so this is where a test gets to say
    "and by the next tick, the database is back" without racing it.
    """

    def __init__(
        self, script: Iterable[Callable[[], None]], *, stop_after: int = 1
    ) -> None:
        super().__init__(stop_after=stop_after)
        self._script = list(script)

    async def __call__(self, seconds: float) -> None:
        if self._script:
            self._script.pop(0)()
        await super().__call__(seconds)


def pending(
    event: DomainEvent, *, attempts: int = 0, event_name: str | None = None
) -> PendingEvent:
    """A claimed row, as the store would have handed it over."""
    return PendingEvent(
        id=uuid.uuid4(),
        event_id=event.event_id,
        event_name=event_name or type(event).event_name,
        payload=FrozenDict(default_codec().encode(event)),
        occurred_at=event.occurred_at,
        attempts=attempts,
    )


def a_registration(**overrides: object) -> UserRegistered:
    fields: dict[str, object] = {
        "event_id": str(uuid.uuid4()),
        "occurred_at": PINNED_AT,
        "user_id": "user-1",
        "email": "new@example.com",
    }
    fields.update(overrides)
    return UserRegistered(**fields)  # type: ignore[arg-type]


@pytest.fixture
def bus() -> EventBus:
    return EventBus()


@pytest.fixture
def delivered(bus: EventBus) -> list[DomainEvent]:
    seen: list[DomainEvent] = []

    async def record(event: DomainEvent) -> None:
        seen.append(event)

    bus.subscribe(DomainEvent, record)
    return seen


def relay_over(
    outbox: FakeOutbox,
    bus: EventBus,
    *,
    config: RelayConfig | None = None,
    sleep: RecordingSleeper | None = None,
) -> OutboxRelay:
    return OutboxRelay(
        batches=outbox.scope(),
        dispatcher=bus,
        config=config if config is not None else RelayConfig(jitter=False),
        sleep=sleep if sleep is not None else RecordingSleeper(),
        rng=random.Random(0),
    )


# --- the happy path ------------------------------------------------------


async def test_a_delivered_event_is_completed(
    bus: EventBus, delivered: list[DomainEvent]
) -> None:
    outbox = FakeOutbox(pending=[pending(a_registration())])

    result = await relay_over(outbox, bus).drain_once()

    assert result == DrainResult(claimed=1, delivered=1, failed=0)
    assert len(outbox.completed) == 1
    assert outbox.failures == []
    assert outbox.commits == 1


async def test_the_subscriber_receives_the_event_that_was_published(
    bus: EventBus, delivered: list[DomainEvent]
) -> None:
    """End to end through the codec: what comes out of the row is the event,
    not a dictionary that resembles one."""
    published = a_registration(user_id="user-7", email="seven@example.com", via="oauth")
    outbox = FakeOutbox(pending=[pending(published)])

    await relay_over(outbox, bus).drain_once()

    (received,) = delivered
    assert received == published


async def test_an_empty_claim_dispatches_nothing(
    bus: EventBus, delivered: list[DomainEvent]
) -> None:
    outbox = FakeOutbox()

    result = await relay_over(outbox, bus).drain_once()

    assert result.empty
    assert delivered == []
    assert outbox.completed == []


async def test_an_event_with_no_subscribers_is_delivered(bus: EventBus) -> None:
    """The bus's own rule — an event nobody observes is the pattern working —
    and the reason `src/main.py` stops the relay *before* clearing the bus. A
    batch dispatched to an emptied bus would be deleted as delivered."""
    outbox = FakeOutbox(pending=[pending(a_registration())])

    result = await relay_over(outbox, bus).drain_once()

    assert result.delivered == 1
    assert len(outbox.completed) == 1


async def test_the_claim_asks_for_a_whole_batch(bus: EventBus) -> None:
    outbox = FakeOutbox(pending=[pending(a_registration()) for _ in range(3)])

    await relay_over(outbox, bus, config=RelayConfig(batch_size=7)).drain_once()

    assert outbox.claims == [7]


# --- failure, per event --------------------------------------------------


async def test_a_failing_subscriber_retries_the_whole_event(bus: EventBus) -> None:
    """`raise_for_failures` is what makes this a failure at all: the bus
    reports subscriber errors rather than raising them, so a relay that ignored
    the report would delete the row of a half-delivered event.

    The cost is visible here: a second subscriber that succeeded will see this
    event again on the retry. Nothing in one process can commit half a
    delivery, which is why subscribers must be idempotent on `event_id`.
    """

    async def explodes(event: DomainEvent) -> None:
        raise RuntimeError("the mail queue is down")

    bus.subscribe(DomainEvent, explodes)
    entry = pending(a_registration())
    outbox = FakeOutbox(pending=[entry])

    result = await relay_over(outbox, bus).drain_once()

    assert result == DrainResult(claimed=1, delivered=0, failed=1)
    assert outbox.completed == []
    (failure,) = outbox.failures
    assert failure.entry == entry
    assert failure.retry_in > 0
    assert "EventDispatchError" in failure.error
    # Still committed: the reschedule is what the transaction was for.
    assert outbox.commits == 1


async def test_the_stored_error_names_the_subscribers_own_failure(
    bus: EventBus,
) -> None:
    """`EventDispatchError` on its own says only how many subscribers failed.

    The row is what someone reads when an event has been stuck for an hour,
    possibly long after the log line with the traceback has rolled off, so the
    underlying failures are unpacked into it.
    """

    async def explodes(event: DomainEvent) -> None:
        raise RuntimeError("smtp: connection refused")

    bus.subscribe(DomainEvent, explodes)
    outbox = FakeOutbox(pending=[pending(a_registration())])

    await relay_over(outbox, bus).drain_once()

    (failure,) = outbox.failures
    assert "smtp: connection refused" in failure.error
    assert "RuntimeError" in failure.error


async def test_one_poison_event_does_not_stop_the_batch(
    bus: EventBus, delivered: list[DomainEvent]
) -> None:
    """Abandoning the batch would put every event behind this one, forever."""
    good_first = pending(a_registration(user_id="first"))
    poison = pending(a_registration(user_id="poison"), event_name="test.unknown")
    good_last = pending(a_registration(user_id="last"))
    outbox = FakeOutbox(pending=[good_first, poison, good_last])

    result = await relay_over(outbox, bus).drain_once()

    assert result == DrainResult(claimed=3, delivered=2, failed=1)
    assert [entry.id for entry in outbox.completed] == [good_first.id, good_last.id]
    assert [failure.entry.id for failure in outbox.failures] == [poison.id]


async def test_an_unknown_event_type_is_retried_rather_than_dropped(
    bus: EventBus,
) -> None:
    """A relay one deploy behind the producer sees exactly this. Dropping the
    row would lose every event published during the rollout."""
    entry = pending(a_registration(), event_name="test.from.the.future")
    outbox = FakeOutbox(pending=[entry])

    await relay_over(outbox, bus).drain_once()

    assert outbox.completed == []
    (failure,) = outbox.failures
    assert "UnknownEventTypeError" in failure.error


async def test_a_hanging_subscriber_is_bounded_by_the_dispatch_timeout(
    bus: EventBus,
) -> None:
    """Without the timeout that subscriber holds a transaction, a pooled
    connection and a row lock for as long as the process lives."""
    started = asyncio.Event()

    async def never_returns(event: DomainEvent) -> None:
        started.set()
        await asyncio.sleep(3600)

    bus.subscribe(DomainEvent, never_returns)
    outbox = FakeOutbox(pending=[pending(a_registration())])

    result = await relay_over(
        outbox, bus, config=RelayConfig(dispatch_timeout=0.05, jitter=False)
    ).drain_once()

    assert started.is_set()
    assert result.failed == 1
    (failure,) = outbox.failures
    assert "TimeoutError" in failure.error


# --- the backoff ---------------------------------------------------------


async def test_the_backoff_comes_from_the_rows_attempt_count(bus: EventBus) -> None:
    """Held in the row rather than in the relay's memory, so a deploy in the
    middle of an incident does not hand every failing event a fresh burst of
    retries."""

    async def explodes(event: DomainEvent) -> None:
        raise RuntimeError("nope")

    bus.subscribe(DomainEvent, explodes)
    first_try = pending(a_registration(), attempts=0)
    fourth_try = pending(a_registration(), attempts=3)
    outbox = FakeOutbox(pending=[first_try, fourth_try])

    await relay_over(
        outbox, bus, config=RelayConfig(retry_base_delay=2.0, jitter=False)
    ).drain_once()

    # attempt 1 → base; attempt 4 → base * 2^3.
    assert [failure.retry_in for failure in outbox.failures] == [2.0, 16.0]


async def test_the_backoff_is_capped(bus: EventBus) -> None:
    async def explodes(event: DomainEvent) -> None:
        raise RuntimeError("nope")

    bus.subscribe(DomainEvent, explodes)
    outbox = FakeOutbox(pending=[pending(a_registration(), attempts=40)])

    await relay_over(
        outbox,
        bus,
        config=RelayConfig(retry_base_delay=1.0, retry_max_delay=30.0, jitter=False),
    ).drain_once()

    assert outbox.failures[0].retry_in == 30.0


# --- the transaction -----------------------------------------------------


async def test_a_claim_that_fails_rolls_the_batch_back(bus: EventBus) -> None:
    outbox = FakeOutbox(claim_error=RuntimeError("connection reset"))

    with pytest.raises(RuntimeError, match="connection reset"):
        await relay_over(outbox, bus).drain_once()

    assert outbox.rollbacks == 1
    assert outbox.commits == 0


async def test_cancellation_rolls_back_without_blaming_the_event(
    bus: EventBus,
) -> None:
    """A relay shut down mid-batch has learned nothing about the event it was
    carrying. Recording a failure would push a perfectly good event out by the
    backoff interval every time the application restarted.
    """
    started = asyncio.Event()

    async def never_returns(event: DomainEvent) -> None:
        started.set()
        await asyncio.sleep(3600)

    bus.subscribe(DomainEvent, never_returns)
    outbox = FakeOutbox(pending=[pending(a_registration())])
    task = asyncio.create_task(relay_over(outbox, bus).drain_once())
    await started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert outbox.failures == []
    assert outbox.completed == []
    assert outbox.rollbacks == 1
    assert outbox.commits == 0


# --- the loop ------------------------------------------------------------


async def test_a_partial_batch_is_followed_by_a_sleep(bus: EventBus) -> None:
    outbox = FakeOutbox(pending=[pending(a_registration())])
    sleeper = RecordingSleeper(stop_after=1)

    with pytest.raises(StopRelay):
        await relay_over(
            outbox,
            bus,
            config=RelayConfig(batch_size=10, poll_interval=2.5),
            sleep=sleeper,
        ).run()

    assert sleeper.waits == [2.5]


async def test_a_full_batch_is_followed_immediately(bus: EventBus) -> None:
    """Sleeping after a full batch would pace delivery at `batch_size` per
    `poll_interval`, which is the wrong speed exactly when there is a backlog."""
    outbox = FakeOutbox(pending=[pending(a_registration()) for _ in range(5)])
    sleeper = RecordingSleeper(stop_after=1)

    with pytest.raises(StopRelay):
        await relay_over(
            outbox,
            bus,
            config=RelayConfig(batch_size=2, poll_interval=2.5),
            sleep=sleeper,
        ).run()

    # Two full batches ran back to back; the third was partial and slept.
    assert outbox.claims == [2, 2, 2]
    assert sleeper.waits == [2.5]


async def test_a_failing_tick_does_not_end_the_loop(bus: EventBus) -> None:
    """An exception escaping `run` would be stored on a task nobody awaits: the
    application would go on publishing into a table nothing drains, and the
    first symptom would be a report that emails stopped days ago.
    """
    outbox = FakeOutbox(claim_error=RuntimeError("database is having a moment"))
    sleeper = RecordingSleeper(stop_after=3)

    with pytest.raises(StopRelay):
        await relay_over(
            outbox,
            bus,
            config=RelayConfig(retry_base_delay=1.0, jitter=False),
            sleep=sleeper,
        ).run()

    # Three ticks, each failing, each backing off further than the last.
    assert len(outbox.claims) == 3
    assert sleeper.waits == [1.0, 2.0, 4.0]


async def test_the_failure_backoff_resets_after_a_good_tick(bus: EventBus) -> None:
    outbox = FakeOutbox(claim_error=RuntimeError("transient"))

    def heal() -> None:
        outbox.claim_error = None

    def break_again() -> None:
        outbox.claim_error = RuntimeError("transient again")

    sleeper = ScriptedSleeper([heal, break_again], stop_after=3)

    with pytest.raises(StopRelay):
        await relay_over(
            outbox,
            bus,
            config=RelayConfig(retry_base_delay=1.0, jitter=False),
            sleep=sleeper,
        ).run()

    # Failure, then a clean tick that sleeps the poll interval, then a failure
    # that starts the backoff again at the base delay rather than at 2s.
    assert sleeper.waits == [1.0, RelayConfig().poll_interval, 1.0]


# --- start and stop ------------------------------------------------------


async def test_start_runs_in_the_background_and_stop_waits(bus: EventBus) -> None:
    outbox = FakeOutbox(pending=[pending(a_registration())])
    relay = OutboxRelay(
        batches=outbox.scope(), dispatcher=bus, config=RelayConfig(poll_interval=0.01)
    )

    relay.start()
    assert relay.running
    for _ in range(100):
        if outbox.completed:
            break
        await asyncio.sleep(0.01)

    await relay.stop()

    assert len(outbox.completed) == 1
    assert not relay.running


async def test_stopping_mid_tick_cancels_the_tick(bus: EventBus) -> None:
    """The ordinary shutdown, since a relay spends its time in a claim rather
    than between them. Cancellation has to reach the transaction so the scope
    can roll it back; swallowing it in the loop would leave the batch's rows
    locked until the connection dropped."""
    claiming = asyncio.Event()

    class BlockingOutbox(FakeOutbox):
        async def claim(self, *, limit: int) -> tuple[PendingEvent, ...]:
            claiming.set()
            await asyncio.sleep(3600)
            raise AssertionError("unreachable")  # pragma: no cover

    outbox = BlockingOutbox()
    relay = OutboxRelay(batches=outbox.scope(), dispatcher=bus)

    relay.start()
    await asyncio.wait_for(claiming.wait(), 5)
    await relay.stop()

    assert outbox.rollbacks == 1
    assert outbox.commits == 0
    assert not relay.running


async def test_start_is_idempotent(bus: EventBus) -> None:
    relay = OutboxRelay(
        batches=FakeOutbox().scope(),
        dispatcher=bus,
        config=RelayConfig(poll_interval=0.01),
    )

    relay.start()
    relay.start()

    # Counted by task name rather than by looking at the relay's attribute: two
    # loops draining the same outbox is the failure being ruled out, and that
    # is a fact about the event loop.
    running = [task for task in asyncio.all_tasks() if task.get_name() == TASK_NAME]
    assert len(running) == 1
    assert relay.running

    await relay.stop()


async def test_stopping_a_relay_that_never_started_is_a_no_op(bus: EventBus) -> None:
    relay = OutboxRelay(batches=FakeOutbox().scope(), dispatcher=bus)

    await relay.stop()
    await relay.stop()

    assert not relay.running


# --- configuration -------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"batch_size": 0},
        {"poll_interval": -1.0},
        {"dispatch_timeout": 0.0},
        {"retry_base_delay": 0.0},
        {"retry_base_delay": 10.0, "retry_max_delay": 5.0},
    ],
)
def test_impossible_configuration_is_refused(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError):
        RelayConfig(**kwargs)  # type: ignore[arg-type]


def test_the_config_is_frozen() -> None:
    """It is read on every tick from a background task; a value that could move
    under it would change the relay's behaviour mid-batch."""
    config = RelayConfig()

    with pytest.raises(AttributeError):
        config.batch_size = 5  # type: ignore[misc]


def test_drain_result_reports_emptiness() -> None:
    assert DrainResult(claimed=0, delivered=0, failed=0).empty
    assert not DrainResult(claimed=1, delivered=1, failed=0).empty


def test_the_relay_exposes_the_config_it_was_given(bus: EventBus) -> None:
    config = RelayConfig(batch_size=3)

    assert relay_over(FakeOutbox(), bus, config=config).config is config


def test_a_fake_outbox_is_an_outbox_batch() -> None:
    """A fake that stopped implementing the port would fail tests for reasons
    that look like relay bugs."""
    assert isinstance(FakeOutbox(), OutboxBatch)


def test_pending_events_are_hashable() -> None:
    """Which is what `FrozenDict` buys over a plain payload dict, and why the
    relay can put one in a set or a log line without copying it first."""
    entry = pending(a_registration())

    assert entry in {entry}


def test_a_logged_in_event_round_trips_too(bus: EventBus) -> None:
    """Not a duplicate of the registration case: `UserLoggedIn` is a different
    catalogue entry, and the registry is what would drop it."""
    assert "user.logged_in" in default_codec().registered
    assert default_codec().registered["user.logged_in"] is UserLoggedIn
