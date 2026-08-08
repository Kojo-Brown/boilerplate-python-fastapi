"""Dispatch, isolation, concurrency and the guardrails around them.

Every test builds its own `EventBus`. The process-wide one exists for the
application; sharing it between tests would make each test's subscribers
another test's problem.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import pytest

from src.events.base import DomainEvent, EventCycleError, EventDispatchError
from src.events.bus import EventBus, PublishResult, SubscriberOutcome


@dataclass(frozen=True, kw_only=True)
class Ping(DomainEvent):
    payload: str = "ping"


@dataclass(frozen=True, kw_only=True)
class LoudPing(Ping):
    volume: int = 11


@dataclass(frozen=True, kw_only=True)
class Unrelated(DomainEvent):
    pass


@pytest.fixture
def bus() -> EventBus:
    return EventBus()


# --- dispatch ---


async def test_subscriber_receives_the_event(bus: EventBus) -> None:
    seen: list[Ping] = []

    async def handler(event: Ping) -> None:
        seen.append(event)

    bus.subscribe(Ping, handler)
    event = Ping(payload="hello")
    result = await bus.publish(event)

    assert seen == [event]
    assert result.ok
    assert result.delivered == 1


async def test_publishing_with_no_subscribers_is_not_an_error(bus: EventBus) -> None:
    result = await bus.publish(Ping())

    assert result.outcomes == ()
    assert result.ok
    assert result.delivered == 0


async def test_subscribers_of_other_events_are_not_called(bus: EventBus) -> None:
    calls: list[str] = []

    async def on_ping(event: Ping) -> None:
        calls.append("ping")

    async def on_unrelated(event: Unrelated) -> None:
        calls.append("unrelated")

    bus.subscribe(Ping, on_ping)
    bus.subscribe(Unrelated, on_unrelated)
    await bus.publish(Ping())

    assert calls == ["ping"]


async def test_a_base_class_subscriber_sees_subclass_events(bus: EventBus) -> None:
    """What makes an audit log a subscriber like any other."""
    seen: list[str] = []

    async def audit(event: DomainEvent) -> None:
        seen.append(type(event).event_name)

    bus.subscribe(DomainEvent, audit)
    await bus.publish(LoudPing())
    await bus.publish(Unrelated())

    assert seen == ["LoudPing", "Unrelated"]


async def test_a_subclass_event_reaches_both_levels(bus: EventBus) -> None:
    seen: list[str] = []

    async def on_ping(event: Ping) -> None:
        seen.append("ping")

    async def on_loud(event: LoudPing) -> None:
        seen.append("loud")

    bus.subscribe(Ping, on_ping)
    bus.subscribe(LoudPing, on_loud)
    await bus.publish(LoudPing())

    assert sorted(seen) == ["loud", "ping"]


async def test_a_parent_event_does_not_reach_subclass_subscribers(
    bus: EventBus,
) -> None:
    seen: list[str] = []

    async def on_loud(event: LoudPing) -> None:
        seen.append("loud")

    bus.subscribe(LoudPing, on_loud)
    await bus.publish(Ping())

    assert seen == []


def test_subscribers_are_ordered_most_specific_first(bus: EventBus) -> None:
    async def on_loud(event: LoudPing) -> None: ...

    async def on_ping_first(event: Ping) -> None: ...

    async def on_ping_second(event: Ping) -> None: ...

    bus.subscribe(Ping, on_ping_first, name="ping-1")
    bus.subscribe(LoudPing, on_loud, name="loud")
    bus.subscribe(Ping, on_ping_second, name="ping-2")

    assert [s.name for s in bus.subscribers_for(LoudPing)] == [
        "loud",
        "ping-1",
        "ping-2",
    ]


async def test_the_same_handler_can_subscribe_twice(bus: EventBus) -> None:
    calls: list[int] = []

    async def handler(event: Ping) -> None:
        calls.append(1)

    bus.subscribe(Ping, handler)
    bus.subscribe(Ping, handler)
    result = await bus.publish(Ping())

    assert len(calls) == 2
    assert result.delivered == 2


# --- the decorator form ---


async def test_on_registers_and_returns_the_function_unchanged(bus: EventBus) -> None:
    seen: list[str] = []

    @bus.on(Ping)
    async def handler(event: Ping) -> None:
        seen.append(event.payload)

    await bus.publish(Ping(payload="via-decorator"))
    # Still an ordinary coroutine function, so its own test never needs the bus.
    await handler(Ping(payload="called-directly"))

    assert seen == ["via-decorator", "called-directly"]


# --- registration is checked ---


def test_a_synchronous_handler_is_refused(bus: EventBus) -> None:
    def blocking(event: Ping) -> None: ...

    with pytest.raises(TypeError, match="must be async"):
        bus.subscribe(Ping, blocking)  # type: ignore[arg-type]


def test_a_non_event_type_is_refused(bus: EventBus) -> None:
    async def handler(event: Any) -> None: ...

    with pytest.raises(TypeError, match="DomainEvent subclass"):
        bus.subscribe(str, handler)  # type: ignore[type-var]


def test_a_non_positive_timeout_is_refused(bus: EventBus) -> None:
    async def handler(event: Ping) -> None: ...

    with pytest.raises(ValueError, match="timeout must be positive"):
        bus.subscribe(Ping, handler, timeout=0)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"max_depth": 0}, "max_depth must be at least 1"),
        ({"default_timeout": -1.0}, "default_timeout must be positive"),
    ],
)
def test_bus_rejects_impossible_configuration(
    kwargs: dict[str, Any], match: str
) -> None:
    with pytest.raises(ValueError, match=match):
        EventBus(**kwargs)


async def test_a_callable_object_counts_as_async(bus: EventBus) -> None:
    """`is_async_callable` looks through `__call__`, so a stateful subscriber
    is registrable without wrapping it in a function."""

    class Counter:
        def __init__(self) -> None:
            self.calls = 0

        async def __call__(self, event: Ping) -> None:
            self.calls += 1

    counter = Counter()
    bus.subscribe(Ping, counter)
    await bus.publish(Ping())

    assert counter.calls == 1


def test_subscription_name_defaults_to_the_handler(bus: EventBus) -> None:
    async def handler(event: Ping) -> None: ...

    subscription = bus.subscribe(Ping, handler)

    assert subscription.name.endswith("handler")


# --- failure isolation ---


async def test_one_failing_subscriber_does_not_stop_the_others(
    bus: EventBus,
) -> None:
    survivors: list[str] = []

    async def explodes(event: Ping) -> None:
        raise RuntimeError("boom")

    async def survives(event: Ping) -> None:
        survivors.append("ran")

    bus.subscribe(Ping, explodes, name="explodes")
    bus.subscribe(Ping, survives, name="survives")
    result = await bus.publish(Ping())

    assert survivors == ["ran"]
    assert result.delivered == 1
    assert [f.subscriber for f in result.failures] == ["explodes"]
    assert isinstance(result.failures[0].error, RuntimeError)


async def test_a_subscriber_failure_does_not_reach_the_publisher(
    bus: EventBus,
) -> None:
    async def explodes(event: Ping) -> None:
        raise RuntimeError("boom")

    bus.subscribe(Ping, explodes)
    result = await bus.publish(Ping())  # must not raise

    assert not result.ok


async def test_raise_for_failures_opts_back_in(bus: EventBus) -> None:
    async def explodes(event: Ping) -> None:
        raise RuntimeError("boom")

    bus.subscribe(Ping, explodes)
    result = await bus.publish(Ping())

    with pytest.raises(EventDispatchError) as exc_info:
        result.raise_for_failures()

    assert exc_info.value.errors and isinstance(exc_info.value.errors[0], RuntimeError)
    # The original failure stays reachable from the traceback.
    assert isinstance(exc_info.value.__cause__, RuntimeError)


async def test_raise_for_failures_is_silent_when_everything_worked(
    bus: EventBus,
) -> None:
    async def handler(event: Ping) -> None: ...

    bus.subscribe(Ping, handler)
    result = await bus.publish(Ping())

    result.raise_for_failures()


def test_raise_for_failures_ignores_a_failure_with_no_exception() -> None:
    """`ok=False` with no error should not raise an empty dispatch error."""
    result = PublishResult(
        event=Ping(),
        outcomes=(SubscriberOutcome(subscriber="odd", ok=False, duration_ms=0.0),),
    )

    result.raise_for_failures()
    assert not result.ok


async def test_outcomes_carry_a_duration(bus: EventBus) -> None:
    ticks = iter([0.0, 0.25])
    timed_bus = EventBus(timer=lambda: next(ticks))

    async def handler(event: Ping) -> None: ...

    timed_bus.subscribe(Ping, handler)
    result = await timed_bus.publish(Ping())

    assert result.outcomes[0].duration_ms == 250.0


# --- concurrency, timeouts and cancellation ---


async def test_subscribers_run_concurrently(bus: EventBus) -> None:
    """Both handlers are inside their bodies at the same moment; a sequential
    dispatch would deadlock this test rather than slow it down."""
    first_started = asyncio.Event()
    second_started = asyncio.Event()

    async def first(event: Ping) -> None:
        first_started.set()
        await asyncio.wait_for(second_started.wait(), timeout=1)

    async def second(event: Ping) -> None:
        second_started.set()
        await asyncio.wait_for(first_started.wait(), timeout=1)

    bus.subscribe(Ping, first)
    bus.subscribe(Ping, second)
    result = await bus.publish(Ping())

    assert result.delivered == 2


async def test_a_subscriber_that_overruns_its_timeout_is_a_failure(
    bus: EventBus,
) -> None:
    cancelled = asyncio.Event()

    async def slow(event: Ping) -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    bus.subscribe(Ping, slow, timeout=0.01)
    result = await bus.publish(Ping())

    assert isinstance(result.failures[0].error, TimeoutError)
    assert cancelled.is_set(), "the overrunning handler should have been cancelled"


async def test_the_bus_default_timeout_applies_to_new_subscriptions() -> None:
    slow_bus = EventBus(default_timeout=0.01)

    async def slow(event: Ping) -> None:
        await asyncio.sleep(10)

    subscription = slow_bus.subscribe(Ping, slow)
    result = await slow_bus.publish(Ping())

    assert subscription.timeout == 0.01
    assert isinstance(result.failures[0].error, TimeoutError)


async def test_a_per_subscription_timeout_overrides_the_bus_default() -> None:
    slow_bus = EventBus(default_timeout=0.01)

    async def handler(event: Ping) -> None:
        await asyncio.sleep(0.05)

    slow_bus.subscribe(Ping, handler, timeout=5.0)
    result = await slow_bus.publish(Ping())

    assert result.ok


async def test_cancelling_the_publisher_cancels_the_subscribers(
    bus: EventBus,
) -> None:
    """Cancellation is not a subscriber failure: nothing is waiting for the
    answer, so the handlers stop instead of being recorded as broken."""
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def handler(event: Ping) -> None:
        started.set()
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    bus.subscribe(Ping, handler)
    task = asyncio.create_task(bus.publish(Ping()))
    await asyncio.wait_for(started.wait(), timeout=1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled.is_set()


# --- nesting ---


async def test_a_subscriber_may_publish(bus: EventBus) -> None:
    seen: list[str] = []

    async def on_ping(event: Ping) -> None:
        await bus.publish(Unrelated())

    async def on_unrelated(event: Unrelated) -> None:
        seen.append("unrelated")

    bus.subscribe(Ping, on_ping)
    bus.subscribe(Unrelated, on_unrelated)
    result = await bus.publish(Ping())

    assert seen == ["unrelated"]
    assert result.ok


async def test_a_publish_cycle_is_stopped_rather_than_hanging() -> None:
    shallow = EventBus(max_depth=3)
    depth = 0

    async def republish(event: Ping) -> None:
        nonlocal depth
        depth += 1
        result = await shallow.publish(Ping())
        result.raise_for_failures()

    shallow.subscribe(Ping, republish)
    result = await shallow.publish(Ping())

    assert not result.ok
    assert depth == 3
    innermost = result.failures[0].error
    assert isinstance(innermost, EventDispatchError | EventCycleError)


async def test_depth_is_released_after_publishing(bus: EventBus) -> None:
    """A publish that nested must not leave later publishes closer to the cap."""
    shallow = EventBus(max_depth=2)

    async def nests(event: Ping) -> None:
        await shallow.publish(Unrelated())

    subscription = shallow.subscribe(Ping, nests)
    assert (await shallow.publish(Ping())).ok

    subscription.unsubscribe()
    for _ in range(3):
        assert (await shallow.publish(Ping())).ok


# --- lifecycle ---


async def test_unsubscribe_stops_delivery(bus: EventBus) -> None:
    calls: list[int] = []

    async def handler(event: Ping) -> None:
        calls.append(1)

    subscription = bus.subscribe(Ping, handler)
    await bus.publish(Ping())
    subscription.unsubscribe()
    await bus.publish(Ping())

    assert len(calls) == 1
    assert bus.subscribers_for(Ping) == ()


async def test_unsubscribing_twice_is_a_no_op_with_siblings_left(
    bus: EventBus,
) -> None:
    """The second call finds the event still registered but this handle gone,
    which is a different branch from the last subscriber having been removed."""

    async def handler(event: Ping) -> None: ...

    async def sibling(event: Ping) -> None: ...

    subscription = bus.subscribe(Ping, handler)
    bus.subscribe(Ping, sibling)
    subscription.unsubscribe()
    subscription.unsubscribe()

    assert len(bus.subscribers_for(Ping)) == 1


async def test_unsubscribing_twice_is_a_no_op(bus: EventBus) -> None:
    async def handler(event: Ping) -> None: ...

    subscription = bus.subscribe(Ping, handler)
    subscription.unsubscribe()
    subscription.unsubscribe()

    assert bus.subscribers_for(Ping) == ()


def test_unsubscribing_removes_only_the_subscription_it_was_given(
    bus: EventBus,
) -> None:
    async def handler(event: Ping) -> None: ...

    first = bus.subscribe(Ping, handler, name="same")
    bus.subscribe(Ping, handler, name="same")
    first.unsubscribe()

    assert len(bus.subscribers_for(Ping)) == 1


def test_unsubscribing_a_subscription_from_another_bus_is_a_no_op(
    bus: EventBus,
) -> None:
    other = EventBus()

    async def handler(event: Ping) -> None: ...

    stray = other.subscribe(Ping, handler)
    bus.subscribe(Ping, handler)
    bus.unsubscribe(stray)

    assert len(bus.subscribers_for(Ping)) == 1


def test_clear_drops_everything(bus: EventBus) -> None:
    async def on_ping(event: Ping) -> None: ...

    async def on_unrelated(event: Unrelated) -> None: ...

    bus.subscribe(Ping, on_ping)
    bus.subscribe(Unrelated, on_unrelated)
    bus.clear()

    assert bus.subscribers_for(Ping) == ()
    assert bus.subscribers_for(Unrelated) == ()
