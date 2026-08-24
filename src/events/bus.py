"""An in-process async event bus with type-checked subscribers.

The point of the observer pattern here is that the code which *causes*
something — registering a user — never learns what should happen next.
`AuthService.register` publishes `UserRegistered` and returns; whether that
sends a welcome email, warms a cache or does nothing at all is decided by which
subscribers are registered, and adding one edits no existing call site.

Four decisions this implementation makes, each of which could reasonably have
gone the other way:

**Publishing awaits its subscribers.** `publish` returns once every handler has
finished, so a request that publishes pays for its observers. The alternative —
`asyncio.create_task` and return immediately — loses the exception when nothing
holds the task, lets the task be garbage-collected mid-flight, and outlives the
request scope it borrowed its context from. Work that must not be paid for
inline belongs in a subscriber that enqueues a Celery task (see
`src/tasks/celery_email.py`); that way the handoff is explicit and durable
rather than implicit and lossy.

**Subscribers run concurrently, and failures are isolated.** Handlers for one
event are gathered, so a slow one does not delay the rest, and one that raises
neither cancels its siblings nor propagates to the publisher — the failure is
logged and recorded in the returned `PublishResult`. Callers who need the
failure to matter call `raise_for_failures()`. There is deliberately no
"run these in order" mode: if B must observe the world A left behind, that is a
sequencing requirement the observer pattern does not express, and the honest
encoding is one subscriber that calls both.

**Dispatch follows the class hierarchy.** A subscriber registered for
`DomainEvent` sees every event, one registered for `UserEvent` sees every user
event. That is what makes an audit log or a metrics tap a subscriber like any
other rather than a special case wired into `publish`.

**This bus is in-process and forgets, so nothing publishes to it directly.**
Nothing here is persisted, and a crash between a commit and the subscribers
would lose the notification. The request path therefore publishes to the
transactional outbox (`src/outbox`), which writes an event row in the same
transaction as the state change; the relay reads committed rows and dispatches
them *here*. Everything above still describes what happens once an event
reaches the bus — the difference is who hands it over, and that the handover
survives the process dying.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Final, TypeVar

import structlog

from src.decorators.base import DEFAULT_TIMER, Clock, default_event_name, duration_ms
from src.decorators.base import is_async_callable as _is_async_callable
from src.events.base import DomainEvent, EventCycleError, EventDispatchError

logger = structlog.get_logger(__name__)

E = TypeVar("E", bound=DomainEvent)

#: What a subscriber is: something that takes its event and awaits. The return
#: value is `None` because an observer has nobody to answer — anything that
#: needs to hand a result back to the publisher is not an observer.
Subscriber = Callable[[E], Awaitable[None]]

DEFAULT_MAX_DEPTH: Final[int] = 8

# Nesting depth of the publish currently in flight. A ContextVar rather than an
# attribute because `asyncio.gather` copies the context into each handler's
# task: the depth reaches a subscriber that publishes, and its own increment
# stays in its own branch instead of leaking sideways into its siblings.
_publish_depth: ContextVar[int] = ContextVar("event_publish_depth", default=0)


@dataclass(frozen=True, slots=True)
class SubscriberOutcome:
    """What one subscriber did with one event.

    `error` is a `BaseException | None` rather than a message so a caller can
    re-raise or inspect it; `raise_for_failures` relies on that.
    """

    subscriber: str
    ok: bool
    duration_ms: float
    error: BaseException | None = None


@dataclass(frozen=True, slots=True)
class PublishResult:
    """The outcome of one publish, per subscriber.

    An empty `outcomes` is the ordinary case for an event nobody has subscribed
    to, and is not an error: an event with no observers is the pattern working.
    """

    event: DomainEvent
    outcomes: tuple[SubscriberOutcome, ...]

    @property
    def failures(self) -> tuple[SubscriberOutcome, ...]:
        return tuple(o for o in self.outcomes if not o.ok)

    @property
    def delivered(self) -> int:
        """How many subscribers completed without raising."""
        return sum(1 for o in self.outcomes if o.ok)

    @property
    def ok(self) -> bool:
        return not self.failures

    def raise_for_failures(self) -> None:
        """Raise `EventDispatchError` if any subscriber failed.

        For the caller who has decided that, for this one publish, an observer
        failing means the operation did not really happen. Rare by design —
        reach for it and it is worth asking whether the handler should have
        been a subscriber at all.
        """
        errors = tuple(o.error for o in self.failures if o.error is not None)
        if not errors:
            return
        raise EventDispatchError(type(self.event).event_name, errors) from errors[0]


@dataclass(frozen=True, eq=False)
class Subscription[E: DomainEvent]:
    """A registered handler. Keep it to unsubscribe; discard it to leak nothing.

    Generic over the event type so `unsubscribe` and the registry stay typed
    without anyone naming the type twice.

    `eq=False` — a subscription is a handle, so two of them are the same one
    only if they are the same object. Field-wise equality would make the same
    handler registered twice indistinguishable, and worse, would let a
    subscription belonging to *another* bus match and remove one from this one:
    the bus a subscription came from is not part of what a generated `__eq__`
    would compare unless it were told to.
    """

    event_type: type[E]
    handler: Subscriber[E]
    name: str
    timeout: float | None
    bus: EventBus = field(repr=False)
    #: Registration counter, in registration order. Reported in debug logs and
    #: used by nothing else; dispatch order comes from the list itself.
    seq: int

    def unsubscribe(self) -> None:
        """Remove this handler. Idempotent — unsubscribing twice is a no-op."""
        self.bus.unsubscribe(self)


class EventBus:
    """Routes events to the subscribers registered for their type.

    One process-wide instance (`event_bus`) is the normal case; construct
    another to give a test its own, which is cheaper and more honest than
    registering against the global one and hoping the teardown runs.
    """

    def __init__(
        self,
        *,
        default_timeout: float | None = None,
        max_depth: int = DEFAULT_MAX_DEPTH,
        timer: Clock = DEFAULT_TIMER,
    ) -> None:
        """
        Args:
            default_timeout: Seconds a subscriber may take before it is
                cancelled and recorded as failed. `None` means no limit, which
                is the right default for a bus whose subscribers are local and
                the wrong one as soon as a subscriber talks to a network.
            max_depth: How deeply publishes may nest before `EventCycleError`.
            timer: Elapsed-time source. Injectable so a test can assert on the
                durations without sleeping.
        """
        if max_depth < 1:
            raise ValueError("max_depth must be at least 1.")
        if default_timeout is not None and default_timeout <= 0:
            raise ValueError("default_timeout must be positive when set.")

        self._subscriptions: dict[type[DomainEvent], list[Subscription[Any]]] = {}
        self._default_timeout = default_timeout
        self._max_depth = max_depth
        self._timer = timer
        self._seq = 0

    def subscribe(
        self,
        event_type: type[E],
        handler: Subscriber[E],
        *,
        name: str | None = None,
        timeout: float | None = None,
    ) -> Subscription[E]:
        """Register `handler` for `event_type` and every subclass of it.

        The type checker ties the two together: a handler that takes
        `UserLoggedIn` cannot be registered for `UserRegistered`, and inside
        the handler the event is the concrete type rather than `DomainEvent`.

        Args:
            event_type: The event class to observe. Subclasses match too.
            handler: An async callable taking the event.
            name: Label used in logs and in `SubscriberOutcome`. Defaults to
                the handler's `module.qualname`.
            timeout: Per-call limit in seconds, overriding the bus default.

        Raises:
            TypeError: `event_type` is not a `DomainEvent` subclass, or
                `handler` is not awaitable. Both are programming errors, caught
                at registration rather than on the first event in production.
        """
        if not (isinstance(event_type, type) and issubclass(event_type, DomainEvent)):
            raise TypeError(
                f"event_type must be a DomainEvent subclass, got {event_type!r}."
            )
        # A plain `def` handler would run to completion inside the event loop
        # and block every other subscriber — and `await` on its return value
        # would fail with something far less obvious than this.
        if not _is_async_callable(handler):
            raise TypeError(
                f"Subscriber {default_event_name(handler)} must be async; "
                "a synchronous handler blocks the event loop."
            )
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be positive when set.")

        self._seq += 1
        subscription = Subscription(
            event_type=event_type,
            handler=handler,
            name=name or default_event_name(handler),
            timeout=timeout if timeout is not None else self._default_timeout,
            bus=self,
            seq=self._seq,
        )
        self._subscriptions.setdefault(event_type, []).append(subscription)
        logger.debug(
            "events.subscribed",
            event_name=event_type.event_name,
            subscriber=subscription.name,
        )
        return subscription

    def on(
        self,
        event_type: type[E],
        *,
        name: str | None = None,
        timeout: float | None = None,
    ) -> Callable[[Subscriber[E]], Subscriber[E]]:
        """Decorator form of `subscribe`.

            @event_bus.on(UserRegistered)
            async def send_welcome(event: UserRegistered) -> None: ...

        The function is returned unchanged, so it stays directly callable —
        which is how its test calls it without going through the bus at all.
        """

        def decorate(handler: Subscriber[E]) -> Subscriber[E]:
            self.subscribe(event_type, handler, name=name, timeout=timeout)
            return handler

        return decorate

    def unsubscribe(self, subscription: Subscription[Any]) -> None:
        """Remove a subscription. No-op if it is already gone, or not ours.

        `Subscription.eq` is identity, so `list.remove` removes exactly the
        handle it was given; the `bus` check makes a subscription from another
        bus a no-op rather than a lookup that happens to miss.
        """
        if subscription.bus is not self:
            return
        handlers = self._subscriptions.get(subscription.event_type)
        if handlers is None:
            return
        try:
            handlers.remove(subscription)
        except ValueError:
            return
        if not handlers:
            del self._subscriptions[subscription.event_type]
        logger.debug(
            "events.unsubscribed",
            event_name=subscription.event_type.event_name,
            subscriber=subscription.name,
        )

    def clear(self) -> None:
        """Drop every subscription. For test teardown, not for production."""
        self._subscriptions.clear()

    def subscribers_for(
        self, event_type: type[DomainEvent]
    ) -> tuple[Subscription[Any], ...]:
        """Subscriptions that would receive `event_type`, most specific first.

        Walking `__mro__` is what makes a `DomainEvent` subscriber an audit log
        for everything. Within one class, registration order is preserved, so
        the sequence is deterministic even though the calls are concurrent —
        which matters for reading `PublishResult`, not for execution order.
        """
        matched: list[Subscription[Any]] = []
        for klass in event_type.__mro__:
            if not (isinstance(klass, type) and issubclass(klass, DomainEvent)):
                continue
            matched.extend(self._subscriptions.get(klass, ()))
        return tuple(matched)

    async def publish(self, event: DomainEvent) -> PublishResult:
        """Deliver `event` to every matching subscriber and report the outcome.

        Returns when all of them have finished. Subscriber failures are logged
        and returned, never raised — see the module docstring, and
        `PublishResult.raise_for_failures` for the opt-out.

        Raises:
            EventCycleError: subscribers have nested publishes past `max_depth`.
            asyncio.CancelledError: the caller was cancelled. Handlers are
                cancelled with it; this is not treated as a subscriber failure,
                because nothing is waiting for the answer any more.
        """
        event_name = type(event).event_name
        depth = _publish_depth.get()
        if depth >= self._max_depth:
            raise EventCycleError(event_name, self._max_depth)

        subscriptions = self.subscribers_for(type(event))
        if not subscriptions:
            logger.debug("events.published", event_name=event_name, subscribers=0)
            return PublishResult(event=event, outcomes=())

        token = _publish_depth.set(depth + 1)
        try:
            outcomes = await asyncio.gather(
                *(self._invoke(s, event) for s in subscriptions)
            )
        finally:
            _publish_depth.reset(token)

        result = PublishResult(event=event, outcomes=tuple(outcomes))
        logger.debug(
            "events.published",
            event_name=event_name,
            event_id=event.event_id,
            subscribers=len(subscriptions),
            failed=len(result.failures),
        )
        return result

    async def _invoke(
        self, subscription: Subscription[Any], event: DomainEvent
    ) -> SubscriberOutcome:
        """Run one subscriber, converting any failure into an outcome.

        `Exception` and not `BaseException`: `CancelledError` is re-raised so
        that a cancelled publish cancels its handlers instead of recording
        eight spurious failures, and `KeyboardInterrupt` and `SystemExit` have
        no business being swallowed by an observer.
        """
        started = self._timer()
        try:
            if subscription.timeout is None:
                await subscription.handler(event)
            else:
                # `wait_for` cancels the handler and raises TimeoutError, which
                # is an ordinary Exception here — a subscriber that overran is
                # a failed subscriber, not a cancelled publish.
                await asyncio.wait_for(
                    subscription.handler(event), subscription.timeout
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            elapsed = duration_ms(self._timer() - started)
            logger.exception(
                "events.subscriber_failed",
                event_name=type(event).event_name,
                event_id=event.event_id,
                subscriber=subscription.name,
                duration_ms=elapsed,
                error=str(exc),
            )
            return SubscriberOutcome(
                subscriber=subscription.name,
                ok=False,
                duration_ms=elapsed,
                error=exc,
            )

        return SubscriberOutcome(
            subscriber=subscription.name,
            ok=True,
            duration_ms=duration_ms(self._timer() - started),
        )


#: The bus the application publishes to. `src.events.subscribers` registers the
#: built-in handlers against it from the FastAPI lifespan — at start-up rather
#: than at import, so importing a module never quietly turns on a side effect
#: and a unit test gets an empty bus unless it asks for one.
event_bus: Final[EventBus] = EventBus()
