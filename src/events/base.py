"""The event contract, and the failures publishing one can produce.

Nothing here imports SQLAlchemy, FastAPI or the settings object. A domain event
is a statement about something that already happened — past tense, immutable,
carrying the facts a subscriber needs rather than a handle it can use to go
looking for more. Handing a subscriber an ORM row instead would tie every
observer to a session that is, by the time they run, already committed and
closed.

Events are `kw_only` dataclasses on purpose. The base carries defaulted fields
(`event_id`, `occurred_at`) and every subclass adds required ones; without
`kw_only` that ordering is a `TypeError` at class-definition time, and the
usual workaround — giving the subclass fields dummy defaults — turns a missing
`user_id` into an empty string at runtime instead of an error at the call site.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar, Protocol, runtime_checkable

from src.exceptions import AppException


class EventBusError(AppException):
    """Base for the failures publishing an event can produce."""

    status_code = 500
    error_code = "EVENT_BUS_ERROR"

    def __init__(self, message: str, details: object = None) -> None:
        super().__init__(message, details)


class EventDispatchError(EventBusError):
    """Raised by `PublishResult.raise_for_failures`, never by `publish` itself.

    Publishing isolates subscriber failures because an observer is, by
    definition, not part of the thing it observes: a welcome email that could
    not be sent has no business turning a successful registration into a 500.
    A caller that disagrees for one specific publish asks for this explicitly.

    It is an `AppException` rather than an `ExceptionGroup` because the edge
    turns exceptions into status codes by looking at their type, and a group
    carries no status. The underlying exceptions are on `.errors`, and the
    first one is chained as `__cause__` so a traceback still leads somewhere.
    """

    status_code = 500
    error_code = "EVENT_DISPATCH_FAILED"

    def __init__(self, event_name: str, errors: tuple[BaseException, ...]) -> None:
        self.errors = errors
        super().__init__(
            f"{len(errors)} subscriber(s) of '{event_name}' failed.",
            details={
                "event": event_name,
                "errors": [f"{type(e).__name__}: {e}" for e in errors],
            },
        )


class EventCycleError(EventBusError):
    """Raised when subscribers publish each other's events without end.

    A subscriber may publish; that is how a domain reacts to itself. What it
    may not do is form a cycle, because each hop is an `await` inside the
    previous one and the loop shows up as a request that never returns rather
    than as a stack overflow. The depth cap turns that into an error naming the
    event that closed the ring.
    """

    error_code = "EVENT_CYCLE_DETECTED"

    def __init__(self, event_name: str, depth: int) -> None:
        super().__init__(
            f"Publishing '{event_name}' exceeded the maximum nesting depth "
            f"of {depth}; subscribers are probably publishing in a cycle.",
            details={"event": event_name, "max_depth": depth},
        )


@dataclass(frozen=True, kw_only=True)
class DomainEvent:
    """A fact about something that has already happened.

    Subclass it, add the facts, and leave the identity fields alone:

        @dataclass(frozen=True, kw_only=True)
        class InvoicePaid(DomainEvent):
            invoice_id: str
            amount_cents: int

    `event_id` and `occurred_at` are defaulted rather than assigned by the bus
    so that an event is complete the moment it is constructed — it can be
    logged, compared or persisted before anything publishes it, which is what
    the transactional outbox will need. Both are overridable by keyword, which
    is how a test pins them.

    Frozen because a subscriber runs concurrently with its siblings: a mutable
    event would let the first handler to run rewrite what the others observe,
    and the winner would depend on scheduling order.
    """

    #: Stable name for this event type. Defaults to the class name; set it
    #: explicitly when the value is written somewhere durable (a log query, an
    #: outbox row) and must survive the class being renamed.
    event_name: ClassVar[str] = "DomainEvent"

    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    occurred_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Only derive a name for subclasses that did not choose one. Reading
        # `cls.event_name` instead would find the parent's and silently give
        # two event types the same name, which is exactly the confusion the
        # attribute exists to prevent.
        if "event_name" not in cls.__dict__:
            cls.event_name = cls.__name__


@runtime_checkable
class EventPublisher(Protocol):
    """The publishing half of the bus, for code that only announces things.

    `AuthService` needs to say a registration happened. It does not subscribe,
    unsubscribe, inspect the subscriber table or reset it, so depending on the
    concrete `EventBus` gave it a class whose surface is mostly other people's
    business — and made "what does registering publish?" a question you could
    only answer by building a bus.

    The return is `object` rather than `PublishResult` on purpose. `publish`
    reports which subscribers failed, and a caller that means to act on that
    should say so by depending on the bus itself; every caller here deliberately
    ignores it, because a broken mail queue must not fail a registration that
    has already committed. Promising a `PublishResult` here would oblige every
    fake publisher to construct one for a value nothing reads.
    """

    async def publish(self, event: DomainEvent) -> object:
        """Deliver `event` to whatever is listening."""
        ...
