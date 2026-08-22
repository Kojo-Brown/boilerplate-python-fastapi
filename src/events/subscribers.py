"""The subscribers this application ships with, and how they get registered.

This is the only module that knows both a domain event and something that
should happen because of it. `AuthService` publishes; nothing there imports
this file, which is the whole point — the list below can grow without a single
edit to the code that caused the event.

Registration happens from the FastAPI lifespan (`src/main.py`), not at import
time. Importing a module should never quietly start sending email, and a unit
test that imports the service gets a bus with nothing on it unless it asks.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Final

import structlog

from src.events.base import DomainEvent
from src.events.bus import EventBus, Subscriber, Subscription, event_bus
from src.events.catalog import UserEvent, UserRegistered
from src.tasks.celery_email import send_welcome_email_task

logger = structlog.get_logger(__name__)


async def record_user_activity(event: UserEvent) -> None:
    """Emit one structured log line per user event.

    Registered against `UserEvent` rather than each concrete class, so a new
    user event appears in the audit trail the day it is added and nobody has to
    remember this file. `event_name` and `event_id` are what make the line
    joinable against whatever else the request logged.

    Nothing about the account beyond its id and address is logged: the event
    does not carry more, which is the reason it does not carry more.
    """
    await asyncio.sleep(0)
    logger.info(
        "audit.user_activity",
        event_name=type(event).event_name,
        event_id=event.event_id,
        occurred_at=event.occurred_at.isoformat(),
        user_id=event.user_id,
    )


async def send_welcome_email_on_registration(event: UserRegistered) -> None:
    """Queue the welcome email for a newly registered account.

    The subscriber enqueues; it does not deliver. Delivering here would put an
    SMTP round trip — and its retries — inside the request that registered the
    user, and would lose the message entirely if the process died between the
    commit and the send. Celery owns the retry policy and the durability
    (`src/tasks/celery_email.py`).

    `.delay` is a blocking call into the broker, so it goes through
    `asyncio.to_thread`: it is milliseconds against a healthy Redis and an
    unbounded stall against a sick one, and neither belongs on the event loop.
    """
    await asyncio.to_thread(send_welcome_email_task.delay, to=event.email)
    logger.debug("events.welcome_email_queued", user_id=event.user_id)


@dataclass(frozen=True, slots=True)
class SubscriberSpec:
    """One built-in registration: what to observe, with what, under what name."""

    event_type: type[DomainEvent]
    handler: Subscriber[Any]
    name: str
    timeout: float | None = None


#: Add a subscriber by adding a line here. `timeout` is set on anything that
#: talks to another process — a broker that has stopped answering must not be
#: able to hold a registration request open indefinitely.
DEFAULT_SUBSCRIBERS: Final[tuple[SubscriberSpec, ...]] = (
    SubscriberSpec(
        event_type=UserEvent,
        handler=record_user_activity,
        name="audit.user_activity",
    ),
    SubscriberSpec(
        event_type=UserRegistered,
        handler=send_welcome_email_on_registration,
        name="email.welcome",
        timeout=5.0,
    ),
)


def register_default_subscribers(
    bus: EventBus | None = None,
) -> tuple[Subscription[Any], ...]:
    """Attach `DEFAULT_SUBSCRIBERS` to `bus`, and return what was attached.

    Idempotent: a spec whose name is already registered for its event type is
    skipped rather than duplicated, so a second call — a lifespan that runs
    twice under a test client, an app instantiated per test — does not double
    every welcome email. The check is by name because that is what makes two
    registrations *the same* one; re-registering a renamed handler is a
    different subscriber and should appear twice.
    """
    target = bus if bus is not None else event_bus
    registered: list[Subscription[Any]] = []

    for spec in DEFAULT_SUBSCRIBERS:
        existing = {s.name for s in target.subscribers_for(spec.event_type)}
        if spec.name in existing:
            continue
        registered.append(
            target.subscribe(
                spec.event_type,
                spec.handler,
                name=spec.name,
                timeout=spec.timeout,
            )
        )

    logger.info(
        "events.subscribers_registered",
        registered=len(registered),
        total=len(DEFAULT_SUBSCRIBERS),
    )
    return tuple(registered)


# A tuple, not a list. `Final` would leave `__all__.append(...)` legal, and the
# export list of a module is not something an importer gets to extend.
__all__: Final[tuple[str, ...]] = (
    "DEFAULT_SUBSCRIBERS",
    "SubscriberSpec",
    "record_user_activity",
    "register_default_subscribers",
    "send_welcome_email_on_registration",
)
