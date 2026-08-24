"""The outbox contracts: what a row looks like on the way in and on the way out.

Nothing here imports SQLAlchemy. That is what lets the relay's *policy* — the
backoff, the per-event isolation, the dispatch timeout, the shutdown — be
tested without a database, while the one part that genuinely is Postgres, the
`FOR UPDATE SKIP LOCKED` claim, is tested against a real server.

Three ports, and the asymmetry between them is the pattern:

`RowSink` is the write side, and it is one method wide on purpose. The producer
needs somewhere to *put a row inside the caller's transaction*; it does not
need to query, flush or commit, and asking for an `AsyncSession` to reach
`add()` would tie every producer — and every fake — to SQLAlchemy for the sake
of one call.

`OutboxBatch` is the read side: claim, complete, fail. It is a batch rather
than a store because the transaction boundary is load-bearing here, not
incidental. Claiming holds a row lock, and that lock is what makes "claimed"
mean "no other relay will touch this"; the lock lives until the transaction
ends, so the claim and its outcome have to be the same transaction. Handing out
a `BatchScope` — a callable returning an async context manager — puts that
boundary where a reader can see it, instead of hiding a commit inside a method
that reads like a setter.

`EventDispatcher` is what the relay delivers *to*, and it deliberately returns
a `PublishResult` where `EventPublisher` returns `object`. The bus isolates
subscriber failures and reports them rather than raising; a caller that ignores
the report cannot tell a delivered event from a dropped one, which for the
relay is the difference between deleting the row and retrying it. `EventBus`
satisfies this structurally — see the note on the return type in
`src.events.base.EventPublisher`, which names this exact caller.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from src.events.base import DomainEvent
from src.events.bus import PublishResult
from src.exceptions import AppException
from src.immutable import FrozenDict


class OutboxError(AppException):
    """Base for the failures the outbox can produce."""

    status_code = 500
    error_code = "OUTBOX_ERROR"

    def __init__(self, message: str, details: object = None) -> None:
        super().__init__(message, details)


class EventNotSerializableError(OutboxError):
    """The event cannot be written as a row, so the request that caused it fails.

    Raised at publish time, inside the transaction, which is the whole point.
    The alternative is a row that inserts and then cannot be read back, or one
    the driver rejects at commit — either way the failure surfaces far from the
    event that caused it, and in the second case it fails a request whose body
    gives no hint why. Here it fails the request that introduced the event, in
    the run that introduced it, naming the field.
    """

    error_code = "OUTBOX_EVENT_NOT_SERIALIZABLE"


class EventNotDecodableError(OutboxError):
    """A row's payload does not fit the event class it names.

    What a renamed or removed field looks like from the reading side. Unlike an
    unknown event type this does not heal on its own, so the row retries at the
    capped backoff interval and shows its `attempts` and `last_error` to
    whoever goes looking — the Phase 8 dead-letter queue is where such a row
    eventually stops. It is still not dropped: a payload nobody can read is a
    fact that happened, and deleting it is the one outcome that cannot be
    undone.
    """

    error_code = "OUTBOX_EVENT_NOT_DECODABLE"


class UnknownEventTypeError(OutboxError):
    """A row names an event type this process does not know.

    Ordinary during a rolling deploy: the instance writing rows is one version
    ahead of the instance reading them. Treated as a *retryable* failure rather
    than a poison row for exactly that reason — the row waits and the deploy
    finishes, where dropping it would lose events for the length of the
    rollout.
    """

    error_code = "OUTBOX_UNKNOWN_EVENT_TYPE"


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    """What `publish` hands back: the row it wrote, not the row itself.

    An ORM instance would be a handle to a session the caller does not own and
    must not use, and it would be expired the moment the transaction commits.
    This is three values, safe to log.
    """

    id: uuid.UUID
    event_id: str
    event_name: str


@dataclass(frozen=True, slots=True)
class PendingEvent:
    """One claimed row, detached from whatever loaded it.

    `payload` is a `FrozenDict` rather than a `dict` because a frozen dataclass
    holding a mutable container is frozen in name only: the relay hands the same
    object to the codec and to the log line, and a subscriber-visible payload
    that one of them rewrote would be a bug nobody could see from here.
    """

    id: uuid.UUID
    event_id: str
    event_name: str
    payload: FrozenDict[str, Any]
    occurred_at: datetime
    #: Failures *so far*, not counting the delivery about to be attempted. The
    #: backoff for the next retry is computed from `attempts + 1`.
    attempts: int


@runtime_checkable
class RowSink(Protocol):
    """Somewhere a pending row can be put so the current transaction carries it.

    `AsyncSession` satisfies this structurally, which is why there is no
    adapter: an adapter here would forward one call and obscure the fact that
    the producer and the repositories are writing into the same session, which
    is the entire guarantee the outbox rests on.
    """

    def add(self, instance: object) -> None:
        """Stage `instance` for insertion when this transaction commits."""
        ...


@runtime_checkable
class OutboxBatch(Protocol):
    """One transaction's worth of relay work.

    Every method belongs to the same transaction, and the transaction is what
    `BatchScope` opens and closes. A `complete` that outlived its `claim` would
    be deleting a row it no longer holds the lock on.
    """

    async def claim(self, *, limit: int) -> tuple[PendingEvent, ...]:
        """Take up to `limit` ready rows, locked against other relays.

        Rows another relay holds are skipped rather than waited for, so a short
        result is normal and an empty one means "nothing free right now" — not
        "the outbox is empty".
        """
        ...

    async def complete(self, entry: PendingEvent) -> None:
        """Delete a row whose event has been delivered."""
        ...

    async def fail(self, entry: PendingEvent, *, error: str, retry_in: float) -> None:
        """Record a failed attempt and make the row claimable again later.

        Args:
            entry: The row that failed.
            error: Short description, stored for diagnosis. Truncated to the
                column width; the traceback belongs in the log.
            retry_in: Seconds from *the database's* now until the row is ready
                again.
        """
        ...


#: Opens one relay transaction. A callable returning a context manager rather
#: than a context manager, because the relay opens a new one per tick and a
#: single instance would be exhausted after the first.
BatchScope = Callable[[], AbstractAsyncContextManager[OutboxBatch]]


@runtime_checkable
class EventDispatcher(Protocol):
    """What the relay delivers to, once a row is safely committed."""

    async def publish(self, event: DomainEvent) -> PublishResult:
        """Deliver `event` and report what each subscriber did with it."""
        ...


__all__ = [
    "BatchScope",
    "EventDispatcher",
    "EventNotDecodableError",
    "EventNotSerializableError",
    "OutboxBatch",
    "OutboxError",
    "OutboxRecord",
    "PendingEvent",
    "RowSink",
    "UnknownEventTypeError",
]
