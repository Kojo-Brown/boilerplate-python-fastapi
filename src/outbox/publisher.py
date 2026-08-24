"""The producer half: publishing an event means writing a row, right here.

`OutboxPublisher` is an `EventPublisher`, so `AuthService` and anything else
that announces things keeps the same one-line call. What changes is what the
call *does*: instead of dispatching to subscribers in this process, it stages a
row in the caller's own transaction, and the relay dispatches it once that
transaction has committed.

That swap is the entire pattern, and it moves the failure window rather than
narrowing it. Publishing after the commit — which is what this codebase did,
deliberately, for the in-process bus — makes it impossible to react to a
transaction that then rolls back, at the price of losing the reaction outright
if the process dies in the gap. Writing the row *inside* the transaction closes
that gap: the row and the state change commit together or neither does, so
there is no moment at which one exists without the other. The price is that
delivery becomes at-least-once and asynchronous, which is a bill the
subscribers pay (see `docs/outbox.md`).

**So the call has to come before the commit**, and that is a rule about the
producer, not about this class. `AuthService` publishes and then commits; a
publish that ran after the commit would add its row to a fresh transaction that
`get_db` closes without committing, and the event would vanish with no error
anywhere. `tests/test_auth_events.py` asserts the ordering for that reason.

Nothing here flushes. The INSERT rides along with the commit the producer was
going to make anyway, so an event costs a round trip only if something else
needed one.
"""

from __future__ import annotations

import uuid

import structlog

from src.events.base import DomainEvent
from src.models.outbox import (
    EVENT_ID_MAX_LENGTH,
    EVENT_NAME_MAX_LENGTH,
    OutboxEvent,
)
from src.outbox.base import EventNotSerializableError, OutboxRecord, RowSink
from src.outbox.codec import OutboxCodec, default_codec

logger = structlog.get_logger(__name__)


class OutboxPublisher:
    """An `EventPublisher` that records events instead of dispatching them."""

    def __init__(self, sink: RowSink, *, codec: OutboxCodec | None = None) -> None:
        """
        Args:
            sink: Where the row goes — in the request path, the session the
                repositories are writing through. Anything narrower than a
                session works too; only `add` is used.
            codec: How events become payloads. Defaults to the application
                catalogue.
        """
        self._sink = sink
        self._codec = codec if codec is not None else default_codec()

    @property
    def sink(self) -> RowSink:
        """Where rows are staged.

        Exposed for the same reason `BaseRepository.session` is: "did the
        wiring hand this the request's session, or a second one?" is a question
        only the concrete object can answer, and it is worth being able to ask.
        """
        return self._sink

    async def publish(self, event: DomainEvent) -> OutboxRecord:
        """Stage `event` as a row in the caller's transaction.

        `async` because `EventPublisher` is, not because anything here awaits:
        staging a row is a local call, and the round trip happens at the commit
        the caller was already making.

        Returns the identifiers of the row that was staged, which is what a
        caller can usefully log. The row itself is not returned: it belongs to
        a session the caller does not own, and it is expired the moment that
        session commits.

        Raises:
            EventNotSerializableError: The event cannot be written faithfully —
                a non-scalar field, a non-finite float, or an `event_id` or
                `event_name` too long for its column. Raised *before* anything
                is staged, so a refused event leaves no partial row behind, and
                raised in the request that published it rather than at the
                commit or in the relay.
        """
        event_name = type(event).event_name
        self._check_fits(event, "event_id", event.event_id, EVENT_ID_MAX_LENGTH)
        self._check_fits(event, "event_name", event_name, EVENT_NAME_MAX_LENGTH)
        payload = self._codec.encode(event)

        # Generated here rather than left to the column default, which the ORM
        # would only resolve at flush time: the caller is handed this id before
        # any round trip happens, so a log line about the row it staged is
        # possible without one.
        row_id = uuid.uuid4()
        self._sink.add(
            OutboxEvent(
                id=row_id,
                event_id=event.event_id,
                event_name=event_name,
                payload=payload,
                occurred_at=event.occurred_at,
            )
        )
        logger.debug(
            "outbox.staged",
            outbox_id=str(row_id),
            event_name=event_name,
            event_id=event.event_id,
        )
        return OutboxRecord(id=row_id, event_id=event.event_id, event_name=event_name)

    @staticmethod
    def _check_fits(
        event: DomainEvent, field_name: str, value: str, limit: int
    ) -> None:
        """Refuse a value the column would truncate or reject.

        Postgres raises on an over-long `varchar` rather than truncating, so
        without this the failure is a `DataError` at `COMMIT` — a 500 on a
        request whose body mentions neither events nor this column.
        """
        if len(value) > limit:
            raise EventNotSerializableError(
                f"{field_name} is {len(value)} characters; the outbox column "
                f"holds {limit}.",
                details={"event": type(event).event_name, "field": field_name},
            )


__all__ = ["OutboxPublisher"]
