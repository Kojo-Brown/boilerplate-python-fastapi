"""The outbox table: events waiting to be relayed, and nothing else.

This is a queue, not a log. A row exists because something committed a state
change and owes the rest of the system a notification about it; the row is
deleted the moment that notification has been delivered. Keeping delivered rows
would give a nice audit trail and, in a system doing any volume, the largest
table in the database with the hottest index in it — retention then becomes a
job somebody has to write, and until they do the queue's own scans get slower
every day. The audit trail belongs to the subscribers (`audit.user_activity`
already writes one), so this table holds only what is still owed.

Three columns carry the delivery state and are worth reading as a group:

`available_at` is when the relay may next try this row. It is set to `now()` on
insert, so a fresh row is immediately claimable, and pushed into the future by
each failure. That single column is both the retry schedule and the ordering
key: rows come out oldest-available first, which is FIFO while nothing is
failing and puts a failing row behind everything ready as soon as it does.

`attempts` and `last_error` are what make a stuck row diagnosable without
turning on debug logging in production. `attempts` is also the input to the
backoff, so the schedule survives a relay restart — a counter held in the
relay's memory would reset every deploy.

Two things this table deliberately does not have:

**No unique index on `event_id`.** It would be an index on the write path
guarding against a collision between two random UUIDs, and it could not do the
job people expect of it anyway: rows are deleted on delivery, so it cannot
recognise an event id that has already been through. `event_id` is here because
delivery is at-least-once and *consumers* need a key to dedupe on.

**No `published_at` and no status column.** A row's state is entirely "still
owed", so a status would have exactly one value, and the partial index that
usually accompanies one would index nothing.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import UUID, DateTime, Index, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from src.database import Base

#: Longest `event_id` the column accepts. `DomainEvent.event_id` is a string
#: rather than a UUID — a test pins it, and a future producer may carry
#: someone else's id — so the column is sized rather than typed, and
#: `src/outbox/publisher.py` refuses anything longer *before* the INSERT.
EVENT_ID_MAX_LENGTH = 64

#: Longest `event_name`. Names default to the class name and are otherwise
#: chosen by hand, so this is a guard rail rather than a real constraint.
EVENT_NAME_MAX_LENGTH = 255

#: How much of a failure's description is kept. A traceback is not stored here:
#: it belongs in the log, joined by `event_id`, and rows are read by a queue
#: scan that has no business dragging kilobytes of text along with it.
LAST_ERROR_MAX_LENGTH = 1000


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    #: The `DomainEvent.event_id` this row carries. Not a key here; it is the
    #: key a consumer deduplicates on, because the relay delivers at least once.
    event_id: Mapped[str] = mapped_column(String(EVENT_ID_MAX_LENGTH), nullable=False)

    #: `type(event).event_name`, which is why that attribute exists: the class
    #: may be renamed, this string may not, and the relay looks the class up by
    #: it when the row is read back — possibly by a different deployment.
    event_name: Mapped[str] = mapped_column(
        String(EVENT_NAME_MAX_LENGTH), nullable=False
    )

    #: The event's own fields, minus the two below that have columns. JSONB
    #: rather than JSON: this is Postgres, and jsonb parses once on write
    #: instead of on every read.
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

    #: When the thing happened, as the event recorded it. Distinct from
    #: `created_at`, which is when the row was written, and from the delivery
    #: time, which nothing stores — the gap between the first two is the lag
    #: between the domain and the transaction.
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    #: When this row may next be claimed. The database clock sets it, on insert
    #: and on every reschedule, so a relay whose host clock has drifted cannot
    #: claim a row early or park one for an hour.
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    #: Failed delivery attempts so far. Drives the backoff, so it must be
    #: durable: a counter in the relay's memory would reset on every deploy and
    #: hand a poison event a fresh burst of retries each time.
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    last_error: Mapped[str | None] = mapped_column(
        String(LAST_ERROR_MAX_LENGTH), nullable=True
    )

    # The relay's only query is `WHERE available_at <= now() ORDER BY
    # available_at, id LIMIT n FOR UPDATE SKIP LOCKED`, and this index serves
    # the filter and the ordering with one scan. `id` is in it as the
    # tie-break, so every relay walks ready rows in the same order rather than
    # in whatever order the heap happens to give it.
    __table_args__ = (Index("ix_outbox_events_available_at_id", "available_at", "id"),)
