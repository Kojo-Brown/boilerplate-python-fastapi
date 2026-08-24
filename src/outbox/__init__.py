"""The transactional outbox: events that commit with the change that caused them.

The problem is a gap of a few milliseconds. A request writes a row and then
tells the rest of the system about it, and those are two different systems —
Postgres and a broker, a mail queue, another service — with no transaction
spanning them. Whichever order you choose, one of them can be the only one that
happened:

- Announce first and the announcement survives a rollback: subscribers react to
  a user who does not exist.
- Commit first (which is what `AuthService` used to do, deliberately) and a
  process that dies in the gap loses the reaction entirely. Nothing retries it,
  because nothing recorded that it was owed.

The outbox removes the second system from the transaction. The event is written
as a *row*, in the same session and therefore the same transaction as the state
change, so the two commit together or neither does. A separate relay reads
committed rows and dispatches them, retrying until it succeeds. The failure
window does not move somewhere else — it stops existing — and the price is
paid in a different currency: delivery becomes asynchronous and at-least-once,
so subscribers must be idempotent and are keyed on `event_id`.

Four pieces:

- `OutboxPublisher` (`publisher.py`) is an `EventPublisher`, so producers keep
  the one-line `await self.events.publish(...)` call. It must be called
  *before* the commit; see its module docstring.
- `OutboxCodec` (`codec.py`) turns an event into a JSON payload and back, and
  refuses at publish time anything that would not survive the round trip.
- `SqlAlchemyOutboxBatch` (`store.py`) claims rows with
  `FOR UPDATE SKIP LOCKED`, so any number of relays can run.
- `OutboxRelay` (`relay.py`) is the loop: claim, dispatch, delete or
  reschedule, forever.

See `docs/outbox.md`.
"""

from src.outbox.base import (
    BatchScope,
    EventDispatcher,
    EventNotDecodableError,
    EventNotSerializableError,
    OutboxBatch,
    OutboxError,
    OutboxRecord,
    PendingEvent,
    RowSink,
    UnknownEventTypeError,
)
from src.outbox.codec import OutboxCodec, default_codec
from src.outbox.factory import create_outbox_relay, get_outbox_relay
from src.outbox.publisher import OutboxPublisher
from src.outbox.relay import DrainResult, OutboxRelay, RelayConfig
from src.outbox.store import SqlAlchemyOutboxBatch, session_batches

__all__ = [
    "BatchScope",
    "DrainResult",
    "EventDispatcher",
    "EventNotDecodableError",
    "EventNotSerializableError",
    "OutboxBatch",
    "OutboxCodec",
    "OutboxError",
    "OutboxPublisher",
    "OutboxRecord",
    "OutboxRelay",
    "PendingEvent",
    "RelayConfig",
    "RowSink",
    "SqlAlchemyOutboxBatch",
    "UnknownEventTypeError",
    "create_outbox_relay",
    "default_codec",
    "get_outbox_relay",
    "session_batches",
]
