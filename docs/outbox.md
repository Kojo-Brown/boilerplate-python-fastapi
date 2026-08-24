# The transactional outbox

`src/outbox` — the durable half of `docs/events.md`.

## The problem

A request changes state and then tells the rest of the system about it. Those
are two different systems — Postgres, and a mail queue or a broker or another
service — and no transaction spans both of them. Whichever order you pick, one
of them can happen without the other:

```python
# Announce first: the announcement survives a rollback.
await events.publish(UserRegistered(...))
await session.commit()          # ← raises. A welcome email is now owed to
                                #   a user who does not exist.

# Commit first: the announcement is lost if this process dies.
await session.commit()
# ← SIGKILL, OOM, a node draining. The user exists, nothing is owed to
#   anyone, and no record of the debt was ever written.
await events.publish(UserRegistered(...))
```

This codebase used to do the second, deliberately, and said so in
`AuthService`'s docstring: reacting to a rolled-back transaction is worse than
losing a reaction, so publish last. Both are still bad, and the gap does not
close by moving the call.

## The answer

Write the notification as a **row**, in the same session — and therefore the
same transaction — as the state change. The two commit together or neither
does. A separate **relay** reads committed rows and dispatches them, deleting
each one only once it has been delivered.

```
request                                    relay (background task)
───────                                    ───────────────────────
BEGIN
  INSERT INTO users ...
  INSERT INTO outbox_events ...            SELECT ... WHERE available_at <= now()
COMMIT ────────────────────────────────▶     ORDER BY available_at, id
                                             LIMIT n FOR UPDATE SKIP LOCKED
                                           bus.publish(event)
                                           DELETE FROM outbox_events WHERE id = ...
                                           COMMIT
```

Nothing is lost, because the debt is recorded transactionally. Nothing reacts
to a rollback, because the relay only ever sees committed rows. What you pay
instead:

- **Delivery is asynchronous.** A subscriber runs after the request has
  answered, by up to `OUTBOX_POLL_INTERVAL_SECONDS` plus its own runtime.
- **Delivery is at least once.** A relay that dies between dispatching and
  committing the delete will dispatch again. Subscribers must be idempotent;
  `event_id` is the key to dedupe on.
- **Ordering is not guaranteed.** Concurrent relays and per-event retries both
  reorder. If two events must be applied in order, that ordering has to live in
  the events themselves.

## Using it

Nothing changes at the call site. `OutboxPublisher` is an `EventPublisher`, so
a producer still writes:

```python
await self.events.publish(UserRegistered(user_id=str(user.id), email=user.email))
```

**One rule: publish before you commit.**

```python
user = await self.users.create(...)
await self.events.publish(UserRegistered(...))   # row joins this transaction
await self.uow.commit()                          # both, or neither
```

A publish placed *after* `commit()` stages its row in a fresh transaction that
`get_db` closes without committing. Nothing raises: the request succeeds, the
user exists, and the event silently vanishes.
`tests/test_outbox_db.py::test_publishing_after_the_commit_writes_nothing`
demonstrates it, and `tests/test_auth_events.py` holds the ordering in place
for the three places `AuthService` publishes.

### Adding an event

1. Add the class to `src/events/catalog.py`, as before.
2. Add it to `EVENT_TYPES` in the same module.

Step 2 is what lets the relay turn a row back into an event. Forgetting it does
not fail near the omission — it fails in the relay, as rows accumulating behind
an "unknown event type" error — so `tests/test_outbox_codec.py` fails the build
instead.

Event fields must be JSON scalars: `str`, `int`, `float`, `bool`, `None`.
Anything else is refused at publish time, by the request that introduced it,
naming the field. That is stricter than it needs to be for a reason:

- A tuple encodes as a JSON array and decodes as a list; a `StrEnum` decodes as
  a plain string. The event the subscriber receives would differ from the one
  that was published, in a frozen dataclass nobody thinks to doubt.
- Coercing values back by their declared type means resolving annotations —
  which are strings here — and owning a type registry forever after.
- A field that will not fit in a scalar is usually a document that has been put
  in an event. Name the facts a subscriber needs instead.

`event_id` and `occurred_at` are columns rather than payload, so they cannot
disagree with themselves.

## The table

`outbox_events` (migration `0005`) is a queue, not a log. A delivered row is
deleted; keeping them would produce a nice audit trail, the largest table in
the database, and a retention job somebody has to write. The audit trail is a
subscriber's job — `audit.user_activity` already writes one.

| column | why |
|---|---|
| `event_id` | The consumer's dedupe key. Not unique here: rows are deleted on delivery, so a unique index could not recognise a repeat anyway |
| `event_name` | Looked up in the codec's registry. The reason `DomainEvent.event_name` exists: the class may be renamed, this string may not |
| `payload` | JSONB. The event's fields, minus the two with columns |
| `occurred_at` | When it happened, per the event |
| `created_at` | When the row was written. The gap between the two is domain lag |
| `available_at` | When the relay may next claim it. Both the retry schedule and the ordering key |
| `attempts` | Failures so far. Drives the backoff, and is durable so a redeploy does not reset it |
| `last_error` | What went wrong last time, truncated. The log has the traceback; this outlives the log |

One index, `(available_at, id)`, which serves the relay's only query — filter
and ordering — in one scan.

## The relay

`SELECT ... FOR UPDATE SKIP LOCKED` is the whole claim, and it is why any
number of relays can run: each walks away with a disjoint batch instead of
queueing behind the others, and one that dies mid-batch releases its rows as
soon as its transaction rolls back. There is no lease to expire and no reaper
to write.

The claim, the dispatch and the outcome are **one transaction**, because a row
lock lives until COMMIT — that is what makes "claimed" mean "no other relay
will touch this". The cost is that a slow subscriber holds a transaction open,
which is why every dispatch is bounded by `OUTBOX_DISPATCH_TIMEOUT_SECONDS`.

A **distributed lock is deliberately not used**, though
`docs/distributed-locking.md` guessed the relay would be its first consumer. It
would make the relay a singleton — the opposite of what `SKIP LOCKED` buys — to
solve a problem the database has already solved with the row locks it was going
to take anyway.

Failures are handled **per event**: one poison event reschedules itself and the
rest of the batch continues. Abandoning the batch would put every event behind
that one, forever, which is how an outbox becomes an outage.

The backoff is full-jitter exponential (`backoff_delay`, shared with
`src/decorators/retry.py` and `src/locking/retry.py`), computed from the row's
`attempts` and capped at `OUTBOX_RETRY_MAX_DELAY_SECONDS`. There is **no
maximum attempt count and no dead-letter queue** — that is the next item in
Phase 8. Until then a permanently poison event retries at the capped interval
forever, visible in `attempts` and `last_error`, and it cannot starve the queue
because the backoff pushes it behind everything that is ready.

Two failure modes are worth naming:

**A subscriber failing retries the whole event.** The bus isolates subscriber
failures and *reports* them; the relay calls `raise_for_failures()`, because a
caller that ignored the report could not tell a delivered event from a dropped
one. So the subscribers that already succeeded will see the event again. No
process can commit half a delivery — hence "idempotent subscribers", again.

**An event with no subscribers is delivered.** That is the bus's own rule, and
it makes the shutdown order load-bearing: `src/main.py` stops the relay
*before* clearing the bus, or the last batch would be "delivered" to nothing.

## Configuration

| setting | default | notes |
|---|---|---|
| `OUTBOX_RELAY_ENABLED` | `true` | Whether *this* process drains. Turn it off in API pods and run dedicated relay processes if you want delivery isolated from request traffic |
| `OUTBOX_BATCH_SIZE` | `100` | With the dispatch timeout, bounds how long one batch can hold a connection and its row locks |
| `OUTBOX_POLL_INTERVAL_SECONDS` | `1.0` | Delivery latency when the queue is empty, and one query per interval per relay |
| `OUTBOX_DISPATCH_TIMEOUT_SECONDS` | `30.0` | Per event, subscribers included |
| `OUTBOX_RETRY_BASE_DELAY_SECONDS` | `1.0` | First retry |
| `OUTBOX_RETRY_MAX_DELAY_SECONDS` | `300.0` | Backoff ceiling |

There is no setting for *writing* outbox rows. A flag that quietly sent events
back through the in-process bus would reintroduce the lost-event window in
whichever deployment least expected it.

A full batch is followed immediately by another claim rather than a sleep: a
backlog is exactly when pacing delivery at `batch_size` per poll interval is
wrong.

## Testing

The relay's policy — scheduling, isolation, backoff, shutdown — is tested over
an in-memory outbox in `tests/test_outbox_relay.py`, with a real `EventBus` as
the dispatcher, because subscriber isolation is the bus's behaviour and a fake
one would be asserting on a reimplementation of it.

The parts that are genuinely Postgres are in `tests/test_outbox_db.py`, against
a real server: the row committing with the state change, the rollback taking it
with it, two relays claiming disjoint batches, the schedule using the database's
clock. These skip when `DATABASE_URL` names nothing reachable, and CI always has
a Postgres service.

To drain the outbox synchronously in a test — or from a one-shot script — call
`drain_once()`; everything `run()` adds beyond it is scheduling:

```python
relay = OutboxRelay(batches=session_batches(sessions), dispatcher=bus)
result = await relay.drain_once()
assert result.delivered == 1
```

## What this still does not give you

- **Exactly-once delivery.** Nothing does, across two systems. At-least-once
  plus an idempotent consumer is the achievable shape.
- **Ordering.** See above.
- **A dead-letter queue.** Phase 8.
- **Delivery to anything but the in-process bus.** The relay dispatches to an
  `EventDispatcher`; pointing that at Kafka or a webhook fan-out is a different
  implementation of one protocol, and a later item.
