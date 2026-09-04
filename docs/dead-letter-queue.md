# Dead-letter queue and an exponential-backoff retry ladder

`src/kafka` stops a partition at its first failing record and keeps it stopped.
That is the correct default — it is what ordering costs — but one poison record
holds up every record behind it in its partition, indefinitely. `src/dlq` is
the escape: a record that fails is *moved*, and the partition carries on.

- `src/dlq/base.py` — the ladder as values: tier delays, topic names, and which
  rung a record goes to next. No broker, no clock.
- `src/dlq/envelope.py` — the `x-dlq-*` headers a record carries between tiers.
- `src/dlq/router.py` — the only part that publishes.
- `src/dlq/handler.py` — two wrappers that compose a plain `MessageHandler`
  into one the ladder can drive.
- `src/dlq/replay.py` — putting a dead letter back.
- `src/dlq/factory.py` — settings, and the two runners it takes to run a ladder.

## The shape

```
orders.events            the application's topic
orders.events.retry.1    5s
orders.events.retry.2    25s
orders.events.retry.3    125s
orders.events.dlt        terminal
```

A failure on `orders.events` publishes the record to `.retry.1` and commits
past it. A separate consumer reads all three tiers, waits until each record is
due, and runs the same handler. A failure on the last tier goes to `.dlt`. Four
handler attempts in total, spread over 155 seconds.

## A delay is a topic, not a sleep

The obvious implementation of "retry this in thirty seconds" inside a consumer
is `await asyncio.sleep(30)`. It is wrong twice over, and both failures look
fine in development.

**It stalls the partition.** Records behind the sleeping one are not being
handled either, so a 1% failure rate at a thirty-second retry is a partition
that spends most of its time asleep — the head-of-line blocking a dead-letter
queue exists to end, reintroduced inside the thing meant to fix it.

**It gets the member evicted.** A consumer must return to `poll` within
`max.poll.interval.ms` — five minutes by default — and the broker's answer to a
member that does not is to declare it dead and hand its partitions, including
the records it is still holding, to somebody else. A sleep longer than that
interval does not delay a record; it redelivers the whole assignment
elsewhere, repeatedly.

So a tier is a topic, and the waiting is done by a consumer whose entire job is
to wait.

## The tier delay is fixed, and never jittered

This is the one place in the repository where the backoff call goes the
opposite way to everywhere else. `backoff_delay` — used by
`src/decorators/retry.py`, `src/locking/retry.py`, `OutboxRelay` and
`ConsumerRunner` — applies full jitter, because retries of a shared failure
that are not spread out arrive as one spike. Here jitter would be a bug.

A tier topic is consumed in offset order and the consumer stalls at the first
record that is not due. That is only *complete* if nothing behind it is due
earlier. With one fixed delay per tier, due time is `produced_at + delay`, so
due time rises with offset: when the head becomes due, everything behind it is
due or will be, in order. Add jitter and the invariant is gone — a record drawn
forty seconds at offset 0 makes a record drawn ten seconds at offset 1 wait the
full forty, thirty seconds after it was due.

The spreading that jitter would have bought is there anyway: records enter a
tier at the moment they fail, which is spread out by whatever spread the
failures had.

## What you give up: ordering per key, for records that fail

Records 5 and 6 share a key. 5 fails and is routed to `orders.events.retry.1`;
6 is handled immediately; 5 is handled five seconds later, **after** 6.

If the handler is "apply this update to this row", that is last-write-wins with
the wrong last write. A ladder is the right answer when records are independent
of one another — an email to send, a webhook to deliver, a document to index —
and the wrong one when a key's records are a sequence of edits. For those the
honest behaviour is the stall, and the way to keep it is to not wrap the
handler. That is why `with_dead_letter` is opt-in per handler rather than
something `ConsumerRunner` does.

## The record is never dropped

Everything in the router falls out of one rule. The routing publish is what
makes it safe for the consumer to commit past a failed record, so if the
publish does not land, the commit must not happen either. `route` lets
`PublishError` propagate, `with_dead_letter` lets it reach the runner, and the
runner stalls the partition and retries — back to head-of-line blocking, and
correct, because the alternative during a broker outage is a topic quietly
emptying itself into nowhere.

What that does *not* cover: a crash between the publish and the commit replays
the origin record, so the retry topic gets a duplicate. That is at-least-once,
the same guarantee the runner already gave, arrived at the same way. **Handlers
must be idempotent** — the ladder does not add a requirement, it adds
occasions.

## Two failures that skip the ladder

**A record whose bytes will not decode** fails identically in fifteen minutes.
`MessageNotDecodableError` goes straight to the dead-letter topic; the set is
the `non_retryable` argument and its default is that one class and nothing
else. Widening it is a decision about the application's own exceptions —
treating every `ValueError` as permanent would dead-letter a validation failure
caused by a dependency that was briefly returning nonsense.

**A record whose own `x-dlq-*` headers cannot be read** also goes straight
there. Where it goes next is a function of an attempt count that cannot be
read, and reading a corrupt count as zero would send it round the ladder again
on every pass, forever, while every log line about it said "first failure". The
destination is derived from the topic name (`RetryPolicy.origin_of`) rather
than from the headers that are the problem.

## The headers

| Header | Meaning |
| --- | --- |
| `x-dlq-origin-topic` | The topic the record was first consumed from. |
| `x-dlq-origin-partition`, `x-dlq-origin-offset` | Its coordinates there, stamped once at the first failure and never updated — so a dead letter points at the record the application actually produced. |
| `x-dlq-attempts` | Handler attempts that have failed, 1-based. Its presence is what marks a record as having failed at all. |
| `x-dlq-first-failed-at` | When this started. Survives every hop. |
| `x-dlq-not-before` | The earliest a consumer may hand the record to a handler. |
| `x-dlq-error` | `Type: message`, truncated to 512 characters. |
| `x-dlq-replays` | How many times an operator has put the record back. |

The application's own headers — a trace context, a schema id, a tenant tag —
are preserved through every hop. The `x-dlq-*` ones are **replaced**, not
appended: Kafka headers are an ordered sequence and duplicate names are legal,
`ConsumedMessage.header` returns the *first* match, so an appended
`x-dlq-attempts: 2` behind an existing `1` would leave the record reading as
attempt 1 forever. It would climb one rung and then circle it, and nothing
about it would look wrong.

The payload and the key are never touched. A dead letter has to be replayable
byte-for-byte, and a router that wrapped the value in an envelope would hand
the handler something it did not publish.

## A handler can say "not yet"

`RetryAfter` (in `src/kafka/base.py`) is the third thing a handler can do,
alongside returning and raising. The runner stops the partition at the record
and seeks back to it, as with a failure, but counts no failure, starts no
exponential backoff, and waits exactly the named delay.

It exists because a retry tier's consumer spends most of its time holding
records that are not due. Counting those as failures would start a backoff
against a partition in perfect health, log `kafka.partition_stalled` every time
the tier did its job, and — since the runner's own backoff is full-jittered and
capped at `KAFKA_RETRY_MAX_DELAY_SECONDS` — poll a record due in fifteen
minutes several hundred times to be told no.

It is not specific to this package: a handler backing off a downstream that
answered `429` with a `Retry-After` header has the same thing to say.

## Running a ladder

```python
# src/consumers/orders.py
from src.dlq import create_dead_letter_runners
from src.kafka import ConsumedMessage, decode_json


async def handle(message: ConsumedMessage) -> None:
    if message.is_tombstone:
        return
    order = decode_json(message)
    ...


origin, retries = create_dead_letter_runners("orders.events", handle)
origin.start()
if retries is not None:  # None when DLQ_RETRY_TIERS is 0
    retries.start()
...
await origin.stop()
await retries.stop()
```

**Start both.** A ladder whose retry consumer is not running looks perfect from
the origin topic — its lag is zero, because every failure is being published
away promptly — while the retry topics fill up and nothing in them is ever
handled.

Two runners because they run different handlers: the origin consumer runs the
application's handler directly and the retry consumer runs it behind the
due-time gate. Two consumer groups because offsets are stored against a group,
and sharing one would make the tiers' lag and the origin topic's the same
number on every dashboard — during an incident, which of the two is behind is
the first question.

One retry runner for all the tiers, not one per tier: tiers are separate topics
and therefore separate partitions, which `ConsumerRunner` already stalls
independently, and the loop wakes at the *soonest* due time across the
partitions it holds. The long tiers cost one cheap poll-and-defer each time a
short one comes due.

**Create the topics first.** On a cluster with auto-creation disabled — most
production clusters — the router's first publish to a tier that does not exist
fails at the moment a record first fails, which is the worst moment for a
second problem. `ladder_for("orders.events").topics` names all five.

## Draining the dead-letter topic

A dead-letter topic that is never drained is a landfill with retention on it:
the records expire, and the incident they were evidence of is closed by the log
rolling over. So the way back is code.

```python
from src.dlq import create_dead_letter_replayer, replay_handler
from src.kafka import ConsumerRunner, create_message_source

drain = ConsumerRunner(
    source=create_message_source(["orders.events.dlt"], group_id="orders.drain"),
    handler=replay_handler(create_dead_letter_replayer()),
    name="drain",
)
```

Drive it once — `consume_once` until the result is empty — **after the bug is
fixed**, and do not leave it running. A permanent replay consumer is a loop:
origin, ladder, dead letter, origin, and round again for as long as whatever
broke the record stays broken. Each lap costs the full ladder in latency and
four topics' worth of writes, and the group's lag stays at zero throughout, so
the one signal an operator would look at says everything is fine.

`DLQ_MAX_REPLAYS` is the guard for when it is left running anyway, because it
will be.

A replayed record keeps its value, its key and every header the application
set. Its attempt count is **reset**, so it gets the whole ladder again rather
than being dead-lettered on its first failure. `x-dlq-replays` is the one
`x-dlq-*` header that survives and grows, because without it each replay's
failure looks like a first failure.

Four records are refused rather than replayed, and each raises
`ReplayNotPossibleError`:

- no envelope — nothing records which topic it was consumed from;
- an unreadable envelope — same, for a different reason;
- a synthesised key (see below) — replaying it would place the record on a
  partition it never came from;
- one already replayed `DLQ_MAX_REPLAYS` times.

`replay_handler` logs a refusal and commits past it rather than re-raising: the
record would be refused identically forever, and the point of a drain is to get
through the ones that can be saved.

## The keyless, valueless record

A record with no key *and* no value is a tombstone for no key, and
`validate_record` refuses to produce one. It cannot come from this codebase's
publisher, only from a foreign producer — but republishing it verbatim would
raise `ValueError` out of `route`, stall the partition on a record that can
never be routed anywhere, and turn one poison record into an outage.

Such a record carries no application data at all: no key, no value, only
headers and a position. So it is given a key derived from where it was, which
loses nothing (a null key carries no information and round-robins) and is
stable across redeliveries, and it is flagged with `x-dlq-synthetic-key` so
`replay` refuses it. Restoring the original null key is exactly what the
publisher will not do.

A null value *with* a key is a real tombstone and passes through untouched.

## Configuration

| Setting | Default | Why |
| --- | --- | --- |
| `DLQ_RETRY_TIERS` | `3` | Rungs before the dead-letter topic. Zero means "dead-letter on the first failure", and there is then no retry runner to start. |
| `DLQ_RETRY_BASE_DELAY_SECONDS` | `5.0` | Tier 1. Short, because everything the first tier fixes never reaches the second. |
| `DLQ_RETRY_MULTIPLIER` | `5.0` | Five rather than two: with a small tier count, doubling barely separates the rungs (5s / 10s / 20s is 35 seconds of total patience) and the point of a ladder is that the last rung outlasts a real outage. |
| `DLQ_RETRY_MAX_DELAY_SECONDS` | `900.0` | Clamp per tier. Also the ceiling on how stale a record handed to a handler can be — check it against whatever the handler assumes about freshness. |
| `DLQ_RETRY_TOPIC_SUFFIX` | `.retry` | An interface, not a preference. Every consumer derives the same names from it, so changing it orphans whatever is already in the old topics. |
| `DLQ_DEAD_LETTER_TOPIC_SUFFIX` | `.dlt` | As above. Must differ from the retry suffix, or a tier and the dead-letter topic collide. |
| `DLQ_RETRY_GROUP_SUFFIX` | `.retry` | Appended to `KAFKA_CONSUMER_GROUP` for the tier consumer. |
| `DLQ_MAX_REPLAYS` | `3` | The common case for a replay is that the outage is over; the second-commonest is that it was not quite over. |

## What to watch

- `dlq.retry_scheduled` — a record moved down a rung. `tier`, `delay`,
  `remaining_attempts`, and the whole envelope.
- `dlq.dead_lettered` — a record reached the end. `reason` is
  `ladder_exhausted`, `non_retryable` or `malformed_envelope`, and
  `age_seconds` is how long it had been failing.
- `dlq.replayed` / `dlq.replay_refused` — a drain in progress.
- `dlq.synthetic_key_assigned` — a foreign producer is writing keyless,
  valueless records.
- `kafka.handler_deferred` — debug level, one line per not-due record. Below
  `warning` deliberately: it is the normal state of a retry tier, and logging
  it as a warning would bury the partition that is genuinely stuck among
  thousands that are merely waiting.

The number worth alerting on is the dead-letter topic's message rate, not the
retry topics' — a record in a tier is a record the system still expects to
handle.

## Tests

- `tests/test_dlq_base.py` — the arithmetic: tier delays, topic names, and what
  the policy refuses.
- `tests/test_dlq_envelope.py` — the headers, and the difference between an
  absent envelope and an unreadable one.
- `tests/test_dlq_router.py` — which topic and which headers, over a recording
  publisher.
- `tests/test_dlq_handler.py` — the wrappers, and the composition order that
  decides where `RetryAfter` and `MalformedEnvelopeError` each end up.
- `tests/test_dlq_replay.py` — the way back, and the four records that cannot
  take it.
- `tests/test_dlq_factory.py` — what settings turn into.
- `tests/test_dlq_end_to_end.py` — the whole arrangement through the in-memory
  broker, with a clock the test winds forward so a 125-second tier costs
  nothing.
- `tests/test_kafka_runner.py` — `TestARecordThatIsNotDueYet` and
  `TestHowLongTheLoopWaits`, for the runner's side of `RetryAfter`.

## Not done here

- **Nothing in this application uses it.** What to consume is an application
  question, and a demonstration consumer started in the lifespan would join two
  consumer groups on every deployment of a repository that consumes nothing.
- **Topic creation.** The tiers have to exist; `ladder_for` names them.
- **Per-key ordering across a detour.** Given up by construction — see above.
  Preserving it would need a per-key hold, which is a different design and a
  much larger one.
- **A dead-letter queue for `OutboxRelay`.** That is a row in a table, not a
  record on a topic, and none of the ladder applies to it: the relay's backoff
  is already computed from the row and what it lacks is a maximum attempt count
  and somewhere to move the row to. Still open.
- **Metrics.** Everything above is log lines. A counter per outcome is a
  Phase 9 item, and `RouteOutcome` is deliberately returned rather than only
  logged so that it has somewhere to attach.
