"""A dead-letter queue with an exponential-backoff retry ladder.

`src/kafka` stops a partition at its first failing record and keeps it stopped.
That is the correct default — it is what ordering costs — but it means one
poison record holds up every record behind it in its partition, indefinitely.
This package is the escape: a record that fails is *moved*, and the partition
carries on.

Five modules, and the split follows what each one is allowed to know:

- `base.py` — the ladder as values. Tier delays, topic names, and the
  arithmetic of which rung a record goes to next. No broker, no clock.
- `envelope.py` — the `x-dlq-*` headers a record carries between tiers, and
  the parsing that decides whether a record is on its first pass, its fourth,
  or is unreadable.
- `router.py` — the only part that publishes. Where a failed record goes, and
  the rule that it is never dropped.
- `handler.py` — two wrappers that compose a plain `MessageHandler` into one
  the ladder can drive.
- `replay.py` — putting a dead letter back, which is the only reason to keep
  one.
- `factory.py` — settings, and the two runners it takes to run a ladder.

## The shape

    orders.events            the application's topic
    orders.events.retry.1    5s
    orders.events.retry.2    25s
    orders.events.retry.3    125s
    orders.events.dlt        terminal

A failure on `orders.events` publishes the record to `.retry.1` and commits
past it. A separate consumer reads the three tiers, waits until each record is
due, and runs the same handler. A failure on the last tier goes to `.dlt`.

## The three decisions worth knowing before using it

**A delay is a topic, not a sleep.** Sleeping inside a consumer stalls the
partition — reintroducing the problem this package exists to solve — and a
sleep longer than `max.poll.interval.ms` gets the member evicted and its
partitions reassigned mid-batch. See `base.py`.

**The tier delay is fixed and never jittered**, which is the opposite of every
other backoff here and is the property that makes a tier consumer correct: it
stalls at the first record that is not due, and that is only complete if due
time rises with offset. See `base.py`.

**Per-key ordering is given up for records that fail.** A record that detours
through a five-second tier is handled after records that were produced behind
it. For independent records — an email, a webhook, an index update — that is
free. For a key whose records are a sequence of edits it is last-write-wins
with the wrong last write, and the right answer there is to not use a ladder
and to accept the stall. See `handler.py`.

## What this package does not do

- **It does not deduplicate.** Delivery is at-least-once, as it was before, and
  the ladder adds occasions rather than requirements: a crash between the
  routing publish and the origin commit puts the record in the tier twice.
  Handlers must be idempotent, which was already true.
- **It does not create topics.** On a cluster with auto-creation disabled the
  tiers have to exist before the first failure; `factory.ladder_for` names
  them.
- **It does not schedule a replay.** Draining a dead-letter topic is an act,
  and `replay.py` says why it should not be a running consumer.
- **It is not wired into this application.** Nothing here consumes anything,
  for the same reason `src/kafka` does not: what to consume is an application
  question. `docs/dead-letter-queue.md` has the entry point to copy.

See `docs/dead-letter-queue.md`.
"""

from src.dlq.base import (
    DeadLetterError,
    MalformedEnvelopeError,
    ReplayNotPossibleError,
    RetryLadder,
    RetryPolicy,
    RetryTier,
)
from src.dlq.envelope import DeadLetterEnvelope
from src.dlq.factory import (
    create_dead_letter_replayer,
    create_dead_letter_router,
    create_dead_letter_runners,
    ladder_for,
    retry_policy,
)
from src.dlq.handler import retry_tier_handler, with_dead_letter, with_due_time
from src.dlq.replay import DeadLetterReplayer, ReplayOutcome, replay_handler
from src.dlq.router import DeadLetterRouter, RouteOutcome

__all__ = [
    "DeadLetterEnvelope",
    "DeadLetterError",
    "DeadLetterReplayer",
    "DeadLetterRouter",
    "MalformedEnvelopeError",
    "ReplayNotPossibleError",
    "ReplayOutcome",
    "RetryLadder",
    "RetryPolicy",
    "RetryTier",
    "RouteOutcome",
    "create_dead_letter_replayer",
    "create_dead_letter_router",
    "create_dead_letter_runners",
    "ladder_for",
    "replay_handler",
    "retry_policy",
    "retry_tier_handler",
    "with_dead_letter",
    "with_due_time",
]
