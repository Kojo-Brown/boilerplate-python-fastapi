# Redis Streams: consumer groups, and claiming back what stalled

`src/redis_streams` is a consumer group over Redis Streams and a consume loop
that acknowledges messages itself and claims back the ones another consumer
took and never finished. This document is the reasoning; the code has the
details.

- `src/redis_streams/base.py` — messages, entry ids and the protocol, free of
  `redis`.
- `src/redis_streams/redis_group.py` — the commands, and the four of them whose
  real behaviour decides the design.
- `src/redis_streams/consumer.py` — the policy: claim, read, handle,
  acknowledge.
- `src/redis_streams/memory.py` — an in-process model, so the contract suite
  has a second implementation and a developer without a server can run this.
- `src/redis_streams/factory.py` — backend selection and runner assembly.

## The one fact everything follows from

**A pending entries list acknowledges messages; a Kafka offset does not.**

This is the difference between this package and `src/kafka`, and it is why they
are separate packages rather than two transports behind one port. A Kafka group
stores one number per partition, so "record 5 failed, record 6 succeeded"
cannot be expressed. A Redis Streams group stores a *set*: every message handed
to a consumer sits in the group's pending entries list — the PEL — until it is
acknowledged by id.

Three consequences follow.

**Failure isolation is per message.** A handler that raises leaves its message
unacknowledged, and everything else in the batch continues. There is no
head-of-line blocking, so there is no retry-tier ladder here as there is in
`src/dlq` — a message retries in place, by being claimed back.

**Nothing redelivers a message on its own.** This is the part that surprises
people arriving from Kafka or from a queue. An uncommitted Kafka record comes
back at the next rebalance or restart. A pending entry is *owned*, by name, by
the consumer that read it — and if that process is gone, it stays owned by a
name nobody is running, indefinitely. The group's lag reads zero, every
dashboard is green, and the work is simply not being done. Redelivery is
something another consumer has to ask for, and asking is `XPENDING` plus
`XCLAIM`.

**Redelivery is counted.** The PEL carries a delivery counter per message, so a
poison message is a number rather than a partition whose lag grows.
`REDIS_STREAMS_MAX_DELIVERIES` is what stops it circling the group forever.

## What a cycle does

Every cycle claims first and reads second:

1. `XPENDING ... IDLE <min_idle>` lists up to `claim_batch` entries that have
   been untouched for too long — ids, owners and delivery counts, no payloads.
2. `XCLAIM` takes them, with the same `min_idle` as a *condition* so anything
   whose owner touched it in between is left alone. Each claim increments that
   message's delivery counter.
3. Whatever budget is left in `batch_size` is spent on `XREADGROUP >`, which
   returns messages never delivered to this group.
4. Each message is handled. Success is acknowledged; a failure or a timeout is
   not, and the message stays pending for a later claim.
5. Anything whose delivery count is over the cap skips the handler and is
   copied to `<stream>.dead`, then acknowledged.
6. One `XACK` for the whole cycle.

**Claiming first is the design, not a preference.** With new messages first, a
busy stream starves the stalled ones forever: the read returns a full batch
every time, the scan never gets a turn, and yesterday's crashed consumer's work
is still pending next week. Claiming first bounds recovery at `min_idle` plus
one cycle however busy the stream is, and `claim_batch` bounds the cost in the
other direction, so a backlog of ten thousand stalled messages cannot stop new
ones being read.

**A cycle that claimed something does not then block on an empty stream.** More
stalled work is probably waiting, and parking for the full `block_timeout`
would delay the next scan for nothing.

## `min_idle` is a bet about your own handlers

Claiming a message whose owner is alive and still working on it gets that
message processed twice, concurrently. `min_idle` is what makes that unlikely,
and the number to set it from is not the average handler duration but the
worst — comfortably above `REDIS_STREAMS_HANDLER_TIMEOUT_SECONDS`. The shipped
defaults are 60 seconds against a 30-second handler timeout, and
`tests/test_redis_streams_factory.py` asserts the gap so a later edit to one
cannot silently close it.

Redis makes the race narrow but not impossible: `XCLAIM` re-checks the idle time
server-side, so two consumers scanning at once cannot both take a message. What
it cannot know is whether the original owner is still working. **Handlers must
be idempotent.**

## Four things measured, not assumed

Each of these was driven against a real Redis 7 while writing this package, and
each fails silently rather than loudly.

**`XGROUP DELCONSUMER` discards the consumer's pending entries.** Reading four
messages, leaving them unacknowledged and removing the consumer:

```
pending summary          -> {'pending': 4, 'consumers': [{'name': b'c1', 'pending': 4}]}
XGROUP DELCONSUMER       -> 4
pending after            -> {'pending': 0, 'consumers': []}
XAUTOCLAIM 0-0           -> [b'0-0', [], []]
XREADGROUP >             -> []
```

Four messages, gone: not pending, not claimable, and not returned by a read
either, because the group's last-delivered-id is already past them. The return
value reads like a receipt and is a body count. This is why
`RedisStreamGroup.stop` removes this consumer only when it owes nothing —
tidying up a group on shutdown is otherwise how you lose exactly the messages
that were still in flight. `tests/test_redis_streams_redis.py` drives the raw
command so that a future change making `stop` unconditional has to delete a
test that says what it costs.

**A claim increments the delivery counter; `JUSTID` does not.** That is the
documented behaviour and it is the right way round, but it makes `JUSTID` a
trap for exactly this use: claiming ids without payloads to "check on them
first" freezes the counter, and a poison message whose counter never rises
never reaches its cap. Nothing in this package uses it.

**An entry can be in the PEL and gone from the stream.** `XDEL` and `MAXLEN`
trimming delete entries without consulting any group:

```
XDEL <id>                -> 1
XAUTOCLAIM 0-0 COUNT 2   -> [cursor, [(<other id>, {...})], [<deleted id>]]
```

The third element is the dangling entries, which the server drops from the PEL
as it goes. `XCLAIM` does the same thing silently — it simply omits them — so
`claim` compares what it asked for against what came back and reports the
difference as `missing`. Those messages are gone; the acknowledgement they will
never get is not a leak.

**`BLOCK 0` blocks forever.** Zero is "no timeout", not "do not block", and a
shutdown then waits on a read that only a new message can interrupt. `read`
converts a non-positive block into no `BLOCK` argument at all.

## Trimming is not lag-aware

`REDIS_STREAMS_MAXLEN` caps the stream at publish time with `~`, which trims
whole macro nodes rather than counting to an exact length — bounded, not
precise, and O(1) amortised instead of a latency spike on an unlucky `XADD`.

It does not consult any consumer group. A cap low enough to overtake a slow
consumer deletes messages that consumer has not read, and no error is raised
anywhere: the messages are simply not there. Default is 0, meaning unbounded,
because an unbounded stream is a memory problem you can see coming and a
too-small cap is data loss you cannot.

## Consumer names

`default_consumer_name()` is `host-pid-uuid`, which is stable for a process and
unique across them. Both halves matter and pull opposite ways: stability lets a
process find its own pending entries where it left them, and uniqueness keeps
two replicas from sharing one PEL, where each would see the other's in-flight
work as its own.

The cost is one consumer record per process in `XINFO CONSUMERS`, which on a
rolling deployment would otherwise accumulate for the lifetime of the group.
That is what the empty-PEL branch of `stop()` is for.

## Where a new group starts

`XGROUP CREATE ... $` — a new group reads what arrives after it exists, not the
stream's history. This is the opposite of `src/kafka`'s `earliest` default, so
it is set explicitly rather than inherited from a convention: a first deploy
that replays a month of a retained stream is a worse surprise than one that
starts empty.

`MKSTREAM` is on, which is what makes a consumer that deploys before its
producer legal rather than making deployment order a correctness question.

## Using it

Publishing, from anywhere:

```python
from src.redis_streams.factory import create_stream_group

group = create_stream_group("orders")
await group.start()
await group.publish({"type": b"order.created", "id": b"42"})
```

Consuming, from a worker or the application's lifespan:

```python
from src.redis_streams.base import StreamMessage
from src.redis_streams.factory import create_stream_runner


async def handle(message: StreamMessage) -> None:
    # Idempotent: this message may have been handled before. See `min_idle`.
    ...


runner = create_stream_runner("orders", handle)
runner.start()      # inside a running event loop
...
await runner.stop()  # waits for the loop to unwind and leave the group
```

Nothing in this application consumes or publishes anything: what to put on a
stream is an application question, and a demonstration consumer in the lifespan
would join a group on every deployment. `create_stream_runner` is the seam, and
a runner is not held anywhere by the factory — whoever builds one owns it,
because a runner is a background task and a module-level cache would be the
thing keeping a cancelled consumer alive.

## Choosing between this and `src/kafka`

Not interchangeable, and the choice is not about scale:

| | Redis Streams | Kafka |
| --- | --- | --- |
| Acknowledgement | per message | per partition offset |
| A failed message | left pending, claimed back | stalls its partition |
| Redelivery | only when someone claims | at rebalance or restart |
| Delivery count | in the PEL, per message | nowhere |
| Ordering | per stream, lost on a claim | per partition, kept |
| Retention | until trimmed or acknowledged | time or size, independent of consumers |

Take Redis Streams when the work items are independent and one bad message must
not hold up the others. Take Kafka when order per key is the point, or when
several unrelated consumer groups need to read the same history at their own
pace.

## What is not done

- **`XAUTOCLAIM`.** The `XPENDING` + `XCLAIM` pair costs one extra round trip
  and buys the delivery count *before* the claim, which is what lets a message
  over its cap be dead-lettered without being handed to a handler that has
  already failed on it. It also makes the "could not claim" cases visible;
  `XAUTOCLAIM` reports deleted entries but not skipped ones.
- **Replay from the dead-letter stream.** It is an ordinary stream, so a group
  on it is all that is needed, but which messages deserve replaying is a
  decision this package should not make for you. Compare `src/dlq/replay.py`,
  which has the same shape and a replay counter to stop a loop.
- **Consumer-group lag as a metric.** `XINFO GROUPS` reports it, and
  `pending_summary` shapes a scan for a log line, but nothing here exports
  either.
- **Cross-stream ordering, and ordering across a claim.** A claimed message is
  handled after messages produced behind it. That is free when work items are
  independent and wrong when they are a sequence of edits to one key — for
  which Kafka's per-key partitioning, next door, is the answer.
