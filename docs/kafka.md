# Kafka: producing, consuming, and committing offsets by hand

`src/kafka` is a producer, a consumer group and a consume loop that commits
offsets itself. This document is the reasoning; the code has the details.

- `src/kafka/base.py` — records, partitions and the two protocols, free of
  `aiokafka`.
- `src/kafka/producer.py`, `src/kafka/consumer.py` — the driver, and the
  configuration that decides what it guarantees.
- `src/kafka/runner.py` — the policy: poll, handle in order, commit what
  succeeded.
- `src/kafka/memory.py` — an in-process broker, so the contract suite has a
  second implementation and a developer without a cluster can still run this.
- `src/kafka/factory.py` — backend selection and the process-wide publisher.

## The one fact everything follows from

**An offset is a per-partition watermark, not an acknowledgement of a record.**

A queue lets you acknowledge message 5 and leave 4 outstanding. Kafka stores
one number per partition per group — the offset the group reads next — so
committing 6 says 5 *and* 4 are done. There is no way to say otherwise, and
two consequences follow that are silent when you get them wrong.

**A committed offset is `record.offset + 1`.** Committing the record's own
offset replays it after every restart, forever. It looks perfect in a test that
never restarts a consumer, which is most tests. `ConsumedMessage.next_offset`
exists so the `+ 1` is written once.

**Per-record failure isolation is not available.** `OutboxRelay` retries one bad
row and delivers the rest of the batch, and transplanting that shape here would
*drop* records: skipping record 4 and committing through 5 does not retry 4, it
declares it done. So the runner stops a partition at its first failing record
and carries on with the others — per-partition isolation, the finest grain the
storage model offers.

## What a batch does

For a batch holding records 4, 5, 6 of one partition where 5 fails:

1. record 4 is handled, and `5` goes into the commit map;
2. record 5 fails, so the partition stops there and is seeked back to 5;
3. record 6 is not handled at all — it is behind an unresolved record;
4. the commit still happens, so the work done on 4 is never repeated.

The seek in step 2 is what makes the retry *soon*. The consumer's position has
already moved past record 5 inside the client, so without it the next poll
returns record 6 onwards and record 5 comes back only after a restart or a
rebalance — hours later, in a batch whose other records are unrelated.

Partitions other than the failing one are unaffected, which is the whole point
of the grain: one poison record stalls one partition's worth of traffic, not
the topic.

## Head-of-line blocking is the honest outcome

A partition that keeps failing stops, and its lag grows. That is what ordering
costs. Releasing record 6 while 5 is unresolved means the topic is no longer
ordered, and ordering is the reason to have chosen a key in the first place.

Left alone, a poison record retries at the capped backoff interval, visible in
the log (`kafka.partition_stalled`, with `attempts`) and in the group's lag,
rather than silently dropped.

`src/dlq` is the escape — a record that has failed enough times is moved aside
so the partition continues — and it is opt-in per handler, because giving up
ordering is only free when a key's records are independent of one another. See
[docs/dead-letter-queue.md](./dead-letter-queue.md).

## At-least-once, and where the duplicates come from

The commit happens after the handlers, so any crash in between replays the
batch. That is deliberate: the alternative replays nothing and loses it.
**Handlers must be idempotent.** Duplicates arrive from four places:

- a process killed mid-batch;
- a rebalance that reassigns a partition whose commit had not landed;
- a commit that failed (this member had already left the group);
- a cancelled shutdown, which deliberately does not commit — the records were
  handled, but committing on a task that is being torn down is the await most
  likely to be cut, and a redelivered batch is the cheaper failure.

The runner does not try to shrink that window with a commit per record. It
would pay a round trip per record and would not close the window anyway, since
the crash can land between the handler and the commit however small the batch.

## Configuration with reasons

The settings live in `src/config.py`; these are the four that decide what the
system guarantees rather than how fast it is.

| Setting | Value | Why |
| --- | --- | --- |
| `acks` | `all` | The default acknowledges once the *leader* has the record. A leader that dies before its followers replicate takes the record with it, and the producer was already told it succeeded. |
| `enable_idempotence` | `True` | The producer retries internally, and a retry of a *lost acknowledgement* writes twice. Idempotence lets the broker drop the duplicate — within one producer session, and saying nothing about the application publishing twice. |
| `enable_auto_commit` | `False`, not configurable | Auto-commit stores offsets for records the fetcher handed over, on a timer, whether or not they were handled. A crash in that window skips them: at-most-once delivery, arrived at by leaving a default alone. |
| `auto_offset_reset` | `earliest` | The driver's default is `latest`, which for a group with no committed offset means ignoring everything produced before it started. The consumer is healthy, its lag is zero, and the records are gone. |

Two numbers that have to be considered together:
`KAFKA_MAX_RECORDS × KAFKA_HANDLER_TIMEOUT_SECONDS` is the worst case for one
batch, and it must stay well under `KAFKA_MAX_POLL_INTERVAL_MS`. Past that the
broker treats the member as gone and hands its partitions — and its in-flight
records — to somebody else while this process is still working on them.

## Running a consumer

There is no consumer wired into this application. What to consume is an
application question, and a demonstration topic in the lifespan would join a
consumer group on every deployment, which is a real effect on a shared cluster.
This is the whole of a consumer process:

```python
# src/consumers/orders.py
import asyncio

from src.kafka import ConsumedMessage, create_consumer_runner, decode_json
from src.logging_config import configure_logging


async def handle(message: ConsumedMessage) -> None:
    order = decode_json(message)
    ...  # idempotent, please: this record may arrive twice


async def main() -> None:
    configure_logging("INFO")
    runner = create_consumer_runner(["orders.events"], handle, name="orders")
    runner.start()
    try:
        await asyncio.Event().wait()  # until SIGTERM
    finally:
        await runner.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

`runner.stop()` cancels the loop and waits for it to unwind, and the unwinding
is what leaves the consumer group. Returning before that lets the process exit
with a membership the broker keeps believing in until the session times out,
during which the partitions have no owner.

Publishing, from a request handler:

```python
from src.dependencies import MessagePublisherDep
from src.kafka import encode_json


@router.post("/orders")
async def create_order(data: OrderRequest, publisher: MessagePublisherDep) -> ...:
    order = await service.create(data)
    await publisher.publish(
        "orders.events",
        value=encode_json({"id": str(order.id)}),
        key=str(order.id),
    )
```

The key is what decides the partition, so **choosing it is a design decision**:
keying by order id gives per-order ordering, keying by nothing gives none.

Publishing inside a request is at-most-once with respect to the transaction —
the request can commit and then fail to publish. Where that matters, write an
outbox row instead (`src/outbox`) and publish from the relay; the two are
complementary, and the outbox is what makes the publish survive a crash.

## The in-memory broker

`KAFKA_BACKEND=memory` is the default so that `uv run pytest` and
`docker compose up` work without a cluster. It models partitions, key
placement, groups with committed offsets, assignment across members, and
positions that are not offsets. It does not model replication, retention,
compaction, transactions, or the group protocol's timings, and it keeps
everything in one process — so it is for tests and single-process development
only, which it warns about outside `development`/`test`.

## Tests

- `tests/test_kafka_contract.py` — one suite over both backends: round trip,
  key placement, commit-and-resume, redelivery, group splitting, fan-out.
- `tests/test_kafka_end_to_end.py` — the runner driven through a real broker.
- `tests/test_kafka_runner.py` — the policy, over a source that does exactly
  what the test says: which offsets were committed, and when a seek happened.
- `tests/test_kafka_producer.py`, `tests/test_kafka_consumer.py` — what the
  wrappers ask of the driver, which against a real broker is visible only in
  its consequences.
- `tests/test_kafka_memory.py`, `tests/test_kafka_base.py`,
  `tests/test_kafka_codec.py`, `tests/test_kafka_factory.py`.

The Kafka legs skip when nothing is listening on `KAFKA_BOOTSTRAP_SERVERS`, and
CI runs a broker, so they are measured on every pull request. Topics are
created explicitly with two partitions: a cluster's `num.partitions` defaults to
1, and half of what these assert is invisible on a single-partition topic.

## Not done here

- **Anything the dead-letter queue does.** That lives in `src/dlq` and is a
  wrapper around a handler rather than a change to this package;
  [docs/dead-letter-queue.md](./dead-letter-queue.md) has it.
- **Transactions / exactly-once.** The producer is idempotent, which
  deduplicates its own retries and nothing else. Read-process-write with
  `send_offsets_to_transaction` is a different design, and it only reaches
  exactly-once when the whole loop is Kafka-to-Kafka.
- **Schema registry.** The codec here is JSON with two refusals; anything
  stricter is a deployment's own choice.
- **Cross-cluster or per-partition concurrency inside one member.** The way to
  handle partitions in parallel is to run more members, which is what a
  consumer group already gives you.
