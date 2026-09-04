"""Kafka: a producer, a consumer group, and offsets committed by hand.

Four modules, and the split is the point:

- `base.py` — the contracts, free of `aiokafka`. Records, partitions, the two
  protocols, and the one fact everything else follows from: an offset is a
  per-partition watermark, not an acknowledgement of a record.
- `producer.py` / `consumer.py` — the driver. Configuration with reasons, and
  the translation between `aiokafka`'s objects and this package's.
- `runner.py` — the policy. Poll, handle in partition order, commit what
  succeeded, stop a partition at its first failure. No `aiokafka` import, which
  is what makes it testable without a broker.
- `memory.py` — an in-process broker modelling partitions, groups, committed
  offsets and assignment, so the contract suite has a second implementation and
  a developer without a cluster can still run the application.

## What this package guarantees

At-least-once, and it is deliberate: the commit happens after the handlers, so
a crash in between replays the batch rather than losing it. Handlers must be
idempotent. `enable_auto_commit` is off and is not a setting — see the module
docstring in `consumer.py` for why leaving it on is at-most-once delivery
arrived at by leaving a default alone.

## A poison record, and the escape from it

Left alone, a poison record retries forever at the capped backoff interval and
its partition does not advance while it does. That is the honest cost of
ordering. `src/dlq` is the escape — wrap a handler in `with_dead_letter` and a
failure is published to a retry topic so this loop can commit past it — and it
is opt-in per handler, because the stall is the right behaviour for a topic
whose records are a sequence of edits to one key.

## What it does not do

Transactions (the producer is idempotent, which deduplicates its own retries
and nothing else), schema registry integration, and any consumer wired into
this application — `docs/kafka.md` has the entry point, and
`create_consumer_runner` is what it calls.

See `docs/kafka.md`.
"""

from src.kafka.base import (
    ConsumedMessage,
    ConsumerError,
    Headers,
    LifecycleError,
    MessageNotDecodableError,
    MessageNotSerializableError,
    MessagePublisher,
    MessageSource,
    MessagingError,
    Partition,
    PublishedMessage,
    PublishError,
    RetryAfter,
)
from src.kafka.codec import decode_json, encode_json
from src.kafka.consumer import ConsumerConnectionConfig, KafkaMessageSource
from src.kafka.factory import (
    create_consumer_runner,
    create_message_publisher,
    create_message_source,
    get_message_publisher,
)
from src.kafka.memory import InMemoryBroker
from src.kafka.producer import KafkaMessagePublisher, ProducerConfig
from src.kafka.runner import (
    ConsumerConfig,
    ConsumeResult,
    ConsumerRunner,
    MessageHandler,
)

__all__ = [
    "ConsumeResult",
    "ConsumedMessage",
    "ConsumerConfig",
    "ConsumerConnectionConfig",
    "ConsumerError",
    "ConsumerRunner",
    "Headers",
    "InMemoryBroker",
    "KafkaMessagePublisher",
    "KafkaMessageSource",
    "LifecycleError",
    "MessageHandler",
    "MessageNotDecodableError",
    "MessageNotSerializableError",
    "MessagePublisher",
    "MessageSource",
    "MessagingError",
    "Partition",
    "ProducerConfig",
    "PublishError",
    "PublishedMessage",
    "RetryAfter",
    "create_consumer_runner",
    "create_message_publisher",
    "create_message_source",
    "decode_json",
    "encode_json",
    "get_message_publisher",
]
