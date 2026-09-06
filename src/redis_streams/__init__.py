"""Redis Streams consumer groups, with the stalled messages claimed back.

Named `redis_streams` rather than `streams` because `src/streaming` already
exists and is about HTTP response bodies. Nothing here is related to it.

The package is the same three layers as `src/kafka`: contracts in `base.py`
that name no client library, one real implementation over `redis.asyncio`, one
in-process model for tests and local runs, and the consume policy in
`consumer.py`, which imports neither implementation.

Read `docs/redis-streams.md` first if you are choosing between this and
`src/kafka` — they are not interchangeable, and the difference is the one in
`base.py`: a consumer group's pending entries list acknowledges *messages*,
where a Kafka group commits an *offset per partition*.
"""

from src.redis_streams.base import (
    ClaimedMessage,
    PendingEntry,
    StreamConsumerGroup,
    StreamEntryId,
    StreamError,
    StreamGroupError,
    StreamLifecycleError,
    StreamMessage,
    StreamPublishError,
    StreamUnavailableError,
    default_consumer_name,
)
from src.redis_streams.consumer import (
    ConsumeResult,
    StreamConsumerConfig,
    StreamConsumerRunner,
    StreamHandler,
)
from src.redis_streams.factory import (
    create_stream_group,
    create_stream_runner,
    get_memory_stream_server,
)
from src.redis_streams.memory import InMemoryStreamGroup, InMemoryStreamServer
from src.redis_streams.redis_group import RedisStreamGroup

__all__ = [
    "ClaimedMessage",
    "ConsumeResult",
    "InMemoryStreamGroup",
    "InMemoryStreamServer",
    "PendingEntry",
    "RedisStreamGroup",
    "StreamConsumerConfig",
    "StreamConsumerGroup",
    "StreamConsumerRunner",
    "StreamEntryId",
    "StreamError",
    "StreamGroupError",
    "StreamHandler",
    "StreamLifecycleError",
    "StreamMessage",
    "StreamPublishError",
    "StreamUnavailableError",
    "create_stream_group",
    "create_stream_runner",
    "default_consumer_name",
    "get_memory_stream_server",
]
