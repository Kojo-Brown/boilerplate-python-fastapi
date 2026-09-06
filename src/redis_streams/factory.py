"""Backend selection, and the assembly of a runner from settings.

Nothing outside this module names `RedisStreamGroup` or `InMemoryStreamServer`:
callers depend on the protocol in `base.py` and configuration decides the
implementation — the arrangement `src/kafka/factory.py`,
`src/idempotency/factory.py` and `src/distributed_lock/factory.py` all use.

One asymmetry worth pointing at, and it is the same one the Kafka factory has:
the in-memory server is cached process-wide, because a publisher and a consumer
that do not share one share nothing at all. The messages would go into one
object and be read from another, and the failure would look like a stream that
is permanently empty rather than like a misconfiguration.
"""

from __future__ import annotations

from functools import lru_cache

import structlog

from src.config import Settings, settings
from src.redis_streams.base import StreamConsumerGroup, default_consumer_name
from src.redis_streams.consumer import (
    StreamConsumerConfig,
    StreamConsumerRunner,
    StreamHandler,
)
from src.redis_streams.memory import InMemoryStreamServer
from src.redis_streams.redis_group import RedisStreamGroup

logger = structlog.get_logger(__name__)


@lru_cache(maxsize=1)
def get_memory_stream_server() -> InMemoryStreamServer:
    """The one in-process server, shared by every memory-backed group.

    Call `get_memory_stream_server.cache_clear()` between tests that want empty
    streams; the contract suite builds its own server instead, which is better
    still because nothing else can reach it.
    """
    return InMemoryStreamServer()


def _warn_if_memory_outside_development(name: str, config: Settings) -> None:
    if name == "memory" and config.ENVIRONMENT not in ("test", "development"):
        logger.warning(
            "stream.memory_backend_outside_development",
            environment=config.ENVIRONMENT,
            detail=(
                "The in-memory server keeps messages in this process and loses "
                "them on exit; another replica publishes and consumes nothing "
                "in common with this one."
            ),
        )


def consumer_config(config: Settings | None = None) -> StreamConsumerConfig:
    """The configured delivery policy, as one value.

    Separate from `create_stream_runner` so a caller assembling a runner by
    hand — a worker process with its own handler, say — starts from the
    deployment's policy rather than from the dataclass defaults.
    """
    resolved = config if config is not None else settings
    return StreamConsumerConfig(
        batch_size=resolved.REDIS_STREAMS_BATCH_SIZE,
        block_timeout=resolved.REDIS_STREAMS_BLOCK_TIMEOUT_SECONDS,
        min_idle=resolved.REDIS_STREAMS_MIN_IDLE_SECONDS,
        claim_batch=resolved.REDIS_STREAMS_CLAIM_BATCH,
        max_deliveries=resolved.REDIS_STREAMS_MAX_DELIVERIES,
        handler_timeout=resolved.REDIS_STREAMS_HANDLER_TIMEOUT_SECONDS,
        retry_base_delay=resolved.REDIS_STREAMS_RETRY_BASE_DELAY_SECONDS,
        retry_max_delay=resolved.REDIS_STREAMS_RETRY_MAX_DELAY_SECONDS,
        shutdown_timeout=resolved.REDIS_STREAMS_SHUTDOWN_TIMEOUT_SECONDS,
        dead_letter_suffix=resolved.REDIS_STREAMS_DEAD_LETTER_SUFFIX,
    )


def create_stream_group(
    stream: str,
    *,
    group: str | None = None,
    consumer: str | None = None,
    backend: str | None = None,
    config: Settings | None = None,
) -> StreamConsumerGroup:
    """Return a new consumer group for `stream`. Not started — the runner does that.

    `group` defaults to `REDIS_STREAMS_CONSUMER_GROUP`. Passing one explicitly
    is the normal case for a service with more than one kind of consumer: the
    group is the identity the pending entries list belongs to, so two unrelated
    consumers sharing a group would split the messages between them and each
    see half.
    """
    resolved = config if config is not None else settings
    name = backend if backend is not None else resolved.REDIS_STREAMS_BACKEND
    group_name = group if group is not None else resolved.REDIS_STREAMS_CONSUMER_GROUP
    member = consumer if consumer is not None else default_consumer_name()

    if name == "memory":
        _warn_if_memory_outside_development(name, resolved)
        return get_memory_stream_server().group(
            stream=stream,
            group=group_name,
            consumer=member,
            maxlen=resolved.REDIS_STREAMS_MAXLEN,
        )
    if name == "redis":
        return RedisStreamGroup.from_url(
            resolved.REDIS_STREAMS_URL or resolved.REDIS_URL,
            stream=stream,
            group=group_name,
            consumer=member,
            maxlen=resolved.REDIS_STREAMS_MAXLEN,
        )
    # Unreachable through settings — the field is a Literal, so pydantic
    # rejects an unknown name at start-up — but reachable from a direct call,
    # and falling back to the in-process server would be a deployment that
    # consumes from an object nothing else can see and never says so.
    raise ValueError(
        f"Unknown Redis Streams backend '{name}'. Available: redis, memory."
    )


def create_stream_runner(
    stream: str,
    handler: StreamHandler,
    *,
    group: str | None = None,
    consumer: str | None = None,
    name: str | None = None,
    backend: str | None = None,
    config: Settings | None = None,
) -> StreamConsumerRunner:
    """Assemble a runner for `stream` from the configured policy.

    Not started, and not held anywhere: whoever builds one owns it, because a
    runner is a background task and this module would otherwise be the thing
    keeping a cancelled consumer alive. The application's lifespan is the
    ordinary owner; see `docs/redis-streams.md`.
    """
    resolved = config if config is not None else settings
    return StreamConsumerRunner(
        group=create_stream_group(
            stream,
            group=group,
            consumer=consumer,
            backend=backend,
            config=resolved,
        ),
        handler=handler,
        name=name if name is not None else stream,
        config=consumer_config(resolved),
    )


__all__ = [
    "consumer_config",
    "create_stream_group",
    "create_stream_runner",
    "get_memory_stream_server",
]
