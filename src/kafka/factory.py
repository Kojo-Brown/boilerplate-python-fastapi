"""Backend selection and the process-wide publisher.

Nothing outside this module names `KafkaMessagePublisher` or `InMemoryBroker`:
callers depend on the protocols in `base.py`, and configuration decides which
implementation they get. That is the same arrangement `src/idempotency/factory.py`
and `src/distributed_lock/factory.py` use, and it is what lets the whole
package be exercised without a cluster.

The one asymmetry worth pointing at: the in-memory broker is cached
process-wide, because a publisher and a source that do not share a broker share
nothing at all — the records would go into one object and be read from another,
and the failure would look like a topic that is always empty rather than like a
misconfiguration.
"""

from __future__ import annotations

from collections.abc import Sequence
from functools import lru_cache

import structlog

from src.config import Settings, settings
from src.kafka.base import MessagePublisher, MessageSource
from src.kafka.consumer import ConsumerConnectionConfig, KafkaMessageSource
from src.kafka.memory import InMemoryBroker
from src.kafka.producer import KafkaMessagePublisher, ProducerConfig
from src.kafka.runner import ConsumerConfig, ConsumerRunner, MessageHandler

logger = structlog.get_logger(__name__)


@lru_cache(maxsize=1)
def get_memory_broker() -> InMemoryBroker:
    """The one in-process broker, shared by every memory-backed client.

    Call `get_memory_broker.cache_clear()` between tests that want a clean
    topic set; the contract suite builds its own broker instead, which is
    better still because nothing else can reach it.
    """
    return InMemoryBroker(default_partitions=settings.KAFKA_MEMORY_PARTITIONS)


def _warn_if_memory_outside_development(name: str, config: Settings) -> None:
    if name == "memory" and config.ENVIRONMENT not in ("test", "development"):
        logger.warning(
            "kafka.memory_backend_outside_development",
            environment=config.ENVIRONMENT,
            detail=(
                "The in-memory broker keeps records in this process and loses "
                "them on exit; another replica publishes and consumes nothing "
                "in common with this one."
            ),
        )


def create_message_publisher(
    backend: str | None = None, *, config: Settings | None = None
) -> MessagePublisher:
    """Return a new publisher. Not started — the lifespan does that."""
    resolved = config if config is not None else settings
    name = backend if backend is not None else resolved.KAFKA_BACKEND

    if name == "memory":
        _warn_if_memory_outside_development(name, resolved)
        return get_memory_broker().publisher()
    if name == "kafka":
        return KafkaMessagePublisher(
            bootstrap_servers=resolved.KAFKA_BOOTSTRAP_SERVERS,
            config=ProducerConfig(
                client_id=resolved.KAFKA_CLIENT_ID,
                request_timeout_ms=resolved.KAFKA_REQUEST_TIMEOUT_MS,
                linger_ms=resolved.KAFKA_LINGER_MS,
            ),
        )
    # Unreachable through settings — the field is a Literal, so pydantic
    # rejects an unknown name at start-up — but reachable from a direct call,
    # and falling back to the in-memory broker would be a deployment that
    # publishes into a process-local object and never says so.
    raise ValueError(f"Unknown Kafka backend '{name}'. Available: kafka, memory.")


def create_message_source(
    topics: Sequence[str],
    *,
    group_id: str | None = None,
    backend: str | None = None,
    config: Settings | None = None,
) -> MessageSource:
    """Return a new consumer for `topics`. Not started — the runner does that.

    `group_id` defaults to `KAFKA_CONSUMER_GROUP`. Passing one explicitly is
    the normal case for a service with more than one kind of consumer: the
    group is the unit offsets are stored against, so two unrelated consumers
    sharing a group would split the partitions between them and each see half
    the records.
    """
    resolved = config if config is not None else settings
    name = backend if backend is not None else resolved.KAFKA_BACKEND
    group = group_id if group_id is not None else resolved.KAFKA_CONSUMER_GROUP

    if name == "memory":
        _warn_if_memory_outside_development(name, resolved)
        reset = (
            resolved.KAFKA_AUTO_OFFSET_RESET
            if resolved.KAFKA_AUTO_OFFSET_RESET != "none"
            else "earliest"
        )
        return get_memory_broker().source(
            topics=topics, group_id=group, auto_offset_reset=reset
        )
    if name == "kafka":
        return KafkaMessageSource(
            bootstrap_servers=resolved.KAFKA_BOOTSTRAP_SERVERS,
            topics=topics,
            config=ConsumerConnectionConfig(
                group_id=group,
                client_id=resolved.KAFKA_CLIENT_ID,
                auto_offset_reset=resolved.KAFKA_AUTO_OFFSET_RESET,
                max_poll_records=resolved.KAFKA_MAX_RECORDS,
                session_timeout_ms=resolved.KAFKA_SESSION_TIMEOUT_MS,
                heartbeat_interval_ms=resolved.KAFKA_HEARTBEAT_INTERVAL_MS,
                max_poll_interval_ms=resolved.KAFKA_MAX_POLL_INTERVAL_MS,
            ),
        )
    raise ValueError(f"Unknown Kafka backend '{name}'. Available: kafka, memory.")


def consumer_config(config: Settings | None = None) -> ConsumerConfig:
    """The delivery policy from settings, for a runner built anywhere."""
    resolved = config if config is not None else settings
    return ConsumerConfig(
        max_records=resolved.KAFKA_MAX_RECORDS,
        poll_timeout=resolved.KAFKA_POLL_TIMEOUT_SECONDS,
        handler_timeout=resolved.KAFKA_HANDLER_TIMEOUT_SECONDS,
        retry_base_delay=resolved.KAFKA_RETRY_BASE_DELAY_SECONDS,
        retry_max_delay=resolved.KAFKA_RETRY_MAX_DELAY_SECONDS,
        shutdown_timeout=resolved.KAFKA_SHUTDOWN_TIMEOUT_SECONDS,
    )


def create_consumer_runner(
    topics: Sequence[str],
    handler: MessageHandler,
    *,
    name: str = "default",
    group_id: str | None = None,
    backend: str | None = None,
    config: Settings | None = None,
) -> ConsumerRunner:
    """Assemble a source and a runner for `topics` from settings.

    The whole of what a consumer process needs:

        runner = create_consumer_runner(["users.events"], handle)
        runner.start()
        ...
        await runner.stop()

    Deliberately not called anywhere in `src/`. What this service should
    consume is an application question, and a demonstration topic wired into
    the lifespan would be a worked example pretending to be a requirement —
    it would also join a consumer group on every deployment, which is a real
    effect on a shared cluster. `docs/kafka.md` has the entry point to copy.
    """
    return ConsumerRunner(
        source=create_message_source(
            topics, group_id=group_id, backend=backend, config=config
        ),
        handler=handler,
        name=name,
        config=consumer_config(config),
    )


@lru_cache(maxsize=1)
def get_message_publisher() -> MessagePublisher:
    """The process-wide configured publisher.

    Cached because the Kafka publisher owns sockets, a metadata cache and a
    background sender that must not be rebuilt per request, and because the
    in-memory one is only meaningful when every caller shares an instance.
    Call `get_message_publisher.cache_clear()` after changing the backend in a
    test.
    """
    return create_message_publisher()


__all__ = [
    "consumer_config",
    "create_consumer_runner",
    "create_message_publisher",
    "create_message_source",
    "get_memory_broker",
    "get_message_publisher",
]
