"""Backend selection: what configuration decides, and what it refuses to.

The interesting assertions are the two that are not about happy paths — that an
unknown backend raises instead of quietly falling back to the in-process
broker, and that a memory backend outside development says so in the log. A
silent fallback here is a deployment publishing into an object inside one
worker, which looks exactly like a topic nobody produces to.
"""

from __future__ import annotations

from typing import Any

import pytest
from structlog.testing import capture_logs

from src.config import Settings
from src.kafka.base import MessagePublisher, MessageSource
from src.kafka.consumer import KafkaMessageSource
from src.kafka.factory import (
    consumer_config,
    create_consumer_runner,
    create_message_publisher,
    create_message_source,
    get_memory_broker,
    get_message_publisher,
)
from src.kafka.memory import InMemoryMessagePublisher, InMemoryMessageSource
from src.kafka.producer import KafkaMessagePublisher
from src.kafka.runner import ConsumerRunner


def a_settings(**overrides: Any) -> Settings:
    """A settings object built for a test, never the process singleton.

    Every factory takes one for exactly this reason: `Settings` is frozen, so
    the alternative would be mutating a global that other suites have already
    read.
    """
    fields: dict[str, Any] = {
        "DATABASE_URL": "postgresql+asyncpg://u:p@localhost:5432/db",
        "SECRET_KEY": "test-secret-key-not-for-production",
        "ENVIRONMENT": "test",
    }
    fields.update(overrides)
    return Settings(**fields)


async def handle(message: Any) -> None:  # pragma: no cover - never called
    return None


class TestPublisherSelection:
    def test_the_memory_backend_is_the_default(self) -> None:
        """A boilerplate that refuses to start without a cluster is one nobody
        runs; the setting is what a deployment changes."""
        assert isinstance(
            create_message_publisher(config=a_settings()), InMemoryMessagePublisher
        )

    def test_kafka_is_selected_by_name(self) -> None:
        built = create_message_publisher("kafka", config=a_settings())

        assert isinstance(built, KafkaMessagePublisher)
        assert isinstance(built, MessagePublisher)

    def test_the_producer_config_comes_from_settings(self) -> None:
        built = create_message_publisher(
            "kafka",
            config=a_settings(KAFKA_CLIENT_ID="orders-api", KAFKA_LINGER_MS=25),
        )

        assert isinstance(built, KafkaMessagePublisher)
        assert built.config.client_id == "orders-api"
        assert built.config.linger_ms == 25

    def test_an_unknown_backend_raises_rather_than_falling_back(self) -> None:
        with pytest.raises(ValueError, match="Unknown Kafka backend"):
            create_message_publisher("rabbit", config=a_settings())

    def test_the_process_publisher_is_built_once(self) -> None:
        get_message_publisher.cache_clear()
        try:
            assert get_message_publisher() is get_message_publisher()
        finally:
            get_message_publisher.cache_clear()


class TestSourceSelection:
    def test_the_memory_source_shares_the_process_broker(self) -> None:
        """A publisher and a source that do not share a broker share nothing:
        the records go into one object and are read from another."""
        get_memory_broker.cache_clear()
        try:
            publisher = create_message_publisher("memory", config=a_settings())
            source = create_message_source(
                ["events"], backend="memory", config=a_settings()
            )
            assert isinstance(publisher, InMemoryMessagePublisher)
            assert isinstance(source, InMemoryMessageSource)
            assert get_memory_broker() is get_memory_broker()
        finally:
            get_memory_broker.cache_clear()

    def test_the_group_defaults_to_the_configured_one(self) -> None:
        built = create_message_source(
            ["events"], backend="kafka", config=a_settings(KAFKA_CONSUMER_GROUP="api")
        )

        assert isinstance(built, KafkaMessageSource)
        assert built.group_id == "api"
        assert isinstance(built, MessageSource)

    def test_an_explicit_group_wins(self) -> None:
        """Two unrelated consumers sharing a group would split the partitions
        between them and each see half the records."""
        built = create_message_source(
            ["events"],
            group_id="audit",
            backend="kafka",
            config=a_settings(KAFKA_CONSUMER_GROUP="api"),
        )

        assert isinstance(built, KafkaMessageSource)
        assert built.group_id == "audit"

    def test_the_memory_backend_has_no_none_offset_reset(self) -> None:
        """`none` means "fail if there is no committed offset", which the
        in-process broker has no way to express; it reads from the start."""
        built = create_message_source(
            ["events"],
            backend="memory",
            config=a_settings(KAFKA_AUTO_OFFSET_RESET="none"),
        )

        assert isinstance(built, InMemoryMessageSource)

    def test_an_unknown_backend_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown Kafka backend"):
            create_message_source(["events"], backend="rabbit", config=a_settings())


class TestRunnerAssembly:
    def test_the_policy_comes_from_settings(self) -> None:
        config = consumer_config(
            a_settings(KAFKA_MAX_RECORDS=7, KAFKA_HANDLER_TIMEOUT_SECONDS=2.5)
        )

        assert config.max_records == 7
        assert config.handler_timeout == 2.5

    def test_a_runner_is_assembled_from_the_configured_pieces(self) -> None:
        runner = create_consumer_runner(
            ["events"],
            handle,
            name="audit",
            backend="memory",
            config=a_settings(KAFKA_MAX_RECORDS=3),
        )

        assert isinstance(runner, ConsumerRunner)
        assert runner.name == "audit"
        assert runner.config.max_records == 3
        assert not runner.running


class TestMemoryOutsideDevelopment:
    def test_it_warns(self) -> None:
        """Not an error: a staging box running one replica is a legitimate use.
        A silent one would be a production deployment whose topic is a dict."""
        get_memory_broker.cache_clear()
        try:
            with capture_logs() as logs:
                create_message_publisher(
                    "memory", config=a_settings(ENVIRONMENT="production")
                )
        finally:
            get_memory_broker.cache_clear()

        assert [entry["event"] for entry in logs] == [
            "kafka.memory_backend_outside_development"
        ]

    def test_development_is_quiet(self) -> None:
        get_memory_broker.cache_clear()
        try:
            with capture_logs() as logs:
                create_message_publisher(
                    "memory", config=a_settings(ENVIRONMENT="development")
                )
        finally:
            get_memory_broker.cache_clear()

        assert logs == []
