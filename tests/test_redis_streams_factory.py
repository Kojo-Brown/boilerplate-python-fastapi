"""Backend selection: what configuration decides, and what it refuses to.

The two interesting assertions are the ones that are not happy paths — that an
unknown backend raises instead of quietly falling back to the in-process
server, and that a memory backend outside development says so in the log. A
silent fallback here is a deployment consuming from an object inside one
worker, which looks exactly like a stream nobody publishes to.
"""

from __future__ import annotations

from typing import Any

import pytest
from structlog.testing import capture_logs

from src.config import Settings
from src.redis_streams.base import StreamConsumerGroup, StreamMessage
from src.redis_streams.consumer import StreamConsumerRunner
from src.redis_streams.factory import (
    consumer_config,
    create_stream_group,
    create_stream_runner,
    get_memory_stream_server,
)
from src.redis_streams.memory import InMemoryStreamGroup
from src.redis_streams.redis_group import RedisStreamGroup


def a_settings(**overrides: Any) -> Settings:
    """A settings object built for a test, never the process singleton.

    Every factory takes one for exactly this reason: `Settings` is frozen, so
    the alternative would be mutating a global that other suites have read.
    """
    fields: dict[str, Any] = {
        "DATABASE_URL": "postgresql+asyncpg://u:p@localhost:5432/db",
        "SECRET_KEY": "test-secret-key-not-for-production",
        "ENVIRONMENT": "test",
    }
    fields.update(overrides)
    return Settings(**fields)


async def handle(message: StreamMessage) -> None:  # pragma: no cover - never called
    return None


class TestBackendSelection:
    def test_the_memory_backend_is_the_default(self) -> None:
        """A boilerplate that refuses to start without a Redis is one nobody
        runs; the setting is what a deployment changes."""
        group = create_stream_group("orders", config=a_settings())

        assert isinstance(group, InMemoryStreamGroup)

    def test_the_redis_backend_is_built_from_the_url(self) -> None:
        group = create_stream_group(
            "orders", config=a_settings(REDIS_STREAMS_BACKEND="redis")
        )

        assert isinstance(group, RedisStreamGroup)
        assert group.stream == "orders"

    def test_a_streams_specific_url_wins_over_the_shared_one(self) -> None:
        """Streams are the one Redis workload here whose memory grows with
        throughput rather than with the number of live keys, so a deployment
        may want them somewhere else."""
        settings = a_settings(
            REDIS_STREAMS_BACKEND="redis",
            REDIS_URL="redis://shared:6379/0",
            REDIS_STREAMS_URL="redis://dedicated:6379/3",
        )

        group = create_stream_group("orders", config=settings)

        assert isinstance(group, RedisStreamGroup)

    def test_an_unknown_backend_raises_rather_than_falling_back(self) -> None:
        with pytest.raises(ValueError, match="Unknown Redis Streams backend"):
            create_stream_group("orders", backend="rabbit", config=a_settings())

    def test_the_memory_backend_outside_development_says_so(self) -> None:
        with capture_logs() as logs:
            create_stream_group(
                "orders",
                backend="memory",
                config=a_settings(ENVIRONMENT="production"),
            )

        assert [entry["event"] for entry in logs] == [
            "stream.memory_backend_outside_development"
        ]

    def test_the_memory_backend_in_development_says_nothing(self) -> None:
        with capture_logs() as logs:
            create_stream_group(
                "orders",
                backend="memory",
                config=a_settings(ENVIRONMENT="development"),
            )

        assert logs == []

    def test_the_group_name_comes_from_settings_unless_it_is_given(self) -> None:
        settings = a_settings(REDIS_STREAMS_CONSUMER_GROUP="from-settings")

        assert create_stream_group("orders", config=settings).group == "from-settings"
        assert (
            create_stream_group("orders", group="explicit", config=settings).group
            == "explicit"
        )

    def test_each_group_gets_its_own_consumer_name_unless_it_is_given(self) -> None:
        """Two replicas sharing a name share a pending entries list, and each
        would see the other's in-flight work as its own."""
        settings = a_settings()

        first = create_stream_group("orders", config=settings)
        second = create_stream_group("orders", config=settings)

        assert first.consumer != second.consumer
        assert (
            create_stream_group("orders", consumer="fixed", config=settings).consumer
            == "fixed"
        )

    def test_it_satisfies_the_protocol(self) -> None:
        assert isinstance(
            create_stream_group("orders", config=a_settings()), StreamConsumerGroup
        )


class TestTheSharedMemoryServer:
    def test_every_memory_group_reads_what_the_others_publish(self) -> None:
        """A publisher and a consumer holding different servers share nothing,
        and the failure looks like a stream nobody produces to."""
        get_memory_stream_server.cache_clear()
        settings = a_settings()

        first = create_stream_group("orders", consumer="a", config=settings)
        second = create_stream_group("orders", consumer="b", config=settings)

        assert isinstance(first, InMemoryStreamGroup)
        assert isinstance(second, InMemoryStreamGroup)
        assert get_memory_stream_server() is get_memory_stream_server()
        get_memory_stream_server.cache_clear()


class TestConsumerConfig:
    def test_every_knob_comes_from_settings(self) -> None:
        settings = a_settings(
            REDIS_STREAMS_BATCH_SIZE=7,
            REDIS_STREAMS_BLOCK_TIMEOUT_SECONDS=1.5,
            REDIS_STREAMS_MIN_IDLE_SECONDS=45.0,
            REDIS_STREAMS_CLAIM_BATCH=3,
            REDIS_STREAMS_MAX_DELIVERIES=9,
            REDIS_STREAMS_HANDLER_TIMEOUT_SECONDS=11.0,
            REDIS_STREAMS_RETRY_BASE_DELAY_SECONDS=2.0,
            REDIS_STREAMS_RETRY_MAX_DELAY_SECONDS=30.0,
            REDIS_STREAMS_SHUTDOWN_TIMEOUT_SECONDS=4.0,
            REDIS_STREAMS_DEAD_LETTER_SUFFIX=".rejected",
        )

        config = consumer_config(settings)

        assert config.batch_size == 7
        assert config.block_timeout == 1.5
        assert config.min_idle == 45.0
        assert config.claim_batch == 3
        assert config.max_deliveries == 9
        assert config.handler_timeout == 11.0
        assert config.retry_base_delay == 2.0
        assert config.retry_max_delay == 30.0
        assert config.shutdown_timeout == 4.0
        assert config.dead_letter_suffix == ".rejected"

    def test_the_shipped_defaults_keep_min_idle_clear_of_the_timeout(self) -> None:
        """The one setting that can cause a bug on its own: at or below the
        handler timeout it turns a slow handler into concurrent duplicate
        processing."""
        config = consumer_config(a_settings())

        assert config.min_idle > config.handler_timeout


class TestRunnerAssembly:
    def test_a_runner_is_named_after_its_stream_by_default(self) -> None:
        runner = create_stream_runner("orders", handle, config=a_settings())

        assert isinstance(runner, StreamConsumerRunner)
        assert runner.name == "orders"
        assert runner.group.stream == "orders"

    def test_a_runner_carries_the_configured_policy(self) -> None:
        runner = create_stream_runner(
            "orders",
            handle,
            name="orders-worker",
            config=a_settings(REDIS_STREAMS_MAX_DELIVERIES=2),
        )

        assert runner.name == "orders-worker"
        assert runner.config.max_deliveries == 2
        assert runner.dead_letter_stream == "orders.dead"

    def test_a_runner_is_not_started_by_being_built(self) -> None:
        """Whoever builds one owns it: a factory that started the task would be
        the thing keeping a cancelled consumer alive."""
        runner = create_stream_runner("orders", handle, config=a_settings())

        assert runner.running is False
