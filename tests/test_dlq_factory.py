"""What settings turn into: a ladder's shape, and the two runners that walk it.

The one assertion worth reading is `TestRunnerAssembly` — the two runners share
a router, and they do not share a consumer group. Sharing the group would make
the retry topics' lag indistinguishable from the origin topic's on every
dashboard, and during an incident which of the two is behind is the first
question.
"""

from __future__ import annotations

from typing import Any

import pytest

from src.config import Settings
from src.dlq.base import RetryPolicy
from src.dlq.factory import (
    create_dead_letter_replayer,
    create_dead_letter_router,
    create_dead_letter_runners,
    ladder_for,
    retry_policy,
)
from src.dlq.router import DeadLetterRouter
from src.kafka.base import ConsumedMessage
from src.kafka.factory import get_memory_broker, get_message_publisher
from src.kafka.memory import InMemoryMessageSource
from src.kafka.runner import ConsumerRunner


def a_settings(**overrides: Any) -> Settings:
    """A settings object built for a test, never the process singleton."""
    fields: dict[str, Any] = {
        "DATABASE_URL": "postgresql+asyncpg://u:p@localhost:5432/db",
        "SECRET_KEY": "test-secret-key-not-for-production",
        "ENVIRONMENT": "test",
    }
    fields.update(overrides)
    return Settings(**fields)


async def handle(message: ConsumedMessage) -> None:  # pragma: no cover - never called
    return None


@pytest.fixture(autouse=True)
def _fresh_broker() -> Any:
    """Both caches are process-wide; a test that builds one must not leak it."""
    get_memory_broker.cache_clear()
    get_message_publisher.cache_clear()
    yield
    get_memory_broker.cache_clear()
    get_message_publisher.cache_clear()


class TestThePolicyFromSettings:
    def test_it_reads_every_knob(self) -> None:
        policy = retry_policy(
            a_settings(
                DLQ_RETRY_TIERS=4,
                DLQ_RETRY_BASE_DELAY_SECONDS=2.0,
                DLQ_RETRY_MULTIPLIER=3.0,
                DLQ_RETRY_MAX_DELAY_SECONDS=100.0,
                DLQ_RETRY_TOPIC_SUFFIX=".delay",
                DLQ_DEAD_LETTER_TOPIC_SUFFIX=".dead",
            )
        )

        assert policy == RetryPolicy(
            base_delay=2.0,
            multiplier=3.0,
            tiers=4,
            max_delay=100.0,
            retry_suffix=".delay",
            dead_letter_suffix=".dead",
        )

    def test_the_defaults_are_a_working_ladder(self) -> None:
        ladder = ladder_for("orders.events", config=a_settings())

        assert ladder.topics == (
            "orders.events",
            "orders.events.retry.1",
            "orders.events.retry.2",
            "orders.events.retry.3",
            "orders.events.dlt",
        )
        assert [tier.delay for tier in ladder.tiers] == [5.0, 25.0, 125.0]

    def test_a_setting_that_cannot_work_fails_where_it_is_read(self) -> None:
        """`RetryPolicy.__post_init__` is the gate, and the factory is what
        walks a deployment into it — better here than at the first failure."""
        with pytest.raises(ValueError, match="must differ"):
            retry_policy(
                a_settings(
                    DLQ_RETRY_TOPIC_SUFFIX=".x", DLQ_DEAD_LETTER_TOPIC_SUFFIX=".x"
                )
            )

    def test_ladder_for_names_the_topics_to_create_before_deploying(self) -> None:
        """On a cluster with auto-creation off, the router's first publish to a
        missing tier fails at the moment a record first fails — the worst
        moment for a second problem."""
        assert ladder_for("orders.events", config=a_settings(DLQ_RETRY_TIERS=1)).topics


class TestRouterAndReplayerAssembly:
    def test_the_router_uses_the_process_publisher_by_default(self) -> None:
        router = create_dead_letter_router(config=a_settings())

        assert isinstance(router, DeadLetterRouter)
        assert router.policy == retry_policy(a_settings())

    def test_the_replayer_reads_its_limit_from_settings(self) -> None:
        replayer = create_dead_letter_replayer(config=a_settings(DLQ_MAX_REPLAYS=7))

        assert replayer.max_replays == 7


class TestRunnerAssembly:
    def test_it_returns_an_origin_runner_and_a_retry_runner(self) -> None:
        origin, retries = create_dead_letter_runners(
            "orders.events", handle, config=a_settings()
        )

        assert isinstance(origin, ConsumerRunner)
        assert isinstance(retries, ConsumerRunner)

    def test_the_origin_runner_reads_only_the_origin_topic(self) -> None:
        origin, _ = create_dead_letter_runners(
            "orders.events", handle, config=a_settings()
        )
        source = origin.source

        assert isinstance(source, InMemoryMessageSource)
        assert source.topics == ("orders.events",)

    def test_the_retry_runner_reads_every_tier_and_no_more(self) -> None:
        """One consumer over all the tiers: they are separate topics and
        therefore separate partitions, which the runner already stalls
        independently."""
        _, retries = create_dead_letter_runners(
            "orders.events", handle, config=a_settings(DLQ_RETRY_TIERS=3)
        )
        assert retries is not None
        source = retries.source

        assert isinstance(source, InMemoryMessageSource)
        assert source.topics == (
            "orders.events.retry.1",
            "orders.events.retry.2",
            "orders.events.retry.3",
        )

    def test_the_two_runners_do_not_share_a_consumer_group(self) -> None:
        """Sharing one would make the tiers' lag and the origin topic's the
        same number on every dashboard."""
        origin, retries = create_dead_letter_runners(
            "orders.events", handle, config=a_settings(KAFKA_CONSUMER_GROUP="app")
        )
        assert retries is not None
        origin_source = origin.source
        retry_source = retries.source

        assert isinstance(origin_source, InMemoryMessageSource)
        assert isinstance(retry_source, InMemoryMessageSource)
        assert origin_source.group_id == "app"
        assert retry_source.group_id == "app.retry"

    def test_the_runners_are_named_apart(self) -> None:
        origin, retries = create_dead_letter_runners(
            "orders.events", handle, name="orders", config=a_settings()
        )
        assert retries is not None

        assert (origin.name, retries.name) == ("orders", "orders-retry")

    def test_a_policy_with_no_tiers_has_no_retry_runner(self) -> None:
        """`DLQ_RETRY_TIERS=0` means "dead-letter on the first failure", and a
        consumer subscribed to no topics is a background task that polls
        forever and reports healthy."""
        origin, retries = create_dead_letter_runners(
            "orders.events", handle, config=a_settings(DLQ_RETRY_TIERS=0)
        )

        assert isinstance(origin, ConsumerRunner)
        assert retries is None

    def test_an_injected_router_is_the_one_both_runners_use(self) -> None:
        """One router, so the ladder's arithmetic has one implementation in the
        process however many rungs a record has already climbed."""
        router = create_dead_letter_router(config=a_settings())

        origin, retries = create_dead_letter_runners(
            "orders.events", handle, config=a_settings(), router=router
        )

        assert isinstance(origin, ConsumerRunner)
        assert retries is not None

    def test_it_refuses_to_build_a_ladder_on_a_ladder_topic(self) -> None:
        with pytest.raises(ValueError, match="already a retry or dead-letter topic"):
            create_dead_letter_runners(
                "orders.events.retry.1", handle, config=a_settings()
            )
