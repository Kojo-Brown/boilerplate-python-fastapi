"""The ladder as arithmetic: which topic, which delay, and what it refuses.

Everything here is a pure function of the policy, which is the point of
`base.py` being free of a broker and a clock — the decision that sends a record
to its fourth tier or to the dead-letter topic is one call away from a test.
"""

from __future__ import annotations

import pytest

from src.dlq.base import RetryLadder, RetryPolicy, RetryTier


class TestTierDelays:
    def test_the_first_tier_waits_the_base_delay(self) -> None:
        assert RetryPolicy(base_delay=5.0).tier_delay(1) == 5.0

    def test_each_tier_multiplies_the_last(self) -> None:
        policy = RetryPolicy(base_delay=5.0, multiplier=5.0, max_delay=10_000)

        assert [policy.tier_delay(index) for index in (1, 2, 3)] == [5.0, 25.0, 125.0]

    def test_max_delay_clamps_the_ladder(self) -> None:
        policy = RetryPolicy(base_delay=5.0, multiplier=5.0, max_delay=30.0)

        assert [policy.tier_delay(index) for index in (1, 2, 3)] == [5.0, 25.0, 30.0]

    def test_a_huge_tier_index_clamps_rather_than_overflowing(self) -> None:
        """`base * multiplier ** index` is an int power and raises past ~2**2000.

        A misconfigured tier count should be a long delay clamped by
        `max_delay`, not an `OverflowError` from inside the arithmetic.
        """
        assert RetryPolicy(max_delay=900.0).tier_delay(5_000) == 900.0

    def test_a_multiplier_of_one_is_a_flat_ladder(self) -> None:
        policy = RetryPolicy(base_delay=5.0, multiplier=1.0)

        assert [policy.tier_delay(index) for index in (1, 2, 3)] == [5.0, 5.0, 5.0]

    def test_tier_indexes_are_one_based(self) -> None:
        with pytest.raises(ValueError, match="1-based"):
            RetryPolicy().tier_delay(0)


class TestPolicyValidation:
    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"base_delay": 0}, "base_delay must be positive"),
            ({"base_delay": -1}, "base_delay must be positive"),
            ({"multiplier": 0.5}, "wait less than the last"),
            ({"tiers": -1}, "tiers cannot be negative"),
            ({"max_delay": 1.0}, "max_delay cannot be below base_delay"),
            ({"retry_suffix": ""}, "retry_suffix must not be empty"),
            ({"dead_letter_suffix": ""}, "dead_letter_suffix must not be empty"),
        ],
    )
    def test_it_refuses_a_policy_that_cannot_work(
        self, kwargs: dict[str, object], message: str
    ) -> None:
        with pytest.raises(ValueError, match=message):
            RetryPolicy(**kwargs)  # type: ignore[arg-type]

    def test_the_two_suffixes_must_differ(self) -> None:
        """Otherwise a tier topic and the dead-letter topic collide.

        Not a cosmetic clash: records that still have retries left would be
        published into the topic nothing consumes, and would look dead-lettered
        while their attempt count said they were not.
        """
        with pytest.raises(ValueError, match="must differ"):
            RetryPolicy(retry_suffix=".x", dead_letter_suffix=".x")


class TestLadderNames:
    def test_it_names_every_topic_from_the_origin(self) -> None:
        ladder = RetryPolicy(tiers=2).ladder_for("orders.events")

        assert ladder.topics == (
            "orders.events",
            "orders.events.retry.1",
            "orders.events.retry.2",
            "orders.events.dlt",
        )

    def test_retry_topics_are_what_a_tier_consumer_subscribes_to(self) -> None:
        ladder = RetryPolicy(tiers=2).ladder_for("orders.events")

        assert ladder.retry_topics == (
            "orders.events.retry.1",
            "orders.events.retry.2",
        )

    def test_a_ladder_with_no_tiers_is_origin_and_dead_letter(self) -> None:
        ladder = RetryPolicy(tiers=0).ladder_for("orders.events")

        assert ladder.retry_topics == ()
        assert ladder.max_attempts == 1
        assert ladder.destination(1) is None

    def test_it_refuses_to_build_a_ladder_on_a_ladder(self) -> None:
        """`orders.retry.1.retry.1` is how a topic count grows without bound.

        A record consumed from a tier has to derive its ladder from the origin
        topic in its headers; building one from the topic in hand would give
        every record its own private ladder, one rung deeper each pass.
        """
        policy = RetryPolicy()

        with pytest.raises(ValueError, match="already a retry or dead-letter topic"):
            policy.ladder_for("orders.events.retry.1")
        with pytest.raises(ValueError, match="already a retry or dead-letter topic"):
            policy.ladder_for("orders.events.dlt")

    def test_it_refuses_an_empty_origin_topic(self) -> None:
        with pytest.raises(ValueError, match="must not be empty"):
            RetryPolicy().ladder_for("")


class TestInvertingTheNames:
    """`origin_of` is what is left when the headers are the broken part."""

    @pytest.mark.parametrize(
        ("topic", "origin"),
        [
            ("orders.events", None),
            ("orders.events.retry.1", "orders.events"),
            ("orders.events.retry.12", "orders.events"),
            ("orders.events.dlt", "orders.events"),
            # A topic whose own name contains the suffix but is not a tier: the
            # segment after the marker has to be a number, or `orders.retry.log`
            # would be read as tier "log" of a topic called `orders`.
            ("orders.retry.log", None),
            ("orders.events.retry.", None),
        ],
    )
    def test_it_recovers_the_origin_from_the_topic_name(
        self, topic: str, origin: str | None
    ) -> None:
        assert RetryPolicy().origin_of(topic) == origin

    def test_a_ladder_topic_round_trips_through_the_policy(self) -> None:
        policy = RetryPolicy(tiers=3)
        ladder = policy.ladder_for("orders.events")

        for topic in ladder.retry_topics + (ladder.dead_letter_topic,):
            assert policy.origin_of(topic) == "orders.events"
            assert policy.is_ladder_topic(topic)

        assert not policy.is_ladder_topic("orders.events")


class TestDestination:
    def test_each_failure_moves_one_rung_down(self) -> None:
        ladder = RetryPolicy(tiers=3).ladder_for("orders.events")

        assert [ladder.destination(n) for n in (1, 2, 3)] == list(ladder.tiers)

    def test_the_ladder_runs_out_after_max_attempts(self) -> None:
        ladder = RetryPolicy(tiers=3).ladder_for("orders.events")

        assert ladder.max_attempts == 4
        assert ladder.destination(4) is None
        assert ladder.destination(99) is None

    def test_attempts_are_one_based_and_count_the_failure_just_seen(self) -> None:
        ladder = RetryPolicy().ladder_for("orders.events")

        with pytest.raises(ValueError, match="1-based"):
            ladder.destination(0)


class TestLadderReporting:
    def test_total_delay_is_how_long_a_record_can_be_retried_for(self) -> None:
        ladder = RetryPolicy(
            base_delay=5.0, multiplier=5.0, tiers=3, max_delay=10_000
        ).ladder_for("orders.events")

        assert ladder.total_delay == 155.0

    def test_tier_for_topic_finds_the_rung_a_record_arrived_on(self) -> None:
        ladder = RetryPolicy(tiers=2).ladder_for("orders.events")

        assert ladder.tier_for_topic("orders.events.retry.2") == RetryTier(
            index=2, topic="orders.events.retry.2", delay=25.0
        )
        assert ladder.tier_for_topic("orders.events") is None
        assert ladder.tier_for_topic("orders.events.dlt") is None

    def test_a_ladder_is_a_value(self) -> None:
        """Frozen, so nothing can edit the delays of a ladder in flight."""
        ladder = RetryPolicy().ladder_for("orders.events")

        assert isinstance(ladder, RetryLadder)
        with pytest.raises((AttributeError, TypeError)):
            ladder.origin_topic = "other"  # type: ignore[misc]
