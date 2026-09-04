"""Assembling a ladder from settings, and the two runners it takes to drive one.

Same arrangement as `src/kafka/factory.py`: callers depend on the values and
protocols, configuration decides the numbers, and nothing here is wired into
the application's lifespan. What a service consumes is an application question,
and a demonstration consumer started at boot would join two consumer groups on
every deployment of a repository that consumes nothing.

## Why two runners and not one

`create_dead_letter_runners` returns an origin runner and a retry runner. They
cannot be the same runner, and the reason is `handler_timeout` rather than
anything about the topics: the origin consumer runs the application's handler
directly, and the retry consumer runs it behind a due-time gate that refuses
records. Those are different handlers, and a runner has one.

They also want different consumer groups. Offsets are stored against a group,
so sharing one would make the retry topics' lag indistinguishable from the
origin topic's on every dashboard that exists — and the first question during
an incident is which of the two is behind.

## Why one retry runner for all the tiers

One consumer over `orders.retry.1`, `orders.retry.2` and `orders.retry.3`
rather than three consumers. Tiers are separate topics and therefore separate
partitions, and the runner stalls partitions independently, so a tier-3
partition waiting fifteen minutes does not hold up tier 1 — the property that
would have justified three runners is already there. The loop wakes at the
*soonest* due time across the partitions it holds (`ConsumeResult.wait`), so
the long tiers cost one cheap poll-and-defer each time a short one comes due.
"""

from __future__ import annotations

from src.config import Settings, settings
from src.dlq.base import RetryLadder, RetryPolicy
from src.dlq.handler import retry_tier_handler, with_dead_letter
from src.dlq.replay import DeadLetterReplayer
from src.dlq.router import Clock, DeadLetterRouter
from src.kafka.base import MessageNotDecodableError, MessagePublisher, utc_now
from src.kafka.factory import (
    consumer_config,
    create_message_source,
    get_message_publisher,
)
from src.kafka.runner import ConsumerRunner, MessageHandler


def retry_policy(config: Settings | None = None) -> RetryPolicy:
    """The ladder's shape, from settings."""
    resolved = config if config is not None else settings
    return RetryPolicy(
        base_delay=resolved.DLQ_RETRY_BASE_DELAY_SECONDS,
        multiplier=resolved.DLQ_RETRY_MULTIPLIER,
        tiers=resolved.DLQ_RETRY_TIERS,
        max_delay=resolved.DLQ_RETRY_MAX_DELAY_SECONDS,
        retry_suffix=resolved.DLQ_RETRY_TOPIC_SUFFIX,
        dead_letter_suffix=resolved.DLQ_DEAD_LETTER_TOPIC_SUFFIX,
    )


def ladder_for(origin_topic: str, *, config: Settings | None = None) -> RetryLadder:
    """The topics one origin topic's ladder needs, for `create_topics` or docs.

    Worth calling before deploying a consumer: on a cluster with topic
    auto-creation disabled — which is most production clusters — the router's
    first publish to a tier that does not exist fails, and it fails at the
    moment the first record fails, which is the worst moment for a second
    problem.
    """
    return retry_policy(config).ladder_for(origin_topic)


def create_dead_letter_router(
    *,
    publisher: MessagePublisher | None = None,
    config: Settings | None = None,
    clock: Clock = utc_now,
    non_retryable: tuple[type[BaseException], ...] = (MessageNotDecodableError,),
) -> DeadLetterRouter:
    """A router over the process-wide publisher unless given another one."""
    return DeadLetterRouter(
        publisher=publisher if publisher is not None else get_message_publisher(),
        policy=retry_policy(config),
        clock=clock,
        non_retryable=non_retryable,
    )


def create_dead_letter_replayer(
    *,
    publisher: MessagePublisher | None = None,
    config: Settings | None = None,
    clock: Clock = utc_now,
) -> DeadLetterReplayer:
    """A replayer for draining a dead-letter topic back onto its origin."""
    resolved = config if config is not None else settings
    return DeadLetterReplayer(
        publisher=publisher if publisher is not None else get_message_publisher(),
        policy=retry_policy(resolved),
        max_replays=resolved.DLQ_MAX_REPLAYS,
        clock=clock,
    )


def create_dead_letter_runners(
    origin_topic: str,
    handler: MessageHandler,
    *,
    name: str = "default",
    group_id: str | None = None,
    backend: str | None = None,
    config: Settings | None = None,
    router: DeadLetterRouter | None = None,
    clock: Clock = utc_now,
) -> tuple[ConsumerRunner, ConsumerRunner | None]:
    """The origin runner and the retry runner for one topic's ladder.

    The whole of what a consumer process with a ladder needs:

        origin, retries = create_dead_letter_runners("orders.events", handle)
        origin.start()
        if retries is not None:
            retries.start()
        ...
        await origin.stop()

    Both runners share one router, and therefore one publisher: a record that
    fails on tier 2 is routed by the same object that routed it out of the
    origin topic, so the ladder's arithmetic has exactly one implementation in
    the process.

    Returns `(origin, retries)`, and `retries` is `None` when the policy has
    zero tiers — a configuration that means "dead-letter on the first failure",
    for which there is nothing to consume. It is a return value rather than an
    exception because that configuration is a legitimate choice, and `None`
    rather than an idle runner because a consumer subscribed to no topics is a
    background task that polls forever and reports healthy.

    **Start both.** A ladder whose retry consumer is not running looks perfect
    from the origin topic — its lag is zero, because every failure is being
    published away promptly — while the retry topics fill up and nothing in
    them is ever handled.
    """
    resolved = config if config is not None else settings
    policy = retry_policy(resolved)
    ladder = policy.ladder_for(origin_topic)
    routes = (
        router
        if router is not None
        else create_dead_letter_router(config=resolved, clock=clock)
    )
    group = group_id if group_id is not None else resolved.KAFKA_CONSUMER_GROUP

    origin = ConsumerRunner(
        source=create_message_source(
            [origin_topic], group_id=group, backend=backend, config=resolved
        ),
        handler=with_dead_letter(handler, routes),
        name=name,
        config=consumer_config(resolved),
    )
    if not ladder.retry_topics:
        return origin, None

    retries = ConsumerRunner(
        source=create_message_source(
            ladder.retry_topics,
            group_id=f"{group}{resolved.DLQ_RETRY_GROUP_SUFFIX}",
            backend=backend,
            config=resolved,
        ),
        handler=retry_tier_handler(handler, routes, clock=clock),
        name=f"{name}-retry",
        config=consumer_config(resolved),
    )
    return origin, retries


__all__ = [
    "create_dead_letter_replayer",
    "create_dead_letter_router",
    "create_dead_letter_runners",
    "ladder_for",
    "retry_policy",
]
