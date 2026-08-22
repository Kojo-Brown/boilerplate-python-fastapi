"""Strategy selection.

The whole point of the pattern is that a caller says *what* to notify about and
never *how*: the recipient's stored preference picks the strategy, and adding a
channel does not edit a single call site.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import cache
from typing import ClassVar, Final, Literal

import structlog

from src.config import Settings, settings
from src.immutable import FrozenDict
from src.notifications.base import (
    Notification,
    NotificationResult,
    NotificationStrategy,
    Recipient,
    UnknownNotificationChannelError,
)
from src.notifications.email import EmailNotificationStrategy
from src.notifications.null import NullNotificationStrategy
from src.notifications.webhook import WebhookNotificationStrategy

logger = structlog.get_logger(__name__)

NotificationChannel = Literal["email", "webhook", "none"]

StrategyBuilder = Callable[[Settings], NotificationStrategy]


def _build_webhook(config: Settings) -> NotificationStrategy:
    return WebhookNotificationStrategy(
        secret=config.NOTIFICATION_WEBHOOK_SECRET,
        timeout=config.NOTIFICATION_WEBHOOK_TIMEOUT_SECONDS,
        max_attempts=config.NOTIFICATION_WEBHOOK_MAX_ATTEMPTS,
        backoff=config.NOTIFICATION_WEBHOOK_BACKOFF_SECONDS,
        allow_private_hosts=config.NOTIFICATION_WEBHOOK_ALLOW_PRIVATE_HOSTS,
    )


DEFAULT_STRATEGIES: Final[FrozenDict[str, StrategyBuilder]] = FrozenDict(
    {
        "email": lambda _: EmailNotificationStrategy(),
        "webhook": _build_webhook,
        "none": lambda _: NullNotificationStrategy(),
    }
)


class NotificationStrategyRegistry:
    """Builds a `NotificationStrategy` from a channel name and settings.

    Extension is registration, not modification: an SMS or Slack channel is a
    new module plus one `register` call, and `for_recipient` starts routing to
    it without this class or any caller changing.
    """

    _builders: ClassVar[dict[str, StrategyBuilder]] = dict(DEFAULT_STRATEGIES)

    @classmethod
    def register(cls, channel: str, builder: StrategyBuilder) -> None:
        """Add or replace the builder for `channel`."""
        cls._builders[channel] = builder

    @classmethod
    def unregister(cls, channel: str) -> None:
        """Remove a builder. No-op if it was never registered."""
        cls._builders.pop(channel, None)

    @classmethod
    def reset(cls) -> None:
        """Restore the built-in registry. For tests that call `register`."""
        cls._builders = dict(DEFAULT_STRATEGIES)

    @classmethod
    def available(cls) -> tuple[str, ...]:
        return tuple(sorted(cls._builders))

    @classmethod
    def create(
        cls,
        channel: str | None = None,
        *,
        config: Settings | None = None,
    ) -> NotificationStrategy:
        """Return a new strategy instance for `channel`.

        `channel` defaults to `NOTIFICATION_DEFAULT_CHANNEL` and `config` to the
        process settings, so both arguments exist for tests and for callers
        that need a specific channel regardless of configuration.
        """
        resolved_config = config if config is not None else settings
        name = (
            channel
            if channel is not None
            else resolved_config.NOTIFICATION_DEFAULT_CHANNEL
        )

        try:
            builder = cls._builders[name]
        except KeyError as exc:
            raise UnknownNotificationChannelError(name, cls.available()) from exc

        instance = builder(resolved_config)
        logger.debug("notification.strategy_created", channel=name)
        return instance

    @classmethod
    def for_recipient(
        cls, recipient: Recipient, *, config: Settings | None = None
    ) -> NotificationStrategy:
        """Return the strategy `recipient` has asked to be reached through.

        An empty preference falls back to the configured default rather than
        raising: rows predating the column, and users who have never touched
        their settings, are ordinary cases and not errors.
        """
        resolved_config = config if config is not None else settings
        channel = recipient.channel or resolved_config.NOTIFICATION_DEFAULT_CHANNEL
        return cls.create(channel, config=resolved_config)


@cache
def get_strategy(channel: str) -> NotificationStrategy:
    """Return the process-wide strategy for `channel`.

    Cached per channel because `WebhookNotificationStrategy` owns an
    `httpx.AsyncClient`, and building one per notification would throw away the
    connection pool that makes it worth having. Call
    `get_strategy.cache_clear()` after changing settings or registering a
    channel in a test.
    """
    return NotificationStrategyRegistry.create(channel)


async def notify(
    recipient: Recipient, notification: Notification
) -> NotificationResult:
    """Send `notification` over whichever channel `recipient` prefers.

    The one function most callers need. It resolves the channel, so application
    code never imports a concrete strategy:

        await notify(recipient_from_user(user), Notification(
            subject="Your export is ready", body="..."
        ))
    """
    channel = recipient.channel or settings.NOTIFICATION_DEFAULT_CHANNEL
    strategy = get_strategy(channel)
    return await strategy.send(recipient, notification)
