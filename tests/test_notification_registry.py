"""Strategy selection: configuration, per-user preference and extension."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest

from src.config import Settings
from src.models.user import User
from src.notifications.base import (
    Notification,
    NotificationResult,
    NotificationStrategy,
    Recipient,
    UnknownNotificationChannelError,
)
from src.notifications.email import EmailNotificationStrategy
from src.notifications.null import NullNotificationStrategy
from src.notifications.recipients import recipient_from_user
from src.notifications.registry import (
    NotificationStrategyRegistry,
    get_strategy,
    notify,
)
from src.notifications.webhook import WebhookNotificationStrategy


def make_settings(**overrides: object) -> Settings:
    """A Settings instance that ignores the ambient .env and environment."""
    base: dict[str, object] = {
        "DATABASE_URL": "postgresql+asyncpg://fake:fake@localhost/fake",
        "SECRET_KEY": "not-a-real-secret-key-for-tests-only",
    }
    return Settings(_env_file=None, **{**base, **overrides})  # type: ignore[arg-type]


@pytest.fixture(autouse=True)
def restore_registry() -> Iterator[None]:
    """Undo `register`/`unregister` and the cached strategies per test."""
    yield
    NotificationStrategyRegistry.reset()
    get_strategy.cache_clear()


class SpyStrategy:
    """A channel the registry has never heard of until a test registers it."""

    def __init__(self) -> None:
        self.sent: list[Notification] = []

    @property
    def name(self) -> str:
        return "carrier-pigeon"

    def supports(self, recipient: Recipient) -> bool:
        return True

    async def send(
        self, recipient: Recipient, notification: Notification
    ) -> NotificationResult:
        self.sent.append(notification)
        return NotificationResult(
            channel=self.name, delivered=True, detail="Pigeon dispatched."
        )


# --- create ---


def test_create_returns_the_configured_default() -> None:
    strategy = NotificationStrategyRegistry.create(
        config=make_settings(NOTIFICATION_DEFAULT_CHANNEL="none")
    )

    assert isinstance(strategy, NullNotificationStrategy)
    assert strategy.name == "none"


def test_create_explicit_channel_overrides_configuration() -> None:
    config = make_settings(NOTIFICATION_DEFAULT_CHANNEL="none")

    strategy = NotificationStrategyRegistry.create("email", config=config)

    assert isinstance(strategy, EmailNotificationStrategy)


def test_webhook_strategy_is_built_from_settings() -> None:
    config = make_settings(
        NOTIFICATION_WEBHOOK_SECRET="configured-secret",
        NOTIFICATION_WEBHOOK_MAX_ATTEMPTS=5,
        NOTIFICATION_WEBHOOK_BACKOFF_SECONDS=2.0,
        NOTIFICATION_WEBHOOK_TIMEOUT_SECONDS=3.0,
        NOTIFICATION_WEBHOOK_ALLOW_PRIVATE_HOSTS=True,
    )

    strategy = NotificationStrategyRegistry.create("webhook", config=config)

    assert isinstance(strategy, WebhookNotificationStrategy)
    assert strategy._secret == "configured-secret"
    assert strategy._max_attempts == 5
    assert strategy._backoff == 2.0
    assert strategy._timeout == 3.0
    assert strategy._allow_private_hosts is True


def test_unknown_channel_raises_and_names_the_alternatives() -> None:
    with pytest.raises(UnknownNotificationChannelError) as excinfo:
        NotificationStrategyRegistry.create("smoke-signal", config=make_settings())

    assert excinfo.value.details == {
        "requested": "smoke-signal",
        "available": ["email", "none", "webhook"],
    }


def test_create_returns_a_fresh_instance_each_call() -> None:
    config = make_settings()

    first = NotificationStrategyRegistry.create("email", config=config)
    second = NotificationStrategyRegistry.create("email", config=config)

    assert first is not second


def test_every_built_in_channel_satisfies_the_protocol() -> None:
    config = make_settings()

    for channel in NotificationStrategyRegistry.available():
        strategy = NotificationStrategyRegistry.create(channel, config=config)
        assert isinstance(strategy, NotificationStrategy)
        assert strategy.name == channel


# --- for_recipient ---


@pytest.mark.parametrize(
    ("channel", "expected"),
    [
        ("email", EmailNotificationStrategy),
        ("webhook", WebhookNotificationStrategy),
        ("none", NullNotificationStrategy),
    ],
)
def test_the_recipients_preference_picks_the_strategy(
    channel: str, expected: type
) -> None:
    recipient = Recipient(id="u1", channel=channel, email="a@example.com")

    strategy = NotificationStrategyRegistry.for_recipient(
        recipient, config=make_settings()
    )

    assert isinstance(strategy, expected)


def test_an_unset_preference_falls_back_to_the_default() -> None:
    """Rows predating the column are an ordinary case, not an error."""
    recipient = Recipient(id="u1", channel="", email="a@example.com")

    strategy = NotificationStrategyRegistry.for_recipient(
        recipient, config=make_settings(NOTIFICATION_DEFAULT_CHANNEL="none")
    )

    assert isinstance(strategy, NullNotificationStrategy)


def test_an_unknown_preference_raises_rather_than_guessing() -> None:
    """Silently defaulting would deliver over a channel the user did not pick."""
    recipient = Recipient(id="u1", channel="smoke-signal")

    with pytest.raises(UnknownNotificationChannelError):
        NotificationStrategyRegistry.for_recipient(recipient, config=make_settings())


# --- Extension ---


def test_a_new_channel_needs_no_change_to_the_registry() -> None:
    spy = SpyStrategy()
    NotificationStrategyRegistry.register("carrier-pigeon", lambda _: spy)

    strategy = NotificationStrategyRegistry.for_recipient(
        Recipient(id="u1", channel="carrier-pigeon"), config=make_settings()
    )

    assert strategy is spy
    assert "carrier-pigeon" in NotificationStrategyRegistry.available()


def test_register_can_replace_a_built_in_channel() -> None:
    spy = SpyStrategy()
    NotificationStrategyRegistry.register("email", lambda _: spy)

    assert NotificationStrategyRegistry.create("email", config=make_settings()) is spy


def test_unregister_removes_a_channel() -> None:
    NotificationStrategyRegistry.unregister("webhook")

    assert "webhook" not in NotificationStrategyRegistry.available()
    with pytest.raises(UnknownNotificationChannelError):
        NotificationStrategyRegistry.create("webhook", config=make_settings())


def test_unregister_is_a_no_op_for_an_unknown_channel() -> None:
    NotificationStrategyRegistry.unregister("never-registered")

    assert NotificationStrategyRegistry.available() == ("email", "none", "webhook")


def test_reset_restores_the_built_ins() -> None:
    NotificationStrategyRegistry.register("carrier-pigeon", lambda _: SpyStrategy())
    NotificationStrategyRegistry.unregister("email")

    NotificationStrategyRegistry.reset()

    assert NotificationStrategyRegistry.available() == ("email", "none", "webhook")


# --- get_strategy ---


def test_get_strategy_caches_per_channel() -> None:
    """One webhook strategy means one connection pool, not one per delivery."""
    assert get_strategy("webhook") is get_strategy("webhook")
    assert get_strategy("webhook") is not get_strategy("email")


def test_cache_clear_picks_up_a_newly_registered_channel() -> None:
    before = get_strategy("email")
    spy = SpyStrategy()
    NotificationStrategyRegistry.register("email", lambda _: spy)

    assert get_strategy("email") is before  # still cached

    get_strategy.cache_clear()
    assert get_strategy("email") is spy


# --- notify ---


async def test_notify_routes_through_the_recipients_channel() -> None:
    spy = SpyStrategy()
    NotificationStrategyRegistry.register("carrier-pigeon", lambda _: spy)
    get_strategy.cache_clear()
    notification = Notification(subject="Hi", body="There")

    result = await notify(Recipient(id="u1", channel="carrier-pigeon"), notification)

    assert result.delivered is True
    assert spy.sent == [notification]


async def test_notify_on_an_opted_out_recipient_reports_no_delivery() -> None:
    result = await notify(
        Recipient(id="u1", channel="none", email="a@example.com"),
        Notification(subject="Hi", body="There"),
    )

    assert result.delivered is False
    assert result.channel == "none"


# --- recipient_from_user ---


def make_user(**overrides: object) -> User:
    fields: dict[str, object] = {
        "id": uuid.uuid4(),
        "email": "user@example.com",
        "hashed_password": "not-a-real-hash",
        "is_active": True,
        "is_verified": True,
        "role": "user",
        "created_at": datetime.now(UTC),
        "updated_at": datetime.now(UTC),
        "notification_channel": "email",
        "notification_webhook_url": None,
    }
    return User(**{**fields, **overrides})


def test_recipient_is_projected_from_the_user_row() -> None:
    user = make_user(
        notification_channel="webhook",
        notification_webhook_url="https://hooks.example.com/u1",
    )

    recipient = recipient_from_user(user)

    assert recipient == Recipient(
        id=str(user.id),
        channel="webhook",
        email="user@example.com",
        webhook_url="https://hooks.example.com/u1",
    )


def test_the_projection_drops_everything_a_strategy_has_no_use_for() -> None:
    """A webhook body assembled from a wider object would ship the hash."""
    recipient = recipient_from_user(make_user(hashed_password="argon2-hash-here"))

    assert not hasattr(recipient, "hashed_password")
    assert not hasattr(recipient, "role")
    assert set(Recipient.__dataclass_fields__) == {
        "id",
        "channel",
        "email",
        "webhook_url",
    }


def test_a_users_default_channel_is_email() -> None:
    """The column default, so a new row is reachable without any setup."""
    assert User.__table__.c.notification_channel.default.arg == "email"
    assert User.__table__.c.notification_channel.nullable is False
