"""Backend selection, and the warnings that keep a weakened setting visible."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from src.config import Settings
from src.webhooks import factory as factory_module
from src.webhooks.errors import WebhookConfigurationError
from src.webhooks.factory import (
    close_replay_guard,
    create_replay_guard,
    create_verifier,
    get_replay_guard,
    get_webhook_verifier,
)
from src.webhooks.redis_guard import RedisReplayGuard
from src.webhooks.replay import InMemoryReplayGuard
from tests.conftest import LogCapturer

SECRET = "a-test-secret-of-adequate-length!!!!"


def a_settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "DATABASE_URL": "postgresql+asyncpg://test:test@localhost/test",
        "SECRET_KEY": "test-secret-key",
        "REDIS_URL": "redis://localhost:6379/0",
        "WEBHOOK_SIGNING_SECRETS": f"test:{SECRET}",
        "ENVIRONMENT": "test",
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


@pytest.fixture(autouse=True)
def clear_process_caches() -> Iterator[None]:
    """Both getters are `lru_cache`d, so a test must not inherit or leak one."""
    get_replay_guard.cache_clear()
    get_webhook_verifier.cache_clear()
    yield
    get_replay_guard.cache_clear()
    get_webhook_verifier.cache_clear()


class TestGuardSelection:
    async def test_memory(self) -> None:
        guard = create_replay_guard("memory", config=a_settings())

        assert isinstance(guard, InMemoryReplayGuard)

    async def test_redis(self) -> None:
        """redis-py connects lazily, so this builds a client and no socket."""
        guard = create_replay_guard("redis", config=a_settings())

        assert isinstance(guard, RedisReplayGuard)
        await guard.close()

    async def test_none_returns_no_guard(self) -> None:
        """A supported answer, not an error.

        Signature verification is worth having on its own, and a deployment with
        no Redis should get it rather than get nothing.
        """
        assert create_replay_guard("none", config=a_settings()) is None

    async def test_the_backend_defaults_to_the_setting(self) -> None:
        guard = create_replay_guard(config=a_settings(WEBHOOK_REPLAY_BACKEND="memory"))

        assert isinstance(guard, InMemoryReplayGuard)

    async def test_the_configured_ttl_reaches_the_guard(self) -> None:
        guard = create_replay_guard(
            "memory", config=a_settings(WEBHOOK_REPLAY_TTL_SECONDS=1200)
        )

        assert guard is not None
        assert guard.ttl_seconds == 1200.0

    async def test_the_replay_url_overrides_the_shared_one(self) -> None:
        """A deployment that wants a different instance or database number."""
        guard = create_replay_guard(
            "redis",
            config=a_settings(WEBHOOK_REPLAY_REDIS_URL="redis://localhost:6379/9"),
        )

        assert isinstance(guard, RedisReplayGuard)
        await guard.close()

    async def test_an_unknown_backend_raises(self) -> None:
        """Unreachable through settings, reachable from a direct call.

        A silent fallback would be a deployment that believes it is guarding
        replays and is not.
        """
        with pytest.raises(ValueError, match="Unknown webhook replay backend"):
            create_replay_guard("memcached", config=a_settings())

    async def test_the_error_lists_every_available_backend(self) -> None:
        with pytest.raises(ValueError) as caught:
            create_replay_guard("memcached", config=a_settings())

        message = str(caught.value)
        assert "memory" in message
        assert "redis" in message
        assert "none" in message


class TestWarnings:
    async def test_switching_replay_protection_off_warns(
        self, capture_module_logs: LogCapturer
    ) -> None:
        """ "We verify signatures" and "a capture cannot be reused" are different
        claims, and this is the setting that separates them."""
        logs = capture_module_logs(factory_module)

        create_replay_guard("none", config=a_settings())

        entry = next(
            log for log in logs if log["event"] == "webhook.replay_protection_disabled"
        )
        assert entry["log_level"] == "warning"
        assert entry["tolerance_seconds"] == 300

    async def test_the_memory_guard_outside_development_warns(
        self, capture_module_logs: LogCapturer
    ) -> None:
        """Per-process: a replay delivered to another worker is accepted."""
        logs = capture_module_logs(factory_module)

        create_replay_guard("memory", config=a_settings(ENVIRONMENT="production"))

        entry = next(
            log
            for log in logs
            if log["event"] == "webhook.memory_replay_guard_outside_development"
        )
        assert entry["log_level"] == "warning"
        assert entry["environment"] == "production"

    async def test_the_memory_guard_in_development_does_not_warn(
        self, capture_module_logs: LogCapturer
    ) -> None:
        logs = capture_module_logs(factory_module)

        create_replay_guard("memory", config=a_settings(ENVIRONMENT="development"))

        assert not [
            log
            for log in logs
            if log["event"] == "webhook.memory_replay_guard_outside_development"
        ]

    async def test_the_redis_guard_does_not_warn(
        self, capture_module_logs: LogCapturer
    ) -> None:
        logs = capture_module_logs(factory_module)

        guard = create_replay_guard("redis", config=a_settings())

        assert not [log for log in logs if log["log_level"] == "warning"]
        assert guard is not None
        await guard.close()


class TestTheProcessWideGuard:
    async def test_it_is_cached(self) -> None:
        """The Redis backend owns a pool that must not be rebuilt per request,
        and the in-memory one only means anything when every caller shares it."""
        assert get_replay_guard() is get_replay_guard()

    async def test_the_suite_gets_the_in_process_guard(self) -> None:
        """Set in conftest, so importing `src.main` opens no Redis pool."""
        assert isinstance(get_replay_guard(), InMemoryReplayGuard)

    async def test_closing_it_is_safe_when_nothing_used_it(self) -> None:
        """A process that mounted no receiving route closes a client that never
        opened a socket — the bargain the lifespan makes."""
        await close_replay_guard()

    async def test_closing_it_is_safe_with_no_guard_configured(self) -> None:
        await close_replay_guard()
        await close_replay_guard()


class TestBuildingAVerifier:
    async def test_it_reads_the_configuration(self) -> None:
        verifier = create_verifier(
            config=a_settings(
                WEBHOOK_TOLERANCE_SECONDS=60, WEBHOOK_REPLAY_TTL_SECONDS=120
            )
        )

        assert verifier.tolerance_seconds == 60

    async def test_it_takes_the_configured_header_name(self) -> None:
        verifier = create_verifier(
            config=a_settings(WEBHOOK_SIGNATURE_HEADER="Stripe-Signature")
        )

        assert verifier.signature_header == "Stripe-Signature"

    async def test_it_uses_the_process_wide_guard(self) -> None:
        verifier = create_verifier(config=a_settings())

        assert verifier.guard_name == get_replay_guard().name  # type: ignore[union-attr]

    async def test_missing_secrets_raise_rather_than_accept_anything(self) -> None:
        with pytest.raises(WebhookConfigurationError, match="empty"):
            create_verifier(config=a_settings(WEBHOOK_SIGNING_SECRETS=""))

    async def test_a_ttl_that_cannot_cover_the_window_raises(self) -> None:
        """The two settings are checked against each other, not in isolation."""
        with pytest.raises(WebhookConfigurationError, match="cannot cover"):
            create_verifier(
                config=a_settings(
                    WEBHOOK_TOLERANCE_SECONDS=600, WEBHOOK_REPLAY_TTL_SECONDS=600
                )
            )

    async def test_the_defaults_are_consistent_with_each_other(self) -> None:
        """A shipped default pair that failed the check would make the feature
        unusable until somebody read the error."""
        assert create_verifier(config=a_settings()).tolerance_seconds == 300

    async def test_the_process_wide_verifier_is_cached(self) -> None:
        assert get_webhook_verifier() is get_webhook_verifier()
