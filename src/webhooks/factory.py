"""Building the guard and the verifier from configuration.

Nothing outside this module decides which replay backend is in use or assembles
a verifier: callers depend on `WebhookVerifier`, and configuration chooses what
it is made of — the same split as `src/idempotency/factory.py`.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import lru_cache
from typing import Final, Literal

import structlog

from src.config import Settings, settings
from src.immutable import FrozenDict
from src.webhooks.redis_guard import RedisReplayGuard
from src.webhooks.replay import InMemoryReplayGuard, ReplayGuard
from src.webhooks.secrets import build_signing_secrets
from src.webhooks.verifier import WebhookVerifier

logger = structlog.get_logger(__name__)

ReplayBackendName = Literal["redis", "memory", "none"]

GuardBuilder = Callable[[Settings], ReplayGuard]


def _build_redis(config: Settings) -> ReplayGuard:
    return RedisReplayGuard.from_url(
        config.WEBHOOK_REPLAY_REDIS_URL or config.REDIS_URL,
        ttl_seconds=config.WEBHOOK_REPLAY_TTL_SECONDS,
    )


def _build_memory(config: Settings) -> ReplayGuard:
    return InMemoryReplayGuard(ttl_seconds=float(config.WEBHOOK_REPLAY_TTL_SECONDS))


BUILDERS: Final[FrozenDict[str, GuardBuilder]] = FrozenDict[str, GuardBuilder](
    {
        "redis": _build_redis,
        "memory": _build_memory,
    }
)


def create_replay_guard(
    backend: str | None = None, *, config: Settings | None = None
) -> ReplayGuard | None:
    """Return a new guard, or `None` when replay protection is switched off.

    `"none"` is a supported answer rather than an error: signature verification
    is worth having on its own, and a deployment with no Redis should get it
    rather than get nothing. It is not a quiet answer — the width of the window
    it leaves open is logged as a warning, because "we verify signatures" and
    "a captured delivery cannot be reused" are different claims and this is the
    setting that separates them.
    """
    resolved = config if config is not None else settings
    name = backend if backend is not None else resolved.WEBHOOK_REPLAY_BACKEND

    if name == "none":
        logger.warning(
            "webhook.replay_protection_disabled",
            tolerance_seconds=resolved.WEBHOOK_TOLERANCE_SECONDS,
            detail=(
                "WEBHOOK_REPLAY_BACKEND is 'none': a captured delivery can be "
                "replayed for as long as its timestamp stays inside the "
                "tolerance window."
            ),
        )
        return None

    builder = BUILDERS.get(name)
    if builder is None:
        # Unreachable through settings — the field is a Literal, so pydantic
        # refuses an unknown name at start-up — but reachable from a direct
        # call, where a silent fallback would be a deployment that believes it
        # is guarding replays and is not.
        raise ValueError(
            f"Unknown webhook replay backend '{name}'. "
            f"Available: {', '.join(sorted([*BUILDERS, 'none']))}."
        )

    if name == "memory" and resolved.ENVIRONMENT not in ("test", "development"):
        logger.warning(
            "webhook.memory_replay_guard_outside_development",
            environment=resolved.ENVIRONMENT,
            detail=(
                "The in-memory guard is per-process: a replay delivered to "
                "another worker is not recognised and is accepted."
            ),
        )

    logger.debug("webhook.replay_guard_created", backend=name)
    return builder(resolved)


@lru_cache(maxsize=1)
def get_replay_guard() -> ReplayGuard | None:
    """The process-wide configured guard.

    Cached because the Redis backend owns a connection pool that must not be
    rebuilt per request, and because the in-memory backend only means anything
    when every caller shares one instance. Call
    `get_replay_guard.cache_clear()` after changing the backend in a test.
    """
    return create_replay_guard()


def create_verifier(*, config: Settings | None = None) -> WebhookVerifier:
    """Assemble a verifier from configuration and the process-wide guard.

    Raises `WebhookConfigurationError` when the secrets are missing or the
    guard's TTL cannot cover the tolerance window. Both are start-up-shaped
    failures that this codebase would rather have loudly, on the first delivery
    to a route that has never worked, than resolve by verifying less.
    """
    resolved = config if config is not None else settings
    return WebhookVerifier(
        secrets=build_signing_secrets(resolved),
        tolerance_seconds=resolved.WEBHOOK_TOLERANCE_SECONDS,
        replay_guard=get_replay_guard(),
        signature_header=resolved.WEBHOOK_SIGNATURE_HEADER,
        fail_open=resolved.WEBHOOK_REPLAY_FAIL_OPEN,
    )


@lru_cache(maxsize=1)
def get_webhook_verifier() -> WebhookVerifier:
    """The process-wide verifier, built on first use.

    Built lazily rather than in the lifespan because the default application
    receives no webhooks: a deployment that has configured no secret is an
    ordinary deployment, and making this a start-up requirement would break it
    for a feature it does not use. Call
    `get_webhook_verifier.cache_clear()` after changing configuration in a test.
    """
    return create_verifier()


async def close_replay_guard() -> None:
    """Close the process-wide guard, if one was ever built.

    Safe to call when nothing has used a verifier: building the guard to close
    it costs a redis-py client that connects lazily, so an unused deployment
    closes a pool that never opened a socket — the same bargain
    `get_lock_backend().close()` makes in the lifespan.
    """
    guard = get_replay_guard()
    if guard is not None:
        await guard.close()
