"""Building the relay from configuration, and holding the one the app runs.

Separated from `src/outbox/relay.py` for the reason every factory here is: the
relay takes its collaborators as arguments and knows nothing about `Settings`,
`AsyncSessionLocal` or the process-wide bus, which is what lets a test build
one over fakes without touching configuration.
"""

from __future__ import annotations

from functools import lru_cache

from src.config import Settings, settings
from src.database import AsyncSessionLocal
from src.events.bus import event_bus
from src.outbox.relay import OutboxRelay, RelayConfig
from src.outbox.store import session_batches


def create_outbox_relay(*, config: Settings | None = None) -> OutboxRelay:
    """A relay over the application's sessions and the process-wide bus."""
    resolved = config if config is not None else settings
    return OutboxRelay(
        batches=session_batches(AsyncSessionLocal),
        dispatcher=event_bus,
        config=RelayConfig(
            batch_size=resolved.OUTBOX_BATCH_SIZE,
            poll_interval=resolved.OUTBOX_POLL_INTERVAL_SECONDS,
            dispatch_timeout=resolved.OUTBOX_DISPATCH_TIMEOUT_SECONDS,
            retry_base_delay=resolved.OUTBOX_RETRY_BASE_DELAY_SECONDS,
            retry_max_delay=resolved.OUTBOX_RETRY_MAX_DELAY_SECONDS,
        ),
    )


@lru_cache(maxsize=1)
def get_outbox_relay() -> OutboxRelay:
    """The relay this process runs.

    Cached because `start` and `stop` have to reach the same object: a lifespan
    that started one instance and shut down another would leave a task draining
    the outbox after the application had let go of its connection pool. Call
    `get_outbox_relay.cache_clear()` in a test that needs a fresh one.
    """
    return create_outbox_relay()


__all__ = ["create_outbox_relay", "get_outbox_relay"]
