"""Redis-backed replay guard.

`SET key 1 NX EX ttl` is the whole implementation: one round trip that records
the fingerprint and reports whether it was already there, which is exactly the
atomic claim the contract asks for, with expiry as the server's problem rather
than a sweeper this process would have to own and cancel.

The stored value is a single byte. Nothing reads it — the key's existence is the
entire fact — and putting the body, the timestamp or the digest in there would
be storing a delivery's contents in a second place for no reader.
"""

from __future__ import annotations

import structlog
from redis.asyncio import Redis
from redis.exceptions import RedisError

from src.webhooks.errors import ReplayGuardUnavailableError

logger = structlog.get_logger(__name__)


class RedisReplayGuard:
    """`ReplayGuard` over a Redis server.

    The client is injected rather than built here so a caller can hand in a pool
    it already owns and a test can point at a throwaway database; `from_url` is
    the ordinary case.
    """

    def __init__(
        self,
        client: Redis,
        *,
        namespace: str = "webhook-replay",
        ttl_seconds: int = 900,
    ) -> None:
        if ttl_seconds < 1:
            # Redis rejects `EX 0`, and it would mean a guard that forgets
            # every claim immediately — which is to say no guard at all.
            raise ValueError("ttl_seconds must be at least 1.")
        self._client = client
        self._namespace = namespace
        self._ttl = ttl_seconds

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        namespace: str = "webhook-replay",
        ttl_seconds: int = 900,
    ) -> RedisReplayGuard:
        """Build a guard owning its own connection pool."""
        return cls(
            Redis.from_url(url, decode_responses=False),
            namespace=namespace,
            ttl_seconds=ttl_seconds,
        )

    @property
    def name(self) -> str:
        return "redis"

    @property
    def ttl_seconds(self) -> float:
        return float(self._ttl)

    def _key(self, fingerprint: str) -> str:
        return f"{self._namespace}:{fingerprint}"

    async def claim(self, fingerprint: str) -> bool:
        try:
            claimed = await self._client.set(
                self._key(fingerprint), b"1", nx=True, ex=self._ttl
            )
        except RedisError as exc:
            # Never `True` on an error. A guard that assumes "new" when it
            # cannot reach its store is a guard that stops working exactly when
            # somebody is attacking the store, and says nothing about it.
            raise ReplayGuardUnavailableError(
                "Could not reach the webhook replay guard."
            ) from exc
        return bool(claimed)

    async def close(self) -> None:
        """Close the client and its pool.

        Swallows `RedisError`: this runs during shutdown, and a server that is
        already gone is not a reason to fail a clean exit.
        """
        try:
            await self._client.aclose()
        except RedisError:  # pragma: no cover - shutdown against a dead server
            logger.warning("webhook.replay_guard_close_failed", guard=self.name)
