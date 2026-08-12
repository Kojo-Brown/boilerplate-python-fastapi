"""Redis-backed idempotency store.

Redis is the right shape for this: `SET key value NX EX ttl` is one round trip
that both claims a key and refuses to claim one twice, which is exactly the
atomic reservation the contract asks for, and expiry is the server's problem
rather than a sweeper this process has to own.

Two TTLs, not one. A *reservation* is short-lived (`reservation_ttl_seconds`,
60s by default) because it represents a request that is still running: if the
worker holding it is killed mid-flight, nothing will ever complete or release
that key, and a long TTL would answer every retry with 409 until it expired. A
*completed record* lives far longer (`record_ttl_seconds`, 24h by default),
because that is the window over which a client may still be retrying. Set the
reservation TTL above the longest request this API will serve — a reservation
that expires under a still-running request lets a retry execute alongside it.
"""

from __future__ import annotations

from typing import Any, Final

import structlog
from redis.asyncio import Redis
from redis.exceptions import RedisError

from src.idempotency.base import (
    IdempotencyRecord,
    IdempotencyStoreUnavailableError,
)
from src.idempotency.codec import decode_record, encode_record

logger = structlog.get_logger(__name__)

# A reservation can expire between the failed `SET NX` and the `GET` that asks
# what is there, leaving neither a claim nor a record. Retrying resolves it;
# needing more than a handful of attempts means the TTL is pathologically short
# and is worth surfacing rather than looping on.
_RESERVE_ATTEMPTS: Final[int] = 3


class RedisIdempotencyStore:
    """`IdempotencyStore` over a Redis server.

    The client is injected rather than built here so a caller can hand in a
    pool it already owns, and so tests can point at a throwaway database. Use
    `from_url` for the ordinary case.
    """

    def __init__(
        self,
        client: Redis,
        *,
        namespace: str = "idempotency",
        record_ttl_seconds: int = 86_400,
        reservation_ttl_seconds: int = 60,
    ) -> None:
        self._client = client
        self._namespace = namespace
        self._record_ttl = record_ttl_seconds
        self._reservation_ttl = reservation_ttl_seconds

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        namespace: str = "idempotency",
        record_ttl_seconds: int = 86_400,
        reservation_ttl_seconds: int = 60,
    ) -> RedisIdempotencyStore:
        """Build a store owning its own connection pool.

        `decode_responses` stays off: records are base64-wrapped JSON stored as
        bytes, and asking redis-py to decode them to `str` would only mean
        encoding them back before parsing.
        """
        return cls(
            Redis.from_url(url, decode_responses=False),
            namespace=namespace,
            record_ttl_seconds=record_ttl_seconds,
            reservation_ttl_seconds=reservation_ttl_seconds,
        )

    @property
    def name(self) -> str:
        return "redis"

    def _name(self, key: str) -> str:
        return f"{self._namespace}:{key}"

    async def reserve(self, key: str, fingerprint: str) -> IdempotencyRecord | None:
        name = self._name(key)
        payload = encode_record(IdempotencyRecord(fingerprint=fingerprint))

        for _ in range(_RESERVE_ATTEMPTS):
            try:
                claimed = await self._client.set(
                    name, payload, nx=True, ex=self._reservation_ttl
                )
                if claimed:
                    return None
                raw: Any = await self._client.get(name)
            except RedisError as exc:
                raise IdempotencyStoreUnavailableError(
                    "Could not reach the idempotency store."
                ) from exc

            if raw is None:
                # Expired between the SET and the GET. Nobody owns it now.
                continue

            record = decode_record(bytes(raw))
            if record is not None:
                return record

            # A record from a different schema version. Overwriting it claims
            # the key for this request; the alternative is answering every
            # retry with an error until a TTL that was written by the previous
            # release runs out. The window in which two requests could both do
            # this is one deploy wide and costs at most one re-execution.
            logger.info("idempotency.record_schema_mismatch", key=name)
            try:
                await self._client.set(name, payload, ex=self._reservation_ttl)
            except RedisError as exc:
                raise IdempotencyStoreUnavailableError(
                    "Could not reach the idempotency store."
                ) from exc
            return None

        raise IdempotencyStoreUnavailableError(
            "Could not obtain an idempotency reservation.",
            details={"attempts": _RESERVE_ATTEMPTS},
        )

    async def complete(self, key: str, record: IdempotencyRecord) -> None:
        try:
            await self._client.set(
                self._name(key), encode_record(record), ex=self._record_ttl
            )
        except RedisError as exc:
            raise IdempotencyStoreUnavailableError(
                "Could not store the idempotent response."
            ) from exc

    async def release(self, key: str) -> None:
        try:
            await self._client.delete(self._name(key))
        except RedisError as exc:
            raise IdempotencyStoreUnavailableError(
                "Could not release the idempotency reservation."
            ) from exc

    async def get(self, key: str) -> IdempotencyRecord | None:
        try:
            raw: Any = await self._client.get(self._name(key))
        except RedisError as exc:
            raise IdempotencyStoreUnavailableError(
                "Could not read from the idempotency store."
            ) from exc
        if raw is None:
            return None
        return decode_record(bytes(raw))

    async def close(self) -> None:
        """Close the client and its pool.

        Swallows `RedisError`: this runs during shutdown, and a server that is
        already gone is not a reason to fail a clean exit.
        """
        try:
            await self._client.aclose()
        except RedisError:  # pragma: no cover - shutdown against a dead server
            logger.warning("idempotency.close_failed", store=self.name)
