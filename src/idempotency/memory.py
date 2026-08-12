"""In-process idempotency store.

The store tests inject when they want the real middleware code path without a
Redis server. It is per-process and does not survive a restart, so it is never
a production choice: two uvicorn workers behind the same socket would each hold
their own reservations, and a retry landing on the other worker would execute a
second time — which is the failure the middleware exists to prevent. The
factory logs a warning when it is selected outside a test environment.
"""

from __future__ import annotations

import asyncio
import time

from src.idempotency.base import IdempotencyRecord


class InMemoryIdempotencyStore:
    """Holds records in a dict with monotonic expiry stamps.

    The lock covers the check-then-set in `reserve`; without it two tasks could
    both find a key absent and both believe they own it, which would make this
    store useless for the one thing it is for. `time.monotonic` rather than
    wall-clock time so that an NTP step cannot resurrect an expired record or
    expire a live one.
    """

    def __init__(
        self,
        *,
        record_ttl_seconds: float = 86_400.0,
        reservation_ttl_seconds: float = 60.0,
    ) -> None:
        self._records: dict[str, tuple[IdempotencyRecord, float]] = {}
        self._lock = asyncio.Lock()
        self._record_ttl = record_ttl_seconds
        self._reservation_ttl = reservation_ttl_seconds

    @property
    def name(self) -> str:
        return "memory"

    def _live(self, key: str) -> IdempotencyRecord | None:
        """Return the record if present and unexpired, purging it if not.

        Expiry is evaluated on read rather than by a sweeper task: a background
        loop would keep a reference to this store alive for the life of the
        process and would have to be cancelled by whoever built it.
        """
        entry = self._records.get(key)
        if entry is None:
            return None
        record, expires_at = entry
        if expires_at <= time.monotonic():
            del self._records[key]
            return None
        return record

    async def reserve(self, key: str, fingerprint: str) -> IdempotencyRecord | None:
        async with self._lock:
            existing = self._live(key)
            if existing is not None:
                return existing
            self._records[key] = (
                IdempotencyRecord(fingerprint=fingerprint),
                time.monotonic() + self._reservation_ttl,
            )
            return None

    async def complete(self, key: str, record: IdempotencyRecord) -> None:
        async with self._lock:
            self._records[key] = (record, time.monotonic() + self._record_ttl)

    async def release(self, key: str) -> None:
        async with self._lock:
            self._records.pop(key, None)

    async def get(self, key: str) -> IdempotencyRecord | None:
        async with self._lock:
            return self._live(key)

    async def close(self) -> None:
        """Nothing to close. Present so the app lifespan needs no `isinstance`."""

    async def clear(self) -> None:
        """Drop every record. For tests that reuse a process-wide store."""
        async with self._lock:
            self._records.clear()
