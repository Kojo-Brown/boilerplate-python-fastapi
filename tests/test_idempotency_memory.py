"""Behaviour specific to the in-process store — expiry above all."""

from __future__ import annotations

import asyncio

from src.idempotency.base import IdempotencyRecord, StoredResponse
from src.idempotency.memory import InMemoryIdempotencyStore


def a_record() -> IdempotencyRecord:
    return IdempotencyRecord(
        fingerprint="fp",
        response=StoredResponse(status_code=200, headers=(), body=b"{}"),
    )


class TestExpiry:
    async def test_a_reservation_expires(self) -> None:
        """The property that keeps a crashed worker from wedging a key.

        Without it, a request that dies between `reserve` and `complete` would
        answer every retry of itself with 409 until the process restarted.
        """
        store = InMemoryIdempotencyStore(reservation_ttl_seconds=0.05)
        await store.reserve("k", "fp")

        await asyncio.sleep(0.06)

        assert await store.reserve("k", "fp") is None

    async def test_a_completed_record_outlives_the_reservation_window(self) -> None:
        """The two TTLs are genuinely separate, not one value used twice."""
        store = InMemoryIdempotencyStore(
            reservation_ttl_seconds=0.05, record_ttl_seconds=30.0
        )
        await store.reserve("k", "fp")
        await store.complete("k", a_record())

        await asyncio.sleep(0.06)

        record = await store.get("k")
        assert record is not None
        assert record.in_progress is False

    async def test_a_completed_record_expires_too(self) -> None:
        store = InMemoryIdempotencyStore(record_ttl_seconds=0.05)
        await store.reserve("k", "fp")
        await store.complete("k", a_record())

        await asyncio.sleep(0.06)

        assert await store.get("k") is None

    async def test_expiry_is_evaluated_on_read(self) -> None:
        """No sweeper task, so nothing holds a reference to a dead store."""
        store = InMemoryIdempotencyStore(reservation_ttl_seconds=0.05)
        await store.reserve("k", "fp")

        await asyncio.sleep(0.06)

        assert await store.get("k") is None


class TestConcurrency:
    async def test_the_lock_makes_reserve_atomic(self) -> None:
        """`dict` mutation is atomic under the GIL; check-then-set is not."""
        store = InMemoryIdempotencyStore()

        results = await asyncio.gather(*(store.reserve("k", "fp") for _ in range(50)))

        assert sum(1 for result in results if result is None) == 1


class TestHousekeeping:
    async def test_clear_empties_the_store(self) -> None:
        store = InMemoryIdempotencyStore()
        await store.reserve("k", "fp")

        await store.clear()

        assert await store.get("k") is None

    async def test_close_is_a_no_op(self) -> None:
        """Present only so the app lifespan needs no `isinstance` check."""
        store = InMemoryIdempotencyStore()
        await store.reserve("k", "fp")

        await store.close()

        assert await store.get("k") is not None

    def test_it_names_itself(self) -> None:
        assert InMemoryIdempotencyStore().name == "memory"
