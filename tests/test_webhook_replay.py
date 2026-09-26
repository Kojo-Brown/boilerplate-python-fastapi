"""The in-process replay guard: claiming, forgetting, and not growing forever."""

from __future__ import annotations

import asyncio

from src.webhooks.replay import _PURGE_THRESHOLD, InMemoryReplayGuard, ReplayGuard


class TestClaiming:
    async def test_a_first_claim_succeeds(self) -> None:
        guard = InMemoryReplayGuard()

        assert await guard.claim("fp") is True

    async def test_a_second_claim_of_the_same_fingerprint_fails(self) -> None:
        """The whole point: a delivery is acceptable once."""
        guard = InMemoryReplayGuard()
        await guard.claim("fp")

        assert await guard.claim("fp") is False

    async def test_different_fingerprints_do_not_interfere(self) -> None:
        guard = InMemoryReplayGuard()

        assert await guard.claim("a") is True
        assert await guard.claim("b") is True

    async def test_concurrent_claims_of_one_fingerprint_yield_one_winner(self) -> None:
        """The case a `get`-then-`set` pair gets wrong.

        Two copies of one delivery arriving together would both find nothing and
        both proceed, which is precisely the situation a replay guard exists for.
        """
        guard = InMemoryReplayGuard()

        results = await asyncio.gather(*(guard.claim("fp") for _ in range(20)))

        assert results.count(True) == 1
        assert results.count(False) == 19


class TestExpiry:
    async def test_a_claim_is_forgotten_after_its_ttl(self) -> None:
        """Necessary, or the dict is a leak. Dangerous if it happens too soon —
        see the verifier's TTL check, which is what stops it."""
        guard = InMemoryReplayGuard(ttl_seconds=0.05)
        await guard.claim("fp")

        await asyncio.sleep(0.06)

        assert await guard.claim("fp") is True

    async def test_a_claim_survives_up_to_its_ttl(self) -> None:
        guard = InMemoryReplayGuard(ttl_seconds=5.0)
        await guard.claim("fp")

        await asyncio.sleep(0.02)

        assert await guard.claim("fp") is False

    async def test_expiry_is_evaluated_on_read(self) -> None:
        """No sweeper task, so nothing holds a reference to a dead guard."""
        guard = InMemoryReplayGuard(ttl_seconds=0.05)
        await guard.claim("fp")

        await asyncio.sleep(0.06)
        # Re-claimed above would also prove it, but this asserts that the read
        # itself is what notices, with no other claim having touched the dict.
        assert await guard.claim("fp") is True

    async def test_the_ttl_is_reported(self) -> None:
        """Part of the contract: the verifier checks it at construction."""
        assert InMemoryReplayGuard(ttl_seconds=123.0).ttl_seconds == 123.0


class TestGrowth:
    async def test_expired_entries_are_purged_once_the_dict_is_large(self) -> None:
        """Bounded by TTL times arrival rate, but only if expiry is collected.

        Claims are only ever written after a signature verifies, so this is
        about a busy endpoint's steady state rather than about an attack.
        """
        guard = InMemoryReplayGuard(ttl_seconds=0.01)
        for index in range(_PURGE_THRESHOLD):
            await guard.claim(f"fp-{index}")
        assert len(guard._claims) == _PURGE_THRESHOLD

        await asyncio.sleep(0.02)
        await guard.claim("the-one-that-triggers-the-sweep")

        assert len(guard._claims) == 1

    async def test_live_entries_survive_a_purge(self) -> None:
        """A sweep must not be a way to make a replay acceptable."""
        guard = InMemoryReplayGuard(ttl_seconds=60.0)
        for index in range(_PURGE_THRESHOLD):
            await guard.claim(f"fp-{index}")

        await guard.claim("one-more")

        assert await guard.claim("fp-0") is False


class TestHousekeeping:
    async def test_the_name_is_reported(self) -> None:
        assert InMemoryReplayGuard().name == "memory"

    async def test_close_is_a_no_op(self) -> None:
        """Present so the lifespan needs no `isinstance`."""
        guard = InMemoryReplayGuard()
        await guard.close()

        assert await guard.claim("fp") is True

    async def test_clear_drops_every_claim(self) -> None:
        guard = InMemoryReplayGuard()
        await guard.claim("fp")

        await guard.clear()

        assert await guard.claim("fp") is True

    async def test_it_satisfies_the_protocol(self) -> None:
        assert isinstance(InMemoryReplayGuard(), ReplayGuard)
