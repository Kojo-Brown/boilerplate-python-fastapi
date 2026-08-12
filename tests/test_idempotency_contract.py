"""One suite, run against every `IdempotencyStore` implementation.

The point of a contract suite is that the middleware's correctness rests on
promises the *protocol* makes — above all that `reserve` is atomic — and a
promise tested against only one backend is a promise about that backend. Every
store added later runs these same tests by appending one fixture param.

The Redis leg needs a real server. It is skipped when nothing is listening on
`REDIS_URL`, and CI always has one (see the `redis` service in ci.yml), so the
atomicity claim is measured on the real thing on every pull request rather
than asserted against an emulator.
"""

from __future__ import annotations

import asyncio
import os
import socket
import uuid
from collections.abc import AsyncGenerator
from urllib.parse import urlparse

import pytest

from src.idempotency.base import IdempotencyRecord, IdempotencyStore, StoredResponse
from src.idempotency.memory import InMemoryIdempotencyStore
from src.idempotency.redis_store import RedisIdempotencyStore

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


def redis_reachable(url: str = REDIS_URL) -> bool:
    """Cheap liveness probe used to skip, not to assert.

    A TCP connect proves a listener rather than a Redis, which is deliberate:
    if something else is on the port the tests fail loudly instead of skipping
    quietly, and a silent skip is the failure mode that lets a backend rot.
    """
    parsed = urlparse(url)
    try:
        with socket.create_connection(
            (parsed.hostname or "localhost", parsed.port or 6379), timeout=1
        ):
            return True
    except OSError:
        return False


REDIS_SKIP_REASON = f"no Redis listening on {REDIS_URL}"


def a_response(status_code: int = 201, body: bytes = b'{"id":1}') -> StoredResponse:
    return StoredResponse(
        status_code=status_code,
        headers=(("content-type", "application/json"),),
        body=body,
    )


@pytest.fixture(params=["memory", "redis"])
async def store(request: pytest.FixtureRequest) -> AsyncGenerator[IdempotencyStore]:
    """Every implementation of the protocol, one at a time."""
    if request.param == "memory":
        built: IdempotencyStore = InMemoryIdempotencyStore()
    else:
        if not redis_reachable():
            pytest.skip(REDIS_SKIP_REASON)
        # A namespace per test keeps concurrent runs — and a developer's own
        # Redis — from colliding, without flushing a database that may not be
        # ours to flush.
        built = RedisIdempotencyStore.from_url(
            REDIS_URL,
            namespace=f"test-idempotency:{uuid.uuid4()}",
            record_ttl_seconds=60,
            reservation_ttl_seconds=60,
        )

    yield built
    await built.close()


@pytest.fixture
def key() -> str:
    return f"key-{uuid.uuid4()}"


class TestReserve:
    async def test_a_first_reservation_is_granted(
        self, store: IdempotencyStore, key: str
    ) -> None:
        assert await store.reserve(key, "fp") is None

    async def test_a_second_reservation_returns_the_existing_record(
        self, store: IdempotencyStore, key: str
    ) -> None:
        await store.reserve(key, "fp")
        existing = await store.reserve(key, "fp")

        assert existing is not None
        assert existing.fingerprint == "fp"
        assert existing.in_progress is True

    async def test_the_stored_fingerprint_is_the_first_one(
        self, store: IdempotencyStore, key: str
    ) -> None:
        """A losing request must not overwrite what it is being compared against."""
        await store.reserve(key, "original")
        existing = await store.reserve(key, "different")

        assert existing is not None
        assert existing.fingerprint == "original"

    async def test_different_keys_do_not_interfere(
        self, store: IdempotencyStore
    ) -> None:
        assert await store.reserve("key-a", "fp") is None
        assert await store.reserve("key-b", "fp") is None

    async def test_exactly_one_of_many_concurrent_reservations_wins(
        self, store: IdempotencyStore, key: str
    ) -> None:
        """The atomicity claim, measured rather than assumed.

        A `get`-then-`set` implementation passes every other test in this file
        and fails this one, which is precisely the bug that turns a client's
        double-submit into two charges.
        """
        results = await asyncio.gather(*(store.reserve(key, "fp") for _ in range(20)))

        assert sum(1 for result in results if result is None) == 1


class TestComplete:
    async def test_a_completed_record_replaces_the_reservation(
        self, store: IdempotencyStore, key: str
    ) -> None:
        await store.reserve(key, "fp")
        response = a_response()
        await store.complete(key, IdempotencyRecord("fp", response))

        existing = await store.reserve(key, "fp")
        assert existing is not None
        assert existing.in_progress is False
        assert existing.response == response

    async def test_the_response_survives_verbatim(
        self, store: IdempotencyStore, key: str
    ) -> None:
        response = StoredResponse(
            status_code=207,
            headers=(("content-type", "application/json"), ("x-trace", "abc")),
            body=b"\x00\x01\x02 binary \xff",
        )
        await store.reserve(key, "fp")
        await store.complete(key, IdempotencyRecord("fp", response))

        record = await store.get(key)
        assert record is not None
        assert record.response == response


class TestRelease:
    async def test_release_frees_the_key(
        self, store: IdempotencyStore, key: str
    ) -> None:
        await store.reserve(key, "fp")
        await store.release(key)

        assert await store.reserve(key, "fp") is None

    async def test_release_of_an_unknown_key_is_not_an_error(
        self, store: IdempotencyStore, key: str
    ) -> None:
        """Called from an exception path, where a second failure helps nobody."""
        await store.release(key)


class TestGet:
    async def test_an_unknown_key_is_none(
        self, store: IdempotencyStore, key: str
    ) -> None:
        assert await store.get(key) is None

    async def test_a_reservation_reads_back_as_in_progress(
        self, store: IdempotencyStore, key: str
    ) -> None:
        await store.reserve(key, "fp")

        record = await store.get(key)
        assert record is not None
        assert record.in_progress is True


class TestProtocolConformance:
    async def test_every_store_satisfies_the_protocol(
        self, store: IdempotencyStore
    ) -> None:
        assert isinstance(store, IdempotencyStore)

    async def test_every_store_names_itself(self, store: IdempotencyStore) -> None:
        assert store.name in {"memory", "redis"}

    async def test_close_is_idempotent(self, store: IdempotencyStore) -> None:
        """The fixture closes it again on teardown, which must also be safe."""
        await store.close()
