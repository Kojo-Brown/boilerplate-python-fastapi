"""The middleware as it is actually wired into `src.main`.

`test_idempotency_middleware.py` proves the semantics against routes built to
expose them. This file asks the narrower question that file cannot: is the
thing installed on the real application, in the right place in the stack, and
does a real route stop executing twice because of it?
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator, Iterator

import pytest
from httpx import AsyncClient

from src.config import settings
from src.idempotency.base import (
    IDEMPOTENCY_KEY_HEADER,
    IDEMPOTENCY_REPLAYED_HEADER,
)
from src.idempotency.factory import get_idempotency_store
from src.idempotency.memory import InMemoryIdempotencyStore
from src.limiter import limiter
from src.main import app
from src.middleware.idempotency import IdempotencyMiddleware
from src.middleware.request_id import REQUEST_ID_HEADER, RequestIDMiddleware
from tests.fakes import InMemoryUserStore


@pytest.fixture(autouse=True)
def reset_limiter() -> Iterator[None]:
    """`/auth/register` is limited to 5/minute per address across the suite."""
    storage = limiter._limiter
    if hasattr(storage, "storage") and hasattr(storage.storage, "reset"):
        storage.storage.reset()
    yield


@pytest.fixture(autouse=True)
async def clean_store() -> AsyncGenerator[None]:
    """The app's store is process-wide, so tests must not inherit each other's."""
    store = get_idempotency_store()
    assert isinstance(store, InMemoryIdempotencyStore)
    await store.clear()
    yield
    await store.clear()


@pytest.fixture
def key() -> str:
    return str(uuid.uuid4())


def _middleware_classes() -> list[type]:
    return [entry.cls for entry in app.user_middleware]


class TestWiring:
    def test_the_middleware_is_installed(self) -> None:
        assert IdempotencyMiddleware in _middleware_classes()

    def test_it_runs_inside_the_request_id_middleware(self) -> None:
        """Starlette runs the first entry outermost.

        The order is load-bearing rather than incidental: idempotency logs need
        the *replaying* request's id bound, and a replayed response must be
        stamped with a fresh `X-Request-ID` rather than the original one.
        """
        classes = _middleware_classes()

        assert classes.index(RequestIDMiddleware) < classes.index(IdempotencyMiddleware)

    def test_it_is_configured_from_settings(self) -> None:
        entry = next(
            entry for entry in app.user_middleware if entry.cls is IdempotencyMiddleware
        )
        config = entry.kwargs["config"]

        assert config.enabled is settings.IDEMPOTENCY_ENABLED
        assert config.fail_open is settings.IDEMPOTENCY_FAIL_OPEN
        assert config.max_request_body_bytes == settings.IDEMPOTENCY_MAX_BODY_BYTES


class TestRealRoute:
    async def test_a_retried_registration_creates_one_user(
        self,
        fake_backed_client: AsyncClient,
        user_store: InMemoryUserStore,
        key: str,
    ) -> None:
        """The whole feature in one assertion: two requests, one side effect."""
        payload = {"email": "retry@example.com", "password": "password123"}

        first = await fake_backed_client.post(
            "/api/v1/auth/register",
            json=payload,
            headers={IDEMPOTENCY_KEY_HEADER: key},
        )
        second = await fake_backed_client.post(
            "/api/v1/auth/register",
            json=payload,
            headers={IDEMPOTENCY_KEY_HEADER: key},
        )

        assert first.status_code == 201
        assert second.status_code == 201
        assert second.json() == first.json()
        assert [user.email for user in user_store.users] == ["retry@example.com"]

    async def test_the_replay_is_labelled_and_gets_a_fresh_request_id(
        self, fake_backed_client: AsyncClient, key: str
    ) -> None:
        """Proof of the stack ordering, observed from outside.

        `X-Request-ID` is added by the outer middleware after this one returns,
        so it must differ between the original and its replay — a stored one
        would point a support engineer at the wrong request.
        """
        payload = {"email": "labelled@example.com", "password": "password123"}
        headers = {IDEMPOTENCY_KEY_HEADER: key}

        first = await fake_backed_client.post(
            "/api/v1/auth/register", json=payload, headers=headers
        )
        second = await fake_backed_client.post(
            "/api/v1/auth/register", json=payload, headers=headers
        )

        assert IDEMPOTENCY_REPLAYED_HEADER.lower() not in first.headers
        assert second.headers[IDEMPOTENCY_REPLAYED_HEADER.lower()] == "true"
        assert (
            second.headers[REQUEST_ID_HEADER.lower()]
            != first.headers[REQUEST_ID_HEADER.lower()]
        )

    async def test_a_reused_key_with_a_new_payload_is_refused(
        self,
        fake_backed_client: AsyncClient,
        user_store: InMemoryUserStore,
        key: str,
    ) -> None:
        headers = {IDEMPOTENCY_KEY_HEADER: key}

        await fake_backed_client.post(
            "/api/v1/auth/register",
            json={"email": "one@example.com", "password": "password123"},
            headers=headers,
        )
        conflict = await fake_backed_client.post(
            "/api/v1/auth/register",
            json={"email": "two@example.com", "password": "password123"},
            headers=headers,
        )

        assert conflict.status_code == 422
        assert conflict.json()["error"] == "IDEMPOTENCY_KEY_REUSED"
        assert [user.email for user in user_store.users] == ["one@example.com"]

    async def test_registration_without_a_key_is_unchanged(
        self, fake_backed_client: AsyncClient, user_store: InMemoryUserStore
    ) -> None:
        """Existing clients see no behaviour change; the header is opt-in."""
        response = await fake_backed_client.post(
            "/api/v1/auth/register",
            json={"email": "plain@example.com", "password": "password123"},
        )

        assert response.status_code == 201
        assert IDEMPOTENCY_KEY_HEADER.lower() not in response.headers
        assert [user.email for user in user_store.users] == ["plain@example.com"]

    async def test_a_failed_registration_is_replayed_not_re_executed(
        self,
        fake_backed_client: AsyncClient,
        user_store: InMemoryUserStore,
        key: str,
    ) -> None:
        """A deterministic 4xx is an outcome, and the client asked for it once."""
        payload = {"email": "dup@example.com", "password": "password123"}
        headers = {IDEMPOTENCY_KEY_HEADER: key}

        await fake_backed_client.post("/api/v1/auth/register", json=payload)
        first = await fake_backed_client.post(
            "/api/v1/auth/register", json=payload, headers=headers
        )
        second = await fake_backed_client.post(
            "/api/v1/auth/register", json=payload, headers=headers
        )

        assert first.status_code == 409
        assert second.status_code == 409
        assert second.headers[IDEMPOTENCY_REPLAYED_HEADER.lower()] == "true"
        assert len(user_store.users) == 1


class TestUnaffectedRoutes:
    async def test_health_is_not_touched(
        self, async_client: AsyncClient, key: str
    ) -> None:
        """A GET outside `/api/` — two of the reasons to skip a request at once."""
        first = await async_client.get("/health", headers={IDEMPOTENCY_KEY_HEADER: key})
        second = await async_client.get(
            "/health", headers={IDEMPOTENCY_KEY_HEADER: key}
        )

        assert first.status_code == second.status_code == 200
        assert IDEMPOTENCY_REPLAYED_HEADER.lower() not in second.headers
