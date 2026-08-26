"""Middleware semantics, exercised against a purpose-built app.

The routes here exist to make one thing observable: *did the handler run?* A
counter that only a real execution increments is what separates "the client got
a 201 twice" from "the client got one 201 twice", and that distinction is the
entire feature. Testing this against the real auth routes instead would mostly
be testing registration.

The store is the in-memory one throughout — `test_idempotency_contract.py`
already proves the two backends behave alike, so repeating every case against
Redis would only make this file slower.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections import Counter
from collections.abc import AsyncGenerator

import pytest
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from httpx import ASGITransport, AsyncClient
from starlette.types import Message, Receive, Scope, Send

from src.idempotency.base import (
    IDEMPOTENCY_KEY_HEADER,
    IDEMPOTENCY_REPLAYED_HEADER,
    IdempotencyStore,
    IdempotencyStoreUnavailableError,
)
from src.idempotency.memory import InMemoryIdempotencyStore
from src.middleware.idempotency import IdempotencyConfig, IdempotencyMiddleware


class Harness:
    """The app under test plus the counters that reveal whether it ran."""

    def __init__(self, store: IdempotencyStore, config: IdempotencyConfig) -> None:
        self.store = store
        self.calls: Counter[str] = Counter()
        # Set when `/api/gated` is entered, cleared by `release`. Lets a test
        # observe the in-flight window without sleeping through it.
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.app = self._build(config)

    def _build(self, config: IdempotencyConfig) -> FastAPI:
        app = FastAPI()

        @app.post("/api/orders")
        async def create_order(request: Request) -> JSONResponse:
            self.calls["orders"] += 1
            payload = await request.body()
            return JSONResponse(
                status_code=201,
                content={"executions": self.calls["orders"], "echo": payload.decode()},
                headers={"X-Custom": "kept"},
            )

        @app.get("/api/orders")
        async def list_orders() -> dict[str, int]:
            self.calls["list"] += 1
            return {"executions": self.calls["list"]}

        @app.post("/internal/orders")
        async def internal_order() -> dict[str, int]:
            self.calls["internal"] += 1
            return {"executions": self.calls["internal"]}

        @app.post("/api/status/{code}")
        async def with_status(code: int) -> Response:
            self.calls[f"status-{code}"] += 1
            return Response(status_code=code, content=b'{"seen":true}')

        @app.post("/api/boom")
        async def boom() -> None:
            self.calls["boom"] += 1
            raise RuntimeError("handler exploded")

        @app.post("/api/gated")
        async def gated() -> dict[str, int]:
            self.calls["gated"] += 1
            self.entered.set()
            await self.release.wait()
            return {"executions": self.calls["gated"]}

        @app.post("/api/large")
        async def large() -> Response:
            self.calls["large"] += 1
            return Response(content=b"x" * 4096, media_type="text/plain")

        @app.post("/api/cookie")
        async def cookie() -> Response:
            self.calls["cookie"] += 1
            response = Response(content=b'{"ok":true}', media_type="application/json")
            response.set_cookie("session", "mock-session-value")
            return response

        @app.post("/api/stream")
        async def stream() -> StreamingResponse:
            self.calls["stream"] += 1

            async def chunks() -> AsyncGenerator[bytes]:
                yield b'{"part":1,'
                yield b'"part":2}'

            return StreamingResponse(chunks(), media_type="application/json")

        app.add_middleware(IdempotencyMiddleware, store=self.store, config=config)
        return app


@pytest.fixture
def store() -> InMemoryIdempotencyStore:
    return InMemoryIdempotencyStore()


@pytest.fixture
def config() -> IdempotencyConfig:
    return IdempotencyConfig()


@pytest.fixture
def harness(store: InMemoryIdempotencyStore, config: IdempotencyConfig) -> Harness:
    return Harness(store, config)


@pytest.fixture
async def client(harness: Harness) -> AsyncGenerator[AsyncClient]:
    async with AsyncClient(
        transport=ASGITransport(app=harness.app), base_url="http://test"
    ) as connection:
        yield connection


@pytest.fixture
def key() -> str:
    return str(uuid.uuid4())


def headers(key: str, **extra: str) -> dict[str, str]:
    return {IDEMPOTENCY_KEY_HEADER: key, **extra}


class TestReplay:
    async def test_a_repeated_key_does_not_run_the_handler_again(
        self, client: AsyncClient, harness: Harness, key: str
    ) -> None:
        first = await client.post("/api/orders", json={"a": 1}, headers=headers(key))
        second = await client.post("/api/orders", json={"a": 1}, headers=headers(key))

        assert first.status_code == second.status_code == 201
        assert harness.calls["orders"] == 1

    async def test_the_replayed_body_is_byte_identical(
        self, client: AsyncClient, key: str
    ) -> None:
        first = await client.post("/api/orders", json={"a": 1}, headers=headers(key))
        second = await client.post("/api/orders", json={"a": 1}, headers=headers(key))

        assert second.content == first.content
        assert second.json()["executions"] == 1

    async def test_response_headers_are_replayed(
        self, client: AsyncClient, key: str
    ) -> None:
        await client.post("/api/orders", json={"a": 1}, headers=headers(key))
        replay = await client.post("/api/orders", json={"a": 1}, headers=headers(key))

        assert replay.headers["x-custom"] == "kept"
        assert replay.headers["content-type"].startswith("application/json")

    async def test_a_replay_is_labelled(self, client: AsyncClient, key: str) -> None:
        """A client that cannot tell a replay from an execution learns nothing."""
        first = await client.post("/api/orders", json={"a": 1}, headers=headers(key))
        second = await client.post("/api/orders", json={"a": 1}, headers=headers(key))

        assert IDEMPOTENCY_REPLAYED_HEADER.lower() not in first.headers
        assert second.headers[IDEMPOTENCY_REPLAYED_HEADER.lower()] == "true"

    async def test_the_key_is_echoed_on_both(
        self, client: AsyncClient, key: str
    ) -> None:
        first = await client.post("/api/orders", json={"a": 1}, headers=headers(key))
        second = await client.post("/api/orders", json={"a": 1}, headers=headers(key))

        assert first.headers[IDEMPOTENCY_KEY_HEADER.lower()] == key
        assert second.headers[IDEMPOTENCY_KEY_HEADER.lower()] == key

    async def test_a_streamed_response_replays_as_one_body(
        self, client: AsyncClient, harness: Harness, key: str
    ) -> None:
        """Chunks are reassembled, so a replay is the whole document."""
        first = await client.post("/api/stream", headers=headers(key))
        second = await client.post("/api/stream", headers=headers(key))

        assert first.content == b'{"part":1,"part":2}'
        assert second.content == first.content
        assert harness.calls["stream"] == 1

    async def test_different_keys_execute_separately(
        self, client: AsyncClient, harness: Harness
    ) -> None:
        await client.post("/api/orders", json={"a": 1}, headers=headers("key-one"))
        await client.post("/api/orders", json={"a": 1}, headers=headers("key-two"))

        assert harness.calls["orders"] == 2


class TestPassthrough:
    async def test_a_request_without_a_key_is_untouched(
        self, client: AsyncClient, harness: Harness
    ) -> None:
        """The header is optional; nothing changes for clients that ignore it."""
        first = await client.post("/api/orders", json={"a": 1})
        second = await client.post("/api/orders", json={"a": 1})

        assert harness.calls["orders"] == 2
        assert first.json()["executions"] == 1
        assert second.json()["executions"] == 2
        assert IDEMPOTENCY_KEY_HEADER.lower() not in first.headers

    async def test_safe_methods_are_not_deduplicated(
        self, client: AsyncClient, harness: Harness, key: str
    ) -> None:
        """GET is already idempotent; storing its responses would be a cache."""
        await client.get("/api/orders", headers=headers(key))
        await client.get("/api/orders", headers=headers(key))

        assert harness.calls["list"] == 2

    async def test_paths_outside_the_prefix_are_not_deduplicated(
        self, client: AsyncClient, harness: Harness, key: str
    ) -> None:
        await client.post("/internal/orders", headers=headers(key))
        await client.post("/internal/orders", headers=headers(key))

        assert harness.calls["internal"] == 2

    async def test_the_request_body_reaches_the_handler_intact(
        self, client: AsyncClient, key: str
    ) -> None:
        """The middleware consumes the body to fingerprint it and must replay it."""
        response = await client.post(
            "/api/orders", json={"deeply": {"nested": [1, 2, 3]}}, headers=headers(key)
        )

        assert json.loads(response.json()["echo"]) == {"deeply": {"nested": [1, 2, 3]}}

    async def test_disabling_the_middleware_disables_deduplication(
        self, store: InMemoryIdempotencyStore, key: str
    ) -> None:
        harness = Harness(store, IdempotencyConfig(enabled=False))
        async with AsyncClient(
            transport=ASGITransport(app=harness.app), base_url="http://test"
        ) as client:
            await client.post("/api/orders", json={"a": 1}, headers=headers(key))
            await client.post("/api/orders", json={"a": 1}, headers=headers(key))

        assert harness.calls["orders"] == 2


class TestKeyReuse:
    async def test_a_different_body_under_the_same_key_is_refused(
        self, client: AsyncClient, harness: Harness, key: str
    ) -> None:
        """Answering with the first request's response would be worse than 422."""
        await client.post("/api/orders", json={"a": 1}, headers=headers(key))
        conflict = await client.post("/api/orders", json={"a": 2}, headers=headers(key))

        assert conflict.status_code == 422
        assert conflict.json()["error"] == "IDEMPOTENCY_KEY_REUSED"
        assert harness.calls["orders"] == 1

    async def test_a_different_path_under_the_same_key_is_refused(
        self, client: AsyncClient, key: str
    ) -> None:
        await client.post("/api/orders", json={"a": 1}, headers=headers(key))
        conflict = await client.post("/api/status/200", headers=headers(key))

        assert conflict.status_code == 422

    async def test_a_different_query_string_under_the_same_key_is_refused(
        self, client: AsyncClient, key: str
    ) -> None:
        await client.post("/api/orders?page=1", json={"a": 1}, headers=headers(key))
        conflict = await client.post(
            "/api/orders?page=2", json={"a": 1}, headers=headers(key)
        )

        assert conflict.status_code == 422

    async def test_unrelated_headers_do_not_count_as_a_different_request(
        self, client: AsyncClient, harness: Harness, key: str
    ) -> None:
        """A retry through another proxy, or with a re-issued token, still replays."""
        await client.post(
            "/api/orders",
            json={"a": 1},
            headers=headers(key, **{"X-Request-ID": "first"}),
        )
        replay = await client.post(
            "/api/orders",
            json={"a": 1},
            headers=headers(key, **{"X-Request-ID": "second"}),
        )

        assert replay.status_code == 201
        assert replay.headers[IDEMPOTENCY_REPLAYED_HEADER.lower()] == "true"
        assert harness.calls["orders"] == 1

    async def test_reuse_is_refused_even_while_the_first_is_in_flight(
        self, client: AsyncClient, harness: Harness, key: str
    ) -> None:
        """422 beats 409 here, so the answer does not depend on timing."""
        first = asyncio.create_task(client.post("/api/gated", headers=headers(key)))
        await harness.entered.wait()

        conflict = await client.post("/api/orders", json={"a": 1}, headers=headers(key))

        harness.release.set()
        await first

        assert conflict.status_code == 422


class TestConcurrentExecution:
    async def test_a_second_request_while_the_first_runs_gets_409(
        self, client: AsyncClient, harness: Harness, key: str
    ) -> None:
        first = asyncio.create_task(client.post("/api/gated", headers=headers(key)))
        await harness.entered.wait()

        conflict = await client.post("/api/gated", headers=headers(key))

        assert conflict.status_code == 409
        assert conflict.json()["error"] == "IDEMPOTENCY_KEY_IN_PROGRESS"
        assert conflict.headers["retry-after"] == "1"

        harness.release.set()
        assert (await first).status_code == 200
        assert harness.calls["gated"] == 1

    async def test_the_key_is_replayable_once_the_first_finishes(
        self, client: AsyncClient, harness: Harness, key: str
    ) -> None:
        first = asyncio.create_task(client.post("/api/gated", headers=headers(key)))
        await harness.entered.wait()
        harness.release.set()
        await first

        replay = await client.post("/api/gated", headers=headers(key))

        assert replay.status_code == 200
        assert replay.headers[IDEMPOTENCY_REPLAYED_HEADER.lower()] == "true"
        assert harness.calls["gated"] == 1


class TestInvalidKeys:
    @pytest.mark.parametrize("bad", ["", "has space", "k" * 256])
    async def test_a_malformed_key_is_a_400(
        self, client: AsyncClient, harness: Harness, bad: str
    ) -> None:
        response = await client.post("/api/orders", json={"a": 1}, headers=headers(bad))

        assert response.status_code == 400
        assert response.json()["error"] == "IDEMPOTENCY_KEY_INVALID"
        assert harness.calls["orders"] == 0

    async def test_the_error_envelope_matches_the_rest_of_the_api(
        self, client: AsyncClient
    ) -> None:
        """Middleware short-circuits never reach the exception handlers."""
        response = await client.post(
            "/api/orders", json={"a": 1}, headers=headers("has space")
        )

        assert set(response.json()) >= {"error", "message", "status"}
        assert response.json()["status"] == 400
        assert response.headers["content-type"].startswith("application/json")


class TestWhatIsNotStored:
    @pytest.mark.parametrize("code", [500, 502, 503])
    async def test_server_errors_are_not_replayed(
        self, client: AsyncClient, harness: Harness, key: str, code: int
    ) -> None:
        """A client retrying a 503 has to be allowed to actually run."""
        await client.post(f"/api/status/{code}", headers=headers(key))
        second = await client.post(f"/api/status/{code}", headers=headers(key))

        assert harness.calls[f"status-{code}"] == 2
        assert IDEMPOTENCY_REPLAYED_HEADER.lower() not in second.headers

    @pytest.mark.parametrize("code", [408, 425, 429])
    async def test_retryable_statuses_are_not_replayed(
        self, client: AsyncClient, harness: Harness, key: str, code: int
    ) -> None:
        """Storing a 429 would answer the retry it invites with the same 429."""
        await client.post(f"/api/status/{code}", headers=headers(key))
        await client.post(f"/api/status/{code}", headers=headers(key))

        assert harness.calls[f"status-{code}"] == 2

    @pytest.mark.parametrize("code", [200, 201, 204, 400, 404, 409, 422])
    async def test_client_errors_and_successes_are_replayed(
        self, client: AsyncClient, harness: Harness, key: str, code: int
    ) -> None:
        """A deterministic 404 is an outcome, and replaying it is the point."""
        await client.post(f"/api/status/{code}", headers=headers(key))
        second = await client.post(f"/api/status/{code}", headers=headers(key))

        assert harness.calls[f"status-{code}"] == 1
        assert second.status_code == code

    async def test_an_exploding_handler_releases_the_key(
        self, client: AsyncClient, harness: Harness, key: str
    ) -> None:
        """Otherwise the retry meets a reservation nothing will ever complete."""
        for _ in range(2):
            with pytest.raises(RuntimeError):
                await client.post("/api/boom", headers=headers(key))

        assert harness.calls["boom"] == 2

    async def test_set_cookie_is_not_replayed(
        self, client: AsyncClient, key: str
    ) -> None:
        """A session cookie is minted for one caller, not for whoever replays."""
        first = await client.post("/api/cookie", headers=headers(key))
        second = await client.post("/api/cookie", headers=headers(key))

        assert "set-cookie" in first.headers
        assert "set-cookie" not in second.headers
        assert second.json() == first.json()

    async def test_an_oversized_response_is_returned_but_not_stored(
        self, store: InMemoryIdempotencyStore, key: str
    ) -> None:
        harness = Harness(store, IdempotencyConfig(max_response_body_bytes=1024))
        async with AsyncClient(
            transport=ASGITransport(app=harness.app), base_url="http://test"
        ) as client:
            first = await client.post("/api/large", headers=headers(key))
            second = await client.post("/api/large", headers=headers(key))

        assert len(first.content) == 4096
        assert len(second.content) == 4096
        assert harness.calls["large"] == 2


class TestOversizedRequests:
    async def test_a_body_over_the_cap_is_passed_through(
        self, store: InMemoryIdempotencyStore, key: str
    ) -> None:
        """Refusing a large upload over an optional header would be worse.

        The trade is explicit: requests above the cap are not deduplicated,
        and the handler still receives every byte.
        """
        harness = Harness(store, IdempotencyConfig(max_request_body_bytes=64))
        payload = {"blob": "x" * 512}
        async with AsyncClient(
            transport=ASGITransport(app=harness.app), base_url="http://test"
        ) as client:
            first = await client.post("/api/orders", json=payload, headers=headers(key))
            second = await client.post(
                "/api/orders", json=payload, headers=headers(key)
            )

        assert harness.calls["orders"] == 2
        assert json.loads(first.json()["echo"]) == payload
        assert json.loads(second.json()["echo"]) == payload


class FailingStore:
    """A store that is always unreachable. Stands in for a Redis outage."""

    name = "failing"

    async def reserve(self, key: str, fingerprint: str) -> None:
        raise IdempotencyStoreUnavailableError("down")

    async def complete(self, key: str, record: object) -> None:
        raise IdempotencyStoreUnavailableError("down")

    async def release(self, key: str) -> None:
        raise IdempotencyStoreUnavailableError("down")

    async def get(self, key: str) -> None:
        raise IdempotencyStoreUnavailableError("down")

    async def close(self) -> None:
        return None


class TestStoreOutage:
    async def test_fail_closed_returns_503(self, key: str) -> None:
        """The default. A payments route would rather 503 than double-charge."""
        harness = Harness(FailingStore(), IdempotencyConfig())  # type: ignore[arg-type]
        async with AsyncClient(
            transport=ASGITransport(app=harness.app), base_url="http://test"
        ) as client:
            response = await client.post("/api/orders", json={}, headers=headers(key))

        assert response.status_code == 503
        assert response.json()["error"] == "IDEMPOTENCY_STORE_UNAVAILABLE"
        assert response.headers["retry-after"] == "1"
        assert harness.calls["orders"] == 0

    async def test_fail_open_serves_the_request_undeduplicated(self, key: str) -> None:
        """For deployments where losing the request beats executing it twice."""
        harness = Harness(FailingStore(), IdempotencyConfig(fail_open=True))  # type: ignore[arg-type]
        async with AsyncClient(
            transport=ASGITransport(app=harness.app), base_url="http://test"
        ) as client:
            first = await client.post("/api/orders", json={}, headers=headers(key))
            second = await client.post("/api/orders", json={}, headers=headers(key))

        assert first.status_code == second.status_code == 201
        assert harness.calls["orders"] == 2

    async def test_a_store_failure_after_the_response_is_not_a_failure(
        self, key: str
    ) -> None:
        """The response is already on the wire; there is nothing left to fail.

        `complete` raising leaves the caller with a normal 201 and the next
        retry re-executing — the behaviour it had before idempotency existed.
        """

        class FailsOnlyOnComplete(InMemoryIdempotencyStore):
            async def complete(self, key: str, record: object) -> None:  # type: ignore[override]
                raise IdempotencyStoreUnavailableError("down")

        harness = Harness(FailsOnlyOnComplete(), IdempotencyConfig())
        async with AsyncClient(
            transport=ASGITransport(app=harness.app), base_url="http://test"
        ) as client:
            response = await client.post("/api/orders", json={}, headers=headers(key))

        assert response.status_code == 201


class TestCallerScoping:
    async def test_one_caller_cannot_replay_another_callers_response(
        self, client: AsyncClient, harness: Harness, key: str
    ) -> None:
        """Keys are client-chosen, so two callers will eventually pick the same one."""
        alice = await client.post(
            "/api/orders",
            json={"a": 1},
            headers=headers(key, Authorization="Bearer mock-token-alice"),
        )
        bob = await client.post(
            "/api/orders",
            json={"a": 1},
            headers=headers(key, Authorization="Bearer mock-token-bob"),
        )

        assert harness.calls["orders"] == 2
        assert alice.json()["executions"] == 1
        assert bob.json()["executions"] == 2
        assert IDEMPOTENCY_REPLAYED_HEADER.lower() not in bob.headers

    async def test_the_same_caller_still_replays(
        self, client: AsyncClient, harness: Harness, key: str
    ) -> None:
        token = {"Authorization": "Bearer mock-token-alice"}
        await client.post("/api/orders", json={"a": 1}, headers=headers(key, **token))
        replay = await client.post(
            "/api/orders", json={"a": 1}, headers=headers(key, **token)
        )

        assert harness.calls["orders"] == 1
        assert replay.headers[IDEMPOTENCY_REPLAYED_HEADER.lower()] == "true"

    async def test_an_authenticated_caller_does_not_share_the_anon_namespace(
        self, client: AsyncClient, harness: Harness, key: str
    ) -> None:
        await client.post("/api/orders", json={"a": 1}, headers=headers(key))
        authenticated = await client.post(
            "/api/orders",
            json={"a": 1},
            headers=headers(key, Authorization="Bearer mock-token-alice"),
        )

        assert harness.calls["orders"] == 2
        assert IDEMPOTENCY_REPLAYED_HEADER.lower() not in authenticated.headers


class TestAsgiEdges:
    """Cases that need the ASGI interface directly rather than an HTTP client."""

    async def test_a_disconnect_mid_body_is_forwarded(self) -> None:
        """The client hung up while uploading. The app must still see that.

        Swallowing the `http.disconnect` would leave a handler waiting on a
        body that is never coming.
        """
        seen: list[dict[str, object]] = []

        async def app(scope: object, receive: object, send: object) -> None:
            while True:
                message = await receive()  # type: ignore[operator]
                seen.append(message)
                if message["type"] == "http.disconnect":
                    break
            await send({"type": "http.response.start", "status": 204, "headers": []})  # type: ignore[operator]
            await send({"type": "http.response.body", "body": b""})  # type: ignore[operator]

        middleware = IdempotencyMiddleware(app, store=InMemoryIdempotencyStore())  # type: ignore[arg-type]
        incoming = iter(
            [
                {"type": "http.request", "body": b'{"partial":', "more_body": True},
                {"type": "http.disconnect"},
            ]
        )

        async def receive() -> dict[str, object]:
            return next(incoming)

        sent: list[dict[str, object]] = []

        async def send(message: dict[str, object]) -> None:
            sent.append(message)

        await middleware(
            {
                "type": "http",
                "method": "POST",
                "path": "/api/orders",
                "query_string": b"",
                "headers": [(b"idempotency-key", b"k1")],
            },
            receive,
            send,
        )

        assert [message["type"] for message in seen] == [
            "http.request",
            "http.disconnect",
        ]
        assert sent[0]["status"] == 204

    async def test_a_failing_release_does_not_break_the_response(
        self, key: str
    ) -> None:
        """`release` runs on the error path, where a second failure helps nobody."""

        class FailsOnlyOnRelease(InMemoryIdempotencyStore):
            async def release(self, key: str) -> None:
                raise IdempotencyStoreUnavailableError("down")

        harness = Harness(FailsOnlyOnRelease(), IdempotencyConfig())
        async with AsyncClient(
            transport=ASGITransport(app=harness.app), base_url="http://test"
        ) as client:
            response = await client.post("/api/status/503", headers=headers(key))

        assert response.status_code == 503
        assert harness.calls["status-503"] == 1

    async def test_the_reservation_is_released_despite_a_second_cancellation(
        self,
    ) -> None:
        """The release runs on a task somebody has already cancelled.

        A client disconnect cancels the handler and this middleware starts
        releasing the reservation; a shutdown draining its tasks — or an
        enclosing `TaskGroup` aborting — cancels it *again*, in the middle of
        that release. Without `finalize` the reservation survives the request
        and answers every retry with 409 until its TTL runs out, which is the
        outcome the release exists to prevent.
        """

        class SlowRelease(InMemoryIdempotencyStore):
            def __init__(self) -> None:
                super().__init__()
                self.completed: list[str] = []

            async def release(self, key: str) -> None:
                await asyncio.sleep(0.05)
                await super().release(key)
                self.completed.append(key)

        store = SlowRelease()
        entered = asyncio.Event()

        async def app(scope: Scope, receive: Receive, send: Send) -> None:
            entered.set()
            await asyncio.sleep(30)

        middleware = IdempotencyMiddleware(app, store=store)

        async def receive() -> Message:
            return {"type": "http.request", "body": b"{}", "more_body": False}

        async def send(message: Message) -> None:  # pragma: no cover
            raise AssertionError("no response is produced on this path")

        scope: Scope = {
            "type": "http",
            "method": "POST",
            "path": "/api/orders",
            "query_string": b"",
            "headers": [(b"idempotency-key", b"k-cancel")],
        }
        task = asyncio.ensure_future(middleware(scope, receive, send))
        await entered.wait()

        # Twice: one cancellation is caught by the middleware's own handler,
        # and it takes a second to cut the release that handler started.
        for _ in range(2):
            task.cancel()
            await asyncio.sleep(0.01)

        with pytest.raises(asyncio.CancelledError):
            await task

        assert len(store.completed) == 1

    async def test_a_non_http_scope_is_passed_through(self) -> None:
        """Lifespan and websocket scopes have no headers to inspect."""
        calls: list[str] = []

        async def app(scope: dict[str, object], receive: object, send: object) -> None:
            calls.append(str(scope["type"]))

        middleware = IdempotencyMiddleware(app, store=InMemoryIdempotencyStore())  # type: ignore[arg-type]

        await middleware({"type": "lifespan"}, _unused_receive, _unused_send)  # type: ignore[arg-type]

        assert calls == ["lifespan"]


async def _unused_receive() -> dict[str, object]:  # pragma: no cover - never called
    raise AssertionError("receive must not be read on a passthrough scope")


async def _unused_send(message: dict[str, object]) -> None:  # pragma: no cover
    raise AssertionError("send must not be used on a passthrough scope")
