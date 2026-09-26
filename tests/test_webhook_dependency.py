"""The route parameter, over real HTTP.

`test_webhook_verifier.py` proves the semantics against the verifier directly.
This file asks the narrower question that file cannot: does a route that declares
`VerifiedWebhookDep` actually refuse a forged delivery, does the handler receive
the bytes that were signed, and does a rejection come out as this API's ordinary
error envelope rather than a stack trace?

The app is built here rather than reusing `src.main` because the default
application mounts no receiving route — the dependency is the shipped pattern,
and this is that pattern wired up exactly as `src/webhooks/dependencies.py`
documents it.
"""

from __future__ import annotations

import json
from collections.abc import AsyncGenerator

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from starlette.requests import Request

from src.config import settings
from src.exception_handlers import app_exception_handler
from src.exceptions import AppException
from src.webhooks.dependencies import VerifiedWebhookDep, read_signed_body
from src.webhooks.errors import WebhookPayloadTooLargeError
from src.webhooks.factory import get_webhook_verifier
from src.webhooks.replay import InMemoryReplayGuard
from src.webhooks.secrets import parse_signing_secrets
from src.webhooks.signature import sign
from src.webhooks.verifier import WebhookVerifier

SECRET = "insecure-test-webhook-secret-notreal"
HEADER = "X-Webhook-Signature"
NOW = 1_700_000_000.0
TOLERANCE = 300


@pytest.fixture
def guard() -> InMemoryReplayGuard:
    return InMemoryReplayGuard(ttl_seconds=float(TOLERANCE * 2))


@pytest.fixture
def app(guard: InMemoryReplayGuard) -> FastAPI:
    """A minimal receiver, wired the way the documentation says to wire one."""
    built = FastAPI()
    built.add_exception_handler(AppException, app_exception_handler)  # type: ignore[arg-type]

    @built.post("/hooks")
    async def receive(delivery: VerifiedWebhookDep) -> dict[str, object]:
        # Parsed inside the handler, after authentication — see
        # `src/webhooks/dependencies.py` on why there is no model parameter.
        event = json.loads(delivery.body)
        return {"key_id": delivery.key_id, "event_id": event["id"]}

    built.dependency_overrides[get_webhook_verifier] = lambda: WebhookVerifier(
        secrets=parse_signing_secrets(f"test:{SECRET}"),
        tolerance_seconds=TOLERANCE,
        replay_guard=guard,
        clock=lambda: NOW,
    )
    return built


@pytest.fixture
async def client(app: FastAPI) -> AsyncGenerator[AsyncClient]:
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as built:
        yield built


def a_body(event_id: str = "evt_1") -> bytes:
    return json.dumps({"event": "order.paid", "id": event_id}).encode()


class TestAGenuineDelivery:
    async def test_it_reaches_the_handler(self, client: AsyncClient) -> None:
        body = a_body()

        response = await client.post(
            "/hooks", content=body, headers={HEADER: sign(SECRET, int(NOW), body)}
        )

        assert response.status_code == 200
        assert response.json() == {"key_id": "test", "event_id": "evt_1"}

    async def test_the_handler_sees_the_bytes_that_were_signed(
        self, client: AsyncClient
    ) -> None:
        """Not a re-encoding of them.

        The body here has whitespace a re-serialisation would drop, so a handler
        reading anything other than the delivered bytes would report a different
        event — or the signature would not have matched in the first place.
        """
        body = b'{"event": "order.paid",   "id": "evt_spaced"}'

        response = await client.post(
            "/hooks", content=body, headers={HEADER: sign(SECRET, int(NOW), body)}
        )

        assert response.json()["event_id"] == "evt_spaced"


class TestRejections:
    async def test_no_signature_header_is_a_401(self, client: AsyncClient) -> None:
        response = await client.post("/hooks", content=a_body())

        assert response.status_code == 401
        assert response.json()["error"] == "WEBHOOK_SIGNATURE_MISSING"

    async def test_a_malformed_header_is_a_400(self, client: AsyncClient) -> None:
        response = await client.post(
            "/hooks", content=a_body(), headers={HEADER: "not-a-signature"}
        )

        assert response.status_code == 400
        assert response.json()["error"] == "WEBHOOK_SIGNATURE_MALFORMED"

    async def test_a_forged_signature_is_a_401(self, client: AsyncClient) -> None:
        body = a_body()

        response = await client.post(
            "/hooks",
            content=body,
            headers={
                HEADER: sign("a-forged-secret-of-adequate-len!!!!", int(NOW), body)
            },
        )

        assert response.status_code == 401
        assert response.json()["error"] == "WEBHOOK_SIGNATURE_MISMATCH"

    async def test_a_tampered_body_is_a_401(self, client: AsyncClient) -> None:
        """The delivery an attacker actually wants to send."""
        signature = sign(SECRET, int(NOW), a_body("evt_1"))

        response = await client.post(
            "/hooks", content=a_body("evt_2"), headers={HEADER: signature}
        )

        assert response.status_code == 401

    async def test_an_expired_delivery_is_a_401(self, client: AsyncClient) -> None:
        body = a_body()

        response = await client.post(
            "/hooks",
            content=body,
            headers={HEADER: sign(SECRET, int(NOW) - 10_000, body)},
        )

        assert response.status_code == 401
        payload = response.json()
        assert payload["error"] == "WEBHOOK_TIMESTAMP_OUTSIDE_WINDOW"
        assert payload["details"]["tolerance_seconds"] == TOLERANCE

    async def test_a_replay_is_a_409(self, client: AsyncClient) -> None:
        body = a_body()
        headers = {HEADER: sign(SECRET, int(NOW), body)}
        assert (
            await client.post("/hooks", content=body, headers=headers)
        ).status_code == 200

        response = await client.post("/hooks", content=body, headers=headers)

        assert response.status_code == 409
        assert response.json()["error"] == "WEBHOOK_REPLAYED"

    async def test_the_handler_does_not_run_on_a_rejection(
        self, app: FastAPI, client: AsyncClient
    ) -> None:
        """The whole point of a dependency rather than a check inside the body.

        The route's handler would raise `KeyError` on this payload, which has no
        `id`. A 401 rather than a 500 says it was never entered.
        """
        response = await client.post(
            "/hooks", content=b'{"no":"id"}', headers={HEADER: "t=1,v1=" + "0" * 64}
        )

        assert response.status_code == 401


class TestTheBodyCap:
    """The cap, asserted on `read_signed_body` rather than by posting a megabyte.

    A cap exists to bound the work an unauthenticated caller can ask for, so
    every case here is about what is refused *before* the HMAC runs.
    """

    @staticmethod
    def a_request(body: bytes, *, declared: bytes | None = None) -> Request:
        async def receive() -> dict[str, object]:
            return {"type": "http.request", "body": body, "more_body": False}

        headers = [] if declared is None else [(b"content-length", declared)]
        scope = {"type": "http", "method": "POST", "path": "/", "headers": headers}
        return Request(scope, receive)

    async def test_an_oversized_declared_length_is_refused(self) -> None:
        """Refused on the header, so the body is never read."""
        request = self.a_request(b"x" * 64, declared=b"64")

        with pytest.raises(WebhookPayloadTooLargeError) as caught:
            await read_signed_body(request, max_bytes=32)

        assert caught.value.status_code == 413
        assert caught.value.details == {"max_bytes": 32}

    async def test_an_oversized_body_with_no_declared_length_is_refused(self) -> None:
        """`Content-Length` is a claim: absent under chunked, and able to lie.

        The check after the read is the one that is true; the one before it is
        the one that is cheap.
        """
        request = self.a_request(b"x" * 64)

        with pytest.raises(WebhookPayloadTooLargeError):
            await read_signed_body(request, max_bytes=32)

    async def test_a_body_that_undercuts_its_declared_length_is_still_measured(
        self,
    ) -> None:
        """A header claiming 1 byte does not license 64."""
        request = self.a_request(b"x" * 64, declared=b"1")

        with pytest.raises(WebhookPayloadTooLargeError):
            await read_signed_body(request, max_bytes=32)

    async def test_a_non_numeric_declared_length_does_not_crash(self) -> None:
        """A junk header must not be read as a size, nor reach `int()`."""
        request = self.a_request(b"x" * 8, declared=b"banana")

        assert await read_signed_body(request, max_bytes=32) == b"x" * 8

    async def test_a_body_at_the_cap_is_accepted(self) -> None:
        request = self.a_request(b"x" * 32, declared=b"32")

        assert len(await read_signed_body(request, max_bytes=32)) == 32

    async def test_the_shipped_cap_is_what_the_dependency_uses(self) -> None:
        """The dependency reads `WEBHOOK_MAX_BODY_BYTES`, not a literal."""
        assert settings.WEBHOOK_MAX_BODY_BYTES == 1_048_576
