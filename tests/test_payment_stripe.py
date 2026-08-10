"""Stripe wire format.

What the contract suite cannot see: the exact request Stripe receives. A charge
that produces the right `Payment` from a fake but sends `amount=25.00` in a
JSON body would pass every test in `test_payment_contract.py` and fail against
the real API, so the encoding, the headers and the error mapping are asserted
here directly.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest

from src.payments.base import (
    ChargeRequest,
    Money,
    PaymentDeclinedError,
    PaymentError,
    PaymentGatewayUnavailableError,
    PaymentNotFoundError,
)
from src.payments.stripe import StripeGateway


class Recorder:
    """Captures the requests made and replays canned responses."""

    def __init__(self, *responses: httpx.Response) -> None:
        self.requests: list[httpx.Request] = []
        self._responses = list(responses)

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if len(self._responses) == 1:
            return self._responses[0]
        return self._responses.pop(0)

    @property
    def last_form(self) -> dict[str, str]:
        parsed = parse_qs(self.requests[-1].content.decode())
        return {key: values[0] for key, values in parsed.items()}


def build(*responses: httpx.Response) -> tuple[StripeGateway, Recorder]:
    recorder = Recorder(*responses)
    client = httpx.AsyncClient(transport=httpx.MockTransport(recorder.handle))
    gateway = StripeGateway(
        api_key="sk_test_fake", base_url="https://stripe.test", client=client
    )
    return gateway, recorder


def intent_body(**overrides: Any) -> dict[str, Any]:
    document: dict[str, Any] = {
        "id": "pi_fake_1",
        "status": "succeeded",
        "amount": 2500,
        "currency": "usd",
        "metadata": {"reference": "order-1"},
    }
    document.update(overrides)
    return document


def charge_request(**overrides: Any) -> ChargeRequest:
    fields: dict[str, Any] = {
        "amount": Money(amount_minor=2500, currency="USD"),
        "payment_method_token": "pm_mock_token",
        "reference": "order-1",
    }
    fields.update(overrides)
    return ChargeRequest(**fields)


async def test_charge_posts_form_encoded_minor_units() -> None:
    gateway, recorder = build(httpx.Response(200, json=intent_body()))

    await gateway.charge(charge_request(description="One widget"))

    request = recorder.requests[-1]
    assert request.method == "POST"
    assert str(request.url) == "https://stripe.test/v1/payment_intents"
    assert request.headers["content-type"] == "application/x-www-form-urlencoded"

    form = recorder.last_form
    assert form["amount"] == "2500"
    assert form["currency"] == "usd"
    assert form["payment_method"] == "pm_mock_token"
    assert form["confirm"] == "true"
    assert form["off_session"] == "true"
    assert form["description"] == "One widget"
    assert form["metadata[reference]"] == "order-1"


async def test_charge_sends_auth_and_idempotency_headers() -> None:
    gateway, recorder = build(httpx.Response(200, json=intent_body()))

    await gateway.charge(charge_request())

    headers = recorder.requests[-1].headers
    assert headers["authorization"] == "Bearer sk_test_fake"
    assert headers["idempotency-key"] == "order-1"


async def test_charge_flattens_metadata_and_receipt_email() -> None:
    gateway, recorder = build(httpx.Response(200, json=intent_body()))

    await gateway.charge(
        charge_request(
            customer_email="shopper@example.test",
            metadata={"cart_id": "cart-9"},
        )
    )

    form = recorder.last_form
    assert form["receipt_email"] == "shopper@example.test"
    assert form["metadata[cart_id]"] == "cart-9"


async def test_zero_decimal_currency_is_not_scaled() -> None:
    """¥1000 goes over the wire as 1000, not 100000."""
    gateway, recorder = build(
        httpx.Response(200, json=intent_body(amount=1000, currency="jpy"))
    )

    payment = await gateway.charge(
        charge_request(amount=Money(amount_minor=1000, currency="JPY"))
    )

    assert recorder.last_form["amount"] == "1000"
    assert payment.amount == Money(amount_minor=1000, currency="JPY")


async def test_requires_action_is_pending_not_failed() -> None:
    """A 3-D Secure challenge is unfinished, not refused.

    Mapping it to `failed` would tell a shopper their card was declined when
    the bank merely wants a second factor; mapping it to `succeeded` would ship
    goods against money that has not moved.
    """
    gateway, _ = build(httpx.Response(200, json=intent_body(status="requires_action")))

    payment = await gateway.charge(charge_request())

    assert payment.status == "pending"
    assert payment.provider_status == "requires_action"
    assert not payment.is_settled


async def test_unknown_status_degrades_to_pending() -> None:
    gateway, _ = build(
        httpx.Response(200, json=intent_body(status="requires_teleportation"))
    )

    payment = await gateway.charge(charge_request())

    assert payment.status == "pending"
    assert payment.provider_status == "requires_teleportation"


async def test_card_error_becomes_a_decline_with_its_code() -> None:
    gateway, _ = build(
        httpx.Response(
            402,
            json={
                "error": {
                    "type": "card_error",
                    "code": "insufficient_funds",
                    "message": "Your card has insufficient funds.",
                }
            },
        )
    )

    with pytest.raises(PaymentDeclinedError) as excinfo:
        await gateway.charge(charge_request())

    assert excinfo.value.decline_code == "insufficient_funds"
    assert "insufficient funds" in str(excinfo.value).lower()


async def test_invalid_request_error_is_a_payment_error() -> None:
    """A 400 that is not a decline must not be reported as one.

    Telling a shopper their card was declined when the integration sent a bad
    parameter sends them to their bank over a bug in this process.
    """
    gateway, _ = build(
        httpx.Response(
            400,
            json={
                "error": {
                    "type": "invalid_request_error",
                    "code": "parameter_unknown",
                    "message": "Received unknown parameter: widget",
                }
            },
        )
    )

    with pytest.raises(PaymentError) as excinfo:
        await gateway.charge(charge_request())

    assert not isinstance(excinfo.value, PaymentDeclinedError)
    assert excinfo.value.status_code == 502


async def test_resource_missing_is_a_not_found() -> None:
    gateway, _ = build(
        httpx.Response(
            404,
            json={
                "error": {
                    "type": "invalid_request_error",
                    "code": "resource_missing",
                    "message": "No such payment_intent",
                }
            },
        )
    )

    with pytest.raises(PaymentNotFoundError) as excinfo:
        await gateway.get_payment("pi_missing")

    assert excinfo.value.details == {
        "provider": "stripe",
        "payment_id": "pi_missing",
    }


async def test_non_json_body_is_a_payment_error() -> None:
    gateway, _ = build(httpx.Response(200, text="<html>gateway timeout</html>"))

    with pytest.raises(PaymentError):
        await gateway.get_payment("pi_fake_1")


async def test_json_array_body_is_a_payment_error() -> None:
    gateway, _ = build(httpx.Response(200, json=[1, 2, 3]))

    with pytest.raises(PaymentError):
        await gateway.get_payment("pi_fake_1")


async def test_rate_limit_is_unavailable_and_not_a_decline() -> None:
    gateway, _ = build(httpx.Response(429, json={"error": {"type": "api_error"}}))

    with pytest.raises(PaymentGatewayUnavailableError):
        await gateway.charge(charge_request())


async def test_refund_targets_the_intent_and_keys_on_it() -> None:
    gateway, recorder = build(
        httpx.Response(
            200,
            json={
                "id": "re_fake_1",
                "status": "succeeded",
                "amount": 500,
                "currency": "usd",
            },
        )
    )

    refund = await gateway.refund("pi_fake_1", Money(amount_minor=500, currency="USD"))

    form = recorder.last_form
    assert form == {"payment_intent": "pi_fake_1", "amount": "500"}
    assert recorder.requests[-1].headers["idempotency-key"] == "refund:pi_fake_1:500"
    assert refund.provider_refund_id == "re_fake_1"
    assert refund.amount == Money(amount_minor=500, currency="USD")


async def test_full_refund_sends_no_amount_and_keys_on_full() -> None:
    """Stripe computes the remaining balance; this adapter must not.

    Sending a locally computed total would be wrong after any earlier partial
    refund, and the mistake only shows up on the second refund of a payment.
    """
    gateway, recorder = build(
        httpx.Response(
            200,
            json={
                "id": "re_fake_2",
                "status": "pending",
                "amount": 2500,
                "currency": "usd",
            },
        )
    )

    refund = await gateway.refund("pi_fake_1")

    assert "amount" not in recorder.last_form
    assert recorder.requests[-1].headers["idempotency-key"] == "refund:pi_fake_1:full"
    assert refund.status == "pending"


async def test_refund_without_an_amount_in_the_response_is_an_error() -> None:
    gateway, _ = build(httpx.Response(200, json={"id": "re_x", "status": "succeeded"}))

    with pytest.raises(PaymentError):
        await gateway.refund("pi_fake_1")


async def test_get_payment_reads_the_reference_from_metadata() -> None:
    gateway, recorder = build(httpx.Response(200, json=intent_body()))

    payment = await gateway.get_payment("pi_fake_1")

    assert recorder.requests[-1].method == "GET"
    assert str(recorder.requests[-1].url).endswith("/v1/payment_intents/pi_fake_1")
    assert payment.reference == "order-1"
    # A read is not an idempotent write; sending a key would be noise.
    assert "idempotency-key" not in recorder.requests[-1].headers


async def test_transport_error_is_unavailable() -> None:
    def explode(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timed out", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(explode))
    gateway = StripeGateway(api_key="sk_test_fake", client=client)

    with pytest.raises(PaymentGatewayUnavailableError):
        await gateway.charge(charge_request())


async def test_gateway_closes_only_the_client_it_owns() -> None:
    injected = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={}))
    )
    borrower = StripeGateway(api_key="sk_test_fake", client=injected)

    await borrower.aclose()
    assert not injected.is_closed

    owner = StripeGateway(api_key="sk_test_fake")
    created = owner._get_client()
    await owner.aclose()
    assert created.is_closed
    # Twice is safe.
    await owner.aclose()

    await injected.aclose()
