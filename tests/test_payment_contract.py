"""One suite, run against every adapter.

The adapter pattern only pays off if the two gateways are interchangeable from
above, so the behaviour callers rely on is asserted once and parametrised over
the implementations rather than written twice with two sets of assumptions.
Each adapter participates through an `httpx.MockTransport` that speaks its own
provider's wire format — Stripe's form-encoded requests and flat JSON errors,
PayPal's OAuth handshake and nested order documents — because the translation
between those and this application's types is the whole thing under test.

Provider-specific wire details live in `test_payment_stripe.py` and
`test_payment_paypal.py`. What is here is only what must be true of both.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest

from src.exceptions import BadRequestError
from src.payments.base import (
    ChargeRequest,
    Money,
    PaymentDeclinedError,
    PaymentError,
    PaymentGateway,
    PaymentGatewayUnavailableError,
    PaymentNotFoundError,
    validate_refund_amount,
)
from src.payments.paypal import PayPalGateway
from src.payments.stripe import StripeGateway

USD = "USD"


class FakeStripe:
    """The four Stripe endpoints `StripeGateway` calls, over a dict."""

    def __init__(self) -> None:
        self.intents: dict[str, dict[str, Any]] = {}
        self.decline = False
        self.status_code = 200
        self.transport_error = False
        self._counter = 0

    def handle(self, request: httpx.Request) -> httpx.Response:
        if self.transport_error:
            raise httpx.ConnectError("connection refused", request=request)

        if self.status_code >= 400:
            return httpx.Response(
                self.status_code,
                json={"error": {"type": "api_error", "message": "upstream boom"}},
            )

        path = request.url.path
        if request.method == "POST" and path == "/v1/payment_intents":
            return self._create_intent(request)
        if request.method == "POST" and path == "/v1/refunds":
            return self._create_refund(request)
        if request.method == "GET" and path.startswith("/v1/payment_intents/"):
            return self._get_intent(path.rsplit("/", 1)[-1])

        return httpx.Response(404, json={"error": {"type": "invalid_request_error"}})

    def _form(self, request: httpx.Request) -> dict[str, str]:
        parsed = parse_qs(request.content.decode())
        return {key: values[0] for key, values in parsed.items()}

    def _next_id(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}_{self._counter}"

    def _create_intent(self, request: httpx.Request) -> httpx.Response:
        if self.decline:
            return httpx.Response(
                402,
                json={
                    "error": {
                        "type": "card_error",
                        "code": "card_declined",
                        "message": "Your card was declined.",
                    }
                },
            )

        form = self._form(request)
        # Real idempotency: the same key returns the original intent.
        key = request.headers.get("Idempotency-Key", "")
        for intent in self.intents.values():
            if intent["metadata"].get("reference") == key:
                return httpx.Response(200, json=intent)

        intent = {
            "id": self._next_id("pi"),
            "status": "succeeded",
            "amount": int(form["amount"]),
            "currency": form["currency"],
            "metadata": {"reference": form.get("metadata[reference]", "")},
        }
        self.intents[intent["id"]] = intent
        return httpx.Response(200, json=intent)

    def _create_refund(self, request: httpx.Request) -> httpx.Response:
        form = self._form(request)
        intent = self.intents.get(form["payment_intent"])
        if intent is None:
            return httpx.Response(
                404,
                json={
                    "error": {
                        "type": "invalid_request_error",
                        "code": "resource_missing",
                    }
                },
            )

        amount = int(form.get("amount", intent["amount"]))
        if amount > intent["amount"]:
            return httpx.Response(
                400,
                json={
                    "error": {
                        "type": "invalid_request_error",
                        "code": "amount_too_large",
                        "message": "Refund amount exceeds the charge.",
                    }
                },
            )

        return httpx.Response(
            200,
            json={
                "id": self._next_id("re"),
                "status": "succeeded",
                "amount": amount,
                "currency": intent["currency"],
            },
        )

    def _get_intent(self, intent_id: str) -> httpx.Response:
        intent = self.intents.get(intent_id)
        if intent is None:
            return httpx.Response(
                404,
                json={
                    "error": {
                        "type": "invalid_request_error",
                        "code": "resource_missing",
                    }
                },
            )
        return httpx.Response(200, json=intent)


class FakePayPal:
    """The four PayPal endpoints `PayPalGateway` calls, over a dict."""

    def __init__(self) -> None:
        self.captures: dict[str, dict[str, Any]] = {}
        self.decline = False
        self.status_code = 200
        self.transport_error = False
        self._counter = 0
        self._requests: dict[str, dict[str, Any]] = {}

    def handle(self, request: httpx.Request) -> httpx.Response:
        if self.transport_error:
            raise httpx.ConnectError("connection refused", request=request)

        path = request.url.path
        if path == "/v1/oauth2/token":
            return httpx.Response(
                200, json={"access_token": "mock-paypal-token", "expires_in": 32400}
            )

        if self.status_code >= 400:
            return httpx.Response(
                self.status_code, json={"name": "INTERNAL_SERVER_ERROR"}
            )

        if request.method == "POST" and path == "/v2/checkout/orders":
            return self._create_order(request)
        if path.endswith("/refunds"):
            return self._create_refund(request, path.split("/")[-2])
        if request.method == "GET" and path.startswith("/v2/payments/captures/"):
            return self._get_capture(path.rsplit("/", 1)[-1])

        return httpx.Response(404, json={"name": "RESOURCE_NOT_FOUND"})

    def _next_id(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}{self._counter}"

    def _create_order(self, request: httpx.Request) -> httpx.Response:
        if self.decline:
            return httpx.Response(
                422,
                json={
                    "name": "UNPROCESSABLE_ENTITY",
                    "message": "The instrument was declined.",
                    "details": [{"issue": "INSTRUMENT_DECLINED"}],
                },
            )

        key = request.headers.get("PayPal-Request-Id", "")
        if key in self._requests:
            return httpx.Response(200, json=self._requests[key])

        body = json.loads(request.content.decode())
        unit = body["purchase_units"][0]
        capture = {
            "id": self._next_id("CAP"),
            "status": "COMPLETED",
            "amount": dict(unit["amount"]),
            "custom_id": unit.get("custom_id", ""),
        }
        self.captures[capture["id"]] = capture

        order = {
            "id": self._next_id("ORDER"),
            "status": "COMPLETED",
            "purchase_units": [{"payments": {"captures": [capture]}}],
        }
        self._requests[key] = order
        return httpx.Response(201, json=order)

    def _create_refund(self, request: httpx.Request, capture_id: str) -> httpx.Response:
        capture = self.captures.get(capture_id)
        if capture is None:
            return httpx.Response(404, json={"name": "RESOURCE_NOT_FOUND"})

        body = json.loads(request.content.decode() or "{}")
        amount = body.get("amount", capture["amount"])
        if float(amount["value"]) > float(capture["amount"]["value"]):
            return httpx.Response(
                422,
                json={
                    "name": "UNPROCESSABLE_ENTITY",
                    "message": "Refund exceeds the capture.",
                    "details": [{"issue": "REFUND_AMOUNT_EXCEEDED"}],
                },
            )

        return httpx.Response(
            201,
            json={
                "id": self._next_id("REFUND"),
                "status": "COMPLETED",
                "amount": amount,
            },
        )

    def _get_capture(self, capture_id: str) -> httpx.Response:
        capture = self.captures.get(capture_id)
        if capture is None:
            return httpx.Response(404, json={"name": "RESOURCE_NOT_FOUND"})
        return httpx.Response(200, json=capture)


def _stripe_case() -> tuple[PaymentGateway, FakeStripe]:
    fake = FakeStripe()
    client = httpx.AsyncClient(transport=httpx.MockTransport(fake.handle))
    return StripeGateway(api_key="sk_test_fake", client=client), fake


def _paypal_case() -> tuple[PaymentGateway, FakePayPal]:
    fake = FakePayPal()
    client = httpx.AsyncClient(transport=httpx.MockTransport(fake.handle))
    return (
        PayPalGateway(
            client_id="mock-client-id",
            client_secret="mock-client-secret",
            client=client,
        ),
        fake,
    )


GatewayCase = Callable[[], tuple[PaymentGateway, Any]]

CASES: list[GatewayCase] = [_stripe_case, _paypal_case]


@pytest.fixture(params=CASES, ids=["stripe", "paypal"])
def case(request: pytest.FixtureRequest) -> tuple[PaymentGateway, Any]:
    build: GatewayCase = request.param
    return build()


def charge_request(
    amount_minor: int = 2500, reference: str = "order-1"
) -> ChargeRequest:
    return ChargeRequest(
        amount=Money(amount_minor=amount_minor, currency=USD),
        payment_method_token="mock-payment-method-token",
        reference=reference,
        description="One widget",
    )


async def test_every_adapter_satisfies_the_protocol(
    case: tuple[PaymentGateway, Any],
) -> None:
    gateway, _ = case
    assert isinstance(gateway, PaymentGateway)
    assert gateway.name in {"stripe", "paypal"}


async def test_charge_returns_a_settled_payment(
    case: tuple[PaymentGateway, Any],
) -> None:
    gateway, _ = case
    payment = await gateway.charge(charge_request())

    assert payment.provider == gateway.name
    assert payment.status == "succeeded"
    assert payment.is_settled
    assert payment.provider_payment_id
    assert payment.amount == Money(amount_minor=2500, currency=USD)
    assert payment.reference == "order-1"
    # The provider's own vocabulary is preserved rather than thrown away.
    assert payment.provider_status


async def test_charge_is_idempotent_on_reference(
    case: tuple[PaymentGateway, Any],
) -> None:
    """The same reference twice must charge once.

    A retried checkout is the normal way a customer gets billed twice, and it
    is the reason `ChargeRequest.reference` is required rather than optional.
    Both fakes honour their provider's idempotency header, so this asserts the
    adapter actually sends it.
    """
    gateway, _ = case
    first = await gateway.charge(charge_request())
    second = await gateway.charge(charge_request())

    assert first.provider_payment_id == second.provider_payment_id


async def test_decline_is_distinguishable_from_an_outage(
    case: tuple[PaymentGateway, Any],
) -> None:
    """A refused card is a 402 the caller can act on, not a 5xx."""
    gateway, fake = case
    fake.decline = True

    with pytest.raises(PaymentDeclinedError) as excinfo:
        await gateway.charge(charge_request())

    assert excinfo.value.status_code == 402
    assert excinfo.value.provider == gateway.name
    # The provider's own decline code survives for support to search on.
    assert excinfo.value.decline_code


@pytest.mark.parametrize("status_code", [429, 500, 503])
async def test_retryable_upstream_status_is_unavailable(
    case: tuple[PaymentGateway, Any], status_code: int
) -> None:
    gateway, fake = case
    fake.status_code = status_code

    with pytest.raises(PaymentGatewayUnavailableError) as excinfo:
        await gateway.charge(charge_request())

    assert excinfo.value.status_code == 503


async def test_transport_failure_is_unavailable(
    case: tuple[PaymentGateway, Any],
) -> None:
    gateway, fake = case
    fake.transport_error = True

    with pytest.raises(PaymentGatewayUnavailableError):
        await gateway.charge(charge_request())


async def test_full_refund_returns_the_charged_amount(
    case: tuple[PaymentGateway, Any],
) -> None:
    gateway, _ = case
    payment = await gateway.charge(charge_request())
    refund = await gateway.refund(payment.provider_payment_id)

    assert refund.provider == gateway.name
    assert refund.provider_payment_id == payment.provider_payment_id
    assert refund.status == "succeeded"
    assert refund.amount == payment.amount


async def test_partial_refund_returns_the_partial_amount(
    case: tuple[PaymentGateway, Any],
) -> None:
    gateway, _ = case
    payment = await gateway.charge(charge_request())
    refund = await gateway.refund(
        payment.provider_payment_id, Money(amount_minor=500, currency=USD)
    )

    assert refund.amount == Money(amount_minor=500, currency=USD)


async def test_over_refund_is_rejected_by_the_provider(
    case: tuple[PaymentGateway, Any],
) -> None:
    """Both fakes refuse it, in their provider's own shape.

    The adapters do not pre-check — `validate_refund_amount` is there for
    callers holding the payment — so this asserts the rejection arrives as a
    `PaymentError` rather than as whichever exception the provider's body
    happened to produce.
    """
    gateway, _ = case
    payment = await gateway.charge(charge_request())

    with pytest.raises(PaymentError):
        await gateway.refund(
            payment.provider_payment_id, Money(amount_minor=999_999, currency=USD)
        )


async def test_refund_of_an_unknown_payment_is_a_404(
    case: tuple[PaymentGateway, Any],
) -> None:
    gateway, _ = case

    with pytest.raises(PaymentNotFoundError) as excinfo:
        await gateway.refund("does-not-exist")

    assert excinfo.value.status_code == 404


async def test_get_payment_round_trips_the_charge(
    case: tuple[PaymentGateway, Any],
) -> None:
    gateway, _ = case
    payment = await gateway.charge(charge_request())
    fetched = await gateway.get_payment(payment.provider_payment_id)

    assert fetched.provider_payment_id == payment.provider_payment_id
    assert fetched.amount == payment.amount
    assert fetched.status == payment.status
    assert fetched.reference == payment.reference


async def test_get_payment_of_an_unknown_id_is_a_404(
    case: tuple[PaymentGateway, Any],
) -> None:
    gateway, _ = case

    with pytest.raises(PaymentNotFoundError):
        await gateway.get_payment("does-not-exist")


async def test_zero_decimal_currency_round_trips(
    case: tuple[PaymentGateway, Any],
) -> None:
    """¥1000 must come back as ¥1000 through either provider.

    Stripe takes `1000` and PayPal takes `"1000"`, from the same `Money`. A
    hardcoded division by 100 anywhere in either adapter fails here.
    """
    gateway, _ = case
    request = ChargeRequest(
        amount=Money(amount_minor=1000, currency="JPY"),
        payment_method_token="mock-payment-method-token",
        reference="order-jpy",
    )

    payment = await gateway.charge(request)
    assert payment.amount == Money(amount_minor=1000, currency="JPY")


async def test_unusable_success_body_is_a_payment_error(
    case: tuple[PaymentGateway, Any],
) -> None:
    """A 200 that does not carry an amount is an error, not a zero payment."""
    gateway, _ = case

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/oauth2/token":
            return httpx.Response(
                200, json={"access_token": "mock-paypal-token", "expires_in": 32400}
            )
        return httpx.Response(200, json={"id": "x", "status": "succeeded"})

    broken = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    if gateway.name == "stripe":
        gateway = StripeGateway(api_key="sk_test_fake", client=broken)
    else:
        gateway = PayPalGateway(
            client_id="mock-client-id",
            client_secret="mock-client-secret",
            client=broken,
        )

    with pytest.raises(PaymentError):
        await gateway.get_payment("some-id")


def test_validate_refund_amount_rejects_over_refund() -> None:
    payment_amount = Money(amount_minor=1000, currency=USD)

    with pytest.raises(BadRequestError):
        validate_refund_amount(payment_amount, Money(amount_minor=1001, currency=USD))


def test_validate_refund_amount_rejects_currency_mismatch() -> None:
    with pytest.raises(BadRequestError):
        validate_refund_amount(
            Money(amount_minor=1000, currency=USD),
            Money(amount_minor=100, currency="EUR"),
        )


def test_validate_refund_amount_accepts_a_partial_refund() -> None:
    validate_refund_amount(
        Money(amount_minor=1000, currency=USD), Money(amount_minor=999, currency=USD)
    )
