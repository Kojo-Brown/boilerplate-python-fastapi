"""PayPal wire format and token lifecycle.

Everything the contract suite cannot see: the OAuth handshake and its caching,
the JSON body shape, decimal-string amounts, and the fact that a charge returns
a *capture* id rather than the order id the response leads with. The token
clock is injected so an expiry can be asserted without a test that waits nine
hours for one.
"""

from __future__ import annotations

import json
from typing import Any

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
from src.payments.paypal import PayPalGateway

TOKEN_PATH = "/v1/oauth2/token"


class Recorder:
    """Answers the token endpoint and replays canned responses for the rest."""

    def __init__(self, *responses: httpx.Response, expires_in: float = 32400) -> None:
        self.requests: list[httpx.Request] = []
        self.token_calls = 0
        self._responses = list(responses)
        self._expires_in = expires_in

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)

        if request.url.path == TOKEN_PATH:
            self.token_calls += 1
            return httpx.Response(
                200,
                json={
                    "access_token": f"mock-paypal-token-{self.token_calls}",
                    "expires_in": self._expires_in,
                },
            )

        if len(self._responses) == 1:
            return self._responses[0]
        return self._responses.pop(0)

    @property
    def api_requests(self) -> list[httpx.Request]:
        return [r for r in self.requests if r.url.path != TOKEN_PATH]

    @property
    def last_body(self) -> dict[str, Any]:
        content = self.api_requests[-1].content.decode()
        parsed: dict[str, Any] = json.loads(content) if content else {}
        return parsed


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def build(
    *responses: httpx.Response, expires_in: float = 32400
) -> tuple[PayPalGateway, Recorder, FakeClock]:
    recorder = Recorder(*responses, expires_in=expires_in)
    clock = FakeClock()
    client = httpx.AsyncClient(transport=httpx.MockTransport(recorder.handle))
    gateway = PayPalGateway(
        client_id="mock-client-id",
        client_secret="mock-client-secret",
        base_url="https://paypal.test",
        client=client,
        clock=clock,
    )
    return gateway, recorder, clock


def capture(**overrides: Any) -> dict[str, Any]:
    document: dict[str, Any] = {
        "id": "CAP-1",
        "status": "COMPLETED",
        "amount": {"currency_code": "USD", "value": "25.00"},
        "custom_id": "order-1",
    }
    document.update(overrides)
    return document


def order_body(**capture_overrides: Any) -> dict[str, Any]:
    return {
        "id": "ORDER-1",
        "status": "COMPLETED",
        "purchase_units": [{"payments": {"captures": [capture(**capture_overrides)]}}],
    }


def charge_request(**overrides: Any) -> ChargeRequest:
    fields: dict[str, Any] = {
        "amount": Money(amount_minor=2500, currency="USD"),
        "payment_method_token": "mock-setup-token",
        "reference": "order-1",
    }
    fields.update(overrides)
    return ChargeRequest(**fields)


async def test_charge_posts_json_with_decimal_string_amounts() -> None:
    gateway, recorder, _ = build(httpx.Response(201, json=order_body()))

    await gateway.charge(charge_request(description="One widget"))

    request = recorder.api_requests[-1]
    assert request.method == "POST"
    assert str(request.url) == "https://paypal.test/v2/checkout/orders"
    assert request.headers["content-type"] == "application/json"

    body = recorder.last_body
    assert body["intent"] == "CAPTURE"
    unit = body["purchase_units"][0]
    # 2500 minor units, not the integer, and not "25.0".
    assert unit["amount"] == {"currency_code": "USD", "value": "25.00"}
    assert unit["reference_id"] == "order-1"
    assert unit["custom_id"] == "order-1"
    assert unit["description"] == "One widget"
    assert body["payment_source"]["token"]["id"] == "mock-setup-token"


async def test_charge_sends_the_paypal_idempotency_header() -> None:
    gateway, recorder, _ = build(httpx.Response(201, json=order_body()))

    await gateway.charge(charge_request())

    assert recorder.api_requests[-1].headers["paypal-request-id"] == "order-1"


async def test_zero_decimal_currency_has_no_decimal_places() -> None:
    """¥1000 is `"1000"`, and `"10.00"` would be a hundredfold error."""
    gateway, recorder, _ = build(
        httpx.Response(
            201,
            json=order_body(amount={"currency_code": "JPY", "value": "1000"}),
        )
    )

    payment = await gateway.charge(
        charge_request(amount=Money(amount_minor=1000, currency="JPY"))
    )

    unit = recorder.last_body["purchase_units"][0]
    assert unit["amount"] == {"currency_code": "JPY", "value": "1000"}
    assert payment.amount == Money(amount_minor=1000, currency="JPY")


async def test_three_decimal_currency_keeps_its_third_place() -> None:
    gateway, recorder, _ = build(
        httpx.Response(
            201,
            json=order_body(amount={"currency_code": "KWD", "value": "1.234"}),
        )
    )

    payment = await gateway.charge(
        charge_request(amount=Money(amount_minor=1234, currency="KWD"))
    )

    unit = recorder.last_body["purchase_units"][0]
    assert unit["amount"]["value"] == "1.234"
    assert payment.amount == Money(amount_minor=1234, currency="KWD")


async def test_charge_returns_the_capture_id_not_the_order_id() -> None:
    """The refundable handle is the capture's.

    Returning `ORDER-1` here would work all the way through checkout and fail
    at the first refund, with an id that looks perfectly valid.
    """
    gateway, _, _ = build(httpx.Response(201, json=order_body()))

    payment = await gateway.charge(charge_request())

    assert payment.provider_payment_id == "CAP-1"
    assert payment.status == "succeeded"


async def test_capture_status_wins_over_order_status() -> None:
    """An order is COMPLETED as soon as it is captured — the capture may not be.

    A PENDING capture is money PayPal has not released, so treating the order's
    COMPLETED as settlement would ship goods against a review hold.
    """
    gateway, _, _ = build(httpx.Response(201, json=order_body(status="PENDING")))

    payment = await gateway.charge(charge_request())

    assert payment.status == "pending"
    assert payment.provider_status == "PENDING"
    assert not payment.is_settled


async def test_order_without_a_capture_is_pending_on_the_order_id() -> None:
    gateway, _, _ = build(
        httpx.Response(201, json={"id": "ORDER-2", "status": "PAYER_ACTION_REQUIRED"})
    )

    payment = await gateway.charge(charge_request())

    assert payment.status == "pending"
    assert payment.provider_payment_id == "ORDER-2"
    assert payment.amount == Money(amount_minor=2500, currency="USD")


async def test_junk_purchase_units_are_skipped_rather_than_crashing() -> None:
    """A malformed order must not raise `TypeError` out of the adapter.

    PayPal has more shapes for `purchase_units` than the capture flow uses —
    an authorization instead of a capture, for one — and none of them should
    turn a charge into a 500 with a traceback from a list comprehension.
    """
    gateway, _, _ = build(
        httpx.Response(
            201,
            json={
                "id": "ORDER-4",
                "status": "APPROVED",
                "purchase_units": [
                    "not-a-dict",
                    {"payments": "not-a-dict"},
                    {"payments": {"authorizations": [{"id": "AUTH-1"}]}},
                ],
            },
        )
    )

    payment = await gateway.charge(charge_request())

    assert payment.status == "pending"
    assert payment.provider_payment_id == "ORDER-4"


async def test_amount_without_a_value_is_a_payment_error() -> None:
    gateway, _, _ = build(
        httpx.Response(200, json={"id": "CAP-1", "status": "COMPLETED", "amount": {}})
    )

    with pytest.raises(PaymentError):
        await gateway.get_payment("CAP-1")


async def test_unknown_order_status_degrades_to_pending() -> None:
    gateway, _, _ = build(httpx.Response(201, json={"id": "ORDER-3", "status": "WAT"}))

    payment = await gateway.charge(charge_request())

    assert payment.status == "pending"
    assert payment.provider_status == "WAT"


async def test_unknown_capture_status_degrades_to_pending() -> None:
    gateway, _, _ = build(httpx.Response(201, json=order_body(status="WAT")))

    payment = await gateway.charge(charge_request())

    assert payment.status == "pending"


async def test_instrument_declined_is_a_decline_not_an_error() -> None:
    gateway, _, _ = build(
        httpx.Response(
            422,
            json={
                "name": "UNPROCESSABLE_ENTITY",
                "message": "The instrument presented was either declined.",
                "details": [{"issue": "INSTRUMENT_DECLINED"}],
            },
        )
    )

    with pytest.raises(PaymentDeclinedError) as excinfo:
        await gateway.charge(charge_request())

    assert excinfo.value.decline_code == "INSTRUMENT_DECLINED"
    assert excinfo.value.status_code == 402


async def test_other_422_issues_are_not_declines() -> None:
    """PayPal returns 422 for validation faults too.

    Treating every 422 as a decline would tell shoppers their card failed
    whenever this integration sent a malformed body.
    """
    gateway, _, _ = build(
        httpx.Response(
            422,
            json={
                "name": "UNPROCESSABLE_ENTITY",
                "message": "Currency not supported for this merchant.",
                "details": [{"issue": "CURRENCY_NOT_SUPPORTED"}],
            },
        )
    )

    with pytest.raises(PaymentError) as excinfo:
        await gateway.charge(charge_request())

    assert not isinstance(excinfo.value, PaymentDeclinedError)


async def test_malformed_details_do_not_crash_the_error_mapping() -> None:
    """A failure path that raises `TypeError` hides the real failure."""
    gateway, _, _ = build(
        httpx.Response(400, json={"name": "INVALID_REQUEST", "details": "not-a-list"})
    )

    with pytest.raises(PaymentError):
        await gateway.charge(charge_request())


async def test_token_is_fetched_once_and_reused() -> None:
    gateway, recorder, _ = build(httpx.Response(201, json=order_body()))

    await gateway.charge(charge_request())
    await gateway.charge(charge_request(reference="order-2"))

    assert recorder.token_calls == 1
    assert recorder.api_requests[-1].headers["authorization"] == (
        "Bearer mock-paypal-token-1"
    )


async def test_token_is_refetched_after_it_expires() -> None:
    """The skew matters: a token valid at check time and stale on arrival fails.

    `expires_in=120` with a 60-second skew means the cache is good for 60
    seconds, so t=59 reuses and t=61 refetches.
    """
    gateway, recorder, clock = build(
        httpx.Response(201, json=order_body()), expires_in=120
    )

    await gateway.charge(charge_request())
    clock.now = 59.0
    await gateway.charge(charge_request(reference="order-2"))
    assert recorder.token_calls == 1

    clock.now = 61.0
    await gateway.charge(charge_request(reference="order-3"))
    assert recorder.token_calls == 2


async def test_a_401_triggers_exactly_one_reauth_and_a_replay() -> None:
    gateway, recorder, _ = build(
        httpx.Response(401, json={"name": "NOT_AUTHORIZED"}),
        httpx.Response(201, json=order_body()),
    )

    payment = await gateway.charge(charge_request())

    assert payment.provider_payment_id == "CAP-1"
    assert recorder.token_calls == 2
    # The replay carries the new token, not the revoked one.
    assert recorder.api_requests[-1].headers["authorization"] == (
        "Bearer mock-paypal-token-2"
    )


async def test_a_second_401_is_not_retried_again() -> None:
    """Wrong credentials must not turn into a loop against the token endpoint."""
    gateway, recorder, _ = build(httpx.Response(401, json={"name": "NOT_AUTHORIZED"}))

    with pytest.raises(PaymentError):
        await gateway.charge(charge_request())

    assert recorder.token_calls == 2
    assert len(recorder.api_requests) == 2


async def test_bad_credentials_are_a_configuration_error_not_an_outage() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"error": "invalid_client"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    gateway = PayPalGateway(
        client_id="mock-client-id", client_secret="wrong", client=client
    )

    with pytest.raises(PaymentError) as excinfo:
        await gateway.charge(charge_request())

    # Retrying will not fix a wrong secret, so this is not "unavailable".
    assert not isinstance(excinfo.value, PaymentGatewayUnavailableError)


async def test_token_endpoint_outage_is_unavailable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="try later")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    gateway = PayPalGateway(
        client_id="mock-client-id", client_secret="mock-client-secret", client=client
    )

    with pytest.raises(PaymentGatewayUnavailableError):
        await gateway.charge(charge_request())


async def test_token_response_without_a_token_is_a_payment_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"scope": "https://uri.paypal.test/"})

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    gateway = PayPalGateway(
        client_id="mock-client-id", client_secret="mock-client-secret", client=client
    )

    with pytest.raises(PaymentError):
        await gateway.charge(charge_request())


async def test_refund_targets_the_capture_endpoint() -> None:
    gateway, recorder, _ = build(
        httpx.Response(
            201,
            json={
                "id": "REFUND-1",
                "status": "COMPLETED",
                "amount": {"currency_code": "USD", "value": "5.00"},
            },
        )
    )

    refund = await gateway.refund("CAP-1", Money(amount_minor=500, currency="USD"))

    request = recorder.api_requests[-1]
    assert str(request.url) == (
        "https://paypal.test/v2/payments/captures/CAP-1/refunds"
    )
    assert recorder.last_body == {"amount": {"currency_code": "USD", "value": "5.00"}}
    assert request.headers["paypal-request-id"] == "refund:CAP-1:500"
    assert refund.amount == Money(amount_minor=500, currency="USD")
    assert refund.provider_refund_id == "REFUND-1"


async def test_full_refund_sends_an_empty_body() -> None:
    gateway, recorder, _ = build(
        httpx.Response(
            201,
            json={
                "id": "REFUND-2",
                "status": "PENDING",
                "amount": {"currency_code": "USD", "value": "25.00"},
            },
        )
    )

    refund = await gateway.refund("CAP-1")

    assert recorder.last_body == {}
    assert recorder.api_requests[-1].headers["paypal-request-id"] == (
        "refund:CAP-1:full"
    )
    assert refund.status == "pending"


async def test_refund_of_a_missing_capture_is_a_not_found() -> None:
    gateway, _, _ = build(httpx.Response(404, json={"name": "RESOURCE_NOT_FOUND"}))

    with pytest.raises(PaymentNotFoundError) as excinfo:
        await gateway.refund("CAP-missing")

    assert excinfo.value.details == {"provider": "paypal", "payment_id": "CAP-missing"}


async def test_refund_without_an_amount_in_the_response_is_an_error() -> None:
    gateway, _, _ = build(httpx.Response(201, json={"id": "R", "status": "COMPLETED"}))

    with pytest.raises(PaymentError):
        await gateway.refund("CAP-1")


async def test_refunded_capture_still_reports_the_money_as_taken() -> None:
    """`REFUNDED` describes what happened afterwards, not a failed charge."""
    gateway, _, _ = build(httpx.Response(200, json=capture(status="REFUNDED")))

    payment = await gateway.get_payment("CAP-1")

    assert payment.status == "succeeded"
    assert payment.provider_status == "REFUNDED"


async def test_get_payment_reads_the_reference_from_custom_id() -> None:
    gateway, recorder, _ = build(httpx.Response(200, json=capture()))

    payment = await gateway.get_payment("CAP-1")

    assert recorder.api_requests[-1].method == "GET"
    assert str(recorder.api_requests[-1].url) == (
        "https://paypal.test/v2/payments/captures/CAP-1"
    )
    assert payment.reference == "order-1"
    assert payment.amount == Money(amount_minor=2500, currency="USD")


async def test_non_json_body_is_a_payment_error() -> None:
    gateway, _, _ = build(httpx.Response(200, text="<html>nope</html>"))

    with pytest.raises(PaymentError):
        await gateway.get_payment("CAP-1")


async def test_json_array_body_is_a_payment_error() -> None:
    gateway, _, _ = build(httpx.Response(200, json=["not", "an", "object"]))

    with pytest.raises(PaymentError):
        await gateway.get_payment("CAP-1")


async def test_204_is_treated_as_an_empty_document() -> None:
    gateway, _, _ = build(httpx.Response(204))

    with pytest.raises(PaymentError):
        # No body means no amount, which for a refund is unusable — but it must
        # fail as a payment error rather than a JSON decode crash.
        await gateway.refund("CAP-1")


async def test_transport_error_is_unavailable() -> None:
    def explode(request: httpx.Request) -> httpx.Response:
        if request.url.path == TOKEN_PATH:
            return httpx.Response(
                200, json={"access_token": "mock-paypal-token", "expires_in": 32400}
            )
        raise httpx.ReadTimeout("timed out", request=request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(explode))
    gateway = PayPalGateway(
        client_id="mock-client-id", client_secret="mock-client-secret", client=client
    )

    with pytest.raises(PaymentGatewayUnavailableError):
        await gateway.charge(charge_request())


async def test_gateway_closes_only_the_client_it_owns() -> None:
    injected = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json={}))
    )
    borrower = PayPalGateway(
        client_id="mock-client-id", client_secret="mock-client-secret", client=injected
    )

    await borrower.aclose()
    assert not injected.is_closed

    owner = PayPalGateway(
        client_id="mock-client-id", client_secret="mock-client-secret"
    )
    created = owner._get_client()
    await owner.aclose()
    assert created.is_closed
    await owner.aclose()

    await injected.aclose()
