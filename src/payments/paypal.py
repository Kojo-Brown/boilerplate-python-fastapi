"""PayPal adapter.

The same three operations as `StripeGateway`, against an API that agrees with
Stripe on almost nothing. Everything below is a translation, and the list is
the point of the pattern:

- **Auth.** Stripe takes a static secret key on every request. PayPal issues a
  short-lived bearer token from `/v1/oauth2/token` against Basic-auth client
  credentials, and expects it cached until it expires.
- **Bodies.** JSON, not form encoding.
- **Amounts.** Decimal strings in the major unit with exactly the currency's
  number of decimal places (`{"currency_code": "USD", "value": "10.00"}`),
  where Stripe wants the integer `1000`.
- **Idempotency.** `PayPal-Request-Id`, not `Idempotency-Key`.
- **Handles.** A charge produces an *order*, but only a *capture* can be
  refunded or fetched — so `Payment.provider_payment_id` is the capture id dug
  out of the order's purchase units, not the order id the response leads with.
- **Failures.** A declined instrument is HTTP 422 with
  `details[].issue == "INSTRUMENT_DECLINED"`, not a 402.

Orders are created with `intent: CAPTURE` and a vaulted payment-source token,
which PayPal settles in the same call — the two-step create-then-capture dance
belongs to the redirect flow, where the shopper approves in a PayPal window.
An order that comes back needing that approval is reported `pending` rather
than being forced through here, because finishing it requires the shopper.
"""

from __future__ import annotations

import base64
import time
from collections.abc import Callable
from typing import Any, Final, NoReturn

import httpx
import structlog

from src.immutable import FrozenDict
from src.payments.base import (
    ChargeRequest,
    Money,
    Payment,
    PaymentDeclinedError,
    PaymentError,
    PaymentGatewayUnavailableError,
    PaymentNotFoundError,
    PaymentStatus,
    Refund,
    RefundStatus,
)

logger = structlog.get_logger(__name__)

DEFAULT_API_BASE_URL: Final[str] = "https://api-m.sandbox.paypal.com"

LIVE_API_BASE_URL: Final[str] = "https://api-m.paypal.com"

# Renew this many seconds before the token actually expires. A token that is
# valid when checked and stale when it arrives is the failure this avoids.
TOKEN_EXPIRY_SKEW_SECONDS: Final[float] = 60.0

_ORDER_STATUS: Final[FrozenDict[str, PaymentStatus]] = FrozenDict[str, PaymentStatus](
    {
        "COMPLETED": "succeeded",
        "CREATED": "pending",
        "SAVED": "pending",
        "APPROVED": "pending",
        "PAYER_ACTION_REQUIRED": "pending",
        "VOIDED": "failed",
    }
)

_CAPTURE_STATUS: Final[FrozenDict[str, PaymentStatus]] = FrozenDict[str, PaymentStatus](
    {
        "COMPLETED": "succeeded",
        "PENDING": "pending",
        "PARTIALLY_REFUNDED": "succeeded",
        "REFUNDED": "succeeded",
        "DECLINED": "failed",
        "FAILED": "failed",
    }
)

_REFUND_STATUS: Final[FrozenDict[str, RefundStatus]] = FrozenDict[str, RefundStatus](
    {
        "COMPLETED": "succeeded",
        "PENDING": "pending",
        "CANCELLED": "failed",
        "FAILED": "failed",
    }
)

# PayPal reports a refused instrument as a 422 with an issue code rather than a
# distinct status, so the decline has to be recognised by name.
_DECLINE_ISSUES: Final[frozenset[str]] = frozenset(
    {
        "INSTRUMENT_DECLINED",
        "PAYER_CANNOT_PAY",
        "TRANSACTION_REFUSED",
        "PAYMENT_DENIED",
    }
)

Clock = Callable[[], float]


class PayPalGateway:
    """Adapts the PayPal Orders v2 / Payments v2 APIs to `PaymentGateway`.

    `client` and `clock` are injectable so the token lifecycle can be asserted
    exactly — that a second charge reuses the cached token, that an expired one
    is refetched, that a 401 mid-flight triggers exactly one re-auth — without
    a network and without a test that waits out an expiry.
    """

    def __init__(
        self,
        *,
        client_id: str,
        client_secret: str,
        base_url: str = DEFAULT_API_BASE_URL,
        timeout: float = 15.0,
        client: httpx.AsyncClient | None = None,
        clock: Clock | None = None,
    ) -> None:
        self._client_id = client_id
        self._client_secret = client_secret
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._client = client
        # Only a client this object created is a client this object may close.
        self._owns_client = client is None
        self._clock: Clock = clock if clock is not None else time.monotonic
        self._token: str | None = None
        self._token_expires_at: float = 0.0

    @property
    def name(self) -> str:
        return "paypal"

    def _get_client(self) -> httpx.AsyncClient:
        """Return the shared client, building it on first use.

        Built lazily rather than in `__init__` because constructing an
        `AsyncClient` outside a running loop binds it to the wrong one, and the
        registry builds gateways wherever it happens to be called.
        """
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        return self._client

    async def aclose(self) -> None:
        """Close the client if this gateway owns it. Safe to call twice."""
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None

    # ---- OAuth ---------------------------------------------------------

    async def _fetch_token(self) -> str:
        """Exchange the client credentials for a bearer token.

        The token is cached with its own expiry, minus a skew, because PayPal
        rate-limits this endpoint and a token per API call would spend two
        requests on every charge.
        """
        credentials = f"{self._client_id}:{self._client_secret}".encode()
        headers = {
            "Authorization": f"Basic {base64.b64encode(credentials).decode()}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        try:
            response = await self._get_client().post(
                f"{self._base_url}/v1/oauth2/token",
                data={"grant_type": "client_credentials"},
                headers=headers,
            )
        except httpx.TransportError as exc:
            raise PaymentGatewayUnavailableError(
                self.name, f"{type(exc).__name__}: {exc}"
            ) from exc

        if response.status_code == 429 or response.status_code >= 500:
            raise PaymentGatewayUnavailableError(
                self.name, f"token endpoint returned HTTP {response.status_code}"
            )

        if not response.is_success:
            # Bad credentials are a deployment fault, not a downstream one, and
            # retrying will not fix them — so this is not "unavailable".
            raise PaymentError(
                "PayPal refused the client credentials.",
                details={"status": response.status_code},
            )

        try:
            document = response.json()
            token = str(document["access_token"])
            expires_in = float(document.get("expires_in", 0))
        except (ValueError, KeyError, TypeError) as exc:
            raise PaymentError(
                "PayPal returned an unusable token response.",
            ) from exc

        self._token = token
        self._token_expires_at = self._clock() + max(
            expires_in - TOKEN_EXPIRY_SKEW_SECONDS, 0.0
        )
        logger.debug("payment.token_refreshed", provider=self.name)
        return token

    async def _get_token(self) -> str:
        if self._token is not None and self._clock() < self._token_expires_at:
            return self._token
        return await self._fetch_token()

    # ---- Transport -----------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        request_id: str | None = None,
        resource_id: str = "",
    ) -> dict[str, Any]:
        """Perform one PayPal call and return its parsed body, or raise.

        A 401 is retried exactly once with a fresh token: PayPal can revoke a
        token before its stated expiry, and re-authenticating is the documented
        response. Once, not in a loop — a second 401 after a brand-new token
        means the credentials are wrong, and hammering the token endpoint over
        that gets the whole integration rate-limited.
        """
        token = await self._get_token()

        for attempt in (1, 2):
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
            if request_id is not None:
                headers["PayPal-Request-Id"] = request_id

            try:
                response = await self._get_client().request(
                    method, f"{self._base_url}{path}", json=json_body, headers=headers
                )
            except httpx.TransportError as exc:
                raise PaymentGatewayUnavailableError(
                    self.name, f"{type(exc).__name__}: {exc}"
                ) from exc

            if response.status_code == 401 and attempt == 1:
                self._token = None
                token = await self._fetch_token()
                continue

            if response.status_code == 429 or response.status_code >= 500:
                raise PaymentGatewayUnavailableError(
                    self.name, f"HTTP {response.status_code}"
                )

            # 204 is how a few PayPal endpoints report success; it has no body
            # to parse, and callers of this method only read keys they check.
            if response.status_code == 204:
                return {}

            try:
                document = response.json()
            except ValueError as exc:
                raise PaymentError(
                    f"PayPal returned a non-JSON body with HTTP "
                    f"{response.status_code}.",
                    details={"status": response.status_code},
                ) from exc

            if not isinstance(document, dict):
                raise PaymentError(
                    "PayPal returned a JSON body that is not an object.",
                    details={"status": response.status_code},
                )

            if response.is_success:
                return document

            self._raise_for_error(response.status_code, document, resource_id)

        # Unreachable: the loop either returns or raises on both passes. Kept
        # as an explicit raise rather than a fallthrough so a future edit to the
        # loop cannot silently start returning None.
        raise PaymentError(  # pragma: no cover - defensive, see comment above
            "PayPal request did not complete."
        )

    def _raise_for_error(
        self, status_code: int, document: dict[str, Any], resource_id: str = ""
    ) -> NoReturn:
        """Translate a PayPal error body into this application's exceptions."""
        name = str(document.get("name", ""))
        message = str(document.get("message", f"HTTP {status_code}"))
        raw_details = document.get("details")
        details = raw_details if isinstance(raw_details, list) else []
        issues = [
            str(item.get("issue", "")) for item in details if isinstance(item, dict)
        ]

        declined = next((issue for issue in issues if issue in _DECLINE_ISSUES), None)
        if declined is not None:
            logger.info("payment.declined", provider=self.name, decline_code=declined)
            raise PaymentDeclinedError(self.name, message, declined)

        if status_code == 404 or name == "RESOURCE_NOT_FOUND":
            raise PaymentNotFoundError(self.name, resource_id)

        logger.error(
            "payment.provider_error",
            provider=self.name,
            status=status_code,
            name=name,
            issues=issues,
        )
        raise PaymentError(
            f"PayPal rejected the request: {message}",
            details={"status": status_code, "name": name, "issues": issues},
        )

    # ---- Translation ---------------------------------------------------

    @staticmethod
    def _amount_json(amount: Money) -> dict[str, str]:
        return {
            "currency_code": amount.currency,
            "value": amount.to_decimal_string(),
        }

    def _money_from_json(self, node: Any, context: str) -> Money:
        if not isinstance(node, dict):
            raise PaymentError(
                f"PayPal returned {context} without an amount.",
            )
        try:
            return Money.from_decimal_string(
                str(node["value"]), str(node["currency_code"])
            )
        except (KeyError, TypeError) as exc:
            raise PaymentError(
                f"PayPal returned {context} without a usable amount.",
            ) from exc

    def _capture_from_order(self, order: dict[str, Any]) -> dict[str, Any] | None:
        """Dig the first capture out of an order document.

        `purchase_units[].payments.captures[]` is where the refundable id
        lives. An order that has not settled has no captures at all, which is
        not an error — it is the `pending` case.
        """
        units = order.get("purchase_units")
        if not isinstance(units, list):
            return None

        for unit in units:
            if not isinstance(unit, dict):
                continue
            payments = unit.get("payments")
            if not isinstance(payments, dict):
                continue
            captures = payments.get("captures")
            if isinstance(captures, list):
                for capture in captures:
                    if isinstance(capture, dict):
                        return capture
        return None

    def _to_payment(self, order: dict[str, Any], request: ChargeRequest) -> Payment:
        """Turn an order document into a `Payment`.

        The capture decides the outcome when there is one: an order reports
        `COMPLETED` as soon as it is captured, but the capture itself can be
        `PENDING` while PayPal reviews it, and that difference is whether the
        money is available.
        """
        order_status = str(order.get("status", ""))
        capture = self._capture_from_order(order)

        if capture is None:
            status = _ORDER_STATUS.get(order_status)
            if status is None:
                logger.warning(
                    "payment.unknown_status", provider=self.name, status=order_status
                )
                status = "pending"
            # No capture means no refundable handle yet, so the order id is the
            # only id there is. It is recorded so the payment is traceable, and
            # `is_settled` is False, which is what stops a caller acting on it.
            return Payment(
                provider=self.name,
                provider_payment_id=str(order.get("id", "")),
                status=status,
                amount=request.amount,
                reference=request.reference,
                provider_status=order_status,
            )

        capture_status = str(capture.get("status", ""))
        status = _CAPTURE_STATUS.get(capture_status)
        if status is None:
            logger.warning(
                "payment.unknown_status", provider=self.name, status=capture_status
            )
            status = "pending"

        return Payment(
            provider=self.name,
            provider_payment_id=str(capture.get("id", "")),
            status=status,
            amount=self._money_from_json(capture.get("amount"), "a capture"),
            reference=request.reference,
            provider_status=capture_status,
        )

    # ---- PaymentGateway ------------------------------------------------

    async def charge(self, request: ChargeRequest) -> Payment:
        body: dict[str, Any] = {
            "intent": "CAPTURE",
            "purchase_units": [
                {
                    "reference_id": request.reference,
                    "custom_id": request.reference,
                    "amount": self._amount_json(request.amount),
                }
            ],
            "payment_source": {
                "token": {"id": request.payment_method_token, "type": "SETUP_TOKEN"}
            },
        }

        unit = body["purchase_units"][0]
        if request.description:
            unit["description"] = request.description

        document = await self._request(
            "POST", "/v2/checkout/orders", json_body=body, request_id=request.reference
        )
        payment = self._to_payment(document, request)

        logger.info(
            "payment.charged",
            provider=self.name,
            payment_id=payment.provider_payment_id,
            reference=payment.reference,
            status=payment.status,
        )
        return payment

    async def refund(
        self, provider_payment_id: str, amount: Money | None = None
    ) -> Refund:
        """Refund a capture, fully or partially.

        `provider_payment_id` is the capture id from `charge` — an order id is
        rejected here by PayPal, and that is the asymmetry with Stripe this
        adapter cannot hide, only document.

        An omitted `amount` sends an empty body, which is PayPal's own "refund
        the full remaining balance". The adapter does not compute that figure,
        so it cannot get it wrong after a previous partial refund.
        """
        body: dict[str, Any] = {}
        if amount is not None:
            body["amount"] = self._amount_json(amount)

        document = await self._request(
            "POST",
            f"/v2/payments/captures/{provider_payment_id}/refunds",
            json_body=body,
            request_id=f"refund:{provider_payment_id}:"
            + ("full" if amount is None else str(amount.amount_minor)),
            resource_id=provider_payment_id,
        )

        provider_status = str(document.get("status", ""))
        status = _REFUND_STATUS.get(provider_status, "pending")

        logger.info(
            "payment.refunded",
            provider=self.name,
            payment_id=provider_payment_id,
            refund_id=document.get("id"),
            status=status,
        )
        return Refund(
            provider=self.name,
            provider_refund_id=str(document.get("id", "")),
            provider_payment_id=provider_payment_id,
            status=status,
            amount=self._money_from_json(document.get("amount"), "a refund"),
            provider_status=provider_status,
        )

    async def get_payment(self, provider_payment_id: str) -> Payment:
        """Fetch a capture.

        Reads `/v2/payments/captures/{id}` rather than the order, because the
        capture is what `charge` handed back and what carries the settled
        amount. `custom_id` round-trips the caller's reference.
        """
        document = await self._request(
            "GET",
            f"/v2/payments/captures/{provider_payment_id}",
            resource_id=provider_payment_id,
        )

        provider_status = str(document.get("status", ""))
        status = _CAPTURE_STATUS.get(provider_status)
        if status is None:
            logger.warning(
                "payment.unknown_status", provider=self.name, status=provider_status
            )
            status = "pending"

        return Payment(
            provider=self.name,
            provider_payment_id=str(document.get("id", provider_payment_id)),
            status=status,
            amount=self._money_from_json(document.get("amount"), "a capture"),
            reference=str(document.get("custom_id", "")),
            provider_status=provider_status,
        )
