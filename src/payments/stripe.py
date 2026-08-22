"""Stripe adapter.

Talks to the Stripe REST API directly over httpx rather than through the
`stripe` SDK. The SDK is synchronous by default and would have to be run in a
thread from every async route; more to the point, the whole value of the
adapter pattern here is that the provider's shape stops at this module's edge,
and a vendored SDK's objects have a way of leaking past it into call sites.

What is Stripe-shaped and gets translated here:

- Bodies are `application/x-www-form-urlencoded`, with nested keys spelled
  `metadata[order_id]`. Stripe does not accept JSON on these endpoints.
- Amounts are integer minor units — the same convention as `Money`, which is
  why `amount_minor` goes over the wire untouched while PayPal needs a string.
- Idempotency is an `Idempotency-Key` header, scoped per API key for 24 hours.
- A declined card is HTTP 402 with `error.type == "card_error"`, not an
  exception class of its own and not a 200 with a status field.
"""

from __future__ import annotations

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

DEFAULT_API_BASE_URL: Final[str] = "https://api.stripe.com"

# Stripe's PaymentIntent statuses, mapped onto the three this application
# understands. `requires_action` is the 3-D Secure case: not a failure, not
# money in the bank, and the single most common reason to get this wrong.
_INTENT_STATUS: Final[FrozenDict[str, PaymentStatus]] = FrozenDict[str, PaymentStatus](
    {
        "succeeded": "succeeded",
        "processing": "pending",
        "requires_action": "pending",
        "requires_confirmation": "pending",
        "requires_payment_method": "failed",
        "requires_capture": "pending",
        "canceled": "failed",
    }
)

_REFUND_STATUS: Final[FrozenDict[str, RefundStatus]] = FrozenDict[str, RefundStatus](
    {
        "succeeded": "succeeded",
        "pending": "pending",
        "requires_action": "pending",
        "failed": "failed",
        "canceled": "failed",
    }
)


class StripeGateway:
    """Adapts the Stripe REST API to `PaymentGateway`.

    `client` is injectable so the wire format can be asserted against an
    `httpx.MockTransport` — the request Stripe would receive is the thing worth
    testing, and it is not observable through a mocked-out method on this class.
    """

    def __init__(
        self,
        *,
        api_key: str,
        base_url: str = DEFAULT_API_BASE_URL,
        timeout: float = 15.0,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._client = client
        # Only a client this object created is a client this object may close.
        self._owns_client = client is None

    @property
    def name(self) -> str:
        return "stripe"

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

    async def _request(
        self,
        method: str,
        path: str,
        *,
        form: dict[str, str] | None = None,
        idempotency_key: str | None = None,
        resource_id: str = "",
    ) -> dict[str, Any]:
        """Perform one Stripe call and return its parsed body, or raise.

        Every provider-shaped failure is converted here so the three public
        methods deal only in this application's exceptions.
        """
        headers = {"Authorization": f"Bearer {self._api_key}"}
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key

        try:
            response = await self._get_client().request(
                method, f"{self._base_url}{path}", data=form, headers=headers
            )
        except httpx.TransportError as exc:
            raise PaymentGatewayUnavailableError(
                self.name, f"{type(exc).__name__}: {exc}"
            ) from exc

        # 429 and 5xx are the provider saying "not now" rather than "no". The
        # caller may repeat the call; the idempotency key makes that safe.
        if response.status_code == 429 or response.status_code >= 500:
            raise PaymentGatewayUnavailableError(
                self.name, f"HTTP {response.status_code}"
            )

        try:
            document = response.json()
        except ValueError as exc:
            raise PaymentError(
                f"Stripe returned a non-JSON body with HTTP {response.status_code}.",
                details={"status": response.status_code},
            ) from exc

        if not isinstance(document, dict):
            raise PaymentError(
                "Stripe returned a JSON body that is not an object.",
                details={"status": response.status_code},
            )

        if response.is_success:
            return document

        self._raise_for_error(response.status_code, document, resource_id)

    def _raise_for_error(
        self, status_code: int, document: dict[str, Any], resource_id: str = ""
    ) -> NoReturn:
        """Translate a Stripe error body into this application's exceptions."""
        error = document.get("error")
        error = error if isinstance(error, dict) else {}
        error_type = str(error.get("type", ""))
        code = str(error.get("code", ""))
        message = str(error.get("message", f"HTTP {status_code}"))

        if error_type == "card_error":
            logger.info(
                "payment.declined", provider=self.name, decline_code=code or error_type
            )
            raise PaymentDeclinedError(self.name, message, code)

        if status_code == 404 or code == "resource_missing":
            raise PaymentNotFoundError(
                self.name, resource_id or str(error.get("param", ""))
            )

        logger.error(
            "payment.provider_error",
            provider=self.name,
            status=status_code,
            error_type=error_type,
            code=code,
        )
        raise PaymentError(
            f"Stripe rejected the request: {message}",
            details={"status": status_code, "type": error_type, "code": code},
        )

    def _to_payment(self, intent: dict[str, Any], reference: str = "") -> Payment:
        """Turn a PaymentIntent document into a `Payment`.

        The reference comes back in `metadata.reference` on a fetch, but the
        caller already knows it on a charge, so a passed-in value wins over a
        round-tripped one.
        """
        provider_status = str(intent.get("status", ""))
        metadata = intent.get("metadata")
        metadata = metadata if isinstance(metadata, dict) else {}

        try:
            amount = Money(
                amount_minor=int(intent["amount"]),
                currency=str(intent["currency"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise PaymentError(
                "Stripe returned a PaymentIntent without a usable amount.",
                details={"payment_id": intent.get("id")},
            ) from exc

        status = _INTENT_STATUS.get(provider_status)
        if status is None:
            # A status Stripe added after this was written. Treating an unknown
            # status as "failed" would be a lie in the expensive direction —
            # the money may well have moved — so it is pending, which no code
            # path treats as permission to ship anything.
            logger.warning(
                "payment.unknown_status", provider=self.name, status=provider_status
            )
            status = "pending"

        return Payment(
            provider=self.name,
            provider_payment_id=str(intent.get("id", "")),
            status=status,
            amount=amount,
            reference=reference or str(metadata.get("reference", "")),
            provider_status=provider_status,
        )

    async def charge(self, request: ChargeRequest) -> Payment:
        """Create and confirm a PaymentIntent in one call.

        `confirm=true` with `off_session=true` is the server-side charge: the
        shopper has already handed over a payment method and is not sitting in
        front of a redirect. A card that demands 3-D Secure anyway comes back
        `requires_action`, which is `pending` here rather than an error, and
        the caller resumes it with the client secret.
        """
        form = {
            "amount": str(request.amount.amount_minor),
            "currency": request.amount.currency.lower(),
            "payment_method": request.payment_method_token,
            "confirm": "true",
            "off_session": "true",
            "metadata[reference]": request.reference,
        }

        if request.description:
            form["description"] = request.description
        if request.customer_email:
            form["receipt_email"] = request.customer_email
        for key, value in request.metadata.items():
            form[f"metadata[{key}]"] = value

        document = await self._request(
            "POST",
            "/v1/payment_intents",
            form=form,
            idempotency_key=request.reference,
        )
        payment = self._to_payment(document, reference=request.reference)

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
        """Refund a PaymentIntent, fully or partially.

        Stripe's refund endpoint takes the intent id rather than a charge id,
        so `provider_payment_id` is the same handle `charge` returned. Omitting
        `amount` refunds the full remaining balance, which is Stripe's own
        default — this adapter does not compute it and so cannot get it wrong.
        """
        form = {"payment_intent": provider_payment_id}
        if amount is not None:
            form["amount"] = str(amount.amount_minor)

        document = await self._request(
            "POST",
            "/v1/refunds",
            form=form,
            # A refund has no natural reference from the caller, so the key is
            # derived from what is being refunded: a retried full refund of the
            # same payment is the same operation, not a second one.
            idempotency_key=self._refund_idempotency_key(provider_payment_id, amount),
            resource_id=provider_payment_id,
        )

        try:
            refunded = Money(
                amount_minor=int(document["amount"]),
                currency=str(document["currency"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise PaymentError(
                "Stripe returned a refund without a usable amount.",
                details={"payment_id": provider_payment_id},
            ) from exc

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
            amount=refunded,
            provider_status=provider_status,
        )

    @staticmethod
    def _refund_idempotency_key(provider_payment_id: str, amount: Money | None) -> str:
        suffix = "full" if amount is None else str(amount.amount_minor)
        return f"refund:{provider_payment_id}:{suffix}"

    async def get_payment(self, provider_payment_id: str) -> Payment:
        document = await self._request(
            "GET",
            f"/v1/payment_intents/{provider_payment_id}",
            resource_id=provider_payment_id,
        )
        return self._to_payment(document)
