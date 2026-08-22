"""Provider-agnostic payment contract.

Nothing here imports httpx, a provider SDK or the settings object, so an
adapter can be written against this module without inheriting any of the
three. The concrete adapters live in `stripe.py` and `paypal.py`;
`registry.py` chooses between them.

This is the adapter pattern rather than the strategy pattern, and the
distinction is not cosmetic. The two providers are not interchangeable
implementations of one idea that happened to be written twice — they are
existing, incompatible remote APIs that this application does not control.
Stripe takes form-encoded bodies, amounts as integer minor units, and
idempotency through an `Idempotency-Key` header; PayPal takes JSON, amounts as
decimal strings in the major unit, an OAuth2 bearer token it will only issue on
request, and idempotency through `PayPal-Request-Id`. `Money`, `ChargeRequest`
and `Payment` are the shapes this application wants; each adapter's whole job
is translating between them and the shape its provider insists on, so that
nothing above `PaymentGateway` ever learns which provider is behind it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Final, Literal, Protocol, runtime_checkable

from src.exceptions import AppException, BadRequestError, NotFoundError
from src.immutable import EMPTY_MAPPING, FrozenDict, freeze_mapping

MAX_REFERENCE_LENGTH: Final[int] = 128

MAX_DESCRIPTION_LENGTH: Final[int] = 500

# ISO 4217 minor-unit exponents for the currencies this contract accepts.
#
# There is no safe default here. Assuming two decimals turns ¥1,000 into ¥10
# against Stripe and ¥100,000 against PayPal, in opposite directions, from the
# same `Money`. So an unlisted currency is refused rather than guessed, and
# adding one is a line in this table plus a check that both providers settle
# it. Zero-decimal (JPY, KRW) and three-decimal (BHD, KWD, TND) entries are
# here precisely because they are the ones that break a hardcoded 100.
#
# `FrozenDict` rather than a dict literal, because `Final` only forbids
# rebinding the name: `CURRENCY_EXPONENTS["JPY"] = 2` type-checks under a
# `dict` annotation, succeeds at runtime, and silently divides every later yen
# amount by a hundred for the life of the process.
CURRENCY_EXPONENTS: Final[FrozenDict[str, int]] = FrozenDict(
    {
        "AUD": 2,
        "BHD": 3,
        "CAD": 2,
        "CHF": 2,
        "EUR": 2,
        "GBP": 2,
        "JPY": 0,
        "KRW": 0,
        "KWD": 3,
        "NZD": 2,
        "SEK": 2,
        "SGD": 2,
        "TND": 3,
        "USD": 2,
    }
)

# What this application calls the outcome of a charge. Each adapter maps its
# provider's own vocabulary onto these three; the provider's original string
# survives on `Payment.provider_status` for support tickets and log searches.
PaymentStatus = Literal["succeeded", "pending", "failed"]

RefundStatus = Literal["succeeded", "pending", "failed"]


class PaymentError(AppException):
    """A payment operation failed for a reason the caller cannot fix.

    502 rather than 500: by the time this is raised the request reached a third
    party that answered badly, which is not a defect in this process.
    """

    status_code = 502
    error_code = "PAYMENT_ERROR"

    def __init__(
        self, message: str = "Payment operation failed", details: object = None
    ) -> None:
        super().__init__(message, details)


class PaymentDeclinedError(AppException):
    """The provider understood the charge and refused it.

    402 rather than 502, because nothing is broken: the issuer said no. It is
    the one payment failure a caller can act on — show the shopper a message,
    ask for another card — so it must not be indistinguishable from a gateway
    outage. `decline_code` carries the provider's own reason, unmapped: there
    is no cross-provider vocabulary for declines worth inventing, and a support
    agent searching the provider's dashboard needs the provider's string.
    """

    status_code = 402
    error_code = "PAYMENT_DECLINED"

    def __init__(self, provider: str, reason: str, decline_code: str = "") -> None:
        super().__init__(
            f"Payment declined by {provider}: {reason}",
            details={
                "provider": provider,
                "reason": reason,
                "decline_code": decline_code,
            },
        )
        self.provider = provider
        self.decline_code = decline_code


class PaymentGatewayUnavailableError(PaymentError):
    """The provider could not be reached, or answered 5xx/429.

    503 tells the caller the same charge is worth attempting again. Both
    adapters send an idempotency key derived from `ChargeRequest.reference`, so
    a retry of a request that this error reports cannot double-charge even if
    the provider did process the original.
    """

    status_code = 503
    error_code = "PAYMENT_GATEWAY_UNAVAILABLE"

    def __init__(self, provider: str, reason: str) -> None:
        super().__init__(
            f"Payment provider '{provider}' is unavailable: {reason}",
            details={"provider": provider, "reason": reason},
        )


class PaymentNotFoundError(NotFoundError):
    """No payment or refund with that provider id."""

    error_code = "PAYMENT_NOT_FOUND"

    def __init__(self, provider: str, payment_id: str) -> None:
        super().__init__(
            f"No payment '{payment_id}' at provider '{provider}'.",
            details={"provider": provider, "payment_id": payment_id},
        )


class PaymentConfigurationError(PaymentError):
    """The selected adapter has no usable credentials.

    Raised when the gateway is built, not when a charge is attempted, so a
    missing secret key surfaces at startup rather than at the checkout of the
    first customer to reach it.
    """

    status_code = 500
    error_code = "PAYMENT_CONFIGURATION_ERROR"

    def __init__(self, provider: str, missing: str) -> None:
        super().__init__(
            f"Payment provider '{provider}' is not configured: {missing} is unset.",
            details={"provider": provider, "missing": missing},
        )


class UnknownPaymentGatewayError(PaymentError):
    """Raised when the configured gateway name has no registered builder."""

    status_code = 500
    error_code = "UNKNOWN_PAYMENT_GATEWAY"

    def __init__(self, name: str, available: tuple[str, ...]) -> None:
        super().__init__(
            f"Unknown payment gateway '{name}'.",
            details={"requested": name, "available": list(available)},
        )


class UnsupportedCurrencyError(BadRequestError):
    """Raised for a currency with no entry in `CURRENCY_EXPONENTS`."""

    error_code = "UNSUPPORTED_CURRENCY"

    def __init__(self, currency: str) -> None:
        super().__init__(
            f"Currency '{currency}' is not supported.",
            details={"currency": currency, "supported": sorted(CURRENCY_EXPONENTS)},
        )


@dataclass(frozen=True, slots=True)
class Money:
    """An amount in a currency's smallest unit.

    Integer minor units, never a float and never a bare `Decimal`: money that
    goes through binary floating point acquires rounding error that reconciles
    against nothing, and a `Decimal` alone still leaves "is 10 ten dollars or
    ten cents?" to whoever reads the field name. 1000 USD minor units is
    $10.00; 1000 JPY minor units is ¥1000, because the yen has no minor unit.
    """

    amount_minor: int
    currency: str

    def __post_init__(self) -> None:
        normalised = self.currency.strip().upper()
        if normalised != self.currency:
            # `frozen=True` blocks assignment; this is the documented way to
            # normalise in `__post_init__` and keeps "USD" and "usd" equal.
            object.__setattr__(self, "currency", normalised)

        if self.currency not in CURRENCY_EXPONENTS:
            raise UnsupportedCurrencyError(self.currency)

        if self.amount_minor <= 0:
            raise BadRequestError(
                "Payment amount must be positive.",
                details={"amount_minor": self.amount_minor},
            )

    @property
    def exponent(self) -> int:
        """Number of decimal places this currency settles in."""
        return CURRENCY_EXPONENTS[self.currency]

    def to_decimal_string(self) -> str:
        """Render as a major-unit decimal string, e.g. `"10.00"`, `"1000"`.

        Always exactly `exponent` decimal places, which is what PayPal
        validates against the currency it was given — `"10.0"` for USD is
        rejected by the API, not silently accepted.
        """
        value = Decimal(self.amount_minor).scaleb(-self.exponent)
        return f"{value:.{self.exponent}f}"

    @classmethod
    def from_decimal_string(cls, value: str, currency: str) -> Money:
        """Parse a major-unit decimal string a provider sent back.

        Refuses to round: a provider that reports `"10.005"` on a two-decimal
        currency has said something this code does not understand, and
        quietly turning it into `10.00` would lose a cent per transaction in
        whichever direction the rounding mode happened to fall.
        """
        normalised = currency.strip().upper()
        if normalised not in CURRENCY_EXPONENTS:
            raise UnsupportedCurrencyError(normalised)

        try:
            parsed = Decimal(value)
        except InvalidOperation as exc:
            raise BadRequestError(
                "Payment amount is not a decimal number.",
                details={"value": value},
            ) from exc

        if not parsed.is_finite():
            raise BadRequestError(
                "Payment amount is not a finite number.",
                details={"value": value},
            )

        exponent = CURRENCY_EXPONENTS[normalised]
        scaled = parsed.scaleb(exponent)
        # Compared rather than rounded. `to_integral_exact` would only raise
        # with the `Inexact` trap enabled, which is not the default context and
        # is not something this module should switch on for the whole process.
        if scaled != scaled.to_integral_value():
            raise BadRequestError(
                f"Payment amount '{value}' has more precision than "
                f"{normalised} settles in.",
                details={"value": value, "currency": normalised},
            )

        return cls(amount_minor=int(scaled), currency=normalised)

    def __str__(self) -> str:
        return f"{self.to_decimal_string()} {self.currency}"


@dataclass(frozen=True, slots=True)
class ChargeRequest:
    """What this application wants charged, in its own vocabulary.

    `reference` is the caller's identifier for the thing being paid for — an
    order id — and it is what both adapters turn into their provider's
    idempotency key. It is required rather than optional because an optional
    idempotency key is one nobody passes, and the first time that matters is a
    network timeout that charged a customer twice.

    `payment_method_token` is a token the client obtained from the provider's
    own SDK in the browser. Raw card numbers never reach this process, so they
    are not representable here.
    """

    amount: Money
    payment_method_token: str
    reference: str
    description: str = ""
    customer_email: str | None = None
    #: Frozen on construction — see `__post_init__`. Declared as `Mapping` so
    #: that a caller can pass an ordinary dict and mypy still refuses to write
    #: through the attribute afterwards.
    metadata: Mapping[str, str] = EMPTY_MAPPING

    def __post_init__(self) -> None:
        # `frozen=True` stops assignment to this attribute and nothing else —
        # it keeps the caller's mapping rather than copying it, so the dict the
        # charge was built from is still the object behind `self.metadata`.
        # That matters more here than almost anywhere else in this codebase:
        # both adapters send `metadata` to the provider, so a mutation between
        # construction and the HTTP call reaches the provider's records while
        # the request the caller validated says something else.
        object.__setattr__(self, "metadata", freeze_mapping(self.metadata))

        if not self.payment_method_token.strip():
            raise BadRequestError("Payment method token must not be empty.")

        if not self.reference.strip():
            raise BadRequestError("Charge reference must not be empty.")

        if len(self.reference) > MAX_REFERENCE_LENGTH:
            raise BadRequestError(
                f"Charge reference must be at most {MAX_REFERENCE_LENGTH} characters.",
                details={"length": len(self.reference)},
            )

        if len(self.description) > MAX_DESCRIPTION_LENGTH:
            raise BadRequestError(
                f"Charge description must be at most {MAX_DESCRIPTION_LENGTH} "
                "characters.",
                details={"length": len(self.description)},
            )


@dataclass(frozen=True, slots=True)
class Payment:
    """A charge as this application understands it.

    `provider_payment_id` is deliberately specified by behaviour rather than by
    provider: it is the handle `refund` and `get_payment` accept on the gateway
    that produced it. For Stripe that is the PaymentIntent id; for PayPal it is
    the *capture* id, not the order id, because a PayPal order cannot be
    refunded and a caller storing the wrong one discovers this at refund time.
    Absorbing that asymmetry is exactly the adapters' job — but it also means
    the id is only meaningful to the gateway named in `provider`, which is why
    that field is here and why persisting a payment means persisting both.

    `provider_status` is the raw upstream string (`"requires_action"`,
    `"PAYER_ACTION_REQUIRED"`). `status` is the mapped one. Both are kept: the
    mapping is what code should branch on, the original is what a support agent
    pastes into the provider's dashboard.
    """

    provider: str
    provider_payment_id: str
    status: PaymentStatus
    amount: Money
    reference: str
    provider_status: str = ""

    @property
    def is_settled(self) -> bool:
        """Whether the money has actually moved.

        `status == "pending"` covers a 3-D Secure challenge the shopper has not
        finished and a PayPal order awaiting payer action. Neither is a
        failure, and neither is a reason to ship an order.
        """
        return self.status == "succeeded"


@dataclass(frozen=True, slots=True)
class Refund:
    """A refund as this application understands it.

    `amount` is what was actually refunded, which is not always what was asked
    for — a caller refunding "everything" passes no amount and finds out here.
    """

    provider: str
    provider_refund_id: str
    provider_payment_id: str
    status: RefundStatus
    amount: Money
    provider_status: str = ""


@runtime_checkable
class PaymentGateway(Protocol):
    """The operations every payment provider supports.

    Narrow on purpose, and narrower than either provider's API. There is no
    customer vaulting, no subscription billing and no webhook verification
    here: each provider models those differently enough that a shared signature
    would be a shape only one of them could honour — Stripe verifies webhooks
    with an HMAC over the raw body, PayPal with a signed certificate chain
    fetched from its own CDN. Those stay on the concrete adapters, the same way
    presigned URLs stay on `S3Storage` rather than joining `StorageBackend`.
    """

    @property
    def name(self) -> str:
        """Provider identifier, e.g. `"stripe"`. Used in logs and errors."""
        ...

    async def charge(self, request: ChargeRequest) -> Payment:
        """Take payment, or raise.

        Must be idempotent on `request.reference`: calling twice with the same
        reference and amount charges once and returns the same payment.

        Raises:
            PaymentDeclinedError: the provider refused the charge.
            PaymentGatewayUnavailableError: the provider could not be reached.
            PaymentError: the provider answered something unusable.
        """
        ...

    async def refund(
        self, provider_payment_id: str, amount: Money | None = None
    ) -> Refund:
        """Refund a payment in full, or partially when `amount` is given.

        Raises:
            PaymentNotFoundError: no such payment at this provider.
            PaymentGatewayUnavailableError: the provider could not be reached.
            PaymentError: the refund was rejected or the answer was unusable.
        """
        ...

    async def get_payment(self, provider_payment_id: str) -> Payment:
        """Fetch a payment's current state.

        Raises:
            PaymentNotFoundError: no such payment at this provider.
            PaymentGatewayUnavailableError: the provider could not be reached.
        """
        ...


def validate_refund_amount(payment_amount: Money, refund_amount: Money) -> None:
    """Check a partial refund against the payment before any I/O.

    For callers that already hold the `Payment` — which is the normal case,
    since a payment worth refunding is a payment that was persisted. The
    adapters deliberately do not call this: doing so would mean a `get_payment`
    round trip on every partial refund to re-fetch a figure the caller already
    has, and both providers reject an over-refund anyway. What this buys is
    that they reject it with different status codes and different error bodies,
    so validating up here turns two provider-shaped 502s into one predictable
    400 without spending a request to find out.
    """
    if refund_amount.currency != payment_amount.currency:
        raise BadRequestError(
            "Refund currency must match the payment currency.",
            details={
                "payment_currency": payment_amount.currency,
                "refund_currency": refund_amount.currency,
            },
        )

    if refund_amount.amount_minor > payment_amount.amount_minor:
        raise BadRequestError(
            "Refund amount must not exceed the payment amount.",
            details={
                "payment_amount": payment_amount.amount_minor,
                "refund_amount": refund_amount.amount_minor,
            },
        )
