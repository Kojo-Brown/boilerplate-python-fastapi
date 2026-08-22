"""Gateway selection.

Configuration picks the provider; callers depend on `PaymentGateway` and never
import `StripeGateway` or `PayPalGateway`. Switching providers — or running one
in staging and the other in production — is an environment variable, not a
code change.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import lru_cache
from typing import ClassVar, Final, Literal

import structlog

from src.config import Settings, settings
from src.immutable import FrozenDict
from src.payments.base import (
    PaymentConfigurationError,
    PaymentGateway,
    UnknownPaymentGatewayError,
)
from src.payments.paypal import PayPalGateway
from src.payments.stripe import StripeGateway

logger = structlog.get_logger(__name__)

PaymentGatewayName = Literal["stripe", "paypal"]

GatewayBuilder = Callable[[Settings], PaymentGateway]


def _build_stripe(config: Settings) -> PaymentGateway:
    # Checked at build time rather than at charge time so a deployment missing
    # its key fails on the first request to touch payments, with a message
    # naming the variable, instead of at a customer's checkout with a 502.
    if not config.STRIPE_SECRET_KEY:
        raise PaymentConfigurationError("stripe", "STRIPE_SECRET_KEY")

    return StripeGateway(
        api_key=config.STRIPE_SECRET_KEY,
        base_url=config.STRIPE_API_BASE_URL,
        timeout=config.PAYMENT_TIMEOUT_SECONDS,
    )


def _build_paypal(config: Settings) -> PaymentGateway:
    if not config.PAYPAL_CLIENT_ID:
        raise PaymentConfigurationError("paypal", "PAYPAL_CLIENT_ID")
    if not config.PAYPAL_CLIENT_SECRET:
        raise PaymentConfigurationError("paypal", "PAYPAL_CLIENT_SECRET")

    return PayPalGateway(
        client_id=config.PAYPAL_CLIENT_ID,
        client_secret=config.PAYPAL_CLIENT_SECRET,
        base_url=config.PAYPAL_API_BASE_URL,
        timeout=config.PAYMENT_TIMEOUT_SECONDS,
    )


DEFAULT_GATEWAYS: Final[FrozenDict[str, GatewayBuilder]] = FrozenDict(
    {
        "stripe": _build_stripe,
        "paypal": _build_paypal,
    }
)


class PaymentGatewayRegistry:
    """Builds a `PaymentGateway` from a provider name and settings.

    Extension is registration, not modification: an Adyen or Braintree adapter
    is a new module plus one `register` call, and nothing that already charges
    a card has to change.
    """

    _builders: ClassVar[dict[str, GatewayBuilder]] = dict(DEFAULT_GATEWAYS)

    @classmethod
    def register(cls, name: str, builder: GatewayBuilder) -> None:
        """Add or replace the builder for `name`."""
        cls._builders[name] = builder

    @classmethod
    def unregister(cls, name: str) -> None:
        """Remove a builder. No-op if it was never registered."""
        cls._builders.pop(name, None)

    @classmethod
    def reset(cls) -> None:
        """Restore the built-in registry. For tests that call `register`."""
        cls._builders = dict(DEFAULT_GATEWAYS)

    @classmethod
    def available(cls) -> tuple[str, ...]:
        return tuple(sorted(cls._builders))

    @classmethod
    def create(
        cls, name: str | None = None, *, config: Settings | None = None
    ) -> PaymentGateway:
        """Return a new gateway instance for `name`.

        `name` defaults to `PAYMENT_GATEWAY` and `config` to the process
        settings, so both arguments exist for tests and for callers that need a
        specific provider regardless of configuration.
        """
        resolved_config = config if config is not None else settings
        resolved_name = name if name is not None else resolved_config.PAYMENT_GATEWAY

        try:
            builder = cls._builders[resolved_name]
        except KeyError as exc:
            raise UnknownPaymentGatewayError(resolved_name, cls.available()) from exc

        instance = builder(resolved_config)
        logger.debug("payment.gateway_created", provider=resolved_name)
        return instance


@lru_cache(maxsize=1)
def get_payment_gateway() -> PaymentGateway:
    """Return the process-wide gateway named by `PAYMENT_GATEWAY`.

    Cached because both adapters own an `httpx.AsyncClient`, and building one
    per charge throws away the connection pool that makes it worth having —
    and, for PayPal, the cached OAuth token with it, spending a second request
    on every payment. Call `get_payment_gateway.cache_clear()` after changing
    settings or registering a provider in a test.

    Written to be usable directly as a FastAPI dependency:

        async def checkout(gateway: PaymentGateway = Depends(get_payment_gateway)):
            ...
    """
    return PaymentGatewayRegistry.create()
