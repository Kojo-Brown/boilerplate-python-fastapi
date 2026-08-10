"""Gateway selection from configuration."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from src.config import Settings
from src.payments.base import (
    ChargeRequest,
    Money,
    Payment,
    PaymentConfigurationError,
    PaymentGateway,
    Refund,
    UnknownPaymentGatewayError,
)
from src.payments.paypal import PayPalGateway
from src.payments.registry import PaymentGatewayRegistry, get_payment_gateway
from src.payments.stripe import StripeGateway


def make_settings(**overrides: object) -> Settings:
    defaults: dict[str, object] = {
        "DATABASE_URL": "postgresql+asyncpg://test:test@localhost/test",
        "SECRET_KEY": "mock-secret-key-for-tests",
        "STRIPE_SECRET_KEY": "sk_test_fake",
        "PAYPAL_CLIENT_ID": "mock-client-id",
        "PAYPAL_CLIENT_SECRET": "mock-client-secret",
    }
    defaults.update(overrides)
    return Settings(**defaults)  # type: ignore[arg-type]


@pytest.fixture(autouse=True)
def restore_registry() -> Iterator[None]:
    yield
    PaymentGatewayRegistry.reset()
    get_payment_gateway.cache_clear()


def test_default_selection_follows_the_configured_provider() -> None:
    gateway = PaymentGatewayRegistry.create(config=make_settings())
    assert isinstance(gateway, StripeGateway)

    other = PaymentGatewayRegistry.create(
        config=make_settings(PAYMENT_GATEWAY="paypal")
    )
    assert isinstance(other, PayPalGateway)


def test_an_explicit_name_overrides_the_configuration() -> None:
    gateway = PaymentGatewayRegistry.create("paypal", config=make_settings())
    assert isinstance(gateway, PayPalGateway)


def test_every_built_in_gateway_satisfies_the_protocol() -> None:
    for name in PaymentGatewayRegistry.available():
        gateway = PaymentGatewayRegistry.create(name, config=make_settings())
        assert isinstance(gateway, PaymentGateway)
        assert gateway.name == name


def test_unknown_gateway_names_the_alternatives() -> None:
    with pytest.raises(UnknownPaymentGatewayError) as excinfo:
        PaymentGatewayRegistry.create("bitcoin", config=make_settings())

    assert excinfo.value.details == {
        "requested": "bitcoin",
        "available": ["paypal", "stripe"],
    }


@pytest.mark.parametrize(
    ("provider", "missing"),
    [
        ("stripe", "STRIPE_SECRET_KEY"),
        ("paypal", "PAYPAL_CLIENT_ID"),
    ],
)
def test_missing_credentials_fail_at_build_time(provider: str, missing: str) -> None:
    """Better a 500 naming the variable than a 502 at a customer's checkout."""
    settings = make_settings(**{missing: ""})

    with pytest.raises(PaymentConfigurationError) as excinfo:
        PaymentGatewayRegistry.create(provider, config=settings)

    assert excinfo.value.details == {"provider": provider, "missing": missing}


def test_paypal_reports_a_missing_secret_separately() -> None:
    with pytest.raises(PaymentConfigurationError) as excinfo:
        PaymentGatewayRegistry.create(
            "paypal", config=make_settings(PAYPAL_CLIENT_SECRET="")
        )

    assert excinfo.value.details == {
        "provider": "paypal",
        "missing": "PAYPAL_CLIENT_SECRET",
    }


def test_registering_a_provider_needs_no_change_here() -> None:
    """Open for extension: a third provider is a module plus one call."""

    class AdyenGateway:
        @property
        def name(self) -> str:
            return "adyen"

        async def charge(self, request: ChargeRequest) -> Payment:  # pragma: no cover
            raise NotImplementedError

        async def refund(  # pragma: no cover
            self, provider_payment_id: str, amount: Money | None = None
        ) -> Refund:
            raise NotImplementedError

        async def get_payment(  # pragma: no cover
            self, provider_payment_id: str
        ) -> Payment:
            raise NotImplementedError

    PaymentGatewayRegistry.register("adyen", lambda _: AdyenGateway())

    assert "adyen" in PaymentGatewayRegistry.available()
    gateway = PaymentGatewayRegistry.create("adyen", config=make_settings())
    assert isinstance(gateway, PaymentGateway)

    PaymentGatewayRegistry.unregister("adyen")
    assert "adyen" not in PaymentGatewayRegistry.available()
    # Unregistering something absent is a no-op, not an error.
    PaymentGatewayRegistry.unregister("adyen")


def test_reset_restores_the_built_ins() -> None:
    PaymentGatewayRegistry.unregister("stripe")
    assert "stripe" not in PaymentGatewayRegistry.available()

    PaymentGatewayRegistry.reset()
    assert PaymentGatewayRegistry.available() == ("paypal", "stripe")


def test_get_payment_gateway_is_cached() -> None:
    """One gateway per process: an httpx pool and a PayPal token worth keeping."""
    PaymentGatewayRegistry.register("stripe", lambda _: StripeGateway(api_key="sk_x"))

    first = get_payment_gateway()
    second = get_payment_gateway()
    assert first is second

    get_payment_gateway.cache_clear()
    assert get_payment_gateway() is not first
