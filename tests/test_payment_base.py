"""The value objects the adapters translate to and from.

`Money` is the one that has to be right: it is the only place in this codebase
that knows how many decimal places a currency settles in, and both adapters
depend on it for opposite conversions.
"""

from __future__ import annotations

import pytest

from src.exceptions import BadRequestError
from src.payments.base import (
    MAX_DESCRIPTION_LENGTH,
    MAX_REFERENCE_LENGTH,
    ChargeRequest,
    Money,
    Payment,
    PaymentDeclinedError,
    PaymentGateway,
    PaymentGatewayUnavailableError,
    PaymentNotFoundError,
    Refund,
    UnsupportedCurrencyError,
)


class TestMoney:
    def test_currency_is_normalised_and_compared_case_insensitively(self) -> None:
        assert Money(amount_minor=100, currency="usd") == Money(
            amount_minor=100, currency="USD"
        )

    def test_unknown_currency_is_refused_rather_than_assumed(self) -> None:
        """No default exponent: guessing 2 is a 100x error on a zero-decimal."""
        with pytest.raises(UnsupportedCurrencyError):
            Money(amount_minor=100, currency="XYZ")

    @pytest.mark.parametrize("amount", [0, -1])
    def test_non_positive_amounts_are_refused(self, amount: int) -> None:
        with pytest.raises(BadRequestError):
            Money(amount_minor=amount, currency="USD")

    @pytest.mark.parametrize(
        ("minor", "currency", "expected"),
        [
            (2500, "USD", "25.00"),
            (1, "USD", "0.01"),
            (100000, "USD", "1000.00"),
            (1000, "JPY", "1000"),
            (1234, "KWD", "1.234"),
            (1, "BHD", "0.001"),
        ],
    )
    def test_to_decimal_string_uses_the_currency_exponent(
        self, minor: int, currency: str, expected: str
    ) -> None:
        assert Money(amount_minor=minor, currency=currency).to_decimal_string() == (
            expected
        )

    @pytest.mark.parametrize(
        ("value", "currency", "expected"),
        [
            ("25.00", "USD", 2500),
            ("0.01", "USD", 1),
            ("1000", "JPY", 1000),
            ("1.234", "KWD", 1234),
            ("25", "USD", 2500),
        ],
    )
    def test_from_decimal_string_parses_provider_values(
        self, value: str, currency: str, expected: int
    ) -> None:
        assert Money.from_decimal_string(value, currency).amount_minor == expected

    def test_round_trips_through_the_decimal_string(self) -> None:
        for currency, minor in [("USD", 2500), ("JPY", 1000), ("KWD", 1234)]:
            money = Money(amount_minor=minor, currency=currency)
            assert Money.from_decimal_string(money.to_decimal_string(), currency) == (
                money
            )

    def test_excess_precision_raises_rather_than_rounding(self) -> None:
        """A provider reporting a tenth of a cent is not understood here.

        Rounding it silently loses money in whichever direction the mode falls,
        every transaction, which is the kind of bug nobody finds for a year.
        """
        with pytest.raises(BadRequestError):
            Money.from_decimal_string("10.005", "USD")

    def test_fractional_zero_decimal_currency_raises(self) -> None:
        with pytest.raises(BadRequestError):
            Money.from_decimal_string("10.50", "JPY")

    @pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
    def test_non_finite_values_are_refused(self, value: str) -> None:
        """`Decimal` parses these happily; a payment amount must not be one."""
        with pytest.raises(BadRequestError):
            Money.from_decimal_string(value, "USD")

    def test_unparseable_value_raises(self) -> None:
        with pytest.raises(BadRequestError):
            Money.from_decimal_string("twenty-five", "USD")

    def test_unknown_currency_on_parse_raises(self) -> None:
        with pytest.raises(UnsupportedCurrencyError):
            Money.from_decimal_string("25.00", "XYZ")

    def test_str_is_human_readable(self) -> None:
        assert str(Money(amount_minor=2500, currency="USD")) == "25.00 USD"

    def test_is_hashable_and_frozen(self) -> None:
        money = Money(amount_minor=100, currency="USD")
        assert {money, Money(amount_minor=100, currency="USD")} == {money}

        with pytest.raises(AttributeError):
            money.amount_minor = 200  # type: ignore[misc]


class TestChargeRequest:
    def test_rejects_an_empty_token(self) -> None:
        with pytest.raises(BadRequestError):
            ChargeRequest(
                amount=Money(amount_minor=100, currency="USD"),
                payment_method_token="   ",
                reference="order-1",
            )

    def test_rejects_an_empty_reference(self) -> None:
        """The reference is the idempotency key; blank means no protection."""
        with pytest.raises(BadRequestError):
            ChargeRequest(
                amount=Money(amount_minor=100, currency="USD"),
                payment_method_token="mock-token",
                reference="",
            )

    def test_rejects_an_over_long_reference(self) -> None:
        with pytest.raises(BadRequestError):
            ChargeRequest(
                amount=Money(amount_minor=100, currency="USD"),
                payment_method_token="mock-token",
                reference="x" * (MAX_REFERENCE_LENGTH + 1),
            )

    def test_rejects_an_over_long_description(self) -> None:
        with pytest.raises(BadRequestError):
            ChargeRequest(
                amount=Money(amount_minor=100, currency="USD"),
                payment_method_token="mock-token",
                reference="order-1",
                description="x" * (MAX_DESCRIPTION_LENGTH + 1),
            )

    def test_metadata_defaults_to_empty_and_cannot_be_shared_between_charges(
        self,
    ) -> None:
        """The default is one shared object, and that is now safe.

        This used to assert `first.metadata is not second.metadata`, because
        the default was `field(default_factory=dict)` and a shared mutable
        default is the classic way one charge's metadata turns up on another's.
        The default is now a `FrozenDict`, so the two *are* the same object and
        the leak it guarded against is unrepresentable rather than merely
        absent — which is what this asserts instead.
        """
        first = ChargeRequest(
            amount=Money(amount_minor=100, currency="USD"),
            payment_method_token="mock-token",
            reference="order-1",
        )
        second = ChargeRequest(
            amount=Money(amount_minor=100, currency="USD"),
            payment_method_token="mock-token",
            reference="order-2",
        )
        assert first.metadata == {} and second.metadata == {}

        with pytest.raises(TypeError):
            first.metadata["leaked"] = "yes"  # type: ignore[index]
        assert second.metadata == {}

    def test_metadata_is_copied_away_from_the_caller(self) -> None:
        """A charge is not aliased to the dict it was built from.

        `frozen=True` alone leaves the caller's mapping in place, so a
        `metadata` edited after construction would reach both adapters — both
        send it upstream — and the provider's record of the charge would
        disagree with the request that was validated.
        """
        source = {"order": "1"}
        request = ChargeRequest(
            amount=Money(amount_minor=100, currency="USD"),
            payment_method_token="mock-token",
            reference="order-1",
            metadata=source,
        )

        source["order"] = "2"

        assert request.metadata == {"order": "1"}

    def test_a_charge_request_is_hashable(self) -> None:
        """Which it is not while `metadata` is a plain dict.

        A value object that cannot be hashed cannot be a set member, a dict
        key, or an argument to a `@cached` function — `src/decorators/cache.py`
        raises `UnhashableArgumentError` for exactly this.
        """
        request = ChargeRequest(
            amount=Money(amount_minor=100, currency="USD"),
            payment_method_token="mock-token",
            reference="order-1",
            metadata={"order": "1"},
        )
        assert hash(request) == hash(
            ChargeRequest(
                amount=Money(amount_minor=100, currency="USD"),
                payment_method_token="mock-token",
                reference="order-1",
                metadata={"order": "1"},
            )
        )


class TestPayment:
    @pytest.mark.parametrize(
        ("status", "settled"),
        [("succeeded", True), ("pending", False), ("failed", False)],
    )
    def test_only_succeeded_counts_as_settled(self, status: str, settled: bool) -> None:
        payment = Payment(
            provider="stripe",
            provider_payment_id="pi_1",
            status=status,  # type: ignore[arg-type]
            amount=Money(amount_minor=100, currency="USD"),
            reference="order-1",
        )
        assert payment.is_settled is settled

    def test_refund_carries_both_ids(self) -> None:
        refund = Refund(
            provider="stripe",
            provider_refund_id="re_1",
            provider_payment_id="pi_1",
            status="succeeded",
            amount=Money(amount_minor=100, currency="USD"),
        )
        assert refund.provider_refund_id != refund.provider_payment_id


class TestExceptions:
    def test_decline_is_402_and_keeps_the_provider_code(self) -> None:
        error = PaymentDeclinedError("stripe", "Card declined", "card_declined")
        assert error.status_code == 402
        assert error.decline_code == "card_declined"

    def test_unavailable_is_503(self) -> None:
        assert PaymentGatewayUnavailableError("stripe", "HTTP 500").status_code == 503

    def test_not_found_is_404(self) -> None:
        assert PaymentNotFoundError("stripe", "pi_1").status_code == 404


class TestProtocol:
    def test_an_incomplete_implementation_is_not_a_gateway(self) -> None:
        """`runtime_checkable` checks method presence, which is the point.

        A half-written adapter should fail an `isinstance` gate rather than
        raise `AttributeError` at the first refund.
        """

        class Half:
            @property
            def name(self) -> str:
                return "half"

            async def charge(
                self, request: ChargeRequest
            ) -> Payment:  # pragma: no cover
                raise NotImplementedError

        assert not isinstance(Half(), PaymentGateway)
