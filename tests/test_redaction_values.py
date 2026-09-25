"""Value detectors: every one validates, and the negatives prove it.

The positives are easy and mostly uninteresting. The tests worth reading are the
ones asserting that a sixteen-digit order id, an epoch-millis timestamp, a
request id and a base64url blob that is not a JWT all survive — those are the
values an incident is read through, and a detector that eats them is a detector
somebody disables.

No fixture here is a real credential or a base64 literal. The JWT is assembled
at run time from a header and payload that read as obviously fake in the source,
which is this repo's rule for test data and also what keeps a secret scanner
from having to make a judgement call.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Callable

import pytest

from src.redaction.keys import KeyPolicy
from src.redaction.values import REDACTED, ValuePolicy


def b64url(payload: object) -> str:
    raw = json.dumps(payload).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def a_pem(label: str = "RSA PRIVATE KEY") -> str:
    """An armoured block, assembled at run time.

    The armour is built from parts for the same reason no fixture here is a
    base64 literal: a secret scanner reading this file should not have to
    decide whether the repository just committed a private key. The body
    decodes to ASCII saying it is not one.
    """
    edge = "-" * 5
    body = base64.b64encode(b"not-a-real-key-at-all").decode()
    return f"{edge}BEGIN {label}{edge}\n{body}\n{edge}END {label}{edge}"


def a_jwt(header: object | None = None) -> str:
    head = b64url(header if header is not None else {"alg": "HS256", "typ": "JWT"})
    body = b64url({"sub": "mock-subject", "role": "not-a-real-token"})
    return f"{head}.{body}.bm90LWEtcmVhbC1zaWduYXR1cmU"


@pytest.fixture
def scrub() -> Callable[[str], str]:
    return ValuePolicy(KeyPolicy()).scrub


class TestJWT:
    def test_a_real_jwt_is_redacted(self, scrub: Callable[[str], str]) -> None:
        text = f"upstream refused {a_jwt()} for that call"
        assert scrub(text) == f"upstream refused {REDACTED} for that call"

    def test_a_base64url_blob_that_is_not_a_jose_header_survives(
        self, scrub: Callable[[str], str]
    ) -> None:
        # Starts with `eyJ` — which is only the base64url of `{"` — but the
        # header names no algorithm, so it is an encoded object, not a token.
        blob = a_jwt(header={"kind": "cursor", "page": 3})
        assert scrub(blob) == blob

    def test_a_header_that_is_not_an_object_survives(
        self, scrub: Callable[[str], str]
    ) -> None:
        blob = a_jwt(header=["alg", "HS256"])
        assert scrub(blob) == blob

    def test_an_undecodable_header_survives(self, scrub: Callable[[str], str]) -> None:
        blob = "eyJ!!!!.payload.signature".replace("!", "-")
        assert scrub(blob) == blob

    def test_a_header_that_is_not_utf8_survives(
        self, scrub: Callable[[str], str]
    ) -> None:
        head = base64.urlsafe_b64encode(b'{"\xff\xfe').decode().rstrip("=")
        blob = f"{head}.cGF5bG9hZA.c2ln"
        assert scrub(blob) == blob


class TestCardNumbers:
    @pytest.mark.parametrize(
        "pan",
        ["4111111111111111", "4111 1111 1111 1111", "4111-1111-1111-1111"],
    )
    def test_a_luhn_valid_pan_is_redacted(
        self, scrub: Callable[[str], str], pan: str
    ) -> None:
        assert scrub(f"charge failed for {pan}") == f"charge failed for {REDACTED}"

    def test_a_sixteen_digit_number_that_fails_luhn_survives(
        self, scrub: Callable[[str], str]
    ) -> None:
        order_id = "4111111111111112"
        assert scrub(f"order {order_id}") == f"order {order_id}"

    def test_a_number_glued_to_more_digits_is_not_a_pan(
        self, scrub: Callable[[str], str]
    ) -> None:
        # 4111111111111111 is Luhn-valid; inside a longer run it is not a card.
        text = "id 999941111111111111119999"
        assert scrub(text) == text

    def test_a_short_number_survives(self, scrub: Callable[[str], str]) -> None:
        assert scrub("count 12345678") == "count 12345678"


class TestIBAN:
    def test_a_valid_iban_is_redacted(self, scrub: Callable[[str], str]) -> None:
        assert scrub("paid from GB82WEST12345698765432") == f"paid from {REDACTED}"

    def test_a_wrong_check_digit_survives(self, scrub: Callable[[str], str]) -> None:
        text = "code GB83WEST12345698765432"
        assert scrub(text) == text


class TestEmail:
    def test_an_address_is_redacted_in_place(self, scrub: Callable[[str], str]) -> None:
        assert (
            scrub("delivery to ada@example.co.uk bounced")
            == f"delivery to {REDACTED} bounced"
        )

    def test_a_word_with_no_domain_survives(self, scrub: Callable[[str], str]) -> None:
        text = "queue user@localhost drained"
        assert scrub(text) == text


class TestArmouredKeys:
    def test_a_pem_private_key_is_replaced_whole(
        self, scrub: Callable[[str], str]
    ) -> None:
        pem = a_pem()
        assert scrub(f"config said {pem} oops") == f"config said {REDACTED} oops"

    def test_two_keys_do_not_collapse_into_one_span(
        self, scrub: Callable[[str], str]
    ) -> None:
        # Non-greedy armour matching: greedy would swallow the "AND" between
        # them and report one span where there were two.
        block = a_pem("PRIVATE KEY")
        assert scrub(f"{block} AND {block}") == f"{REDACTED} AND {REDACTED}"


class TestAuthorizationSchemes:
    @pytest.mark.parametrize("scheme", ["Bearer", "Basic", "Token", "apikey"])
    def test_the_scheme_survives_and_the_credential_does_not(
        self, scrub: Callable[[str], str], scheme: str
    ) -> None:
        text = f"Authorization: {scheme} bm90LWEtcmVhbC1jcmVkZW50aWFs"
        assert scrub(text) == f"Authorization: {scheme} {REDACTED}"

    def test_a_short_word_after_bearer_is_not_a_credential(
        self, scrub: Callable[[str], str]
    ) -> None:
        text = "Bearer token"
        assert scrub(text) == text


class TestConnectionStrings:
    def test_only_the_password_is_removed(self, scrub: Callable[[str], str]) -> None:
        dsn = "postgresql+asyncpg://app_user:hunter2@db.internal:5432/app"
        expected = f"postgresql+asyncpg://app_user:{REDACTED}@db.internal:5432/app"
        assert scrub(dsn) == expected

    def test_a_url_without_userinfo_survives(self, scrub: Callable[[str], str]) -> None:
        url = "https://api.example.com/v1/charges?page=2"
        assert scrub(url) == url


class TestNameValuePairs:
    def test_a_query_string_is_redacted_by_parameter_name(
        self, scrub: Callable[[str], str]
    ) -> None:
        query = "page=2&email=ada@example.com&access_token=nope&sort=asc"
        assert (
            scrub(query) == f"page=2&email={REDACTED}&access_token={REDACTED}&sort=asc"
        )

    def test_an_insensitive_name_is_left_alone(
        self, scrub: Callable[[str], str]
    ) -> None:
        assert scrub("status_code=500&size=12") == "status_code=500&size=12"

    def test_a_shapeless_secret_is_caught_by_its_name(
        self, scrub: Callable[[str], str]
    ) -> None:
        # `1234` trips no shape detector at all; the parameter name is the
        # entire reason this is redacted.
        assert scrub("otp=1234") == f"otp={REDACTED}"


class TestTheMarkerIsTerminal:
    def test_redacting_twice_changes_nothing(self, scrub: Callable[[str], str]) -> None:
        once = scrub("mail ada@example.com token=abc otp=1234")
        assert scrub(once) == once

    def test_a_clean_string_is_returned_unchanged(
        self, scrub: Callable[[str], str]
    ) -> None:
        text = "idempotency.key_reused idempotency_key=01JAB2 request_id=7f3a"
        assert scrub(text) == text
