"""The key-name policy: what it catches, and what it deliberately does not.

Half of these tests assert a *negative*, which is the unusual half and the
important one. A redactor that over-matches is not a safe redactor: it fills an
incident's logs with markers where nothing was ever sensitive, and the thing
that gets changed at 3am is the redactor. Every field name asserted here to
survive is one this codebase actually logs — they were taken from the call sites
in `src/`, not invented.
"""

from __future__ import annotations

import pytest

from src.redaction.keys import SENSITIVE_PHRASES, KeyPolicy, words


class TestSplittingAName:
    @pytest.mark.parametrize(
        ("key", "expected"),
        [
            ("email", ("email",)),
            ("user_email", ("user", "email")),
            ("userEmailAddress", ("user", "email", "address")),
            ("STRIPE_API_KEY", ("stripe", "api", "key")),
            ("addr1", ("addr", "1")),
            ("x2Token", ("x", "2", "token")),
            ("HTTPSProxy", ("https", "proxy")),
            ("passengers", ("passengers",)),
            ("kebab-case-key", ("kebab", "case", "key")),
            ("", ()),
        ],
    )
    def test_words(self, key: str, expected: tuple[str, ...]) -> None:
        assert words(key) == expected


class TestWhatIsSensitive:
    @pytest.mark.parametrize(
        "key",
        [
            "password",
            "password_hash",
            "hashedPassword",
            "passphrase",
            "client_secret",
            "SECRET",
            "access_token",
            "refreshToken",
            "authorization",
            "cookie",
            "set_cookie",
            "api_key",
            "apiKey",
            "stripe_api_key",
            "private_key",
            "encryption_key",
            "connection_string",
            "DSN",
            "otp",
            "mfa_code",
            "recovery_code",
            "email",
            "user_email",
            "emailAddress",
            "ssn",
            "social_security_number",
            "passport_number",
            "full_name",
            "firstName",
            "surname",
            "date_of_birth",
            "dob",
            "phone",
            "phone_number",
            "street_address",
            "billing_address",
            "zip_code",
            "postcode",
            "card_number",
            "cvv",
            "iban",
            "routing_number",
        ],
    )
    def test_sensitive(self, key: str) -> None:
        assert KeyPolicy().is_sensitive(key)

    @pytest.mark.parametrize(
        "key",
        [
            # Every one of these is logged by this codebase today.
            "key",
            "idempotency_key",
            "event_id",
            "user_id",
            "request_id",
            "message_id",
            "payment_id",
            "trace_id",
            "event_name",
            "consumer",
            "stream",
            "backend",
            "provider",
            "path",
            "status_code",
            "size",
            "offset",
            "partition",
            "lock",
            "error",
            "reason",
            "detail",
            "attempt",
            "signature",
            "ip_address",
            "client",
            "to",
            # And these are the substring traps.
            "passengers",
            "tokenizer",
            "keyspace",
            "keep_alive",
            "secretary_id",
            "panel_id",
            "companion",
            "biCycle",
        ],
    )
    def test_not_sensitive(self, key: str) -> None:
        assert not KeyPolicy().is_sensitive(key)

    def test_a_phrase_must_be_contiguous(self) -> None:
        # "date of birth" is a phrase; "date" near "birth" is not.
        policy = KeyPolicy()
        assert policy.is_sensitive("customer_date_of_birth")
        assert not policy.is_sensitive("date_registered_birth_country")

    def test_the_verdict_is_cached_not_recomputed(self) -> None:
        policy = KeyPolicy()
        assert policy.is_sensitive("user_email")
        # Second call takes the cached path; the answer must not change.
        assert policy.is_sensitive("user_email")
        assert not policy.is_sensitive("user_id")
        assert not policy.is_sensitive("user_id")


class TestWidening:
    def test_extra_keys_are_added(self) -> None:
        policy = KeyPolicy().widened_with("employeeNumber, badge_id")
        assert policy.is_sensitive("employee_number")
        assert policy.is_sensitive("badgeId")
        # The built-in list survives widening.
        assert policy.is_sensitive("password")

    def test_an_empty_setting_returns_the_same_policy(self) -> None:
        policy = KeyPolicy()
        assert policy.widened_with("") is policy
        assert policy.widened_with("  ,  , ") is policy

    def test_widening_cannot_narrow(self) -> None:
        widened = KeyPolicy().widened_with("employee_number")
        for phrase in SENSITIVE_PHRASES:
            assert widened.is_sensitive("_".join(phrase))

    def test_a_longer_phrase_than_any_built_in_one_still_matches(self) -> None:
        # `_longest` bounds the scan window, so an added phrase longer than
        # every built-in one is the case where an off-by-one would hide it.
        longest_builtin = max(len(phrase) for phrase in SENSITIVE_PHRASES)
        added = "_".join(f"w{index}" for index in range(longest_builtin + 2))
        policy = KeyPolicy().widened_with(added)
        assert policy.is_sensitive(f"prefix_{added}_suffix")
