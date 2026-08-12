"""Contract-level helpers: key validation, fingerprints, caller scoping."""

from __future__ import annotations

import pytest

from src.idempotency.base import (
    MAX_KEY_LENGTH,
    IdempotencyKeyInvalidError,
    IdempotencyRecord,
    StoredResponse,
    request_fingerprint,
    scope_fingerprint,
    storage_key,
    validate_idempotency_key,
)


def _fingerprint(**overrides: object) -> str:
    kwargs: dict[str, object] = {
        "method": "POST",
        "path": "/api/v1/orders",
        "query": b"",
        "body": b'{"amount":100}',
        "content_type": "application/json",
    }
    kwargs.update(overrides)
    return request_fingerprint(**kwargs)  # type: ignore[arg-type]


class TestValidateIdempotencyKey:
    def test_accepts_a_uuid(self) -> None:
        key = "3f1c2b7e-2c19-4a4f-8a1b-1d9f0c7f5f11"
        assert validate_idempotency_key(key) == key

    def test_accepts_the_maximum_length(self) -> None:
        key = "k" * MAX_KEY_LENGTH
        assert validate_idempotency_key(key) == key

    def test_rejects_an_empty_key(self) -> None:
        with pytest.raises(IdempotencyKeyInvalidError):
            validate_idempotency_key("")

    def test_rejects_an_overlong_key(self) -> None:
        with pytest.raises(IdempotencyKeyInvalidError) as exc_info:
            validate_idempotency_key("k" * (MAX_KEY_LENGTH + 1))
        assert exc_info.value.details == {"length": MAX_KEY_LENGTH + 1}

    @pytest.mark.parametrize(
        "key",
        [
            "has space",
            "has\ttab",
            "has\nnewline",
            "unicode-é",
            "trailing\x00",
        ],
    )
    def test_rejects_unsafe_characters(self, key: str) -> None:
        """Whitespace and control characters never reach a Redis key or a log."""
        with pytest.raises(IdempotencyKeyInvalidError):
            validate_idempotency_key(key)

    def test_is_a_bad_request(self) -> None:
        """A malformed key is the client's mistake, not a store failure."""
        with pytest.raises(IdempotencyKeyInvalidError) as exc_info:
            validate_idempotency_key("")
        assert exc_info.value.status_code == 400
        assert exc_info.value.error_code == "IDEMPOTENCY_KEY_INVALID"


class TestRequestFingerprint:
    def test_is_stable_for_identical_requests(self) -> None:
        assert _fingerprint() == _fingerprint()

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("method", "PUT"),
            ("path", "/api/v1/refunds"),
            ("query", b"page=2"),
            ("body", b'{"amount":200}'),
            ("content_type", "application/x-www-form-urlencoded"),
        ],
    )
    def test_changes_with_every_component(self, field: str, value: object) -> None:
        assert _fingerprint(**{field: value}) != _fingerprint()

    def test_ignores_method_case(self) -> None:
        assert _fingerprint(method="post") == _fingerprint(method="POST")

    def test_length_prefixing_prevents_boundary_collisions(self) -> None:
        """`POST /a/b` with no body must not hash like `POST /a` with body `/b`.

        Plain concatenation would make those two identical, which would let one
        request replay the other's response.
        """
        first = request_fingerprint(
            method="POST", path="/a/b", query=b"", body=b"", content_type=""
        )
        second = request_fingerprint(
            method="POST", path="/a", query=b"", body=b"/b", content_type=""
        )
        assert first != second


class TestScopeFingerprint:
    def test_credentials_get_their_own_namespace(self) -> None:
        assert scope_fingerprint("Bearer alice") != scope_fingerprint("Bearer bob")

    def test_the_same_credential_is_stable(self) -> None:
        assert scope_fingerprint("Bearer alice") == scope_fingerprint("Bearer alice")

    @pytest.mark.parametrize("value", [None, ""])
    def test_missing_credentials_share_the_anon_namespace(
        self, value: str | None
    ) -> None:
        assert scope_fingerprint(value) == "anon"

    def test_does_not_leak_the_credential(self) -> None:
        """The token itself must not end up in a Redis key or an error body."""
        token = "Bearer mock-access-token-not-a-real-jwt"
        assert token not in scope_fingerprint(token)
        assert "mock-access-token" not in storage_key(scope_fingerprint(token), "k1")


class TestIdempotencyRecord:
    def test_a_reservation_is_in_progress(self) -> None:
        assert IdempotencyRecord(fingerprint="f").in_progress is True

    def test_a_completed_record_is_not(self) -> None:
        record = IdempotencyRecord(
            fingerprint="f",
            response=StoredResponse(status_code=201, headers=(), body=b"{}"),
        )
        assert record.in_progress is False
