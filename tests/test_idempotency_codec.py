"""The wire format a persistent store reads and writes."""

from __future__ import annotations

import json

import pytest

from src.idempotency.base import (
    IdempotencyRecord,
    IdempotencyStoreUnavailableError,
    StoredResponse,
)
from src.idempotency.codec import SCHEMA_VERSION, decode_record, encode_record


class TestRoundTrip:
    def test_a_reservation_survives(self) -> None:
        record = IdempotencyRecord(fingerprint="abc123")
        assert decode_record(encode_record(record)) == record

    def test_a_completed_record_survives(self) -> None:
        record = IdempotencyRecord(
            fingerprint="abc123",
            response=StoredResponse(
                status_code=201,
                headers=(("content-type", "application/json"), ("x-total", "1")),
                body=b'{"id":1}',
            ),
        )
        assert decode_record(encode_record(record)) == record

    def test_repeated_headers_are_preserved(self) -> None:
        """Headers are pairs, not a mapping, so a repeated name is not collapsed."""
        record = IdempotencyRecord(
            fingerprint="abc123",
            response=StoredResponse(
                status_code=200,
                headers=(("vary", "accept"), ("vary", "origin")),
                body=b"",
            ),
        )
        decoded = decode_record(encode_record(record))
        assert decoded is not None
        assert decoded.response is not None
        assert decoded.response.headers == (("vary", "accept"), ("vary", "origin"))

    def test_a_non_utf8_body_survives(self) -> None:
        """Bodies are arbitrary bytes — an image, a gzip stream — not text."""
        body = b"\x89PNG\r\n\x1a\n\xff\xfe"
        record = IdempotencyRecord(
            fingerprint="abc123",
            response=StoredResponse(status_code=200, headers=(), body=body),
        )
        decoded = decode_record(encode_record(record))
        assert decoded is not None
        assert decoded.response is not None
        assert decoded.response.body == body


class TestSchemaVersioning:
    def test_a_foreign_version_reads_as_a_miss(self) -> None:
        """A record from another release is discarded, not misread.

        The cost is one re-execution across a deploy boundary, which is what a
        cache miss already costs.
        """
        raw = json.dumps(
            {"v": SCHEMA_VERSION + 1, "fingerprint": "abc", "response": None}
        ).encode()
        assert decode_record(raw) is None

    def test_a_missing_version_reads_as_a_miss(self) -> None:
        assert decode_record(json.dumps({"fingerprint": "abc"}).encode()) is None


class TestCorruptPayloads:
    @pytest.mark.parametrize(
        "raw",
        [
            b"not json at all",
            b"\xff\xfe\x00",
            b"[1, 2, 3]",
            b'"a string"',
        ],
    )
    def test_unparseable_payloads_raise(self, raw: bytes) -> None:
        """Something else is writing into this namespace — that is worth an error.

        Treating it as a miss would silently re-execute every request while
        hiding a misconfiguration that affects every key at once.
        """
        with pytest.raises(IdempotencyStoreUnavailableError):
            decode_record(raw)

    @pytest.mark.parametrize(
        "payload",
        [
            {"v": SCHEMA_VERSION},
            {"v": SCHEMA_VERSION, "fingerprint": "abc"},
            {"v": SCHEMA_VERSION, "fingerprint": "abc", "response": {"body": "e30="}},
            {
                "v": SCHEMA_VERSION,
                "fingerprint": "abc",
                "response": {"status_code": "not-a-number", "headers": [], "body": ""},
            },
        ],
    )
    def test_structurally_wrong_payloads_raise(
        self, payload: dict[str, object]
    ) -> None:
        with pytest.raises(IdempotencyStoreUnavailableError):
            decode_record(json.dumps(payload).encode())

    def test_the_error_is_a_503(self) -> None:
        with pytest.raises(IdempotencyStoreUnavailableError) as exc_info:
            decode_record(b"{")
        assert exc_info.value.status_code == 503
