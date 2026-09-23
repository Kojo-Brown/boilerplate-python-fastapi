"""The wire format, including the parts that only matter when someone is attacking it.

Three groups. The layout tests pin the bytes, so a change to the format is a
failing test rather than a column nobody can read. The authentication tests
show what the tag covers — which is the entire header and the context label,
not just the ciphertext. The parsing tests cover malformed input, because the
input is a database column and a database column is exactly what an attacker
who has got that far can write to.
"""

from __future__ import annotations

import pytest

from src.encryption.envelope import (
    FORMAT_VERSION,
    NONCE_BYTES,
    TAG_BYTES,
    envelope_overhead,
    parse,
    seal,
    unseal,
)
from src.encryption.errors import CiphertextFormatError, DecryptionError
from src.encryption.keys import DataKey

KEY = DataKey(key_id="2026-09", material=b"0" * 32)
OTHER_KEY = DataKey(key_id="2026-09", material=b"1" * 32)
CONTEXT = "users.notification_webhook_url"
FIXED_NONCE = bytes(range(NONCE_BYTES))
PLAINTEXT = b"https://hooks.example.test/T000/B000/mock-webhook-token"


class TestLayout:
    def test_known_answer(self) -> None:
        """Pins the format to a literal.

        Not a test of AES — that is `cryptography`'s job — but of the framing
        around it. Every row already in a database was written by this
        function, so a change that makes the bytes come out differently is a
        change that makes those rows unreadable, and it should be impossible to
        make by accident.
        """
        sealed = seal(KEY, b"", context=CONTEXT, nonce=FIXED_NONCE)
        assert sealed.hex() == (
            "0107323032362d3039000102030405060708090a0b2554dbc5a60189cd9bb5dc6546b9e153"
        )

    def test_header_is_version_length_id_nonce(self) -> None:
        sealed = seal(KEY, PLAINTEXT, context=CONTEXT, nonce=FIXED_NONCE)
        assert sealed[0] == FORMAT_VERSION
        assert sealed[1] == len(KEY.key_id)
        assert sealed[2 : 2 + len(KEY.key_id)] == KEY.key_id.encode("ascii")
        assert sealed[2 + len(KEY.key_id) : 2 + len(KEY.key_id) + NONCE_BYTES] == (
            FIXED_NONCE
        )

    def test_overhead_matches_the_documented_formula(self) -> None:
        sealed = seal(KEY, PLAINTEXT, context=CONTEXT)
        assert len(sealed) - len(PLAINTEXT) == envelope_overhead(KEY.key_id)
        assert envelope_overhead(KEY.key_id) == 2 + len(KEY.key_id) + NONCE_BYTES + 16

    def test_tag_is_the_full_128_bits(self) -> None:
        sealed = seal(KEY, b"", context=CONTEXT, nonce=FIXED_NONCE)
        assert len(parse(sealed).ciphertext) == TAG_BYTES

    def test_the_plaintext_is_not_in_the_output(self) -> None:
        sealed = seal(KEY, PLAINTEXT, context=CONTEXT)
        assert PLAINTEXT not in sealed

    def test_two_sealings_differ(self) -> None:
        """Randomised, which is why the comparator in types.py refuses `==`."""
        first = seal(KEY, PLAINTEXT, context=CONTEXT)
        second = seal(KEY, PLAINTEXT, context=CONTEXT)
        assert first != second
        assert parse(first).nonce != parse(second).nonce

    def test_an_empty_plaintext_round_trips(self) -> None:
        sealed = seal(KEY, b"", context=CONTEXT)
        assert unseal(KEY, parse(sealed), context=CONTEXT) == b""

    def test_refuses_a_nonce_of_the_wrong_length(self) -> None:
        with pytest.raises(CiphertextFormatError, match="nonce"):
            seal(KEY, PLAINTEXT, context=CONTEXT, nonce=b"short")


class TestAuthentication:
    def test_round_trip(self) -> None:
        sealed = seal(KEY, PLAINTEXT, context=CONTEXT)
        assert unseal(KEY, parse(sealed), context=CONTEXT) == PLAINTEXT

    def test_a_different_context_does_not_open_it(self) -> None:
        """The property that stops a value being moved between columns.

        Without the context in the additional data, a `bytea` sealed for one
        column opens perfectly in any other column using the same key — so an
        attacker with UPDATE on the table can relocate a value they cannot
        read, which is enough to point a colleague's webhook at their own
        endpoint.
        """
        sealed = seal(KEY, PLAINTEXT, context=CONTEXT)
        with pytest.raises(DecryptionError, match="did not authenticate"):
            unseal(KEY, parse(sealed), context="users.some_other_column")

    def test_a_different_key_does_not_open_it(self) -> None:
        sealed = seal(KEY, PLAINTEXT, context=CONTEXT)
        with pytest.raises(DecryptionError):
            unseal(OTHER_KEY, parse(sealed), context=CONTEXT)

    def test_a_mismatched_key_id_is_refused_before_the_cipher(self) -> None:
        sealed = seal(KEY, PLAINTEXT, context=CONTEXT)
        wrong_id = DataKey(key_id="other", material=KEY.material)
        with pytest.raises(DecryptionError, match="names key"):
            unseal(wrong_id, parse(sealed), context=CONTEXT)

    @pytest.mark.parametrize("index", [2, 5, 9, 20, -1])
    def test_flipping_any_byte_fails_the_tag(self, index: int) -> None:
        """Header bytes included, which is the point of authenticating it.

        The key id and version live in the clear and would otherwise be an
        attacker's to edit: renaming the id in a stored value is a way to make
        a process reach for a different key, and a version byte nobody checks
        is a way to reach a future parser with today's bytes.
        """
        sealed = bytearray(seal(KEY, PLAINTEXT, context=CONTEXT, nonce=FIXED_NONCE))
        sealed[index] ^= 0x01
        payload = bytes(sealed)
        try:
            envelope = parse(payload)
        except CiphertextFormatError:
            return  # The header stopped being parseable, which is also a refusal.
        with pytest.raises(DecryptionError):
            unseal(KEY, envelope, context=CONTEXT)

    def test_truncating_the_ciphertext_fails(self) -> None:
        sealed = seal(KEY, PLAINTEXT, context=CONTEXT)
        with pytest.raises(DecryptionError):
            unseal(KEY, parse(sealed[:-1]), context=CONTEXT)

    def test_the_error_does_not_guess_which_cause_it_was(self) -> None:
        sealed = seal(KEY, PLAINTEXT, context=CONTEXT)
        with pytest.raises(DecryptionError) as caught:
            unseal(OTHER_KEY, parse(sealed), context=CONTEXT)
        message = str(caught.value)
        assert "wrong key" in message
        assert "modified bytes" in message
        assert "different column context" in message


class TestParse:
    def test_reads_back_what_seal_wrote(self) -> None:
        envelope = parse(seal(KEY, PLAINTEXT, context=CONTEXT, nonce=FIXED_NONCE))
        assert envelope.version == FORMAT_VERSION
        assert envelope.key_id == KEY.key_id
        assert envelope.nonce == FIXED_NONCE

    @pytest.mark.parametrize("payload", [b"", b"\x01"])
    def test_refuses_something_too_short_to_have_a_header(self, payload: bytes) -> None:
        with pytest.raises(CiphertextFormatError, match="too short"):
            parse(payload)

    def test_refuses_an_unknown_format_version(self) -> None:
        sealed = bytearray(seal(KEY, PLAINTEXT, context=CONTEXT))
        sealed[0] = FORMAT_VERSION + 1
        with pytest.raises(CiphertextFormatError, match="format version"):
            parse(bytes(sealed))

    def test_refuses_a_zero_length_key_id(self) -> None:
        with pytest.raises(CiphertextFormatError, match="empty key id"):
            parse(bytes((FORMAT_VERSION, 0)) + b"0" * 64)

    def test_refuses_a_payload_shorter_than_its_declared_header(self) -> None:
        with pytest.raises(CiphertextFormatError, match="at least"):
            parse(bytes((FORMAT_VERSION, 64)) + b"0" * 8)

    def test_refuses_a_payload_with_a_header_but_no_tag(self) -> None:
        """An empty plaintext still costs a tag, so this is truncation."""
        sealed = seal(KEY, b"", context=CONTEXT, nonce=FIXED_NONCE)
        with pytest.raises(CiphertextFormatError, match="at least"):
            parse(sealed[:-1])

    def test_refuses_a_non_ascii_key_id(self) -> None:
        with pytest.raises(CiphertextFormatError, match="not ASCII"):
            parse(bytes((FORMAT_VERSION, 2)) + b"\xff\xfe" + b"0" * 64)
