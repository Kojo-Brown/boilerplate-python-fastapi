"""The wire format: what it commits to, and what a parser must refuse.

Most of this file is the parser, because the parser is where a signature scheme
gets quietly weakened. Every rejection here has a specific delivery behind it
that would otherwise be accepted or misread.
"""

from __future__ import annotations

import hashlib
import hmac

import pytest

from src.webhooks.errors import SignatureHeaderMalformedError
from src.webhooks.signature import (
    SIGNATURE_VERSION,
    compute_digest,
    delivery_fingerprint,
    digest_matches,
    parse_signature_header,
    sign,
    signed_material,
)

SECRET = "a-development-secret-thirty-two-plus"
BODY = b'{"event":"order.paid","id":"evt_1"}'
TIMESTAMP = 1_700_000_000


class TestTheSignedMaterial:
    async def test_the_timestamp_is_inside_it(self) -> None:
        """The property the whole scheme rests on.

        A timestamp merely sent next to the signature is one an attacker can
        rewrite. Inside the material, changing it changes the digest.
        """
        assert signed_material(TIMESTAMP, BODY).startswith(b"1700000000.")
        assert signed_material(TIMESTAMP, BODY).endswith(BODY)

    async def test_it_is_the_body_verbatim(self) -> None:
        """No re-encoding, so a receiver signs the bytes that arrived.

        Two JSON documents that differ only in key order are different bodies
        here, which is correct and is why a receiver must not parse before it
        verifies.
        """
        a = signed_material(TIMESTAMP, b'{"a":1,"b":2}')
        b = signed_material(TIMESTAMP, b'{"b":2,"a":1}')
        assert a != b

    async def test_the_separator_cannot_be_confused_with_the_body(self) -> None:
        """A body starting with digits and a dot does not shift the split.

        Nothing is length-prefixed, so this is worth stating: `t` is decimal and
        cannot contain a `.`, so the first `.` always ends it.
        """
        assert signed_material(1, b"2.3") == b"1.2.3"
        assert signed_material(12, b".3") != signed_material(1, b"2.3")

    async def test_the_digest_is_hmac_sha256_of_that_material(self) -> None:
        """Computed independently, so the module cannot define its own truth."""
        expected = hmac.new(
            SECRET.encode(), b"1700000000." + BODY, hashlib.sha256
        ).hexdigest()
        assert compute_digest(SECRET, TIMESTAMP, BODY) == expected

    async def test_a_different_secret_gives_a_different_digest(self) -> None:
        assert compute_digest(SECRET, TIMESTAMP, BODY) != compute_digest(
            SECRET + "!", TIMESTAMP, BODY
        )

    async def test_a_different_timestamp_gives_a_different_digest(self) -> None:
        assert compute_digest(SECRET, TIMESTAMP, BODY) != compute_digest(
            SECRET, TIMESTAMP + 1, BODY
        )

    async def test_a_different_body_gives_a_different_digest(self) -> None:
        assert compute_digest(SECRET, TIMESTAMP, BODY) != compute_digest(
            SECRET, TIMESTAMP, BODY + b" "
        )


class TestSigning:
    async def test_sign_produces_a_header_the_parser_reads_back(self) -> None:
        """The round trip both halves of this codebase depend on."""
        parsed = parse_signature_header(sign(SECRET, TIMESTAMP, BODY))

        assert parsed.timestamp == TIMESTAMP
        assert parsed.digests == (compute_digest(SECRET, TIMESTAMP, BODY),)

    async def test_the_header_names_the_scheme_version(self) -> None:
        assert f",{SIGNATURE_VERSION}=" in sign(SECRET, TIMESTAMP, BODY)


class TestParsingWhatIsAccepted:
    async def test_whitespace_around_elements_is_ignored(self) -> None:
        """Header values pick up spaces in transit and in hand-written clients."""
        digest = compute_digest(SECRET, TIMESTAMP, BODY)
        parsed = parse_signature_header(f" t = {TIMESTAMP} , v1 = {digest} ")

        assert parsed.timestamp == TIMESTAMP
        assert parsed.digests == (digest,)

    async def test_several_v1_values_are_all_offered(self) -> None:
        """How a sender rotates without a flag day: sign under both secrets."""
        first = compute_digest(SECRET, TIMESTAMP, BODY)
        second = compute_digest("another-secret-of-adequate-length!!", TIMESTAMP, BODY)

        parsed = parse_signature_header(f"t={TIMESTAMP},v1={first},v1={second}")

        assert parsed.digests == (first, second)

    async def test_an_unknown_element_is_ignored(self) -> None:
        """Forward compatibility, and the only way a scheme can be upgraded.

        A sender that starts emitting `v2=` alongside its `v1=` must not break
        this receiver on the day it does.
        """
        digest = compute_digest(SECRET, TIMESTAMP, BODY)
        parsed = parse_signature_header(f"t={TIMESTAMP},v2=ignored,v1={digest}")

        assert parsed.digests == (digest,)

    async def test_an_uppercase_digest_is_accepted(self) -> None:
        """Hex case is a rendering detail, unlike the value."""
        digest = compute_digest(SECRET, TIMESTAMP, BODY)
        parsed = parse_signature_header(f"t={TIMESTAMP},v1={digest.upper()}")

        assert parsed.digests == (digest,)

    async def test_a_secret_containing_an_equals_sign_still_round_trips(self) -> None:
        """`partition` splits on the first `=`, so base64 padding survives."""
        parsed = parse_signature_header(sign("padded-secret-abcdefghijklmn==", 5, b"x"))
        assert parsed.timestamp == 5


class TestParsingWhatIsRefused:
    async def test_an_empty_header(self) -> None:
        with pytest.raises(SignatureHeaderMalformedError):
            parse_signature_header("")

    async def test_a_header_of_only_separators(self) -> None:
        with pytest.raises(SignatureHeaderMalformedError):
            parse_signature_header(" , , ")

    async def test_an_element_with_no_equals_sign(self) -> None:
        with pytest.raises(SignatureHeaderMalformedError):
            parse_signature_header(f"t={TIMESTAMP},garbage")

    async def test_a_header_with_no_timestamp(self) -> None:
        digest = compute_digest(SECRET, TIMESTAMP, BODY)
        with pytest.raises(SignatureHeaderMalformedError):
            parse_signature_header(f"v1={digest}")

    async def test_a_header_with_no_digest(self) -> None:
        with pytest.raises(SignatureHeaderMalformedError):
            parse_signature_header(f"t={TIMESTAMP}")

    async def test_two_timestamps(self) -> None:
        """Picking either would be this receiver choosing what the sender meant."""
        digest = compute_digest(SECRET, TIMESTAMP, BODY)
        with pytest.raises(SignatureHeaderMalformedError):
            parse_signature_header(f"t={TIMESTAMP},t={TIMESTAMP + 1},v1={digest}")

    @pytest.mark.parametrize(
        "value",
        [
            "1_0",  # int("1_0") == 10 — parses, and to a value nobody signed
            "+5",  # int("+5") == 5
            "-1",
            " ",
            "abc",
            "1.5",
            "1e9",
            "1" * 20,  # wider than any real second count
        ],
    )
    async def test_a_timestamp_python_would_accept_but_the_format_does_not(
        self, value: str
    ) -> None:
        """`int()` is more liberal than the wire format.

        `int("1_0")` is 10, so a header carrying it would parse to a timestamp
        the sender never signed and then fail on the digest — reporting a
        malformed header as a mismatched secret.
        """
        digest = compute_digest(SECRET, TIMESTAMP, BODY)
        with pytest.raises(SignatureHeaderMalformedError):
            parse_signature_header(f"t={value},v1={digest}")

    @pytest.mark.parametrize(
        "digest",
        [
            "",
            "abc",  # right alphabet, wrong length
            "z" * 64,  # right length, wrong alphabet
            "0" * 63,
            "0" * 65,
            "ff" * 31 + "fg",
        ],
    )
    async def test_a_v1_that_cannot_be_a_sha256_digest(self, digest: str) -> None:
        """Refused rather than ignored.

        Ignoring it would report a sender's bug as a mismatched secret, and the
        two are fixed by different people.
        """
        with pytest.raises(SignatureHeaderMalformedError):
            parse_signature_header(f"t={TIMESTAMP},v1={digest}")

    async def test_a_header_longer_than_the_cap(self) -> None:
        """The header is attacker-chosen and every digest in it costs work."""
        digest = compute_digest(SECRET, TIMESTAMP, BODY)
        with pytest.raises(SignatureHeaderMalformedError):
            parse_signature_header(f"t={TIMESTAMP},v1={digest},x={'y' * 5000}")

    async def test_a_header_with_more_elements_than_the_cap(self) -> None:
        digest = compute_digest(SECRET, TIMESTAMP, BODY)
        header = f"t={TIMESTAMP}," + ",".join(f"v1={digest}" for _ in range(20))
        with pytest.raises(SignatureHeaderMalformedError):
            parse_signature_header(header)

    async def test_a_non_ascii_digest_is_a_400_and_not_a_crash(self) -> None:
        """`hmac.compare_digest` raises `TypeError` on a non-ASCII `str`.

        Without the shape check ahead of it, this header would be a 500 rather
        than a rejected delivery.
        """
        with pytest.raises(SignatureHeaderMalformedError):
            parse_signature_header(f"t={TIMESTAMP},v1={'é' * 64}")


class TestComparison:
    async def test_the_matching_digest_is_found(self) -> None:
        digest = compute_digest(SECRET, TIMESTAMP, BODY)
        assert digest_matches(digest, ("0" * 64, digest)) is True

    async def test_a_non_matching_set_is_rejected(self) -> None:
        digest = compute_digest(SECRET, TIMESTAMP, BODY)
        assert digest_matches(digest, ("0" * 64, "f" * 64)) is False

    async def test_no_candidates_is_no_match(self) -> None:
        assert digest_matches("a" * 64, ()) is False

    async def test_a_prefix_of_the_right_digest_does_not_match(self) -> None:
        """The case `startswith` or a truncating comparison would let through."""
        digest = compute_digest(SECRET, TIMESTAMP, BODY)
        assert digest_matches(digest, (digest[:-1] + "0",)) is False


class TestTheDeliveryFingerprint:
    async def test_it_does_not_depend_on_the_secret(self) -> None:
        """Why the guard keys on this and not on the signature.

        With two secrets live during a rotation, one captured delivery has two
        signatures; keying the replay guard on the signature would give it two
        names, and dropping the retiring secret would rename it — one free
        replay per rotation.
        """
        assert delivery_fingerprint(TIMESTAMP, BODY) == delivery_fingerprint(
            TIMESTAMP, BODY
        )

    async def test_it_distinguishes_the_timestamp(self) -> None:
        """A sender's re-signed retry is a new delivery, not a replay."""
        assert delivery_fingerprint(TIMESTAMP, BODY) != delivery_fingerprint(
            TIMESTAMP + 1, BODY
        )

    async def test_it_distinguishes_the_body(self) -> None:
        assert delivery_fingerprint(TIMESTAMP, BODY) != delivery_fingerprint(
            TIMESTAMP, BODY + b" "
        )

    async def test_it_is_not_the_signature(self) -> None:
        """Stated outright: these are different values with different jobs."""
        assert delivery_fingerprint(TIMESTAMP, BODY) != compute_digest(
            SECRET, TIMESTAMP, BODY
        )
