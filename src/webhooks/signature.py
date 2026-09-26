"""The signature scheme, in one place so the two sides cannot drift.

`src/notifications/webhook.py` signs outgoing deliveries with `sign()` and
`src/webhooks/verifier.py` checks incoming ones against `compute_digest()`. They
were two implementations of one format until this module existed, which is a
shape that works right up until somebody changes the separator on one side.

## The format

    X-Webhook-Signature: t=1700000000,v1=<hex sha256>

`t` is unix seconds. `v1` is `HMAC-SHA256(secret, b"<t>." + body)`, hex. The
header may carry several `v1` values: that is how a sender rotates a secret
without a flag day, signing one delivery under both the outgoing and incoming
secret so that whichever the receiver still holds will match.

Two properties of the material are load-bearing:

**The timestamp is inside it.** A timestamp merely sent alongside the signature
is a value the sender did not commit to, so an attacker who captured one
delivery could resend it forever with the header rewritten to now. With `t` in
the signed bytes, changing it invalidates the digest — which is why
`parse_signature_header` reads the timestamp out of *this* header and why
nothing in the verifier looks at a separate timestamp header, however
conveniently one happens to be sitting there.

**It is the raw body, not a re-encoding of it.** `"{t}." + body` is assembled
from the bytes that arrived. A receiver that parses JSON and re-serialises it
before verifying computes a digest over different bytes than the sender did
whenever key order, whitespace or float formatting differ, and the failure looks
like a wrong secret.

The `.` separator sits between a decimal integer and arbitrary bytes, so the
split point is unambiguous even though nothing is length-prefixed: `t` cannot
contain a `.`, so the first one always ends it.
"""

from __future__ import annotations

import hashlib
import hmac
import re
from dataclasses import dataclass
from typing import Final

from src.webhooks.errors import SignatureHeaderMalformedError

#: The only scheme version this codebase signs or accepts.
SIGNATURE_VERSION: Final[str] = "v1"

#: Header carrying the scheme. Overridable per verifier, because a third party
#: names it whatever it likes (`Stripe-Signature`, `X-Hub-Signature-256`) and
#: the name is not part of what is signed.
DEFAULT_SIGNATURE_HEADER: Final[str] = "X-Webhook-Signature"

_DIGEST_HEX_LENGTH: Final[int] = 64  # sha256

# Bounds on what a parser will look at before rejecting. An attacker chooses
# this header's contents, and every `v1` value in it costs a `compare_digest`
# against every configured secret; without a cap, one request is an arbitrary
# amount of work. 4 KiB and 16 pairs are both far above any real sender —
# rotation needs two `v1` values, not sixteen.
_MAX_HEADER_LENGTH: Final[int] = 4096
_MAX_PAIRS: Final[int] = 16

# Explicitly decimal ASCII, because `int()` is more liberal than the format is:
# `int("1_0")` is 10 and `int(" +5 ")` is 5, so a header carrying either would
# parse to a timestamp the sender never signed and then fail on the digest,
# which is a confusing way to report a malformed header. 19 digits is the last
# width that cannot overflow a 64-bit second count.
_TIMESTAMP_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9]{1,19}$")
_HEX_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]+$")


@dataclass(frozen=True, slots=True)
class ParsedSignature:
    """A header that was syntactically readable.

    Nothing here is trusted yet: `timestamp` is whatever the sender claims and
    `digests` are candidates to compare against, not proof of anything. The type
    exists so the parsing errors and the cryptographic ones stay separate —
    `SignatureHeaderMalformedError` means "unreadable", `SignatureMismatchError`
    means "read, and wrong".
    """

    timestamp: int
    digests: tuple[str, ...]


def signed_material(timestamp: int, body: bytes) -> bytes:
    """The exact bytes both sides run the HMAC over."""
    return f"{timestamp}.".encode() + body


def compute_digest(secret: str, timestamp: int, body: bytes) -> str:
    """Hex `HMAC-SHA256` of the signed material under `secret`."""
    return hmac.new(
        secret.encode(), signed_material(timestamp, body), hashlib.sha256
    ).hexdigest()


def sign(secret: str, timestamp: int, body: bytes) -> str:
    """Build a complete header value for an outgoing delivery."""
    return (
        f"t={timestamp},{SIGNATURE_VERSION}={compute_digest(secret, timestamp, body)}"
    )


def delivery_fingerprint(timestamp: int, body: bytes) -> str:
    """A stable name for one delivery, for the replay guard to remember.

    Deliberately *not* the signature. A digest names the delivery **and** the
    secret that signed it, so with two secrets live during a rotation the same
    captured delivery has two names, and dropping the retiring secret renames it
    — one free replay per rotation. Hashing the signed material instead gives
    one name for the life of the delivery.

    Nothing is lost by using a value an attacker could also compute, because
    reaching the guard at all requires a signature that verified: this is called
    after authentication, never before. A guard keyed on something forgeable but
    written only post-authentication cannot be poisoned; one keyed on an
    unforgeable value but written before authentication can be, by anyone, to
    suppress a delivery that has not arrived yet.
    """
    return hashlib.sha256(signed_material(timestamp, body)).hexdigest()


def parse_signature_header(raw: str) -> ParsedSignature:
    """Read `t=...,v1=...` into a `ParsedSignature`, or raise.

    Unknown element names are ignored rather than refused, so that a sender
    which starts emitting a `v2=` alongside its `v1=` — the only way a scheme
    can be upgraded without a flag day — does not break this receiver on the
    day it does. A `v1` whose value is not a sha256 hex digest is *not* ignored
    on the same grounds: it can only be a bug in the sender, and swallowing it
    would report a malformed header as a mismatched secret.
    """
    if len(raw) > _MAX_HEADER_LENGTH:
        raise SignatureHeaderMalformedError(
            "Signature header is too long.",
            details={"max_length": _MAX_HEADER_LENGTH},
        )

    elements = [part.strip() for part in raw.split(",") if part.strip()]
    if not elements:
        raise SignatureHeaderMalformedError("Signature header is empty.")
    if len(elements) > _MAX_PAIRS:
        raise SignatureHeaderMalformedError(
            "Signature header has too many elements.",
            details={"max_elements": _MAX_PAIRS},
        )

    timestamp: int | None = None
    digests: list[str] = []

    for element in elements:
        name, separator, value = element.partition("=")
        if not separator:
            raise SignatureHeaderMalformedError(
                "Every signature element must be 'name=value'."
            )
        name = name.strip()
        value = value.strip()

        if name == "t":
            if timestamp is not None:
                # Two timestamps mean two readings of the same header, and
                # picking either would be this receiver choosing which one the
                # sender meant.
                raise SignatureHeaderMalformedError(
                    "Signature header carries more than one timestamp."
                )
            if not _TIMESTAMP_RE.match(value):
                raise SignatureHeaderMalformedError(
                    "Signature timestamp must be unix seconds in decimal ASCII."
                )
            timestamp = int(value)
        elif name == SIGNATURE_VERSION:
            # Lower-cased before the shape check so that an uppercase digest is
            # accepted: hex case is a rendering detail, unlike the value.
            candidate = value.lower()
            if len(candidate) != _DIGEST_HEX_LENGTH or not _HEX_RE.match(candidate):
                raise SignatureHeaderMalformedError(
                    f"Each '{SIGNATURE_VERSION}' must be a hex sha256 digest."
                )
            digests.append(candidate)

    if timestamp is None:
        raise SignatureHeaderMalformedError("Signature header has no 't' element.")
    if not digests:
        raise SignatureHeaderMalformedError(
            f"Signature header has no '{SIGNATURE_VERSION}' element."
        )

    return ParsedSignature(timestamp=timestamp, digests=tuple(digests))


def digest_matches(expected: str, candidates: tuple[str, ...]) -> bool:
    """Whether `expected` is among `candidates`, compared in constant time.

    `hmac.compare_digest` rather than `==` because `==` on strings returns as
    soon as two bytes differ, and the time it took to return is a measurement of
    how many leading bytes were right. Repeat that with a digest that is
    adjusted one byte at a time and the comparison hands over the answer it was
    supposed to be checking. Nothing about this data is short enough or private
    enough for the difference to be obvious, which is exactly why it has to be
    the default rather than a judgement call.

    Both sides are known to be lower-case hex by the time they arrive here —
    `compute_digest` produces it, `parse_signature_header` enforces it — which
    also keeps `compare_digest` from raising on a non-ASCII `str`.
    """
    return any(hmac.compare_digest(expected, candidate) for candidate in candidates)
