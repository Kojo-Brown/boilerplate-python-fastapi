"""The bytes that go in the column.

    +---------+------------+------------------+-----------+---------------------+
    | version | id length  | key id           | nonce     | ciphertext ‖ tag    |
    | 1 byte  | 1 byte     | 1..64 bytes      | 12 bytes  | len(plaintext) + 16 |
    +---------+------------+------------------+-----------+---------------------+

Everything before the ciphertext is in the clear, and has to be: a reader has
to know which key to fetch and which nonce to use before it can authenticate
anything. None of it is secret. All of it is *authenticated* — the whole header
is passed to AES-GCM as additional data, so flipping the version byte or
renaming the key id fails the tag check rather than steering the reader.

Three decisions worth stating, because each one is invisible in the happy path:

**The context label is in the AAD and not in the payload.** It is the column's
durable name — `users.notification_webhook_url` — and binding it means a
ciphertext lifted out of one column and written into another fails to
authenticate. Without it, encrypted columns of the same type are
interchangeable: a database-level attacker who cannot read a value can still
*move* one, and moving a row's value into a colleague's row is an attack that
needs no key at all. The label is not stored because it is already known by
whoever is reading the column; storing it would only give an attacker a second
place to change it.

**A 96-bit random nonce, and that is why key rotation is not optional.** GCM
fails catastrophically on nonce reuse under one key — two messages sharing a
nonce leak their XOR and, worse, the authentication subkey, which forgives
forgeries from then on. With random 96-bit nonces the collision probability
reaches 2^-32 at about 2^32 messages under one key (NIST SP 800-38D, Appendix
B.2), so a key is a budget of a few billion writes rather than a permanent
fixture. `docs/field-encryption.md` carries the arithmetic.

**The version byte is first and is checked before anything else.** A format
this release does not recognise raises `CiphertextFormatError` rather than
being parsed on the assumption that the layout held, which is how a future
change to this table stays a loud failure instead of a mis-sliced nonce and a
tag check that fails for a reason nobody can explain.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from src.encryption.errors import CiphertextFormatError, DecryptionError
from src.encryption.keys import DataKey

#: Bumped only for a change to the layout above. A reader refuses anything else.
FORMAT_VERSION: Final[int] = 1

#: 96 bits. The one nonce length GCM uses directly instead of hashing down to,
#: and therefore the only one worth using.
NONCE_BYTES: Final[int] = 12

#: The full 128-bit tag. Truncating it is permitted by the standard and is a
#: bad trade for 16 bytes per row.
TAG_BYTES: Final[int] = 16

_HEADER_PREFIX_BYTES: Final[int] = 2  # version + key id length

#: Separates the header from the context label in the additional data, so that
#: a header ending in the first bytes of one label cannot be re-read as a
#: shorter header and a longer label. The header is fixed-layout and this is
#: belt and braces, which is the correct amount of care for an AAD.
_AAD_SEPARATOR: Final[bytes] = b"|"


@dataclass(frozen=True, slots=True)
class Envelope:
    """A parsed ciphertext: what the header said, and what is left to open."""

    version: int
    key_id: str
    nonce: bytes
    ciphertext: bytes
    #: The exact header bytes this envelope was read from. Kept verbatim rather
    #: than re-serialised, so that a reader authenticates what was actually
    #: stored instead of what it would have written.
    header: bytes


def envelope_overhead(key_id: str) -> int:
    """Bytes a sealed value costs beyond its plaintext length.

    Used by the sizing note in `docs/field-encryption.md` and asserted in the
    tests, so the documented number cannot drift from the format.
    """
    return _HEADER_PREFIX_BYTES + len(key_id.encode("ascii")) + NONCE_BYTES + TAG_BYTES


def _additional_data(header: bytes, context: str) -> bytes:
    return header + _AAD_SEPARATOR + context.encode("utf-8")


def seal(
    key: DataKey, plaintext: bytes, *, context: str, nonce: bytes | None = None
) -> bytes:
    """Encrypt `plaintext` under `key`, bound to `context`.

    `nonce` exists so that the format can have a known-answer test and must be
    left `None` everywhere else: a nonce reused under one key does not degrade
    GCM, it breaks it. There is no code path in `src/` that passes it.
    """
    if nonce is None:
        nonce = os.urandom(NONCE_BYTES)
    elif len(nonce) != NONCE_BYTES:
        raise CiphertextFormatError(
            f"A GCM nonce is {NONCE_BYTES} bytes; got {len(nonce)}."
        )

    encoded_id = key.key_id.encode("ascii")
    header = bytes((FORMAT_VERSION, len(encoded_id))) + encoded_id + nonce
    ciphertext = AESGCM(key.material).encrypt(
        nonce, plaintext, _additional_data(header, context)
    )
    return header + ciphertext


def parse(payload: bytes) -> Envelope:
    """Split stored bytes into header and ciphertext without a key.

    Separate from `unseal` because the key id has to be read before the key can
    be looked up, and because a `UnknownKeyError` and a `DecryptionError` are
    very different incidents that should not be reachable from one call site.
    """
    if len(payload) < _HEADER_PREFIX_BYTES:
        raise CiphertextFormatError(
            f"Stored value is {len(payload)} bytes; too short to be an envelope."
        )

    version = payload[0]
    if version != FORMAT_VERSION:
        raise CiphertextFormatError(
            f"Envelope format version {version} is not supported by this "
            f"release (expected {FORMAT_VERSION})."
        )

    id_length = payload[1]
    if id_length == 0:
        raise CiphertextFormatError("Envelope header declares an empty key id.")

    header_length = _HEADER_PREFIX_BYTES + id_length + NONCE_BYTES
    # An empty string is a legitimate plaintext and seals to header + tag, so
    # the floor is the tag and not one byte more. Anything under it was
    # truncated: there is nothing to authenticate, which is a format error
    # rather than a failed tag check.
    if len(payload) < header_length + TAG_BYTES:
        raise CiphertextFormatError(
            f"Envelope is {len(payload)} bytes; a {id_length}-byte key id needs "
            f"at least {header_length + TAG_BYTES}."
        )

    encoded_id = payload[_HEADER_PREFIX_BYTES : _HEADER_PREFIX_BYTES + id_length]
    try:
        key_id = encoded_id.decode("ascii")
    except UnicodeDecodeError as exc:
        raise CiphertextFormatError("Envelope key id is not ASCII.") from exc

    return Envelope(
        version=version,
        key_id=key_id,
        nonce=payload[_HEADER_PREFIX_BYTES + id_length : header_length],
        ciphertext=payload[header_length:],
        header=payload[:header_length],
    )


def unseal(key: DataKey, envelope: Envelope, *, context: str) -> bytes:
    """Authenticate and decrypt a parsed envelope.

    The error message names the key id and the context and nothing else. Which
    of the three possible causes it was — wrong key, modified bytes, value
    moved between columns — is exactly what an authenticated cipher refuses to
    tell you, and inventing a guess in the message would have people chasing
    the wrong one.
    """
    if key.key_id != envelope.key_id:
        raise DecryptionError(
            f"Envelope names key {envelope.key_id!r} but key {key.key_id!r} "
            "was supplied."
        )
    try:
        return AESGCM(key.material).decrypt(
            envelope.nonce,
            envelope.ciphertext,
            _additional_data(envelope.header, context),
        )
    except InvalidTag as exc:
        raise DecryptionError(
            f"Ciphertext for {context!r} did not authenticate under key "
            f"{envelope.key_id!r}: wrong key, modified bytes, or a value "
            "written under a different column context."
        ) from exc
