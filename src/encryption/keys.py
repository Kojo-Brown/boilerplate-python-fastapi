"""The key ring: which AES-256 keys this process holds, and which one it writes with.

A single key would make this module a two-line settings read. Every line beyond
that exists because keys have to be *replaced* without taking the application
down, and a cipher that cannot name its key cannot be rotated: the ciphertext
would have to be decrypted by trial, which is indistinguishable from an oracle
and stops working the moment two keys are live at once.

So a key has an id, the id travels in the clear inside every envelope (see
`src/encryption/envelope.py`), and rotation is three deploys rather than one:

1. add the new key to `ENCRYPTION_KEYS`, leave `ENCRYPTION_ACTIVE_KEY_ID`
   pointing at the old one — every replica can now *read* the new key's output,
   which matters because the next step is not atomic across replicas;
2. move `ENCRYPTION_ACTIVE_KEY_ID` to the new id — new writes seal under it,
   old rows are still readable;
3. re-encrypt the remaining rows, then drop the old key from `ENCRYPTION_KEYS`.

Dropping the old key before step 3 finishes is the one irreversible mistake
available here, and `UnknownKeyError` is what it looks like afterwards.

The key ids themselves are not secret and are not access control. They name a
secret; they are not one. What must never appear anywhere is the material, and
that is why `DataKey` has a hand-written `__repr__`: a frozen dataclass's
generated one prints its fields, and a key logged once in a traceback is a key
that now lives in whatever aggregates the logs.
"""

from __future__ import annotations

import base64
import binascii
import re
from dataclasses import dataclass
from functools import cache
from typing import Final

from src.config import Settings
from src.encryption.errors import KeyRingConfigurationError, UnknownKeyError

#: AES-256. Not a setting: the item is "AES-256-GCM", and a 16-byte key here
#: would silently give AES-128 with everything else looking identical.
KEY_BYTES: Final[int] = 32

#: The id is length-prefixed with a single byte inside the envelope header, so
#: it has a hard ceiling; 64 is far below it and leaves the field comfortable
#: for a date-based scheme like `2026-09` or a KMS alias.
MAX_KEY_ID_LENGTH: Final[int] = 64

#: Restricted rather than free-form because the id is parsed back out of a byte
#: string and compared, and because it ends up in log lines and metric labels.
#: ASCII, no separators that collide with the `kid:material` settings format.
_KEY_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]*\Z")

#: Key material published in this repository — `.env.example`, the CI workflow,
#: the documentation. It exists so that `cp .env.example .env` produces an
#: application that boots, which is worth a great deal in a boilerplate, and it
#: is refused outright when `ENVIRONMENT` is `production`. The decoded bytes
#: spell out what they are, so a key dumped during an incident identifies
#: itself without anyone having to look it up.
WELL_KNOWN_INSECURE_KEY_MATERIAL: Final[frozenset[bytes]] = frozenset(
    {
        b"insecure-development-key-notreal",
        b"insecure-ci-key-not-for-real-use",
    }
)


@dataclass(frozen=True, slots=True, repr=False)
class DataKey:
    """One named AES-256 key."""

    key_id: str
    material: bytes

    def __post_init__(self) -> None:
        if not _KEY_ID_PATTERN.match(self.key_id):
            raise KeyRingConfigurationError(
                f"Key id {self.key_id!r} must match "
                f"{_KEY_ID_PATTERN.pattern} (letters, digits, '.', '_', '-')."
            )
        if len(self.key_id) > MAX_KEY_ID_LENGTH:
            raise KeyRingConfigurationError(
                f"Key id {self.key_id!r} is longer than {MAX_KEY_ID_LENGTH} characters."
            )
        if len(self.material) != KEY_BYTES:
            # The length is safe to report; it is a property of the encoding
            # mistake, not of the key.
            raise KeyRingConfigurationError(
                f"Key {self.key_id!r} is {len(self.material)} bytes; "
                f"AES-256 needs exactly {KEY_BYTES}."
            )

    def __repr__(self) -> str:
        """Never the material. See the module docstring."""
        return f"DataKey(key_id={self.key_id!r}, material=<redacted>)"

    @property
    def is_well_known(self) -> bool:
        """Whether this key's material is published in this repository."""
        return self.material in WELL_KNOWN_INSECURE_KEY_MATERIAL


@dataclass(frozen=True, slots=True)
class KeyRing:
    """Every key this process can read with, and the one it writes with."""

    active_key_id: str
    keys: tuple[DataKey, ...]

    def __post_init__(self) -> None:
        if not self.keys:
            raise KeyRingConfigurationError("The key ring is empty.")
        ids = [key.key_id for key in self.keys]
        duplicates = sorted({name for name in ids if ids.count(name) > 1})
        if duplicates:
            raise KeyRingConfigurationError(
                f"Duplicate key ids in the key ring: {', '.join(duplicates)}."
            )
        if self.active_key_id not in ids:
            raise KeyRingConfigurationError(
                f"Active key id {self.active_key_id!r} is not in the key ring "
                f"({', '.join(sorted(ids))})."
            )

    @property
    def active(self) -> DataKey:
        """The key new values are sealed under."""
        return self.get(self.active_key_id)

    def get(self, key_id: str) -> DataKey:
        """The key with this id, or `UnknownKeyError`.

        A linear scan on purpose: a ring holds the key being retired, the key
        in use and at most the key being introduced, and an index would be a
        second copy of the material to keep from being printed.
        """
        for key in self.keys:
            if key.key_id == key_id:
                return key
        raise UnknownKeyError(
            f"No key with id {key_id!r}. This process holds: "
            f"{', '.join(sorted(key.key_id for key in self.keys))}. "
            "A row was written under a key that has since been removed from "
            "ENCRYPTION_KEYS."
        )


def parse_key_ring(*, keys: str, active_key_id: str) -> KeyRing:
    """Build a ring from the two settings strings.

    `keys` is `id:base64-material` pairs separated by commas; whitespace and
    line breaks around an entry are ignored so a long ring can be written
    across several lines in a secret manager. Base64 rather than hex because
    that is what every KMS and secret store hands back.
    """
    if not active_key_id.strip():
        raise KeyRingConfigurationError(
            "ENCRYPTION_ACTIVE_KEY_ID is empty. Set it to one of the ids in "
            "ENCRYPTION_KEYS."
        )
    entries = [entry.strip() for entry in keys.split(",") if entry.strip()]
    if not entries:
        raise KeyRingConfigurationError(
            "ENCRYPTION_KEYS is empty. Expected 'id:base64-key' entries "
            "separated by commas; see docs/field-encryption.md."
        )

    parsed: list[DataKey] = []
    for entry in entries:
        key_id, separator, encoded = entry.partition(":")
        if not separator:
            raise KeyRingConfigurationError(
                f"ENCRYPTION_KEYS entry {_redact_entry(entry)} has no ':'. "
                "Expected 'id:base64-key'."
            )
        try:
            # `validate=True` so that a pasted value carrying a stray quote or
            # a `-`/`_` from URL-safe base64 is an error here rather than a key
            # that silently decodes to the wrong bytes and fails every read.
            material = base64.b64decode(encoded.strip(), validate=True)
        except (binascii.Error, ValueError) as exc:
            raise KeyRingConfigurationError(
                f"Key {key_id.strip()!r} is not valid standard base64."
            ) from exc
        parsed.append(DataKey(key_id=key_id.strip(), material=material))

    return KeyRing(active_key_id=active_key_id.strip(), keys=tuple(parsed))


def _redact_entry(entry: str) -> str:
    """An entry as it can safely appear in an error message.

    A malformed entry is still probably a key, so only the part before the
    first ':' is shown — and when there is no ':', nothing is.
    """
    key_id, separator, _ = entry.partition(":")
    return f"{key_id!r}" if separator else "<unparseable>"


@cache
def build_key_ring(settings: Settings) -> KeyRing:
    """The key ring for a configuration, built once per `Settings`.

    Cached on the settings object, which is frozen and therefore hashable, so
    a test can build a different ring simply by constructing its own
    `Settings(...)` — the same seam every factory in this codebase uses.
    """
    ring = parse_key_ring(
        keys=settings.ENCRYPTION_KEYS,
        active_key_id=settings.ENCRYPTION_ACTIVE_KEY_ID,
    )
    if settings.ENVIRONMENT == "production":
        published = sorted(key.key_id for key in ring.keys if key.is_well_known)
        if published:
            # Every key, not just the active one: a published key still in the
            # ring can decrypt every row ever written under it, and the rows do
            # not stop existing when the active id moves on.
            raise KeyRingConfigurationError(
                "ENCRYPTION_KEYS contains key material published in this "
                f"repository ({', '.join(published)}). Generate real keys with "
                "`python scripts/generate_encryption_key.py` and load them from "
                "your secret store."
            )
    return ring
