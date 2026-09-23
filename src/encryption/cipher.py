"""The object a column talks to: a key ring plus the two operations on it.

Thin on purpose. `envelope.py` owns the format and knows nothing about which
keys exist; `keys.py` owns the ring and knows nothing about bytes on the wire.
This is the only place the two meet, which is what keeps "seal under the active
key, open under whichever key the value names" from being restated at every
call site — and that asymmetry is the whole of key rotation.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import cache

from src.config import Settings, settings
from src.encryption import envelope
from src.encryption.keys import KeyRing, build_key_ring


@dataclass(frozen=True, slots=True)
class FieldCipher:
    """Seals with the ring's active key; opens with whichever key a value names."""

    key_ring: KeyRing

    def encrypt(self, plaintext: bytes, *, context: str) -> bytes:
        """Seal `plaintext` under the active key, bound to `context`."""
        return envelope.seal(self.key_ring.active, plaintext, context=context)

    def decrypt(self, payload: bytes, *, context: str) -> bytes:
        """Open a stored value, bound to `context`.

        Raises `CiphertextFormatError` for bytes that are not an envelope,
        `UnknownKeyError` when the named key is not in the ring, and
        `DecryptionError` when the tag does not verify. Three errors rather
        than one because they have three different remediations, and a caller
        that catches the union is a caller that has decided not to know which.
        """
        parsed = envelope.parse(payload)
        key = self.key_ring.get(parsed.key_id)
        return envelope.unseal(key, parsed, context=context)


@cache
def build_field_cipher(config: Settings) -> FieldCipher:
    """The cipher for a configuration, built once per `Settings`."""
    return FieldCipher(key_ring=build_key_ring(config))


def get_field_cipher() -> FieldCipher:
    """The process-wide cipher, from the process-wide settings.

    A module-level function rather than a module-level instance: the columns in
    `src/models/` are constructed at import time, and building the ring there
    would make importing a model fail on a deployment that has not configured
    keys yet — including `alembic`, which has to be able to run the migration
    that *introduces* an encrypted column. Resolution happens on first use, and
    `validate_encryption_configuration` in `src/encryption/startup.py` pulls it
    forward to start-up so that "first use" is not a request in production.
    """
    return build_field_cipher(settings)
