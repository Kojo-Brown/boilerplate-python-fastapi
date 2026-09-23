"""Field-level encryption at rest: AES-256-GCM behind a SQLAlchemy column type.

Start at `docs/field-encryption.md` for the operational shape (generating keys,
rotating them, what the threat model does and does not cover). The short
version:

    from src.encryption import EncryptedString

    notification_webhook_url: Mapped[str | None] = mapped_column(
        EncryptedString("users.notification_webhook_url"), nullable=True
    )

Nothing above the mapper changes. The column is `bytea` in Postgres, `str` in
Python, and a query that tries to compare against it raises instead of quietly
matching nothing.
"""

from src.encryption.cipher import FieldCipher, build_field_cipher, get_field_cipher
from src.encryption.errors import (
    CiphertextFormatError,
    DecryptionError,
    EncryptedColumnComparisonError,
    EncryptionError,
    KeyRingConfigurationError,
    UnknownKeyError,
)
from src.encryption.keys import DataKey, KeyRing, build_key_ring, parse_key_ring
from src.encryption.startup import validate_encryption_configuration
from src.encryption.types import EncryptedBytes, EncryptedString

__all__ = [
    "CiphertextFormatError",
    "DataKey",
    "DecryptionError",
    "EncryptedBytes",
    "EncryptedColumnComparisonError",
    "EncryptedString",
    "EncryptionError",
    "FieldCipher",
    "KeyRing",
    "KeyRingConfigurationError",
    "UnknownKeyError",
    "build_field_cipher",
    "build_key_ring",
    "get_field_cipher",
    "parse_key_ring",
    "validate_encryption_configuration",
]
