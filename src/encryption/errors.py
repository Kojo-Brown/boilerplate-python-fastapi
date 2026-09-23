"""Failure modes of field-level encryption.

Deliberately not `AppException` subclasses. Every one of these means the
process cannot read or write a column it was configured to read or write —
a wrong key, a truncated ciphertext, a column whose context label no longer
matches the one it was written under. None of them is a fact about the request
that happened to hit them, so none of them should render as a tidy 4xx body
with a message a caller could act on. They propagate to
`unhandled_exception_handler`, which is a 500 and an ERROR log, because that is
what they are.

`DecryptionError` in particular must never be caught and turned into "no value"
by anything here. A GCM tag that does not verify is either the wrong key or
modified bytes, and both are incidents; silently reading the column as NULL
would hide the second one entirely.
"""

from __future__ import annotations


class EncryptionError(Exception):
    """Base class for every failure raised by `src.encryption`."""


class KeyRingConfigurationError(EncryptionError):
    """`ENCRYPTION_KEYS` / `ENCRYPTION_ACTIVE_KEY_ID` do not describe a usable key ring.

    Raised while building the ring rather than on first use, so a typo in a
    deployment's configuration is a failed start-up rather than a 500 on
    whichever request first touches an encrypted column.
    """


class UnknownKeyError(EncryptionError):
    """A stored value names a key id this process was not given.

    The usual cause is a key retired from `ENCRYPTION_KEYS` before every row
    written under it had been re-encrypted. Retiring a key is therefore a
    two-step operation, and this is the error that says the second step was
    skipped; see `docs/field-encryption.md`.
    """


class CiphertextFormatError(EncryptionError):
    """Stored bytes are not a well-formed envelope.

    A format version this release does not know, a truncated payload, or a
    header that does not parse. Distinct from a failed tag check: nothing was
    authenticated here because there was nothing to authenticate.
    """


class DecryptionError(EncryptionError):
    """The envelope parsed but did not authenticate.

    Wrong key for the named id, bytes modified in place, or a value moved to a
    column whose context label differs from the one it was sealed with. GCM
    cannot tell these apart — that is the point of an authenticated cipher, and
    the message here says so rather than guessing.
    """


class EncryptedColumnComparisonError(EncryptionError):
    """A SQL comparison was attempted against an encrypted column.

    Randomised encryption means two sealings of the same plaintext differ, so
    `WHERE col = :value` matches nothing and does so quietly — the query runs,
    returns zero rows, and looks like an absent record. Raising is the whole
    reason the comparator exists; see `src/encryption/types.py`.
    """
