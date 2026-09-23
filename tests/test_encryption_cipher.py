"""Rotation, which is the only reason the cipher is not one call to `seal`.

`FieldCipher` is nine lines, and eight of them are about an asymmetry: it
writes with one key and reads with whichever key the value names. Every test
here is a consequence of that.
"""

from __future__ import annotations

import pytest

from src.config import Settings, settings
from src.encryption import envelope
from src.encryption.cipher import FieldCipher, build_field_cipher, get_field_cipher
from src.encryption.errors import (
    CiphertextFormatError,
    DecryptionError,
    UnknownKeyError,
)
from src.encryption.keys import DataKey, KeyRing

OLD = DataKey(key_id="2026-08", material=b"0" * 32)
NEW = DataKey(key_id="2026-09", material=b"1" * 32)
CONTEXT = "users.notification_webhook_url"
PLAINTEXT = b"https://hooks.example.test/mock-webhook-token"


def ring(*keys: DataKey, active: str) -> KeyRing:
    return KeyRing(active_key_id=active, keys=keys)


def settings_with(**overrides: object) -> Settings:
    return Settings(
        DATABASE_URL="postgresql+asyncpg://fake:fake@localhost/fake",
        SECRET_KEY="mock-secret-key-not-a-real-one",
        **overrides,  # type: ignore[arg-type]
    )


class TestRoundTrip:
    def test_encrypt_then_decrypt(self) -> None:
        cipher = FieldCipher(key_ring=ring(NEW, active=NEW.key_id))
        sealed = cipher.encrypt(PLAINTEXT, context=CONTEXT)
        assert cipher.decrypt(sealed, context=CONTEXT) == PLAINTEXT

    def test_seals_under_the_active_key(self) -> None:
        cipher = FieldCipher(key_ring=ring(OLD, NEW, active=NEW.key_id))
        assert envelope.parse(cipher.encrypt(PLAINTEXT, context=CONTEXT)).key_id == (
            NEW.key_id
        )

    def test_context_is_bound(self) -> None:
        cipher = FieldCipher(key_ring=ring(NEW, active=NEW.key_id))
        sealed = cipher.encrypt(PLAINTEXT, context=CONTEXT)
        with pytest.raises(DecryptionError):
            cipher.decrypt(sealed, context="users.other_column")


class TestRotation:
    def test_a_ring_reads_what_an_earlier_key_wrote(self) -> None:
        """Step 2 of a rotation: active id moved, old rows still readable."""
        before = FieldCipher(key_ring=ring(OLD, active=OLD.key_id))
        sealed = before.encrypt(PLAINTEXT, context=CONTEXT)

        after = FieldCipher(key_ring=ring(OLD, NEW, active=NEW.key_id))
        assert after.decrypt(sealed, context=CONTEXT) == PLAINTEXT

    def test_re_encrypting_moves_a_value_onto_the_active_key(self) -> None:
        """Step 3: what the backfill job does to each row."""
        cipher = FieldCipher(key_ring=ring(OLD, NEW, active=NEW.key_id))
        old_bytes = FieldCipher(key_ring=ring(OLD, active=OLD.key_id)).encrypt(
            PLAINTEXT, context=CONTEXT
        )
        new_bytes = cipher.encrypt(
            cipher.decrypt(old_bytes, context=CONTEXT), context=CONTEXT
        )
        assert envelope.parse(new_bytes).key_id == NEW.key_id
        assert cipher.decrypt(new_bytes, context=CONTEXT) == PLAINTEXT

    def test_dropping_a_key_too_early_is_unrecoverable_and_says_so(self) -> None:
        """The one irreversible mistake in this package.

        Retiring `2026-08` while rows still reference it does not corrupt
        anything; it simply makes those rows unreadable until the key comes
        back. The error has to name the key so that "put it back" is an
        available response.
        """
        sealed = FieldCipher(key_ring=ring(OLD, active=OLD.key_id)).encrypt(
            PLAINTEXT, context=CONTEXT
        )
        retired = FieldCipher(key_ring=ring(NEW, active=NEW.key_id))
        with pytest.raises(UnknownKeyError, match=OLD.key_id):
            retired.decrypt(sealed, context=CONTEXT)


class TestErrors:
    def test_garbage_is_a_format_error_not_a_decryption_error(self) -> None:
        """The three failures have three remediations; see `FieldCipher.decrypt`."""
        cipher = FieldCipher(key_ring=ring(NEW, active=NEW.key_id))
        with pytest.raises(CiphertextFormatError):
            cipher.decrypt(b"not an envelope", context=CONTEXT)

    def test_wrong_material_under_a_known_id_is_a_decryption_error(self) -> None:
        sealed = FieldCipher(key_ring=ring(NEW, active=NEW.key_id)).encrypt(
            PLAINTEXT, context=CONTEXT
        )
        impostor = DataKey(key_id=NEW.key_id, material=b"2" * 32)
        with pytest.raises(DecryptionError):
            FieldCipher(key_ring=ring(impostor, active=impostor.key_id)).decrypt(
                sealed, context=CONTEXT
            )


class TestProviders:
    def test_built_once_per_settings_object(self) -> None:
        config = settings_with()
        assert build_field_cipher(config) is build_field_cipher(config)

    def test_the_process_wide_provider_uses_the_process_wide_settings(self) -> None:
        """What every column type calls, by default, on every value.

        Asserted as identity against the process `Settings` rather than by
        comparing key material: the claim is *which* configuration the default
        provider reads, and two different configurations holding the same key
        would satisfy a value comparison.
        """
        assert get_field_cipher() is build_field_cipher(settings)
