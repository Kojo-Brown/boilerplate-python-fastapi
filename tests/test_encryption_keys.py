"""The key ring: parsing, validation, and the two mistakes that are expensive.

The expensive mistakes are shipping the published development key to production
and removing a key that rows still reference. Everything else here is input
validation, which matters mostly because the input is a secret and the error
messages must not quote it.
"""

from __future__ import annotations

import base64

import pytest

from src.config import Settings
from src.encryption.errors import KeyRingConfigurationError, UnknownKeyError
from src.encryption.keys import (
    KEY_BYTES,
    MAX_KEY_ID_LENGTH,
    WELL_KNOWN_INSECURE_KEY_MATERIAL,
    DataKey,
    KeyRing,
    build_key_ring,
    parse_key_ring,
)

REAL_LOOKING = b"0" * KEY_BYTES
OTHER = b"1" * KEY_BYTES
PUBLISHED = b"insecure-development-key-notreal"


def b64(material: bytes) -> str:
    return base64.b64encode(material).decode("ascii")


def settings_with(**overrides: object) -> Settings:
    return Settings(
        DATABASE_URL="postgresql+asyncpg://fake:fake@localhost/fake",
        SECRET_KEY="mock-secret-key-not-a-real-one",
        **overrides,  # type: ignore[arg-type]
    )


class TestDataKey:
    def test_accepts_a_32_byte_key(self) -> None:
        assert DataKey(key_id="2026-09", material=REAL_LOOKING).key_id == "2026-09"

    @pytest.mark.parametrize("length", [16, 24, 31, 33, 64])
    def test_rejects_any_length_but_32(self, length: int) -> None:
        with pytest.raises(KeyRingConfigurationError, match="AES-256"):
            DataKey(key_id="k", material=b"0" * length)

    @pytest.mark.parametrize("key_id", ["", "-leading", "has space", "has:colon", "é"])
    def test_rejects_ids_that_would_not_survive_the_envelope(self, key_id: str) -> None:
        with pytest.raises(KeyRingConfigurationError, match="Key id"):
            DataKey(key_id=key_id, material=REAL_LOOKING)

    def test_rejects_an_id_too_long_for_the_header(self) -> None:
        with pytest.raises(KeyRingConfigurationError, match="longer than"):
            DataKey(key_id="k" * (MAX_KEY_ID_LENGTH + 1), material=REAL_LOOKING)

    def test_repr_does_not_contain_the_material(self) -> None:
        """The one property of this class that is a security control.

        A frozen dataclass's generated `__repr__` prints its fields, and a key
        is printed by every traceback frame that holds one. Asserted on the
        bytes *and* on common encodings of them, because a repr that shows
        base64 is exactly as leaked.
        """
        key = DataKey(key_id="2026-09", material=REAL_LOOKING)
        text = repr(key)
        assert "2026-09" in text
        assert "<redacted>" in text
        assert str(REAL_LOOKING) not in text
        assert b64(REAL_LOOKING) not in text
        assert REAL_LOOKING.hex() not in text

    def test_published_material_identifies_itself(self) -> None:
        assert DataKey(key_id="dev", material=PUBLISHED).is_well_known
        assert not DataKey(key_id="real", material=REAL_LOOKING).is_well_known

    def test_every_published_key_is_a_usable_key(self) -> None:
        """The refusal list must hold keys, not typos.

        A 31-byte entry here would make `.env.example` unusable *and* never
        trigger the production guard, since no valid key could equal it.
        """
        for material in WELL_KNOWN_INSECURE_KEY_MATERIAL:
            assert len(material) == KEY_BYTES


class TestKeyRing:
    def test_active_resolves_to_a_key(self) -> None:
        ring = KeyRing(
            active_key_id="new",
            keys=(
                DataKey(key_id="old", material=REAL_LOOKING),
                DataKey(key_id="new", material=OTHER),
            ),
        )
        assert ring.active.material == OTHER

    def test_rejects_an_empty_ring(self) -> None:
        with pytest.raises(KeyRingConfigurationError, match="empty"):
            KeyRing(active_key_id="k", keys=())

    def test_rejects_duplicate_ids(self) -> None:
        with pytest.raises(KeyRingConfigurationError, match="Duplicate"):
            KeyRing(
                active_key_id="k",
                keys=(
                    DataKey(key_id="k", material=REAL_LOOKING),
                    DataKey(key_id="k", material=OTHER),
                ),
            )

    def test_rejects_an_active_id_that_is_not_in_the_ring(self) -> None:
        with pytest.raises(KeyRingConfigurationError, match="not in the key ring"):
            KeyRing(
                active_key_id="typo", keys=(DataKey(key_id="k", material=REAL_LOOKING),)
            )

    def test_unknown_id_names_what_is_held(self) -> None:
        """The message has to be actionable during an incident.

        This error means rows exist that nothing can read, and the next
        question is always "which keys does this process have?" — so the answer
        is in the message rather than one deploy away.
        """
        ring = KeyRing(
            active_key_id="new", keys=(DataKey(key_id="new", material=REAL_LOOKING),)
        )
        with pytest.raises(UnknownKeyError, match="new") as caught:
            ring.get("retired")
        assert "retired" in str(caught.value)


class TestParseKeyRing:
    def test_parses_a_single_entry(self) -> None:
        ring = parse_key_ring(keys=f"dev:{b64(REAL_LOOKING)}", active_key_id="dev")
        assert ring.active.material == REAL_LOOKING

    def test_parses_several_and_tolerates_whitespace(self) -> None:
        ring = parse_key_ring(
            keys=f"  old:{b64(REAL_LOOKING)} ,\n new:{b64(OTHER)}  ",
            active_key_id=" new ",
        )
        assert [key.key_id for key in ring.keys] == ["old", "new"]
        assert ring.active_key_id == "new"

    def test_rejects_an_empty_key_list(self) -> None:
        with pytest.raises(KeyRingConfigurationError, match="ENCRYPTION_KEYS is empty"):
            parse_key_ring(keys="  ,  ", active_key_id="dev")

    def test_rejects_an_empty_active_id(self) -> None:
        with pytest.raises(KeyRingConfigurationError, match="ACTIVE_KEY_ID is empty"):
            parse_key_ring(keys=f"dev:{b64(REAL_LOOKING)}", active_key_id="   ")

    def test_rejects_an_entry_without_a_separator(self) -> None:
        with pytest.raises(KeyRingConfigurationError, match="has no ':'"):
            parse_key_ring(keys=b64(REAL_LOOKING), active_key_id="dev")

    def test_rejects_non_base64_material(self) -> None:
        with pytest.raises(KeyRingConfigurationError, match="not valid standard"):
            parse_key_ring(keys="dev:not base64!", active_key_id="dev")

    def test_rejects_url_safe_base64(self) -> None:
        """`-` and `_` decode to different bytes under the two alphabets.

        Without `validate=True` the standard decoder ignores them, which
        produces a shorter key that is either rejected for its length or —
        worse — happens to be 32 bytes and silently wrong.
        """
        # Chosen so the standard encoding contains a `+`, which is where the
        # two alphabets diverge; most byte strings encode identically.
        material = bytes((14 + index * 13) % 256 for index in range(KEY_BYTES))
        url_safe = base64.urlsafe_b64encode(material).decode("ascii")
        assert url_safe != b64(material)
        with pytest.raises(KeyRingConfigurationError):
            parse_key_ring(keys=f"dev:{url_safe}", active_key_id="dev")

    def test_a_malformed_entry_is_not_quoted_back(self) -> None:
        """Error messages go to logs; the input here is key material.

        The stand-in is encoded at run time rather than written as a base64
        literal. A high-entropy string in a source file is indistinguishable
        from a leaked key to a secret scanner — GitGuardian flagged exactly
        this line on the first push of this branch — and a fixture that has to
        be decoded before anyone can see it is obviously fake is the wrong
        shape for a fixture anyway.
        """
        secretish = b64(b"pretend-this-was-real-key-material")
        with pytest.raises(KeyRingConfigurationError) as caught:
            parse_key_ring(keys=f"dev:{secretish}", active_key_id="dev")
        assert secretish not in str(caught.value)

    def test_an_entry_with_no_separator_is_reported_without_its_contents(self) -> None:
        secretish = b64(REAL_LOOKING)
        with pytest.raises(KeyRingConfigurationError) as caught:
            parse_key_ring(keys=secretish, active_key_id="dev")
        assert secretish not in str(caught.value)
        assert "<unparseable>" in str(caught.value)


class TestBuildKeyRing:
    def test_reads_the_two_settings(self) -> None:
        ring = build_key_ring(
            settings_with(
                ENCRYPTION_KEYS=f"a:{b64(REAL_LOOKING)},b:{b64(OTHER)}",
                ENCRYPTION_ACTIVE_KEY_ID="b",
            )
        )
        assert ring.active_key_id == "b"

    def test_production_refuses_the_published_key(self) -> None:
        with pytest.raises(KeyRingConfigurationError, match="published"):
            build_key_ring(
                settings_with(
                    ENVIRONMENT="production",
                    ENCRYPTION_KEYS=f"dev:{b64(PUBLISHED)}",
                    ENCRYPTION_ACTIVE_KEY_ID="dev",
                )
            )

    def test_production_refuses_a_published_key_that_is_merely_retiring(self) -> None:
        """Not just the active one.

        A published key left in the ring still decrypts every row ever written
        under it, and those rows do not stop existing when the active id moves
        on. This is the shape a "we rotated away from the example key" incident
        actually has.
        """
        with pytest.raises(KeyRingConfigurationError, match="dev"):
            build_key_ring(
                settings_with(
                    ENVIRONMENT="production",
                    ENCRYPTION_KEYS=f"dev:{b64(PUBLISHED)},real:{b64(REAL_LOOKING)}",
                    ENCRYPTION_ACTIVE_KEY_ID="real",
                )
            )

    def test_production_accepts_keys_that_are_not_published(self) -> None:
        ring = build_key_ring(
            settings_with(
                ENVIRONMENT="production",
                ENCRYPTION_KEYS=f"real:{b64(REAL_LOOKING)}",
                ENCRYPTION_ACTIVE_KEY_ID="real",
            )
        )
        assert ring.active_key_id == "real"

    def test_the_shipped_default_configuration_builds(self) -> None:
        """`cp .env.example .env` has to produce an application that boots."""
        ring = build_key_ring(settings_with())
        assert ring.active.is_well_known

    def test_is_cached_per_settings_object(self) -> None:
        config = settings_with()
        assert build_key_ring(config) is build_key_ring(config)
