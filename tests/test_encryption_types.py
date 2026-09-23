"""The column types, and the fitness function over the columns that use them.

The round-trip tests call the processors directly rather than through a
session: `process_bind_param` and `process_result_value` are the entire
contract with SQLAlchemy, and exercising them without a database keeps these
fast and keeps `test_encryption_db.py` — which needs a real Postgres — about
the things only a real Postgres can show.

The comparator tests are the important half of the file. They assert that a
query which would silently return nothing raises instead.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import Column, LargeBinary, MetaData, Table, select

from src.database import Base
from src.encryption.cipher import FieldCipher, get_field_cipher
from src.encryption.errors import DecryptionError, EncryptedColumnComparisonError
from src.encryption.keys import DataKey, KeyRing
from src.encryption.types import EncryptedBytes, EncryptedString

CONTEXT = "fixtures.secret"
URL = "https://hooks.example.test/T000/B000/mock-webhook-token"

_TEST_CIPHER = FieldCipher(
    key_ring=KeyRing(
        active_key_id="fixture",
        keys=(DataKey(key_id="fixture", material=b"7" * 32),),
    )
)


def fixture_cipher() -> FieldCipher:
    return _TEST_CIPHER


#: A table that exists only for the comparator tests. Kept out of `Base` so it
#: never reaches a migration or `create_all`.
_metadata = MetaData()
secrets_table = Table(
    "encrypted_fixture",
    _metadata,
    Column("id", LargeBinary, primary_key=True),
    Column("secret", EncryptedString(CONTEXT, fixture_cipher)),
)


class TestEncryptedString:
    def test_round_trip(self) -> None:
        column = EncryptedString(CONTEXT, fixture_cipher)
        stored = column.process_bind_param(URL, dialect=None)  # type: ignore[arg-type]
        assert isinstance(stored, bytes)
        assert URL.encode("utf-8") not in stored
        assert column.process_result_value(stored, dialect=None) == URL  # type: ignore[arg-type]

    def test_null_stays_null(self) -> None:
        """Absence is not encrypted, and cannot be without losing IS NULL."""
        column = EncryptedString(CONTEXT, fixture_cipher)
        assert column.process_bind_param(None, dialect=None) is None  # type: ignore[arg-type]
        assert column.process_result_value(None, dialect=None) is None  # type: ignore[arg-type]

    def test_non_ascii_survives(self) -> None:
        column = EncryptedString(CONTEXT, fixture_cipher)
        value = "https://hooks.example.test/café/☎"
        stored = column.process_bind_param(value, dialect=None)  # type: ignore[arg-type]
        assert column.process_result_value(stored, dialect=None) == value  # type: ignore[arg-type]

    def test_accepts_a_memoryview_from_the_driver(self) -> None:
        """Some DBAPIs hand back a memoryview for `bytea`; the cipher slices."""
        column = EncryptedString(CONTEXT, fixture_cipher)
        stored = column.process_bind_param(URL, dialect=None)  # type: ignore[arg-type]
        assert stored is not None
        assert column.process_result_value(memoryview(stored), dialect=None) == URL  # type: ignore[arg-type]

    def test_a_value_from_another_context_does_not_decrypt(self) -> None:
        elsewhere = EncryptedString("fixtures.other", fixture_cipher)
        stored = elsewhere.process_bind_param(URL, dialect=None)  # type: ignore[arg-type]
        with pytest.raises(DecryptionError):
            EncryptedString(CONTEXT, fixture_cipher).process_result_value(
                stored,
                dialect=None,  # type: ignore[arg-type]
            )

    def test_python_type_is_str(self) -> None:
        assert EncryptedString(CONTEXT, fixture_cipher).python_type is str

    def test_stores_as_bytea(self) -> None:
        assert isinstance(EncryptedString(CONTEXT).impl_instance, LargeBinary)


class TestEncryptedBytes:
    def test_round_trip(self) -> None:
        column = EncryptedBytes(CONTEXT, fixture_cipher)
        payload = bytes(range(256))
        stored = column.process_bind_param(payload, dialect=None)  # type: ignore[arg-type]
        assert stored is not None
        assert column.process_result_value(stored, dialect=None) == payload  # type: ignore[arg-type]

    def test_null_stays_null(self) -> None:
        column = EncryptedBytes(CONTEXT, fixture_cipher)
        assert column.process_bind_param(None, dialect=None) is None  # type: ignore[arg-type]
        assert column.process_result_value(None, dialect=None) is None  # type: ignore[arg-type]


class TestStatementCacheKey:
    def test_context_changes_the_key(self) -> None:
        """Otherwise two columns could share a compiled bind processor."""
        assert (
            EncryptedString("a")._static_cache_key
            != EncryptedString("b")._static_cache_key
        )

    def test_cipher_changes_the_key(self) -> None:
        """The reason `cipher_provider` is not keyword-only; see types.py."""
        assert (
            EncryptedString(CONTEXT)._static_cache_key
            != EncryptedString(CONTEXT, fixture_cipher)._static_cache_key
        )

    def test_the_same_declaration_produces_the_same_key(self) -> None:
        assert (
            EncryptedString(CONTEXT, fixture_cipher)._static_cache_key
            == EncryptedString(CONTEXT, fixture_cipher)._static_cache_key
        )


class TestComparatorRefusals:
    """Every comparison that would compile, run, and match nothing."""

    @pytest.mark.parametrize(
        ("name", "call"),
        [
            ("==", lambda column: column == URL),
            ("!=", lambda column: column != URL),
            ("<", lambda column: column < URL),
            ("<=", lambda column: column <= URL),
            (">", lambda column: column > URL),
            (">=", lambda column: column >= URL),
            ("in_", lambda column: column.in_([URL])),
            ("not_in", lambda column: column.not_in([URL])),
            ("like", lambda column: column.like("https://%")),
            ("ilike", lambda column: column.ilike("https://%")),
        ],
    )
    def test_refused(self, name: str, call: Any) -> None:
        with pytest.raises(EncryptedColumnComparisonError, match="encrypted column"):
            call(secrets_table.c.secret)

    def test_is_null_is_allowed(self) -> None:
        """`IS NULL` binds no parameter and means exactly what it says."""
        compiled = str(
            select(secrets_table.c.id).where(secrets_table.c.secret.is_(None))
        )
        assert "IS NULL" in compiled

    def test_equality_against_none_renders_is_null(self) -> None:
        compiled = str(
            select(secrets_table.c.id).where(secrets_table.c.secret == None)  # noqa: E711
        )
        assert "IS NULL" in compiled

    def test_inequality_against_none_renders_is_not_null(self) -> None:
        compiled = str(
            select(secrets_table.c.id).where(secrets_table.c.secret != None)  # noqa: E711
        )
        assert "IS NOT NULL" in compiled

    def test_the_column_is_still_hashable(self) -> None:
        """Defining `__eq__` sets `__hash__` to None unless it is restored,
        and SQLAlchemy puts column expressions in sets all through the ORM."""
        assert len({secrets_table.c.secret, secrets_table.c.secret}) == 1

    def test_the_message_names_the_way_out(self) -> None:
        with pytest.raises(EncryptedColumnComparisonError) as caught:
            _ = secrets_table.c.secret == URL
        assert "blind index" in str(caught.value)


#: Encrypted columns whose context label no longer matches where they live,
#: with the reason. A label is bound into the authentication tag of every value
#: ever written, so a renamed column must keep the old label or every existing
#: row becomes unreadable — the rename goes here rather than into the model.
#: Empty today; the table exists so that the first rename has somewhere to be
#: declared instead of being made silently.
RENAMED_CONTEXT_EXEMPTIONS: dict[str, str] = {}


def _encrypted_columns() -> list[tuple[str, str, str]]:
    """Every mapped column in `src/models/` using one of the types above."""
    found = []
    for table in Base.metadata.tables.values():
        for column in table.columns:
            if isinstance(column.type, EncryptedString | EncryptedBytes):
                found.append((table.name, column.name, column.type.context))
    return found


class TestModelColumns:
    def test_at_least_one_column_is_encrypted(self) -> None:
        """Guards the gate below against passing vacuously."""
        assert _encrypted_columns()

    def test_users_notification_webhook_url_is_encrypted(self) -> None:
        assert ("users", "notification_webhook_url") in [
            (table, column) for table, column, _ in _encrypted_columns()
        ]

    def test_every_context_label_matches_its_column_or_is_exempt(self) -> None:
        wrong = {
            f"{table}.{column}": context
            for table, column, context in _encrypted_columns()
            if context != f"{table}.{column}"
            and f"{table}.{column}" not in RENAMED_CONTEXT_EXEMPTIONS
        }
        assert not wrong, (
            "Encrypted columns whose context label does not match their "
            f"position: {wrong}. A label is correct at the moment it is "
            "written and must not change afterwards, so a deliberate "
            "divergence belongs in RENAMED_CONTEXT_EXEMPTIONS with a reason."
        )

    def test_context_labels_are_unique(self) -> None:
        """Two columns sharing a label are interchangeable to the cipher.

        Which is the exact property the AAD exists to remove: a value could be
        copied from one to the other and would authenticate.
        """
        labels = [context for _, _, context in _encrypted_columns()]
        assert len(labels) == len(set(labels))

    def test_no_encrypted_column_is_indexed_or_unique(self) -> None:
        """Both are silently useless on randomised ciphertext.

        A unique constraint never collides, so it enforces nothing; an index is
        only ever used by a comparison the comparator refuses, so it is pure
        write cost.
        """
        offenders = [
            f"{table.name}.{column.name}"
            for table in Base.metadata.tables.values()
            for column in table.columns
            if isinstance(column.type, EncryptedString | EncryptedBytes)
            and (column.index or column.unique)
        ]
        assert not offenders, offenders

    def test_the_default_provider_is_the_process_wide_one(self) -> None:
        """A model column pinned to a test cipher would ship one."""
        for table in Base.metadata.tables.values():
            for column in table.columns:
                if isinstance(column.type, EncryptedString | EncryptedBytes):
                    assert column.type.cipher_provider is get_field_cipher
