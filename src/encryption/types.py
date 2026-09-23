"""Column types that encrypt on the way in and decrypt on the way out.

A `TypeDecorator` is the right seam for this and not merely a convenient one.
Encryption placed in the service layer has to be remembered at every write; a
column type cannot be forgotten, because there is no way to reach the column
except through it. Everything above the mapper — repositories, the unit of
work, `selectinload`, the streaming export's server-side cursor, Alembic's
`op.bulk_insert` — goes on handling `str`, and the bytes in Postgres are
`bytea`.

**What this does not do, stated up front, because the failure is silent.**
Randomised encryption means the same plaintext seals to different bytes every
time. So:

- `WHERE notification_webhook_url = :value` can never match. That is what
  `_EncryptedComparator` below refuses: a query that returns zero rows for a
  value that is present is the worst shape a bug can take, and it is the
  default behaviour without the guard.
- A `UNIQUE` constraint on the column enforces nothing, since duplicates do not
  collide. Nothing declares one; a reviewer should refuse the first that tries.
- `ORDER BY` sorts ciphertext, and `LIKE` matches it. Both are refused for the
  same reason as equality.
- `NULL` is still `NULL`. Whether a row *has* a value is not hidden and cannot
  be, short of sealing a sentinel and losing the ability to index for absence.

Searchable encryption is a different feature with a different cost — a blind
index, which leaks equality by construction. It is not in this module, and the
comparator makes the absence loud rather than leaving it to be discovered.

**The context label is the column's durable name, not its current one.** It is
written as a literal rather than derived from `table.column` on purpose:
deriving it would mean that renaming a column silently changes the AAD and
makes every existing row in it unreadable, with the first symptom arriving
whenever somebody next reads an old row. Declared, it survives a rename, and
`tests/test_encryption_types.py` asserts that every encrypted column in
`src/models/` declares the label it actually lives at today — so the literal
cannot be wrong at the moment it is written, only deliberately preserved
afterwards.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, NoReturn

from sqlalchemy import Dialect, LargeBinary
from sqlalchemy.sql.operators import ColumnOperators
from sqlalchemy.types import TypeDecorator

from src.encryption.cipher import FieldCipher, get_field_cipher
from src.encryption.errors import EncryptedColumnComparisonError


class _EncryptedComparator(TypeDecorator.Comparator[Any]):
    """Turns every comparison that cannot work into an error instead of zero rows."""

    # Defining `__eq__` would otherwise set `__hash__` to None, and SQLAlchemy
    # puts comparators in sets and dicts.
    __hash__ = ColumnOperators.__hash__

    def _refuse(self, operation: str) -> NoReturn:
        raise EncryptedColumnComparisonError(
            f"{operation} is not supported on an encrypted column: values are "
            "sealed with a random nonce, so no two encryptions of the same "
            "plaintext are equal and the comparison would silently match "
            "nothing. Filter in Python after loading, or add a blind index if "
            "the column has to be searchable."
        )

    # The two ignores below are `object.__eq__`/`__ne__` returning `bool`.
    # SQLAlchemy's own `ColumnOperators` overrides them to return expressions
    # and carries the same ignores; a comparator that returned `bool` could not
    # build a WHERE clause at all.
    def __eq__(self, other: Any) -> ColumnOperators:  # type: ignore[override]
        # `col == None` renders `IS NULL`, binds no parameter and is exactly as
        # meaningful as it is on any other column, so it is allowed through.
        if other is None:
            return super().__eq__(other)
        self._refuse("==")

    def __ne__(self, other: Any) -> ColumnOperators:  # type: ignore[override]
        if other is None:
            return super().__ne__(other)
        self._refuse("!=")

    def __lt__(self, other: Any) -> ColumnOperators:
        self._refuse("<")

    def __le__(self, other: Any) -> ColumnOperators:
        self._refuse("<=")

    def __gt__(self, other: Any) -> ColumnOperators:
        self._refuse(">")

    def __ge__(self, other: Any) -> ColumnOperators:
        self._refuse(">=")

    def in_(self, other: Any) -> ColumnOperators:
        self._refuse("in_()")

    def not_in(self, other: Any) -> ColumnOperators:
        self._refuse("not_in()")

    def like(self, other: Any, escape: str | None = None) -> ColumnOperators:
        self._refuse("like()")

    def ilike(self, other: Any, escape: str | None = None) -> ColumnOperators:
        self._refuse("ilike()")


class EncryptedBytes(TypeDecorator[bytes]):
    """`bytea` holding an AES-256-GCM envelope; `bytes` in Python."""

    impl = LargeBinary
    # Safe to cache: the ciphertext is produced by a Python-side bind
    # processor, so the compiled SQL is identical for every value, and both
    # constructor arguments are folded into SQLAlchemy's cache key.
    cache_ok = True
    comparator_factory = _EncryptedComparator

    def __init__(
        self,
        context: str,
        cipher_provider: Callable[[], FieldCipher] = get_field_cipher,
    ) -> None:
        """`context` is the durable label bound into every value's AAD.

        `cipher_provider` is called per value rather than held, so that the
        column can be defined before any key exists — the case Alembic is in
        while running the migration that creates it — and so a test can point
        one column at a different key ring without touching the process-wide
        settings.

        It is positional-or-keyword, and deliberately not keyword-only:
        SQLAlchemy builds a type's cache key from `get_cls_kwargs`, which reads
        an `__init__`'s positional parameters and ignores its keyword-only
        ones. Behind a `*` this argument would drop out of the key, leaving two
        instances that differ only by cipher indistinguishable to the statement
        cache — and one of them could then execute with the other's compiled
        bind processor. Call it by keyword; just do not declare it that way.
        """
        super().__init__()
        self.context = context
        self.cipher_provider = cipher_provider

    def process_bind_param(self, value: bytes | None, dialect: Dialect) -> bytes | None:
        if value is None:
            return None
        return self.cipher_provider().encrypt(value, context=self.context)

    def process_result_value(self, value: Any, dialect: Dialect) -> bytes | None:
        if value is None:
            return None
        # `bytes(...)` because a DBAPI is entitled to hand back a memoryview,
        # and the cipher slices its argument.
        return self.cipher_provider().decrypt(bytes(value), context=self.context)


class EncryptedString(TypeDecorator[str]):
    """`bytea` holding an AES-256-GCM envelope; `str` in Python.

    UTF-8 in and out. A value that does not survive the round trip cannot
    exist: it was a `str` on the way in.
    """

    impl = LargeBinary
    #: See `EncryptedBytes`.
    cache_ok = True
    comparator_factory = _EncryptedComparator

    def __init__(
        self,
        context: str,
        cipher_provider: Callable[[], FieldCipher] = get_field_cipher,
    ) -> None:
        super().__init__()
        self.context = context
        self.cipher_provider = cipher_provider

    @property
    def python_type(self) -> type[str]:
        return str

    def process_bind_param(self, value: str | None, dialect: Dialect) -> bytes | None:
        if value is None:
            return None
        return self.cipher_provider().encrypt(
            value.encode("utf-8"), context=self.context
        )

    def process_result_value(self, value: Any, dialect: Dialect) -> str | None:
        if value is None:
            return None
        plaintext = self.cipher_provider().decrypt(bytes(value), context=self.context)
        return plaintext.decode("utf-8")
