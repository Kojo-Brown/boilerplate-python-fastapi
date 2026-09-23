"""Field-level encryption measured against a real Postgres.

The unit tests next door prove the cipher and the processors in isolation.
They cannot prove the claim the feature is actually making, which is about
what a person holding the *database* can see and do: that the bytes on disk
are not the URL, that editing them is caught, and — the part that is easy to
overstate — that moving them between rows is not.

They are skipped when `DATABASE_URL` names nothing reachable, and CI always has
a Postgres service, so every claim below is measured on every pull request.
Following `test_optimistic_concurrency_db.py`: reachability and schema are
checked separately, so a missing database skips and a broken one fails.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator

import pytest
from sqlalchemy import Column, LargeBinary, MetaData, Table, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from src.config import settings
from src.database import Base
from src.encryption import envelope
from src.encryption.cipher import FieldCipher, get_field_cipher
from src.encryption.errors import DecryptionError
from src.encryption.keys import DataKey, KeyRing
from src.encryption.types import EncryptedString
from src.models.user import User

COLUMN_CONTEXT = "users.notification_webhook_url"
URL = "https://hooks.example.test/T000/B000/mock-webhook-token"
OTHER_URL = "https://hooks.example.test/T111/B111/another-mock-token"

RETIRING = DataKey(key_id="rotation-old", material=b"8" * 32)
INCOMING = DataKey(key_id="rotation-new", material=b"9" * 32)
ROTATION_CONTEXT = "encrypted_rotation.secret"


def _two_key_cipher() -> FieldCipher:
    """A ring mid-rotation: reads both keys, writes the new one."""
    return FieldCipher(
        key_ring=KeyRing(active_key_id=INCOMING.key_id, keys=(RETIRING, INCOMING))
    )


#: A table of its own, so the rotation tests can use a key ring the application
#: does not have. Kept out of `Base.metadata` so it never reaches a migration.
_rotation_metadata = MetaData()
rotation_table = Table(
    "encrypted_rotation",
    _rotation_metadata,
    Column("id", LargeBinary, primary_key=True),
    Column("secret", EncryptedString(ROTATION_CONTEXT, _two_key_cipher)),
)


@pytest.fixture
async def engine() -> AsyncGenerator[AsyncEngine]:
    """An engine on `DATABASE_URL`, or a skip if there is nothing there."""
    engine = create_async_engine(settings.DATABASE_URL, poolclass=NullPool)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except (OSError, SQLAlchemyError) as exc:
        await engine.dispose()
        pytest.skip(f"no usable Postgres at DATABASE_URL: {exc}")

    # No-op in CI, where `alembic upgrade head` has already run — which is what
    # makes these tests a check on migration 0007 as well as on the mapper. If
    # the migration forgot to convert the column, `create_all` finds the table
    # already there, leaves the `varchar` alone, and every test below fails on
    # a value that will not decode.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.run_sync(_rotation_metadata.create_all)

    yield engine

    async with engine.begin() as conn:
        await conn.run_sync(_rotation_metadata.drop_all)
    await engine.dispose()


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
async def user_id(
    sessions: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[uuid.UUID]:
    """A committed user with a webhook URL, removed again afterwards."""
    async with sessions() as session:
        user = User(
            email=f"encryption-{uuid.uuid4()}@example.test",
            hashed_password="not-a-real-hash",
            notification_webhook_url=URL,
        )
        session.add(user)
        await session.commit()
        created_id = user.id

    yield created_id

    async with sessions() as session:
        row = await session.get(User, created_id)
        if row is not None:
            await session.delete(row)
            await session.commit()


async def raw_column(
    sessions: async_sessionmaker[AsyncSession], row_id: uuid.UUID
) -> bytes | None:
    """The stored bytes, read around the column type.

    `text()` rather than `select(User.notification_webhook_url)`: the point is
    to see what Postgres holds, and going through the mapper would decrypt it
    on the way out, which is the thing being checked.
    """
    async with sessions() as session:
        stored = await session.scalar(
            text("SELECT notification_webhook_url FROM users WHERE id = :id"),
            {"id": row_id},
        )
    return None if stored is None else bytes(stored)


class TestWhatIsOnDisk:
    async def test_the_column_does_not_contain_the_url(
        self, sessions: async_sessionmaker[AsyncSession], user_id: uuid.UUID
    ) -> None:
        """The whole claim, in one assertion, against a real column."""
        stored = await raw_column(sessions, user_id)
        assert stored is not None
        assert URL.encode("utf-8") not in stored
        assert b"hooks.example.test" not in stored

    async def test_it_is_an_envelope_under_the_active_key(
        self, sessions: async_sessionmaker[AsyncSession], user_id: uuid.UUID
    ) -> None:
        stored = await raw_column(sessions, user_id)
        assert stored is not None
        parsed = envelope.parse(stored)
        assert parsed.version == envelope.FORMAT_VERSION
        assert parsed.key_id == get_field_cipher().key_ring.active_key_id

    async def test_the_orm_still_hands_back_a_str(
        self, sessions: async_sessionmaker[AsyncSession], user_id: uuid.UUID
    ) -> None:
        async with sessions() as session:
            user = await session.get(User, user_id)
            assert user is not None
            assert user.notification_webhook_url == URL

    async def test_an_update_re_seals_with_a_fresh_nonce(
        self, sessions: async_sessionmaker[AsyncSession], user_id: uuid.UUID
    ) -> None:
        """Two writes of the *same* value must not produce the same bytes.

        If they did, the column would leak equality across rows and over time —
        "this user's webhook is the one they had last month" — which is exactly
        what deterministic encryption gives up.
        """
        before = await raw_column(sessions, user_id)
        async with sessions() as session:
            user = await session.get(User, user_id)
            assert user is not None
            user.notification_webhook_url = None
            await session.commit()
            user.notification_webhook_url = URL
            await session.commit()
        after = await raw_column(sessions, user_id)
        assert before != after

    async def test_null_is_stored_as_null(
        self, sessions: async_sessionmaker[AsyncSession], user_id: uuid.UUID
    ) -> None:
        """Absence is not hidden, and `IS NULL` still selects on it."""
        async with sessions() as session:
            user = await session.get(User, user_id)
            assert user is not None
            user.notification_webhook_url = None
            await session.commit()

        assert await raw_column(sessions, user_id) is None
        async with sessions() as session:
            found = await session.scalar(
                text(
                    "SELECT count(*) FROM users WHERE notification_webhook_url IS NULL"
                )
            )
        assert found is not None and found >= 1


class TestTampering:
    async def test_editing_the_ciphertext_is_caught_on_read(
        self, sessions: async_sessionmaker[AsyncSession], user_id: uuid.UUID
    ) -> None:
        """What an attacker with UPDATE on `users` gets: a 500, not a change."""
        stored = await raw_column(sessions, user_id)
        assert stored is not None
        modified = bytearray(stored)
        modified[-1] ^= 0x01

        async with sessions() as session:
            await session.execute(
                text(
                    "UPDATE users SET notification_webhook_url = :value WHERE id = :id"
                ),
                {"value": bytes(modified), "id": user_id},
            )
            await session.commit()

        async with sessions() as session:
            with pytest.raises(DecryptionError):
                await session.get(User, user_id)

        # Put it back, so the fixture's cleanup does not have to read the row.
        async with sessions() as session:
            await session.execute(
                text(
                    "UPDATE users SET notification_webhook_url = :value WHERE id = :id"
                ),
                {"value": stored, "id": user_id},
            )
            await session.commit()

    async def test_moving_a_value_between_rows_is_not_caught(
        self, sessions: async_sessionmaker[AsyncSession], user_id: uuid.UUID
    ) -> None:
        """The limitation, asserted rather than left in a docstring.

        The additional data binds a value to its *column*, not to its row: the
        primary key is not available to a `TypeDecorator`, which sees one value
        and no idea whose it is. So an attacker with UPDATE on `users` can copy
        one user's sealed webhook URL onto another user and it will decrypt.
        They still cannot read it, and they cannot forge one — but they can
        redirect notifications to an endpoint that already belongs to someone.

        Binding the row id would need the value to be sealed where the id is
        known, which means a mapper-level event rather than a column type, and
        it would make re-keying a row impossible without knowing its future id.
        The trade is documented in docs/field-encryption.md; this test is here
        so the limit is a measured fact and not a belief.
        """
        stored = await raw_column(sessions, user_id)
        assert stored is not None

        async with sessions() as session:
            victim = User(
                email=f"encryption-victim-{uuid.uuid4()}@example.test",
                hashed_password="not-a-real-hash",
                notification_webhook_url=OTHER_URL,
            )
            session.add(victim)
            await session.commit()
            victim_id = victim.id

        try:
            async with sessions() as session:
                await session.execute(
                    text(
                        "UPDATE users SET notification_webhook_url = :value "
                        "WHERE id = :id"
                    ),
                    {"value": stored, "id": victim_id},
                )
                await session.commit()

            async with sessions() as session:
                moved = await session.get(User, victim_id)
                assert moved is not None
                assert moved.notification_webhook_url == URL
        finally:
            async with sessions() as session:
                row = await session.get(User, victim_id)
                if row is not None:
                    await session.delete(row)
                    await session.commit()


class TestRotationAgainstTheDatabase:
    async def test_a_row_sealed_under_the_retiring_key_still_reads(
        self, engine: AsyncEngine
    ) -> None:
        """Step 2 of a rotation, end to end through Postgres.

        The row is written with the old key only — as a replica that has not
        picked up the new configuration yet would write it — and read back by a
        process whose ring holds both.
        """
        old_only = FieldCipher(
            key_ring=KeyRing(active_key_id=RETIRING.key_id, keys=(RETIRING,))
        )
        row_id = uuid.uuid4().bytes
        # Written around the column type on purpose: going through it would
        # seal under the *incoming* key, which is the state this test needs not
        # to be in.
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO encrypted_rotation (id, secret) VALUES (:id, :value)"
                ),
                {
                    "id": row_id,
                    "value": old_only.encrypt(
                        URL.encode("utf-8"), context=ROTATION_CONTEXT
                    ),
                },
            )

        async with engine.connect() as conn:
            secret = await conn.scalar(
                select(rotation_table.c.secret).where(rotation_table.c.id == row_id)
            )
        assert secret == URL

    async def test_writing_uses_the_incoming_key(self, engine: AsyncEngine) -> None:
        row_id = uuid.uuid4().bytes
        async with engine.begin() as conn:
            await conn.execute(rotation_table.insert().values(id=row_id, secret=URL))

        async with engine.connect() as conn:
            stored = await conn.scalar(
                text("SELECT secret FROM encrypted_rotation WHERE id = :id"),
                {"id": row_id},
            )
        assert stored is not None
        assert envelope.parse(bytes(stored)).key_id == INCOMING.key_id
