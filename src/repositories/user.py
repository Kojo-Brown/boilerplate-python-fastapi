import uuid
from collections.abc import AsyncIterator

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.user import User
from src.repositories.base import BaseRepository
from src.users.export import UserExportRecord


class UserRepository(BaseRepository[User]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, User)

    async def get_by_email(self, email: str) -> User | None:
        result = await self.session.execute(select(User).where(User.email == email))
        return result.scalar_one_or_none()

    async def get_by_oauth(self, provider: str, sub: str) -> User | None:
        result = await self.session.execute(
            select(User).where(
                and_(User.oauth_provider == provider, User.oauth_sub == sub)
            )
        )
        return result.scalar_one_or_none()

    async def list_active(self, limit: int = 20, offset: int = 0) -> list[User]:
        result = await self.session.execute(
            select(User).where(User.is_active.is_(True)).limit(limit).offset(offset)
        )
        return list(result.scalars().all())

    async def exists_by_email(self, email: str) -> bool:
        user = await self.get_by_email(email)
        return user is not None

    async def deactivate(self, id: uuid.UUID) -> User | None:
        return await self.update(id, is_active=False)

    async def stream_export(
        self,
        *,
        batch_size: int,
        active_only: bool,
    ) -> AsyncIterator[UserExportRecord]:
        """Stream the user table for export. Implements `UserExportSource`.

        Three choices here are the difference between an export that streams
        and one that only looks as though it does.

        **Columns, not entities.** `select(User)` would emit `SELECT
        users.hashed_password, ...` and fetch a password hash for every row of
        a file that must not contain one. Naming the published columns keeps
        the hash in the database rather than trusting eight layers of
        serialisation to drop it. See `src/users/export.py` for why this is
        not an argument about memory.

        **`yield_per`, which is what makes it a server-side cursor.** Without
        it asyncpg buffers the whole result before the first row is available,
        and every layer above this would be streaming something that had
        already been materialised.

        **`ORDER BY id`.** The order is arbitrary but *stable*, which is what a
        consumer diffing two exports needs, and it is the primary key index, so
        it costs no sort. Postgres evaluates the cursor against the snapshot
        the statement started with, so the export is consistent even though it
        takes minutes: rows written after it began are not in it, and rows
        deleted after it began still are.

        The result is closed in a `finally`, because the caller may stop early
        — a client disconnect is the ordinary way an export ends. asyncpg
        tolerates an abandoned portal, so nothing breaks without this; what it
        costs is a cursor held open on the server until the transaction ends,
        for every download somebody cancelled.
        """
        statement = select(
            User.id,
            User.email,
            User.role,
            User.is_active,
            User.is_verified,
            User.notification_channel,
            User.created_at,
            User.updated_at,
        ).order_by(User.id)
        if active_only:
            statement = statement.where(User.is_active.is_(True))

        result = await self.session.stream(
            statement.execution_options(yield_per=batch_size)
        )
        try:
            async for row in result:
                yield UserExportRecord.model_validate(row, from_attributes=True)
        finally:
            await result.close()
