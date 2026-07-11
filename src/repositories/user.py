import uuid

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.user import User
from src.repositories.base import BaseRepository


class UserRepository(BaseRepository[User]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, User)

    async def get_by_email(self, email: str) -> User | None:
        result = await self.session.execute(
            select(User).where(User.email == email)
        )
        return result.scalar_one_or_none()

    async def get_by_oauth(self, provider: str, sub: str) -> User | None:
        result = await self.session.execute(
            select(User).where(
                and_(User.oauth_provider == provider, User.oauth_sub == sub)
            )
        )
        return result.scalar_one_or_none()

    async def list_active(
        self, limit: int = 20, offset: int = 0
    ) -> list[User]:
        result = await self.session.execute(
            select(User).where(User.is_active.is_(True)).limit(limit).offset(offset)
        )
        return list(result.scalars().all())

    async def exists_by_email(self, email: str) -> bool:
        user = await self.get_by_email(email)
        return user is not None

    async def deactivate(self, id: uuid.UUID) -> User | None:
        return await self.update(id, is_active=False)
