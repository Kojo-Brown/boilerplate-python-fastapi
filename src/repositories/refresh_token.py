import uuid
from datetime import UTC, datetime

from sqlalchemy import and_, delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.refresh_token import RefreshToken
from src.repositories.base import BaseRepository


class RefreshTokenRepository(BaseRepository[RefreshToken]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, RefreshToken)

    async def get_by_token(self, token: str) -> RefreshToken | None:
        result = await self.session.execute(
            select(RefreshToken).where(RefreshToken.token == token)
        )
        return result.scalar_one_or_none()

    async def revoke(self, token: str) -> bool:
        """Mark a single token as revoked. Returns True if the token existed."""
        stored = await self.get_by_token(token)
        if stored is None:
            return False
        stored.revoked = True
        await self.session.flush()
        return True

    async def revoke_all_for_user(self, user_id: uuid.UUID) -> int:
        """Revoke every active token for a user. Returns the count revoked."""
        result = await self.session.execute(
            select(RefreshToken).where(
                and_(
                    RefreshToken.user_id == user_id,
                    RefreshToken.revoked.is_(False),
                )
            )
        )
        tokens = list(result.scalars().all())
        for t in tokens:
            t.revoked = True
        await self.session.flush()
        return len(tokens)

    async def delete_expired(self) -> int:
        """Hard-delete all expired tokens. Returns the count deleted."""
        now = datetime.now(UTC)
        result = await self.session.execute(
            delete(RefreshToken).where(RefreshToken.expires_at < now)
        )
        await self.session.flush()
        return result.rowcount  # type: ignore[return-value]
