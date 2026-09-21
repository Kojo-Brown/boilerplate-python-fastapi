import uuid
from datetime import UTC, datetime

from sqlalchemy import CursorResult, and_, delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.refresh_token import RefreshToken, RevocationReason
from src.repositories.base import BaseRepository


class RefreshTokenRepository(BaseRepository[RefreshToken]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, RefreshToken)

    async def get_by_token(self, token: str) -> RefreshToken | None:
        result = await self.session.execute(
            select(RefreshToken).where(RefreshToken.token == token)
        )
        return result.scalar_one_or_none()

    async def create(  # type: ignore[override]
        self,
        *,
        token: str,
        user_id: uuid.UUID,
        expires_at: datetime,
        family_id: uuid.UUID,
    ) -> RefreshToken:
        """Store a newly issued refresh token as a member of `family_id`.

        Spelled out rather than inherited from `BaseRepository.create(**kwargs)`
        because `family_id` is the field a caller can most easily omit and least
        easily notice omitting: the model defaults it to a fresh UUID, so a
        rotation that forgot to pass its parent's family would insert
        successfully, work correctly on the happy path, and only be wrong on
        the one path the column exists for. A required keyword makes that a
        mypy error at the call site instead. The `override` is deliberate and
        narrow — the base's signature is `**kwargs: Any`, which this one
        restricts.
        """
        return await super().create(
            token=token,
            user_id=user_id,
            expires_at=expires_at,
            family_id=family_id,
        )

    async def revoke(self, token: str, *, reason: RevocationReason = "logout") -> bool:
        """Mark a single token as revoked. Returns True if the token existed."""
        stored = await self.get_by_token(token)
        if stored is None:
            return False
        _mark_revoked(stored, reason)
        await self.session.flush()
        return True

    async def revoke_family(
        self, family_id: uuid.UUID, *, reason: RevocationReason
    ) -> int:
        """Revoke every live token descended from one login. Returns the count.

        Already-revoked members are left exactly as they are, provenance
        included. That is the point of filtering on `revoked` rather than
        stamping the whole family: the rotated ancestors are the record of how
        the chain got here, and overwriting their reason with
        `"reuse_detected"` would erase the ordinary rotations that a replay is
        only recognisable *against*. The count is therefore "sessions ended by
        this call", which is the number worth putting in an alert.
        """
        result = await self.session.execute(
            select(RefreshToken).where(
                and_(
                    RefreshToken.family_id == family_id,
                    RefreshToken.revoked.is_(False),
                )
            )
        )
        tokens = list(result.scalars().all())
        for t in tokens:
            _mark_revoked(t, reason)
        await self.session.flush()
        return len(tokens)

    async def revoke_all_for_user(
        self, user_id: uuid.UUID, *, reason: RevocationReason = "logout"
    ) -> int:
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
            _mark_revoked(t, reason)
        await self.session.flush()
        return len(tokens)

    async def delete_expired(self) -> int:
        """Hard-delete all expired tokens. Returns the count deleted."""
        now = datetime.now(UTC)
        # A DELETE always yields a CursorResult, which is what carries rowcount;
        # execute() is only typed as the broader Result.
        result: CursorResult[object] = await self.session.execute(  # type: ignore[assignment]
            delete(RefreshToken).where(RefreshToken.expires_at < now)
        )
        await self.session.flush()
        return result.rowcount


def _mark_revoked(stored: RefreshToken, reason: RevocationReason) -> None:
    """Set the flag and its provenance together.

    One function because they are one fact recorded in three columns, and a
    revocation that sets `revoked` without `revoked_at` is a row that says an
    incident happened and refuses to say when.
    """
    stored.revoked = True
    stored.revoked_at = datetime.now(UTC)
    stored.revoked_reason = reason
