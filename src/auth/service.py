import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.schemas import RegisterRequest, TokenResponse, UserResponse
from src.auth.utils import (
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_password,
    verify_password,
)
from src.models.refresh_token import RefreshToken
from src.models.user import User


class AuthService:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db

    async def register(self, data: RegisterRequest) -> UserResponse:
        result = await self.db.execute(select(User).where(User.email == data.email))
        if result.scalar_one_or_none() is not None:
            raise ValueError("Email already registered")

        user = User(
            email=data.email,
            hashed_password=hash_password(data.password),
        )
        self.db.add(user)
        await self.db.commit()
        await self.db.refresh(user)
        return UserResponse.model_validate(user)

    async def login(self, email: str, password: str) -> TokenResponse:
        result = await self.db.execute(select(User).where(User.email == email))
        user = result.scalar_one_or_none()

        if (
            user is None
            or user.hashed_password is None
            or not verify_password(password, user.hashed_password)
        ):
            raise ValueError("Invalid credentials")

        if not user.is_active:
            raise ValueError("Account is inactive")

        tokens = await self._issue_tokens(user)
        await self.db.commit()
        return tokens

    async def refresh(self, refresh_token: str) -> TokenResponse:
        try:
            payload = decode_token(refresh_token)
        except ValueError as exc:
            raise ValueError("Invalid refresh token") from exc

        if payload.get("type") != "refresh":
            raise ValueError("Invalid token type")

        result = await self.db.execute(
            select(RefreshToken).where(RefreshToken.token == refresh_token)
        )
        stored = result.scalar_one_or_none()

        if stored is None or stored.revoked:
            raise ValueError("Refresh token is invalid or revoked")

        expires_at = stored.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        if expires_at < datetime.now(UTC):
            raise ValueError("Refresh token has expired")

        stored.revoked = True
        await self.db.flush()

        result = await self.db.execute(select(User).where(User.id == stored.user_id))
        user = result.scalar_one_or_none()
        if user is None or not user.is_active:
            raise ValueError("User not found or inactive")

        tokens = await self._issue_tokens(user)
        await self.db.commit()
        return tokens

    async def logout(self, refresh_token: str) -> None:
        result = await self.db.execute(
            select(RefreshToken).where(RefreshToken.token == refresh_token)
        )
        stored = result.scalar_one_or_none()
        if stored is not None:
            stored.revoked = True
            await self.db.commit()

    async def _issue_tokens(self, user: User) -> TokenResponse:
        access_token = create_access_token(str(user.id), user.email, user.role)

        jti = str(uuid.uuid4())
        refresh_token_str, expires_at = create_refresh_token(str(user.id), jti)

        db_refresh = RefreshToken(
            token=refresh_token_str,
            user_id=user.id,
            expires_at=expires_at,
        )
        self.db.add(db_refresh)
        await self.db.flush()

        return TokenResponse(
            access_token=access_token,
            refresh_token=refresh_token_str,
        )
