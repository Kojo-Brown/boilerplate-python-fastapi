import uuid
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.schemas import RegisterRequest, TokenResponse, UserResponse
from src.auth.utils import (
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_password,
    verify_password,
)
from src.models.user import User
from src.repositories.refresh_token import RefreshTokenRepository
from src.repositories.user import UserRepository


class AuthService:
    def __init__(self, db: AsyncSession) -> None:
        self.db = db
        self.users = UserRepository(db)
        self.tokens = RefreshTokenRepository(db)

    async def register(self, data: RegisterRequest) -> UserResponse:
        if await self.users.exists_by_email(data.email):
            raise ValueError("Email already registered")

        user = await self.users.create(
            email=data.email,
            hashed_password=hash_password(data.password),
        )
        return UserResponse.model_validate(user)

    async def login(self, email: str, password: str) -> TokenResponse:
        user = await self.users.get_by_email(email)

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

        stored = await self.tokens.get_by_token(refresh_token)
        if stored is None or stored.revoked:
            raise ValueError("Refresh token is invalid or revoked")

        expires_at = stored.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        if expires_at < datetime.now(UTC):
            raise ValueError("Refresh token has expired")

        stored.revoked = True
        await self.db.flush()

        user = await self.users.get(stored.user_id)
        if user is None or not user.is_active:
            raise ValueError("User not found or inactive")

        tokens = await self._issue_tokens(user)
        await self.db.commit()
        return tokens

    async def oauth_login(
        self, provider: str, sub: str, email: str
    ) -> TokenResponse:
        """Find or create a user from an OAuth provider callback."""
        user = await self.users.get_by_oauth(provider, sub)

        if user is None:
            user = await self.users.get_by_email(email)

            if user is None:
                user = await self.users.create(
                    email=email,
                    hashed_password=None,
                    is_active=True,
                    is_verified=True,
                    oauth_provider=provider,
                    oauth_sub=sub,
                )
            else:
                user.oauth_provider = provider
                user.oauth_sub = sub
                user.is_verified = True
                await self.db.flush()

        if not user.is_active:
            raise ValueError("Account is inactive")

        tokens = await self._issue_tokens(user)
        await self.db.commit()
        return tokens

    async def logout(self, refresh_token: str) -> None:
        await self.tokens.revoke(refresh_token)
        await self.db.commit()

    async def _issue_tokens(self, user: User) -> TokenResponse:
        access_token = create_access_token(str(user.id), user.email, user.role)

        jti = str(uuid.uuid4())
        refresh_token_str, expires_at = create_refresh_token(str(user.id), jti)

        await self.tokens.create(
            token=refresh_token_str,
            user_id=user.id,
            expires_at=expires_at,
        )

        return TokenResponse(
            access_token=access_token,
            refresh_token=refresh_token_str,
        )
