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
from src.events.bus import EventBus, event_bus
from src.events.catalog import UserLoggedIn, UserRegistered
from src.exceptions import ConflictError, ForbiddenError, UnauthorizedError
from src.models.user import User
from src.repositories.refresh_token import RefreshTokenRepository
from src.repositories.user import UserRepository


class AuthService:
    """Authentication policy.

    Every rejection is raised as an :class:`~src.exceptions.AppException`
    subclass that already carries its own status code and error code, so no
    caller has to re-derive one. The distinction matters: a wrong password is
    401 — authentication failed, try again — while an account that is switched
    off is 403 — authentication succeeded, access is refused — and retrying
    will never help. Signalling both as one generic error made the answer to
    "is this account inactive?" depend on which route the caller came in
    through.

    What happens *because* an account was created or entered is not decided
    here. This class publishes domain events and returns; `src/events` routes
    them to whatever is subscribed. Every publish is deliberately placed after
    the commit — a subscriber that reacted to a registration the database then
    rolled back would be reacting to a user who does not exist, and a
    subscriber's own failure never travels back to the caller, so a broken
    mail queue cannot fail a registration that already succeeded.
    """

    def __init__(self, db: AsyncSession, events: EventBus | None = None) -> None:
        self.db = db
        self.users = UserRepository(db)
        self.tokens = RefreshTokenRepository(db)
        # Injectable so a test can hand over a bus of its own instead of
        # registering against the process-wide one and racing every other test.
        self.events = events if events is not None else event_bus

    async def register(self, data: RegisterRequest) -> UserResponse:
        if await self.users.exists_by_email(data.email):
            raise ConflictError("Email already registered")

        user = await self.users.create(
            email=data.email,
            hashed_password=hash_password(data.password),
        )
        # get_db() never commits on exit, so an uncommitted registration is
        # rolled back when the session closes.
        await self.db.commit()

        response = UserResponse.model_validate(user)
        await self.events.publish(
            UserRegistered(user_id=str(user.id), email=user.email, via="password")
        )
        return response

    async def login(self, email: str, password: str) -> TokenResponse:
        user = await self.users.get_by_email(email)

        if (
            user is None
            or user.hashed_password is None
            or not verify_password(password, user.hashed_password)
        ):
            raise UnauthorizedError("Invalid credentials")

        if not user.is_active:
            raise ForbiddenError("Account is inactive")

        tokens = await self._issue_tokens(user)
        await self.db.commit()
        await self.events.publish(
            UserLoggedIn(user_id=str(user.id), email=user.email, method="password")
        )
        return tokens

    async def refresh(self, refresh_token: str) -> TokenResponse:
        try:
            payload = decode_token(refresh_token)
        except ValueError as exc:
            raise UnauthorizedError("Invalid refresh token") from exc

        if payload.get("type") != "refresh":
            raise UnauthorizedError("Invalid token type")

        stored = await self.tokens.get_by_token(refresh_token)
        if stored is None or stored.revoked:
            raise UnauthorizedError("Refresh token is invalid or revoked")

        expires_at = stored.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        if expires_at < datetime.now(UTC):
            raise UnauthorizedError("Refresh token has expired")

        stored.revoked = True
        await self.db.flush()

        user = await self.users.get(stored.user_id)
        if user is None:
            raise UnauthorizedError("User not found")
        if not user.is_active:
            raise ForbiddenError("Account is inactive")

        tokens = await self._issue_tokens(user)
        await self.db.commit()
        return tokens

    async def oauth_login(self, provider: str, sub: str, email: str) -> TokenResponse:
        """Find or create a user from an OAuth provider callback."""
        user = await self.users.get_by_oauth(provider, sub)
        created = False

        if user is None:
            user = await self.users.get_by_email(email)

            if user is None:
                created = True
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
            raise ForbiddenError("Account is inactive")

        tokens = await self._issue_tokens(user)
        await self.db.commit()

        user_id, user_email = str(user.id), user.email
        # A first OAuth sign-in is a registration as well as a login, and
        # subscribers care about the difference: the welcome email is owed to
        # both, an address-confirmation mail to neither, since the provider
        # already verified it. Linking a provider to an existing account is
        # neither — that account was registered long ago.
        if created:
            await self.events.publish(
                UserRegistered(user_id=user_id, email=user_email, via="oauth")
            )
        await self.events.publish(
            UserLoggedIn(user_id=user_id, email=user_email, method="oauth")
        )
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
