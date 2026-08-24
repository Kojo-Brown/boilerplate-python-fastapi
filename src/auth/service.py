import uuid
from datetime import UTC, datetime

from src.auth.schemas import RegisterRequest, TokenResponse, UserResponse
from src.auth.utils import (
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_password,
    verify_password,
)
from src.events.base import EventPublisher
from src.events.catalog import UserLoggedIn, UserRegistered
from src.exceptions import ConflictError, ForbiddenError, UnauthorizedError
from src.models.user import User
from src.repositories.protocols import RefreshTokenStore, UserStore
from src.unit_of_work import UnitOfWork


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
    them to whatever is subscribed.

    Every publish is deliberately placed **inside the transaction, before the
    commit**, which is the opposite of where it used to be and for the same
    reason. Publishing means writing an outbox row (`src/outbox`), so the
    notification and the state change now commit together or not at all: a
    subscriber still cannot react to a registration that rolled back, because
    the relay only ever reads committed rows, and the reaction can no longer be
    lost by a process that dies after the commit — which is exactly what
    publishing *after* the commit risked. The ordering is not cosmetic: a
    publish placed after `commit()` would put its row in a fresh transaction
    that `get_db` closes without committing, and the event would disappear with
    no error raised anywhere.

    A subscriber's failure still never travels back to the caller. It cannot:
    subscribers now run in the relay, minutes or milliseconds later, so a
    broken mail queue delays a welcome email and does nothing whatever to the
    registration.

    Nothing it depends on is named concretely. The four collaborators are
    protocols — two stores, a transaction, a publisher — so substituting the
    database means handing over a different object rather than convincing a
    stub to behave like SQLAlchemy. `src/dependencies.py` supplies the real
    ones; `tests/fakes.py` supplies in-memory ones. Construction is
    keyword-only because four same-shaped arguments in a row are exactly the
    signature a positional swap goes unnoticed in.
    """

    def __init__(
        self,
        *,
        users: UserStore,
        tokens: RefreshTokenStore,
        uow: UnitOfWork,
        events: EventPublisher,
    ) -> None:
        self.users = users
        self.tokens = tokens
        self.uow = uow
        self.events = events

    async def register(self, data: RegisterRequest) -> UserResponse:
        if await self.users.exists_by_email(data.email):
            raise ConflictError("Email already registered")

        user = await self.users.create(
            email=data.email,
            hashed_password=hash_password(data.password),
        )
        await self.events.publish(
            UserRegistered(user_id=str(user.id), email=user.email, via="password")
        )
        # get_db() never commits on exit, so an uncommitted registration is
        # rolled back when the session closes — and so is the outbox row above,
        # which is the point of it being above.
        await self.uow.commit()

        return UserResponse.model_validate(user)

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
        await self.events.publish(
            UserLoggedIn(user_id=str(user.id), email=user.email, method="password")
        )
        await self.uow.commit()
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
        await self.uow.flush()

        user = await self.users.get(stored.user_id)
        if user is None:
            raise UnauthorizedError("User not found")
        if not user.is_active:
            raise ForbiddenError("Account is inactive")

        tokens = await self._issue_tokens(user)
        await self.uow.commit()
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
                await self.uow.flush()

        if not user.is_active:
            raise ForbiddenError("Account is inactive")

        tokens = await self._issue_tokens(user)

        user_id, user_email = str(user.id), user.email
        # A first OAuth sign-in is a registration as well as a login, and
        # subscribers care about the difference: the welcome email is owed to
        # both, an address-confirmation mail to neither, since the provider
        # already verified it. Linking a provider to an existing account is
        # neither — that account was registered long ago.
        #
        # Both rows join the transaction that created the account, so a
        # sign-in either produces the account and both notifications or
        # produces none of them.
        if created:
            await self.events.publish(
                UserRegistered(user_id=user_id, email=user_email, via="oauth")
            )
        await self.events.publish(
            UserLoggedIn(user_id=user_id, email=user_email, method="oauth")
        )
        await self.uow.commit()
        return tokens

    async def logout(self, refresh_token: str) -> None:
        await self.tokens.revoke(refresh_token)
        await self.uow.commit()

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
