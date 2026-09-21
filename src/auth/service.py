import uuid
from datetime import UTC, datetime
from typing import NoReturn

from src.auth.schemas import RegisterRequest, TokenResponse, UserResponse
from src.auth.utils import (
    create_access_token,
    create_refresh_token,
    decode_token,
    hash_password,
    verify_password,
)
from src.events.base import EventPublisher
from src.events.catalog import (
    RefreshTokenReuseDetected,
    UserLoggedIn,
    UserRegistered,
)
from src.exceptions import ConflictError, ForbiddenError, UnauthorizedError
from src.models.refresh_token import RefreshToken
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
        """Rotate a refresh token, or answer a replay of one already spent.

        Rotation makes a refresh token single-use, and that is what makes theft
        detectable at all: the chain of tokens descending from one login has
        exactly one live link, so a *revoked* token arriving here means two
        parties hold credentials from the same grant. Which of them is the
        account owner is not knowable from the request — the attacker's copy is
        byte-identical to the victim's, and whoever refreshes second is the one
        who looks wrong. RFC 9700 §4.14.2 resolves that by refusing to guess:
        revoke the whole grant and make both parties reauthenticate, which
        costs the owner a login and costs the attacker everything.

        So the replay branch *writes* before it refuses, and the write is the
        point of the branch — see `docs/refresh-token-reuse.md`.
        """
        try:
            payload = decode_token(refresh_token)
        except ValueError as exc:
            raise UnauthorizedError("Invalid refresh token") from exc

        if payload.get("type") != "refresh":
            raise UnauthorizedError("Invalid token type")

        stored = await self.tokens.get_by_token(refresh_token)
        if stored is None:
            raise UnauthorizedError("Refresh token is invalid or revoked")

        if stored.revoked:
            await self._handle_reuse(stored)

        expires_at = stored.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        if expires_at < datetime.now(UTC):
            # Not reuse. An expired token was never redeemed, so nothing about
            # it suggests a second holder; ending the family here would sign a
            # user out of a session they still have simply for coming back
            # after a holiday.
            raise UnauthorizedError("Refresh token has expired")

        # Through the port rather than `stored.revoked = True`, which is what
        # this line used to be. Assigning to the ORM attribute here made the
        # provenance columns the service's job to remember and the store's job
        # to remember too, in two places that could disagree; it also reached
        # past the seam that `RefreshTokenStore` exists to be. The cost is a
        # second lookup of a row already in hand — one hit on a unique index,
        # against a request that is already decoding a JWT, hashing nothing and
        # inserting a row.
        await self.tokens.revoke(refresh_token, reason="rotated")

        user = await self.users.get(stored.user_id)
        if user is None:
            raise UnauthorizedError("User not found")
        if not user.is_active:
            raise ForbiddenError("Account is inactive")

        # The successor inherits the family: a rotation continues a grant, it
        # does not begin one. A fresh family here would make every refresh an
        # amnesty, since the replay of a token could then only ever reach the
        # single-link family it was issued into.
        tokens = await self._issue_tokens(user, family_id=stored.family_id)
        await self.uow.commit()
        return tokens

    async def _handle_reuse(self, stored: RefreshToken) -> NoReturn:
        """End the grant a replayed token belongs to, then refuse the request.

        Always raises. Split out because the ordering inside it is the whole
        mitigation and is easy to quietly get wrong when it is four lines in
        the middle of a longer method.

        **The commit is not optional and it is not the caller's.** `get_db`
        closes its session without committing, so a revocation left uncommitted
        when `UnauthorizedError` propagates is rolled back on the way out — the
        response would be a 401 and the attacker's live token would survive it,
        which is precisely the mitigation failing while every test that only
        asserts the status code stays green. Committing before the raise is why
        `test_reuse_commits_the_revocation_before_refusing` exists.

        The event is published before the commit, as everywhere else in this
        class: the outbox row and the revocation are one transaction, so an
        alert cannot describe a revocation that rolled back, and a process that
        dies after committing cannot lose one that held.
        """
        revoked = await self.tokens.revoke_family(
            stored.family_id, reason="reuse_detected"
        )

        # Everything published here is already on the token row. The account is
        # deliberately not loaded: a subscriber that wants to mail the owner can
        # do it from `user_id`, in its own session, minutes later — and putting
        # a SELECT on the one path an attacker sets the rate of, to fill a field
        # nothing in this method needs, buys an alert a round trip it can have
        # for free on the other side of the outbox.
        await self.events.publish(
            RefreshTokenReuseDetected(
                user_id=str(stored.user_id),
                family_id=str(stored.family_id),
                sessions_revoked=revoked,
            )
        )
        await self.uow.commit()

        # Deliberately the same message a token that was never stored gets. The
        # caller learning *which* of the two it was would be telling an attacker
        # whether a guessed token is real and whether the replay landed; the
        # detail belongs in the event and the logs, where the account owner's
        # side of it can be acted on.
        raise UnauthorizedError("Refresh token is invalid or revoked")

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
        """End the grant the presented token belongs to.

        The family, not the token. Since rotation the two differ in a way that
        mattered: revoking only the row presented left every *ancestor* of it
        revoked already and its successor — if a refresh had raced the logout —
        live, so "log me out" could leave a working session behind. Revoking
        the family is the same work for the ordinary case (one live link) and
        the correct answer for the racing one.

        A token nobody has heard of is not an error. The caller wanted it dead
        and it is dead, which is also why a logout is never treated as reuse
        even when the token is already revoked: a client retrying a logout it
        was not sure landed is the expected shape of this request, not evidence
        of a second holder.
        """
        stored = await self.tokens.get_by_token(refresh_token)
        if stored is not None:
            await self.tokens.revoke_family(stored.family_id, reason="logout")
        await self.uow.commit()

    async def _issue_tokens(
        self, user: User, *, family_id: uuid.UUID | None = None
    ) -> TokenResponse:
        """Issue an access/refresh pair, into `family_id` or into a new grant.

        `None` means "this is a new authorization grant" — a login, an OAuth
        sign-in — and a fresh family is minted for it. Rotation passes the
        family it is continuing. The default is the new-grant case because
        that is what the two callers who do not think about families want, and
        because the failure it produces is the safe one: an unexpected new
        family revokes too little on reuse, where an unexpectedly *shared* one
        would revoke sessions belonging to a different login.
        """
        access_token = create_access_token(str(user.id), user.email, user.role)

        jti = str(uuid.uuid4())
        refresh_token_str, expires_at = create_refresh_token(str(user.id), jti)

        await self.tokens.create(
            token=refresh_token_str,
            user_id=user.id,
            expires_at=expires_at,
            family_id=family_id if family_id is not None else uuid.uuid4(),
        )

        return TokenResponse(
            access_token=access_token,
            refresh_token=refresh_token_str,
        )
