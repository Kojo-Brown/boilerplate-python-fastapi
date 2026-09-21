"""Refresh-token reuse detection and family revocation.

The behaviour under test is one sentence — presenting a refresh token that has
already been spent revokes every live token descended from the same login — and
almost every way of getting it wrong still returns 401, which is why it gets a
file rather than four more cases in `test_auth.py`. A suite that only asserts
the status code passes against a service that detects nothing.

So the assertions here are about the *rows*: which of them are revoked
afterwards, what provenance they carry, and — the one that a plausible
implementation fails — whether the revocation was committed before the
exception that ends the request. These run against the in-memory stores, which
is the right level for it: every fact asserted is a decision `AuthService`
makes, and none of them needs a database to be true.

`TestFamilyRevocationAgainstTheRepository` is the exception. It covers the same
policy through `RefreshTokenRepository`, so the fake and the adapter are held to
one description of what revoking a family means.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from src.auth.service import AuthService
from src.auth.utils import create_access_token, create_refresh_token, decode_token
from src.config import settings
from src.database import Base
from src.events.catalog import RefreshTokenReuseDetected
from src.exceptions import ForbiddenError, UnauthorizedError
from src.models.refresh_token import REVOCATION_REASONS, RefreshToken
from src.models.user import User
from src.repositories.refresh_token import RefreshTokenRepository
from tests.fakes import (
    CollectingPublisher,
    InMemoryRefreshTokenStore,
    InMemoryUserStore,
    RecordingUnitOfWork,
    apply_column_defaults,
)

# --- Helpers ---------------------------------------------------------------


def make_user() -> User:
    user = User(
        id=uuid.uuid4(),
        email="victim@example.com",
        hashed_password="not-a-real-hash",
        is_active=True,
        is_verified=True,
        role="user",
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    apply_column_defaults(user)
    return user


def issue(
    store: InMemoryRefreshTokenStore,
    user: User,
    *,
    family_id: uuid.UUID,
    expires_in: timedelta = timedelta(days=7),
) -> str:
    """Put a live token for `user` in `family_id` into the store, synchronously.

    A plain function rather than a fixture: several tests need two or three
    tokens in specific families, and naming each one at its call site is what
    makes the arrangement of a chain legible.
    """
    token, _ = create_refresh_token(str(user.id), str(uuid.uuid4()))
    stored = RefreshToken(
        token=token,
        user_id=user.id,
        family_id=family_id,
        expires_at=datetime.now(UTC) + expires_in,
    )
    apply_column_defaults(stored)
    store.tokens.append(stored)
    return token


def row(store: InMemoryRefreshTokenStore, token: str) -> RefreshToken:
    return next(t for t in store.tokens if t.token == token)


@pytest.fixture
def user_store() -> InMemoryUserStore:
    return InMemoryUserStore()


@pytest.fixture
def token_store() -> InMemoryRefreshTokenStore:
    return InMemoryRefreshTokenStore()


@pytest.fixture
def uow() -> RecordingUnitOfWork:
    return RecordingUnitOfWork()


@pytest.fixture
def publisher() -> CollectingPublisher:
    return CollectingPublisher()


@pytest.fixture
def service(
    user_store: InMemoryUserStore,
    token_store: InMemoryRefreshTokenStore,
    uow: RecordingUnitOfWork,
    publisher: CollectingPublisher,
) -> AuthService:
    return AuthService(users=user_store, tokens=token_store, uow=uow, events=publisher)


# --- Families ---------------------------------------------------------------


class TestFamilies:
    """Which grant a token belongs to, and how that is decided."""

    async def test_login_opens_a_new_family(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
    ) -> None:
        from src.auth.password import hash_password

        first = await user_store.create(
            email="a@example.com", hashed_password=hash_password("password123")
        )
        second = await user_store.create(
            email="b@example.com", hashed_password=hash_password("password123")
        )

        await service.login(first.email, "password123")
        await service.login(second.email, "password123")
        await service.login(first.email, "password123")

        families = {t.family_id for t in token_store.tokens}
        # Three logins, three grants — including the two by the same person.
        # Signing in again is not a continuation of an earlier session, and
        # sharing a family between them would make a replay on yesterday's
        # session end today's.
        assert len(families) == 3

    async def test_rotation_keeps_the_family(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
    ) -> None:
        user = make_user()
        user_store.users.append(user)
        family = uuid.uuid4()
        first = issue(token_store, user, family_id=family)

        second = (await service.refresh(first)).refresh_token
        third = (await service.refresh(second)).refresh_token

        assert {t.family_id for t in token_store.tokens} == {family}
        assert len(token_store.tokens) == 3
        # And the chain really did move: each link is a distinct credential.
        assert len({first, second, third}) == 3

    async def test_rotation_records_why_the_spent_token_died(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
    ) -> None:
        user = make_user()
        user_store.users.append(user)
        first = issue(token_store, user, family_id=uuid.uuid4())

        second = (await service.refresh(first)).refresh_token

        spent = row(token_store, first)
        assert spent.revoked is True
        assert spent.revoked_reason == "rotated"
        assert spent.revoked_at is not None
        # The successor is live and carries no provenance yet.
        live = row(token_store, second)
        assert live.revoked is False
        assert live.revoked_reason is None
        assert live.revoked_at is None

    async def test_every_reason_fits_the_column(self) -> None:
        """`revoked_reason` is `String(32)`. A reason longer than that is
        truncated by Postgres or rejected, and either way the provenance an
        incident is read from is wrong in a way no other test would catch."""
        assert all(len(reason) <= 32 for reason in REVOCATION_REASONS)


# --- Reuse ------------------------------------------------------------------


class TestReuseDetection:
    """What happens when a token that was already spent comes back."""

    async def test_replaying_a_rotated_token_revokes_the_live_one(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
    ) -> None:
        """The whole point, in the shape it actually happens in.

        A token is stolen; both parties hold it. The attacker refreshes first
        and gets a working session. The victim then refreshes with the copy
        they still have — now a spent token — and *that* request is what ends
        the attacker's session.
        """
        user = make_user()
        user_store.users.append(user)
        stolen = issue(token_store, user, family_id=uuid.uuid4())

        attackers = (await service.refresh(stolen)).refresh_token
        assert row(token_store, attackers).revoked is False

        with pytest.raises(UnauthorizedError):
            await service.refresh(stolen)

        attacker_row = row(token_store, attackers)
        assert attacker_row.revoked is True
        assert attacker_row.revoked_reason == "reuse_detected"

    async def test_the_attackers_new_token_stops_working(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
    ) -> None:
        """Revoked in the table is only half of it; the credential has to stop
        buying anything. Asserted by using it, because a flag nobody reads is
        exactly the bug this file exists to catch."""
        user = make_user()
        user_store.users.append(user)
        stolen = issue(token_store, user, family_id=uuid.uuid4())
        attackers = (await service.refresh(stolen)).refresh_token

        with pytest.raises(UnauthorizedError):
            await service.refresh(stolen)

        with pytest.raises(UnauthorizedError):
            await service.refresh(attackers)

    async def test_revocation_reaches_the_whole_family_not_just_the_successor(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
    ) -> None:
        """A chain can have more than one live link — two refreshes racing on
        one grant leave two. Revoking "the successor" would leave the other
        alive, so the unit is the family."""
        user = make_user()
        user_store.users.append(user)
        family = uuid.uuid4()
        replayed = issue(token_store, user, family_id=family)
        row(token_store, replayed).revoked = True
        sibling_a = issue(token_store, user, family_id=family)
        sibling_b = issue(token_store, user, family_id=family)

        with pytest.raises(UnauthorizedError):
            await service.refresh(replayed)

        assert row(token_store, sibling_a).revoked is True
        assert row(token_store, sibling_b).revoked is True

    async def test_other_grants_survive(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
    ) -> None:
        """The blast radius is the grant, not the account. A theft on a laptop
        should not sign the same person out on their phone — that is a
        different login, with a different family, and nothing about the replay
        implicates it."""
        user = make_user()
        user_store.users.append(user)
        compromised = uuid.uuid4()
        replayed = issue(token_store, user, family_id=compromised)
        row(token_store, replayed).revoked = True
        other_device = issue(token_store, user, family_id=uuid.uuid4())

        with pytest.raises(UnauthorizedError):
            await service.refresh(replayed)

        assert row(token_store, other_device).revoked is False

    async def test_the_replayed_token_keeps_its_original_provenance(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
    ) -> None:
        """The row that was replayed was already revoked, as `"rotated"`, and
        stays that way. Restamping it would erase the ordinary rotation that
        the replay is only recognisable *against*, which is the one fact an
        incident review needs from it."""
        user = make_user()
        user_store.users.append(user)
        stolen = issue(token_store, user, family_id=uuid.uuid4())
        await service.refresh(stolen)

        with pytest.raises(UnauthorizedError):
            await service.refresh(stolen)

        assert row(token_store, stolen).revoked_reason == "rotated"

    async def test_reuse_commits_the_revocation_before_refusing(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
        uow: RecordingUnitOfWork,
    ) -> None:
        """The failure mode this test exists for is silent and total.

        `get_db` closes its session without committing, so a revocation still
        pending when `UnauthorizedError` propagates is rolled back on the way
        out of the request. The response is a 401 either way and the in-memory
        store shows the rows revoked either way — only the commit distinguishes
        a mitigation that happened from one that was discarded on the doorstep.
        """
        user = make_user()
        user_store.users.append(user)
        stolen = issue(token_store, user, family_id=uuid.uuid4())
        await service.refresh(stolen)
        commits_before = uow.commits

        with pytest.raises(UnauthorizedError):
            await service.refresh(stolen)

        assert uow.commits == commits_before + 1

    async def test_reuse_publishes_before_it_commits(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
        uow: RecordingUnitOfWork,
        publisher: CollectingPublisher,
    ) -> None:
        """Publishing writes an outbox row, so it belongs in the transaction it
        describes. After the commit it would land in a fresh transaction that
        nothing commits, and the alert would vanish with no error raised."""
        user = make_user()
        user_store.users.append(user)
        family = uuid.uuid4()
        replayed = issue(token_store, user, family_id=family)
        row(token_store, replayed).revoked = True
        # The successor the replayed token was rotated into: one live link, as
        # a real chain has when a spent copy of it comes back.
        issue(token_store, user, family_id=family)
        uow.calls.clear()

        with pytest.raises(UnauthorizedError):
            await service.refresh(replayed)

        assert len(publisher.events) == 1
        event = publisher.events[0]
        assert isinstance(event, RefreshTokenReuseDetected)
        assert event.family_id == str(family)
        assert event.user_id == str(user.id)
        # One live token in the family when the replay arrived.
        assert event.sessions_revoked == 1
        # The publish happened while the transaction was still open: the only
        # uow call it could have raced is the commit, and that came after.
        assert uow.calls == ["commit"]

    async def test_a_replay_into_an_already_dead_family_reports_zero(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
        publisher: CollectingPublisher,
    ) -> None:
        """Every member already revoked — a second replay, or one after a
        logout. Still an incident worth publishing, and the count says plainly
        that nothing was cut off this time."""
        user = make_user()
        user_store.users.append(user)
        stolen = issue(token_store, user, family_id=uuid.uuid4())
        await service.refresh(stolen)
        with pytest.raises(UnauthorizedError):
            await service.refresh(stolen)
        publisher.events.clear()

        with pytest.raises(UnauthorizedError):
            await service.refresh(stolen)

        assert len(publisher.events) == 1
        event = publisher.events[0]
        assert isinstance(event, RefreshTokenReuseDetected)
        assert event.sessions_revoked == 0

    async def test_the_caller_cannot_tell_reuse_from_a_token_that_never_existed(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
    ) -> None:
        """Answering the two differently would tell an attacker whether a
        guessed token is real, and whether their replay landed."""
        user = make_user()
        user_store.users.append(user)
        stolen = issue(token_store, user, family_id=uuid.uuid4())
        await service.refresh(stolen)
        never_issued, _ = create_refresh_token(str(user.id), str(uuid.uuid4()))

        with pytest.raises(UnauthorizedError) as reuse:
            await service.refresh(stolen)
        with pytest.raises(UnauthorizedError) as unknown:
            await service.refresh(never_issued)

        assert str(reuse.value) == str(unknown.value)
        assert reuse.value.status_code == unknown.value.status_code == 401


# --- What is not reuse ------------------------------------------------------


class TestNotReuse:
    """Rejections that must not revoke anything.

    A detector that fires on every 401 is not a detector — it is a way for
    anyone holding one expired token to sign a user out of a live session.
    """

    async def test_an_expired_token_is_refused_without_revoking_the_family(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
        publisher: CollectingPublisher,
    ) -> None:
        user = make_user()
        user_store.users.append(user)
        family = uuid.uuid4()
        expired = issue(token_store, user, family_id=family, expires_in=-timedelta(1))
        sibling = issue(token_store, user, family_id=family)

        with pytest.raises(UnauthorizedError, match="expired"):
            await service.refresh(expired)

        assert row(token_store, sibling).revoked is False
        assert publisher.events == []

    async def test_a_token_that_was_never_issued_revokes_nothing(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
        uow: RecordingUnitOfWork,
        publisher: CollectingPublisher,
    ) -> None:
        user = make_user()
        user_store.users.append(user)
        live = issue(token_store, user, family_id=uuid.uuid4())
        forged, _ = create_refresh_token(str(user.id), str(uuid.uuid4()))

        with pytest.raises(UnauthorizedError):
            await service.refresh(forged)

        assert row(token_store, live).revoked is False
        assert publisher.events == []
        # Nothing was written, so nothing should have been committed.
        assert uow.commits == 0

    async def test_an_access_token_presented_as_a_refresh_token_revokes_nothing(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
        publisher: CollectingPublisher,
    ) -> None:
        """Rejected on the `type` claim, before any lookup. A signed token of
        the wrong kind is a client bug far more often than an attack, and it
        names no family to revoke in any case."""
        user = make_user()
        user_store.users.append(user)
        live = issue(token_store, user, family_id=uuid.uuid4())
        access = create_access_token(str(user.id), user.email, user.role)

        with pytest.raises(UnauthorizedError, match="Invalid token type"):
            await service.refresh(access)

        assert row(token_store, live).revoked is False
        assert publisher.events == []

    async def test_a_naive_expiry_is_read_as_utc(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
        publisher: CollectingPublisher,
    ) -> None:
        """A row whose `expires_at` came back without a timezone — a database
        or driver configured to hand back naive datetimes — is read as UTC
        rather than compared against an aware `now`, which would raise a
        `TypeError` and turn a routine expiry into a 500. Still not reuse.
        """
        user = make_user()
        user_store.users.append(user)
        family = uuid.uuid4()
        token = issue(token_store, user, family_id=family)
        sibling = issue(token_store, user, family_id=family)
        stored = row(token_store, token)
        stored.expires_at = (datetime.now(UTC) - timedelta(days=1)).replace(tzinfo=None)

        with pytest.raises(UnauthorizedError, match="expired"):
            await service.refresh(token)

        assert row(token_store, sibling).revoked is False
        assert publisher.events == []

    async def test_a_token_whose_account_is_gone_revokes_nothing_further(
        self,
        service: AuthService,
        token_store: InMemoryRefreshTokenStore,
        uow: RecordingUnitOfWork,
        publisher: CollectingPublisher,
    ) -> None:
        """The account was deleted while the token was still live. Refused, and
        the token is left revoked by the rotation that had already happened —
        there is nothing here to detect and nobody to notify."""
        user = make_user()  # deliberately not added to the user store
        token = issue(token_store, user, family_id=uuid.uuid4())

        with pytest.raises(UnauthorizedError, match="User not found"):
            await service.refresh(token)

        assert publisher.events == []
        # The rejection is not a commit: nothing this request did should
        # outlive it, least of all a rotation whose successor was never issued.
        assert uow.commits == 0

    async def test_a_deactivated_account_is_403_not_a_detected_reuse(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
        publisher: CollectingPublisher,
    ) -> None:
        """Authentication succeeded and access is refused, which is the same
        distinction `login` draws. An administrator switching an account off is
        not evidence of a second holder."""
        user = make_user()
        user.is_active = False
        user_store.users.append(user)
        token = issue(token_store, user, family_id=uuid.uuid4())

        with pytest.raises(ForbiddenError, match="inactive") as exc_info:
            await service.refresh(token)

        assert exc_info.value.status_code == 403
        assert publisher.events == []


# --- Logout -----------------------------------------------------------------


class TestLogout:
    """Logout ends the grant, and is never treated as an attack."""

    async def test_logout_revokes_the_family(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
    ) -> None:
        """Including a successor issued by a refresh that raced the logout,
        which is the case revoking only the presented row got wrong: "log me
        out" left a working session behind."""
        user = make_user()
        user_store.users.append(user)
        family = uuid.uuid4()
        presented = issue(token_store, user, family_id=family)
        raced = issue(token_store, user, family_id=family)

        await service.logout(presented)

        assert row(token_store, presented).revoked is True
        assert row(token_store, raced).revoked is True
        assert row(token_store, presented).revoked_reason == "logout"

    async def test_logout_leaves_other_grants_alone(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
    ) -> None:
        user = make_user()
        user_store.users.append(user)
        here = issue(token_store, user, family_id=uuid.uuid4())
        elsewhere = issue(token_store, user, family_id=uuid.uuid4())

        await service.logout(here)

        assert row(token_store, elsewhere).revoked is False

    async def test_logging_out_twice_is_not_reuse(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
        publisher: CollectingPublisher,
    ) -> None:
        """A client retrying a logout it was not sure landed is the expected
        shape of this request. Publishing an incident for it would bury the
        real ones."""
        user = make_user()
        user_store.users.append(user)
        token = issue(token_store, user, family_id=uuid.uuid4())

        await service.logout(token)
        await service.logout(token)

        assert publisher.events == []
        assert row(token_store, token).revoked_reason == "logout"

    async def test_logout_of_an_unknown_token_is_not_an_error(
        self, service: AuthService
    ) -> None:
        unknown, _ = create_refresh_token(str(uuid.uuid4()), str(uuid.uuid4()))

        await service.logout(unknown)

    async def test_a_token_revoked_by_logout_is_reuse_when_refreshed(
        self,
        service: AuthService,
        user_store: InMemoryUserStore,
        token_store: InMemoryRefreshTokenStore,
        publisher: CollectingPublisher,
    ) -> None:
        """The asymmetry is deliberate. Asking to *end* a session with a token
        already ended is a retry; asking to *extend* one with it is a spent
        credential being redeemed, which is the definition this file works to."""
        user = make_user()
        user_store.users.append(user)
        token = issue(token_store, user, family_id=uuid.uuid4())
        await service.logout(token)
        publisher.events.clear()

        with pytest.raises(UnauthorizedError):
            await service.refresh(token)

        assert len(publisher.events) == 1
        assert isinstance(publisher.events[0], RefreshTokenReuseDetected)


# --- The adapter ------------------------------------------------------------


class TestFamilyRevocationAgainstTheRepository:
    """`RefreshTokenRepository.revoke_family` against a real Postgres.

    The policy above is asserted against the fake, because that is where the
    decisions are. This pins the one thing a fake cannot vouch for: that the
    WHERE clause selects the rows the policy assumes it does — the live members
    of one family, and nothing else.

    Skipped when `DATABASE_URL` names nothing reachable, following
    `test_optimistic_concurrency_db.py`; CI always has a Postgres service, so
    it runs on every pull request. That also makes it a check on migration
    0006: if the migration forgot `family_id`, the table already exists,
    `create_all` skips it, and this fails on an undefined column.
    """

    @pytest.fixture
    async def engine(self) -> AsyncGenerator[AsyncEngine]:
        engine = create_async_engine(settings.DATABASE_URL, poolclass=NullPool)
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        except (OSError, SQLAlchemyError) as exc:
            await engine.dispose()
            pytest.skip(f"no usable Postgres at DATABASE_URL: {exc}")

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        yield engine
        await engine.dispose()

    @pytest.fixture
    async def session(self, engine: AsyncEngine) -> AsyncGenerator[AsyncSession]:
        """One session, rolled back at the end.

        Nothing here commits, so the rows never outlive the test and no cleanup
        can be forgotten. `revoke_family` flushes rather than commits, which is
        exactly the property that makes this safe — and is also why the caller
        owns the commit, as `AuthService._handle_reuse` does.
        """
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            yield session
            await session.rollback()

    async def test_it_revokes_only_live_members_of_the_family(
        self, session: AsyncSession
    ) -> None:
        user = User(
            email=f"reuse-{uuid.uuid4()}@example.test",
            hashed_password="not-a-real-hash",
        )
        session.add(user)
        await session.flush()

        family = uuid.uuid4()
        expires = datetime.now(UTC) + timedelta(days=7)
        live = RefreshToken(
            token=f"mock-live-{uuid.uuid4()}",
            user_id=user.id,
            family_id=family,
            expires_at=expires,
        )
        already_rotated = RefreshToken(
            token=f"mock-rotated-{uuid.uuid4()}",
            user_id=user.id,
            family_id=family,
            expires_at=expires,
            revoked=True,
            revoked_reason="rotated",
        )
        other_family = RefreshToken(
            token=f"mock-other-{uuid.uuid4()}",
            user_id=user.id,
            family_id=uuid.uuid4(),
            expires_at=expires,
        )
        session.add_all([live, already_rotated, other_family])
        await session.flush()

        count = await RefreshTokenRepository(session).revoke_family(
            family, reason="reuse_detected"
        )

        assert count == 1
        assert live.revoked is True
        assert live.revoked_reason == "reuse_detected"
        assert live.revoked_at is not None
        # Untouched: its provenance is the record the replay was detected
        # against, and the other family was never implicated.
        assert already_rotated.revoked_reason == "rotated"
        assert other_family.revoked is False

    async def test_an_expired_member_is_still_revoked(
        self, session: AsyncSession
    ) -> None:
        """Expiry is not revocation. An unexpired *clock* is not what makes a
        token dangerous — a stolen one that has hours left is — and the sweep
        in `delete_expired` is the only thing that should care about the date.
        """
        user = User(
            email=f"reuse-{uuid.uuid4()}@example.test",
            hashed_password="not-a-real-hash",
        )
        session.add(user)
        await session.flush()

        family = uuid.uuid4()
        expired = RefreshToken(
            token=f"mock-expired-{uuid.uuid4()}",
            user_id=user.id,
            family_id=family,
            expires_at=datetime.now(UTC) - timedelta(days=1),
        )
        session.add(expired)
        await session.flush()

        count = await RefreshTokenRepository(session).revoke_family(
            family, reason="reuse_detected"
        )

        assert count == 1
        assert expired.revoked is True


# --- The token itself -------------------------------------------------------


async def test_the_family_is_not_carried_in_the_jwt() -> None:
    """`family_id` is server state, and it stays that way.

    A grant id in the token would be a claim a client can read and an attacker
    can correlate across stolen tokens, in exchange for nothing: the server
    looks the row up by token string on every refresh anyway, and the row is
    where the family has to be for the revocation to be authoritative.
    """
    token, _ = create_refresh_token(str(uuid.uuid4()), str(uuid.uuid4()))

    assert set(decode_token(token)) == {"sub", "jti", "type", "exp"}
