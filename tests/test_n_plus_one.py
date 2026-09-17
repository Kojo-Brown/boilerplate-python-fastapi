"""The N+1, and each of the three loader strategies, measured against Postgres.

Every claim in `docs/n-plus-one.md` is made here first. A tuning guide is the
kind of document that is written once, is right on the day, and is quietly
wrong two SQLAlchemy releases later — so the numbers in it are not prose, they
are assertions, and the guide cites this file.

The shape to copy is `_statements_at_each_size`: the same block is run at two
row counts and the statement counts compared. Asserting a literal count pins an
implementation detail and fails the next time somebody adds a legitimate query;
asserting that the count does not move when the data does is the definition of
"not an N+1", and it is the assertion that would actually have caught one.

Skipped when `DATABASE_URL` names nothing reachable. CI always has a Postgres
service, so all of this is measured on every pull request. Following
`test_locking_db.py`, reachability and schema are separate steps, so a missing
database skips and a broken one fails.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import delete, func, inspect, select, text
from sqlalchemy.exc import InvalidRequestError, MissingGreenlet, SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, contains_eager, joinedload, selectinload
from sqlalchemy.pool import NullPool

from src.config import settings
from src.database import Base
from src.models.refresh_token import RefreshToken
from src.models.user import User
from src.repositories.refresh_token import RefreshTokenRepository
from src.repositories.user import UserRepository
from tests.querycount import (
    NPlusOneDetected,
    assert_no_lazy_loads,
    assert_no_n_plus_one,
    capture_queries,
)

#: The two row counts every invariance test runs at. They differ by more than
#: one so that an off-by-one in a batching scheme cannot make an N+1 look flat.
SMALL = 2
LARGE = 6

#: Children per parent. More than one, so that a joined load really does
#: multiply rows rather than merely appearing to.
TOKENS_PER_USER = 3


@dataclass(frozen=True, slots=True)
class Seed:
    """Rows created for one test, and the filter that selects exactly them."""

    marker: str
    user_ids: list[uuid.UUID]

    @property
    def email_like(self) -> str:
        return f"nplus1-{self.marker}-%"


@pytest.fixture
async def engine() -> AsyncGenerator[AsyncEngine]:
    """An engine on `DATABASE_URL`, or a skip if there is nothing there.

    `NullPool` so that each test's statements are the only ones on the
    connection: a pooled connection carrying another test's `BEGIN` would show
    up in the log as a statement this block did not send.
    """
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
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
async def seed(
    sessions: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[Callable[..., Awaitable[Seed]]]:
    """Create `users` users with `tokens` tokens each; remove them afterwards.

    Returns a factory rather than a fixed dataset because every test here runs
    the same block at two sizes. Each call gets its own marker, so two seeds in
    one test cannot see each other's rows and a rerun after an interrupted test
    cannot trip the unique index on `users.email`.
    """
    created: list[str] = []

    async def _seed(*, users: int, tokens: int = TOKENS_PER_USER) -> Seed:
        marker = uuid.uuid4().hex[:8]
        created.append(marker)
        expires = datetime.now(UTC) + timedelta(days=1)
        async with sessions() as session:
            rows = [
                User(
                    email=f"nplus1-{marker}-{index}@example.test",
                    hashed_password="not-a-real-hash",
                )
                for index in range(users)
            ]
            session.add_all(rows)
            await session.flush()
            for user in rows:
                session.add_all(
                    RefreshToken(
                        token=f"nplus1-{marker}-{user.id}-{n}",
                        user_id=user.id,
                        expires_at=expires,
                    )
                    for n in range(tokens)
                )
            await session.commit()
            return Seed(marker=marker, user_ids=[user.id for user in rows])

    yield _seed

    async with sessions() as session:
        for marker in created:
            # The tokens go with them: the foreign key is ON DELETE CASCADE.
            await session.execute(
                delete(User).where(User.email.like(f"nplus1-{marker}-%"))
            )
        await session.commit()


def _read_every_collection(_session: Session, users: list[User]) -> None:
    """Touch each user's tokens from inside the greenlet `run_sync` provides.

    A plain function rather than a lambda so that the thing being demonstrated
    — an ordinary synchronous relationship traversal — is written the way the
    code it stands in for would be.
    """
    for user in users:
        len(user.refresh_tokens)


async def _statements_at_each_size(
    sessions: async_sessionmaker[AsyncSession],
    seed: Callable[..., Awaitable[Seed]],
    block: Callable[[AsyncSession, Seed], Awaitable[None]],
) -> tuple[int, int]:
    """Run `block` at `SMALL` and at `LARGE` users; return the two counts.

    Each size gets a fresh session, so nothing is served from an identity map
    the previous size populated — which would hide the very loads being
    counted.
    """
    counts: list[int] = []
    for size in (SMALL, LARGE):
        data = await seed(users=size)
        async with sessions() as session:
            with capture_queries(session) as log:
                await block(session, data)
            assert_no_n_plus_one(log)
            counts.append(len(log))
    return counts[0], counts[1]


# ---------------------------------------------------------------------------
# The N+1 itself: what it looks like here, and what hides it
# ---------------------------------------------------------------------------


class TestTheFailure:
    async def test_a_lazy_load_under_asyncio_raises_instead_of_querying(
        self,
        sessions: async_sessionmaker[AsyncSession],
        seed: Callable[..., Awaitable[Seed]],
    ) -> None:
        """The textbook N+1 cannot happen on the ordinary async path.

        Touching an unloaded relationship needs I/O, and an attribute access is
        not a place an `await` can go, so SQLAlchemy raises `MissingGreenlet`
        rather than quietly issuing the query. This is worth pinning both
        because it is the reason the async stack is safer here than the sync
        one, and because the next test shows how thin that protection is.

        Note what the log records: the load *was* attempted — the ORM built the
        statement and the recorder saw it — and only then did the driver call
        fail. A lazy load is therefore visible to `assert_no_lazy_loads` even
        when it never reaches the server.
        """
        data = await seed(users=SMALL)
        async with sessions() as session:
            result = await session.execute(
                select(User).where(User.email.like(data.email_like))
            )
            users = list(result.scalars().all())

            with capture_queries(session) as log:
                with pytest.raises(MissingGreenlet):
                    for user in users:
                        _ = user.refresh_tokens

        assert len(log.lazy_loads) == 1
        assert log.lazy_loads[0].path == "User.refresh_tokens"
        with pytest.raises(NPlusOneDetected, match="User.refresh_tokens"):
            assert_no_lazy_loads(log)

    async def test_run_sync_restores_the_greenlet_and_with_it_the_n_plus_one(
        self,
        sessions: async_sessionmaker[AsyncSession],
        seed: Callable[..., Awaitable[Seed]],
    ) -> None:
        """Inside `run_sync`, the lazy load works — one query per parent.

        `AsyncSession.run_sync` and `AsyncAttrs.awaitable_attrs` both exist to
        let ordinary synchronous ORM code run under asyncio, and both hand back
        the N+1 the previous test said could not happen. That is not an
        argument against them; it is the reason a detector is worth having in a
        codebase that has them available.
        """
        counts: list[int] = []
        for size in (SMALL, LARGE):
            data = await seed(users=size)
            async with sessions() as session:
                result = await session.execute(
                    select(User).where(User.email.like(data.email_like))
                )
                users = list(result.scalars().all())

                with capture_queries(session) as log:
                    await session.run_sync(_read_every_collection, users)

            assert len(log.lazy_loads) == size
            counts.append(len(log))

        # The definition, measured: four more users, four more statements.
        assert counts[1] - counts[0] == LARGE - SMALL

    async def test_a_query_in_a_loop_is_an_n_plus_one_the_orm_never_hears_about(
        self,
        sessions: async_sessionmaker[AsyncSession],
        seed: Callable[..., Awaitable[Seed]],
    ) -> None:
        """The version no loader option can fix, and why both checks exist.

        Every iteration is an ordinary top-level query, so there is no
        relationship load to report and `assert_no_lazy_loads` passes. What
        gives it away is one SQL shape sent N times, which is the other half of
        `assert_no_n_plus_one` — and the fix is not a loader strategy but a
        single `WHERE user_id IN (...)`.
        """
        data = await seed(users=LARGE)
        async with sessions() as session:
            result = await session.execute(
                select(User).where(User.email.like(data.email_like))
            )
            users = list(result.scalars().all())

            with capture_queries(session) as log:
                for user in users:
                    await session.execute(
                        select(RefreshToken).where(RefreshToken.user_id == user.id)
                    )

        assert log.lazy_loads == []
        assert_no_lazy_loads(log)
        assert len(log) == LARGE
        with pytest.raises(NPlusOneDetected, match="more than 1x"):
            assert_no_n_plus_one(log)

        # The fix, measured against the same seed.
        async with sessions() as session:
            with capture_queries(session) as log:
                await session.execute(
                    select(RefreshToken).where(RefreshToken.user_id.in_(data.user_ids))
                )
        assert len(log) == 1
        assert_no_n_plus_one(log)


# ---------------------------------------------------------------------------
# The three strategies
# ---------------------------------------------------------------------------


class TestSelectinload:
    async def test_is_two_statements_whatever_the_row_count(
        self,
        sessions: async_sessionmaker[AsyncSession],
        seed: Callable[..., Awaitable[Seed]],
    ) -> None:
        """One query for the parents, one `IN` query for all their children.

        The second statement is a relationship load and the recorder says so —
        but not a *lazy* one, which is exactly the distinction that lets a
        gate forbid lazy loads without also forbidding the fix for them.
        """

        async def block(session: AsyncSession, data: Seed) -> None:
            result = await session.execute(
                select(User)
                .options(selectinload(User.refresh_tokens))
                .where(User.email.like(data.email_like))
            )
            for user in result.scalars().all():
                assert len(user.refresh_tokens) == TOKENS_PER_USER

        small, large = await _statements_at_each_size(sessions, seed, block)
        assert small == large == 2

    async def test_reports_as_an_eager_relationship_load(
        self,
        sessions: async_sessionmaker[AsyncSession],
        seed: Callable[..., Awaitable[Seed]],
    ) -> None:
        data = await seed(users=SMALL)
        async with sessions() as session:
            with capture_queries(session) as log:
                result = await session.execute(
                    select(User)
                    .options(selectinload(User.refresh_tokens))
                    .where(User.email.like(data.email_like))
                )
                result.scalars().all()

        assert [load.path for load in log.eager_loads] == ["User.refresh_tokens"]
        assert log.lazy_loads == []


class TestJoinedload:
    async def test_is_one_statement_whatever_the_row_count(
        self,
        sessions: async_sessionmaker[AsyncSession],
        seed: Callable[..., Awaitable[Seed]],
    ) -> None:
        """A LEFT OUTER JOIN, so there is no second statement to report.

        It follows that a joined load appears in the log as *no* relationship
        load at all — there is nothing extra to attribute. `assert_no_lazy_loads`
        passing is therefore not evidence that a collection was eager-loaded;
        only the statement count distinguishes a joined load from a collection
        nobody touched.
        """

        async def block(session: AsyncSession, data: Seed) -> None:
            result = await session.execute(
                select(User)
                .options(joinedload(User.refresh_tokens))
                .where(User.email.like(data.email_like))
            )
            for user in result.unique().scalars().all():
                assert len(user.refresh_tokens) == TOKENS_PER_USER

        small, large = await _statements_at_each_size(sessions, seed, block)
        assert small == large == 1

    async def test_a_collection_result_must_be_uniqued_by_hand(
        self,
        sessions: async_sessionmaker[AsyncSession],
        seed: Callable[..., Awaitable[Seed]],
    ) -> None:
        """Forgetting `.unique()` is an error rather than duplicate parents.

        Worth pinning because it is the one difference in *calling convention*
        between the two collection strategies: swapping `selectinload` for
        `joinedload` to save a round trip breaks the call site, and it breaks
        it loudly, which is the good outcome.
        """
        data = await seed(users=SMALL)
        async with sessions() as session:
            result = await session.execute(
                select(User)
                .options(joinedload(User.refresh_tokens))
                .where(User.email.like(data.email_like))
            )
            with pytest.raises(InvalidRequestError, match="unique"):
                result.scalars().all()

    async def test_the_server_returns_a_row_per_child(
        self,
        sessions: async_sessionmaker[AsyncSession],
        seed: Callable[..., Awaitable[Seed]],
    ) -> None:
        """The cost the round-trip count does not show.

        The ORM de-duplicates the parents in memory, so the caller sees the
        right objects and nothing looks wrong. The wire does not de-duplicate:
        the join produces one row per child, every parent column repeated in
        each — which is why `selectinload` is the default advice for a
        collection even though it costs one more statement.

        The join is asserted to be the one `joinedload` emits rather than
        assumed: the statement recorded above is checked for it, and then its
        cardinality is measured directly.
        """
        data = await seed(users=SMALL)
        async with sessions() as session:
            with capture_queries(session) as log:
                result = await session.execute(
                    select(User)
                    .options(joinedload(User.refresh_tokens))
                    .where(User.email.like(data.email_like))
                )
                users = result.unique().scalars().all()

            assert len(users) == SMALL
            assert "LEFT OUTER JOIN refresh_tokens" in log.statements[0].shape

            joined_rows = await session.scalar(
                select(func.count())
                .select_from(User)
                .join(RefreshToken, RefreshToken.user_id == User.id)
                .where(User.email.like(data.email_like))
            )

        assert joined_rows == SMALL * TOKENS_PER_USER

    async def test_limit_still_counts_parents_because_sqlalchemy_wraps_it(
        self,
        sessions: async_sessionmaker[AsyncSession],
        seed: Callable[..., Awaitable[Seed]],
    ) -> None:
        """The famous LIMIT trap is a raw-SQL trap, not a SQLAlchemy one.

        In hand-written SQL, `LIMIT 2` over a join counts *joined* rows and
        returns one user with two of their tokens. SQLAlchemy applies the limit
        to a subquery of the parent table and joins the children to that, so
        the limit means what the caller meant. Pinned because the folklore says
        otherwise, and because a future change here would silently return the
        wrong page rather than fail.
        """
        data = await seed(users=LARGE)
        async with sessions() as session:
            with capture_queries(session) as log:
                result = await session.execute(
                    select(User)
                    .options(joinedload(User.refresh_tokens))
                    .where(User.email.like(data.email_like))
                    .order_by(User.email)
                    .limit(SMALL)
                )
                users = result.unique().scalars().all()

        assert len(users) == SMALL
        assert all(len(user.refresh_tokens) == TOKENS_PER_USER for user in users)
        assert len(log) == 1


class TestContainsEager:
    async def test_a_filtered_join_leaves_the_collection_incomplete(
        self,
        sessions: async_sessionmaker[AsyncSession],
        seed: Callable[..., Awaitable[Seed]],
    ) -> None:
        """One statement, and a collection that disagrees with the database.

        `contains_eager` populates the relationship from rows the caller's own
        join produced, so a `WHERE` on the child table filters the collection
        as well as the parents. That is the point of it — "users with their
        unrevoked tokens" in one statement — and it is also its trap: the
        object now says a user has one token when the table says three, and
        anything that iterates it and writes back is working from a lie. Use it
        for a read model, never for something about to be mutated.
        """
        data = await seed(users=SMALL)
        async with sessions() as session:
            revoked = await session.execute(
                select(RefreshToken).where(RefreshToken.user_id == data.user_ids[0])
            )
            for token in list(revoked.scalars().all())[1:]:
                token.revoked = True
            await session.commit()

        async with sessions() as session:
            with capture_queries(session) as log:
                result = await session.execute(
                    select(User)
                    .join(User.refresh_tokens)
                    .options(contains_eager(User.refresh_tokens))
                    .where(
                        User.email.like(data.email_like),
                        RefreshToken.revoked.is_(False),
                    )
                    .order_by(User.email)
                )
                users = result.unique().scalars().all()

        assert len(log) == 1
        assert_no_n_plus_one(log)
        # The first user has one unrevoked token of three; the collection shows
        # only that one, and nothing about the object says it is a partial view.
        assert [len(user.refresh_tokens) for user in users] == [1, TOKENS_PER_USER]


# ---------------------------------------------------------------------------
# Gates over what this codebase actually does
# ---------------------------------------------------------------------------


class TestRepositoriesAreFlat:
    """The repositories, measured at two row counts.

    These are the tests that would fail on a regression. Everything above
    documents the strategies; this section asserts that the code shipped here
    uses them — or, as it happens, needs none of them, because nothing in
    `src/` traverses a relationship. That is worth a gate rather than a
    comment: the first method that adds a traversal will fail here rather than
    in production.
    """

    async def test_list_active_is_one_statement(
        self,
        sessions: async_sessionmaker[AsyncSession],
        seed: Callable[..., Awaitable[Seed]],
    ) -> None:
        async def block(session: AsyncSession, data: Seed) -> None:
            await UserRepository(session).list_active(limit=LARGE * 2)

        small, large = await _statements_at_each_size(sessions, seed, block)
        assert small == large == 1

    async def test_stream_export_is_one_cursor_not_one_query_per_batch(
        self,
        sessions: async_sessionmaker[AsyncSession],
        seed: Callable[..., Awaitable[Seed]],
    ) -> None:
        """`yield_per` is a server-side cursor, so the batches are fetches.

        A batch size below the row count is the interesting case: if the export
        re-queried per batch it would be an N+1 in the paging, which is the
        classic way a "streaming" endpoint turns out not to be one.
        """

        async def block(session: AsyncSession, data: Seed) -> None:
            repo = UserRepository(session)
            async for _record in repo.stream_export(batch_size=1, active_only=True):
                pass

        small, large = await _statements_at_each_size(sessions, seed, block)
        assert small == large == 1

    async def test_revoke_all_for_user_batches_its_updates(
        self,
        sessions: async_sessionmaker[AsyncSession],
        seed: Callable[..., Awaitable[Seed]],
    ) -> None:
        """Two statements however many tokens there are — a SELECT and a batch.

        This one reads like an N+1 in the source: it loads the tokens and sets
        an attribute on each, which is N updates. The unit of work collapses
        them into a single `executemany`, so the round-trip count does not grow
        — and the parameter-set count does, which is why the recorder keeps the
        two numbers apart rather than reporting "how busy was the database" as
        one figure.
        """
        counts: list[int] = []
        for tokens in (TOKENS_PER_USER, TOKENS_PER_USER * 3):
            data = await seed(users=1, tokens=tokens)
            async with sessions() as session:
                with capture_queries(session) as log:
                    revoked = await RefreshTokenRepository(session).revoke_all_for_user(
                        data.user_ids[0]
                    )
                    await session.commit()

            assert revoked == tokens
            assert_no_n_plus_one(log)
            assert log.parameter_sets >= tokens
            counts.append(len(log))

        assert counts[0] == counts[1] == 2


class TestMapperConfiguration:
    async def test_no_collection_is_eager_loaded_at_the_mapper(
        self, engine: AsyncEngine
    ) -> None:
        """`lazy="joined"` on a collection is a decision no query can undo.

        Configured on the relationship, it joins the child table into *every*
        query for that parent — including the ones that never look at the
        collection, and including a count — and multiplies the rows returned by
        all of them. A query that wants the children can ask for them with an
        option; a query that does not cannot opt out nearly as easily, which is
        why the default belongs at the call site and this is a gate rather than
        a style note.

        Many-to-one is deliberately not covered: joining a single parent row
        multiplies nothing, and `lazy="joined"` there is a reasonable choice.

        `engine` is taken so the mappers are configured before they are read —
        the relationships are string-annotated, and until something resolves
        them `inspect()` would raise rather than report.
        """
        forbidden = {"joined", "subquery", "immediate"}
        offenders = [
            f"{mapper.class_.__name__}.{rel.key} (lazy={rel.lazy!r})"
            for mapper in Base.registry.mappers
            for rel in inspect(mapper.class_).relationships
            if rel.uselist and rel.lazy in forbidden
        ]
        assert offenders == [], (
            "collection relationship(s) eager-loaded at the mapper: "
            f"{', '.join(offenders)}. Put the loader option on the query "
            "instead; see docs/n-plus-one.md."
        )
