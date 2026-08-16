"""Pessimistic locking measured against a real Postgres.

Nothing in this file can be faked usefully. A row lock is an agreement between
two transactions, a deadlock is something only a live server can detect and
break, and the 25P02 that justifies the rollback in `src/locking/retry.py` is a
behaviour of Postgres rather than of any code here. So these open real sessions
and let them contend.

They are skipped when `DATABASE_URL` names nothing reachable, and CI always has
a Postgres service, so every claim below is measured on every pull request.
Following `test_optimistic_concurrency_db.py`: reachability and schema are
checked separately, so a missing database skips and a broken one fails.

`asyncio.wait_for` guards every deliberate block. A test that gets the
interaction wrong should fail in seconds rather than hang a CI job until the
job timeout kills it with no useful output.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncGenerator

import pytest
from sqlalchemy import UUID, Select, select, text
from sqlalchemy.exc import DBAPIError, SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.orm.exc import StaleDataError
from sqlalchemy.pool import NullPool

from src.config import settings
from src.database import Base
from src.locking import (
    IN_FAILED_SQL_TRANSACTION,
    LockMode,
    LockNotAvailableError,
    is_deadlock,
    lock_row,
    lock_rows,
    lock_timeout,
    retry_on_deadlock,
    sqlstate,
)
from src.models.user import User

#: Generous enough that a healthy local or CI Postgres never trips it, short
#: enough that a genuinely stuck lock fails the test instead of the job.
BLOCK_TIMEOUT = 10.0

#: How long to let a deliberately blocked coroutine sit before concluding it is
#: really blocked rather than merely slow.
SETTLE = 0.25


class ProbeBase(DeclarativeBase):
    """A registry of its own, so nothing here reaches `Base.metadata`.

    A composite-key model is needed to exercise one guard in `lock_row`, and
    declaring it on the application `Base` would add a table to every
    `create_all` in the suite for the sake of a check that never runs a query.
    """


class CompositeKeyProbe(ProbeBase):
    __tablename__ = "locking_composite_pk_probe"

    user_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    group_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)


@pytest.fixture
async def engine() -> AsyncGenerator[AsyncEngine]:
    """An engine on `DATABASE_URL`, or a skip if there is nothing there.

    `NullPool` because every test here needs its sessions on *distinct*
    connections — two sessions sharing one connection cannot contend for a
    lock, they would simply take turns, and every assertion below would pass
    for the wrong reason.
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
async def rows(
    sessions: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[list[uuid.UUID]]:
    """Four committed users, removed again afterwards.

    Emails are randomised so a rerun after an interrupted test cannot trip the
    unique index, and so two tests can never collide on one row. Ordered by id
    because several tests below depend on a stable lock order.
    """
    marker = uuid.uuid4().hex[:8]
    async with sessions() as session:
        users = [
            User(
                email=f"lock-{marker}-{index}@example.test",
                hashed_password="not-a-real-hash",
            )
            for index in range(4)
        ]
        session.add_all(users)
        await session.commit()
        created = sorted(user.id for user in users)

    yield created

    async with sessions() as session:
        for row_id in created:
            row = await session.get(User, row_id)
            if row is not None:
                await session.delete(row)
        await session.commit()


async def version_of(
    sessions: async_sessionmaker[AsyncSession], row_id: uuid.UUID
) -> int:
    async with sessions() as session:
        row = await session.get(User, row_id)
        assert row is not None
        return row.version


async def change_url_elsewhere(
    sessions: async_sessionmaker[AsyncSession], row_id: uuid.UUID, url: str
) -> None:
    """Commit a change to `row_id` from a session the test is not holding."""
    async with sessions() as other:
        row = await other.get(User, row_id)
        assert row is not None
        row.notification_webhook_url = url
        await other.commit()


class TestRowLockExcludesWriters:
    async def test_a_second_writer_waits_until_the_first_commits(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        """The defining property, and the one that makes this pessimistic."""
        async with sessions() as holder, sessions() as waiter:
            await lock_row(holder, User, rows[0])

            async def take_it() -> None:
                await lock_row(waiter, User, rows[0])

            attempt = asyncio.create_task(take_it())
            await asyncio.sleep(SETTLE)
            assert not attempt.done(), "the second writer was not blocked at all"

            await holder.commit()
            await asyncio.wait_for(attempt, timeout=BLOCK_TIMEOUT)
            await waiter.rollback()

    async def test_an_uncontended_lock_is_immediate(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        async with sessions() as session:
            locked = await asyncio.wait_for(
                lock_row(session, User, rows[0]), timeout=BLOCK_TIMEOUT
            )
            assert locked is not None
            assert locked.id == rows[0]
            await session.rollback()

    async def test_a_missing_row_is_none_rather_than_an_error(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        async with sessions() as session:
            assert await lock_row(session, User, uuid.uuid4()) is None
            await session.rollback()

    async def test_locking_re_reads_a_row_already_in_the_identity_map(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        """The failure this prevents is silent and total.

        Without `populate_existing`, a `SELECT ... FOR UPDATE` for a row the
        session already holds takes the lock and then hands back the *cached*
        instance — so the caller decides against values read before the lock
        existed, which is the exact race the lock was for. The next test shows
        that happening.
        """
        async with sessions() as reader:
            stale = await reader.get(User, rows[0])
            assert stale is not None
            assert stale.notification_webhook_url is None

            await change_url_elsewhere(sessions, rows[0], "https://example.test/one")

            # Same session, same identity map, same object — and now current.
            fresh = await lock_row(reader, User, rows[0])
            assert fresh is stale
            assert fresh.notification_webhook_url == "https://example.test/one"
            await reader.rollback()

    async def test_a_lock_without_populate_existing_reads_the_stale_copy(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        """The bug being designed around, demonstrated rather than asserted about.

        This is the obvious way to write `lock_row` and it is wrong. The lock
        is genuinely held and the row genuinely re-read, but the identity map
        wins over the result set, so the attribute the caller reads is the one
        from before the lock. Nothing raises, and the code looks correct.
        """
        async with sessions() as reader:
            stale = await reader.get(User, rows[0])
            assert stale is not None

            await change_url_elsewhere(sessions, rows[0], "https://example.test/two")

            unrefreshed = (
                await reader.execute(
                    select(User).where(User.id == rows[0]).with_for_update()
                )
            ).scalar_one()

            assert unrefreshed is stale
            assert unrefreshed.notification_webhook_url is None  # not "two"
            await reader.rollback()

    async def test_session_get_under_a_lock_rejects_a_stale_versioned_row(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        """Why `lock_row` does not use `Session.get(with_for_update=...)`.

        `get` routes through the refresh path, and passing `with_for_update`
        turns on version checking. `User` carries a `version_id_col` for the
        optimistic concurrency in `docs/optimistic-concurrency.md`, so a
        pessimistic reader whose copy has moved on gets a `StaleDataError`
        instead of the current row it asked for.
        """
        async with sessions() as reader:
            # Held deliberately: the identity map keeps only a weak reference,
            # so dropping this one lets the instance be collected and the
            # second `get` becomes a clean load with nothing to compare.
            stale = await reader.get(User, rows[0])
            assert stale is not None

            await change_url_elsewhere(sessions, rows[0], "https://example.test/three")

            with pytest.raises(StaleDataError):
                await reader.get(User, rows[0], with_for_update=True)

            assert stale.version == 1
            await reader.rollback()

    async def test_a_composite_primary_key_is_refused(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        """A single `ident` cannot name a composite key, and matching on the
        first column alone would lock some other row and look like it worked.

        Refused before any SQL is emitted, so the session here is never used.
        """
        async with sessions() as session:
            with pytest.raises(ValueError, match="composite primary key"):
                # Not a `Base` subclass — deliberately, so the probe table stays
                # out of the application metadata every other test calls
                # `create_all` on. `lock_row` only reads the mapper.
                await lock_row(session, CompositeKeyProbe, uuid.uuid4())  # type: ignore[type-var]
            await session.rollback()


class TestNowait:
    async def test_a_held_row_fails_instead_of_waiting(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        async with sessions() as holder, sessions() as impatient:
            await lock_row(holder, User, rows[0])

            with pytest.raises(LockNotAvailableError) as excinfo:
                await asyncio.wait_for(
                    lock_row(impatient, User, rows[0], nowait=True),
                    timeout=BLOCK_TIMEOUT,
                )

            assert excinfo.value.status_code == 409
            assert excinfo.value.error_code == "LOCK_NOT_AVAILABLE"
            # The 55P03 is preserved as the cause, so the SQLSTATE survives the
            # translation into an application error.
            assert sqlstate(excinfo.value.__cause__ or Exception()) == "55P03"

            await impatient.rollback()
            await holder.rollback()

    async def test_an_uncontended_row_is_returned_normally(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        async with sessions() as session:
            locked = await lock_row(session, User, rows[0], nowait=True)
            assert locked is not None
            await session.rollback()


class TestLockModes:
    async def test_two_share_locks_coexist(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        async with sessions() as first, sessions() as second:
            assert await lock_row(first, User, rows[0], mode=LockMode.SHARE)
            assert await lock_row(
                second, User, rows[0], mode=LockMode.SHARE, nowait=True
            )
            await first.rollback()
            await second.rollback()

    async def test_a_share_lock_excludes_a_writer(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        async with sessions() as reader, sessions() as writer:
            await lock_row(reader, User, rows[0], mode=LockMode.SHARE)

            with pytest.raises(LockNotAvailableError):
                await lock_row(writer, User, rows[0], mode=LockMode.UPDATE, nowait=True)

            await writer.rollback()
            await reader.rollback()

    async def test_for_update_blocks_a_key_share(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        """Half of the reason `LockMode` documents the difference at all.

        `KEY SHARE` is what Postgres takes implicitly on a row when another
        table's row is inserted referencing it. `FOR UPDATE` conflicts with it,
        so locking a `users` row this way blocks inserts into `refresh_tokens`
        — a cost that is invisible in the code taking the lock.
        """
        async with sessions() as holder, sessions() as referencer:
            await lock_row(holder, User, rows[0], mode=LockMode.UPDATE)

            with pytest.raises(LockNotAvailableError):
                await lock_row(
                    referencer, User, rows[0], mode=LockMode.KEY_SHARE, nowait=True
                )

            await referencer.rollback()
            await holder.rollback()

    async def test_no_key_update_permits_a_key_share(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        """And the other half: the weaker mode still excludes writers but lets
        foreign-key references through, which is why it is the better default
        for updating a non-key column."""
        async with sessions() as holder, sessions() as referencer:
            await lock_row(holder, User, rows[0], mode=LockMode.NO_KEY_UPDATE)

            assert await lock_row(
                referencer, User, rows[0], mode=LockMode.KEY_SHARE, nowait=True
            )

            with pytest.raises(LockNotAvailableError):
                await lock_row(
                    referencer, User, rows[0], mode=LockMode.UPDATE, nowait=True
                )

            await referencer.rollback()
            await holder.rollback()


class TestSkipLocked:
    async def test_two_workers_claim_disjoint_batches(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        """The work-queue pattern: neither worker queues behind the other, and
        no job is handed to both."""

        def batch(limit: int) -> Select[tuple[User]]:
            return select(User).where(User.id.in_(rows)).order_by(User.id).limit(limit)

        async with sessions() as first, sessions() as second:
            mine = await lock_rows(first, batch(2), skip_locked=True)
            theirs = await asyncio.wait_for(
                lock_rows(second, batch(2), skip_locked=True), timeout=BLOCK_TIMEOUT
            )

            assert [row.id for row in mine] == rows[:2]
            assert [row.id for row in theirs] == rows[2:]
            assert not {row.id for row in mine} & {row.id for row in theirs}

            await first.rollback()
            await second.rollback()

    async def test_an_entirely_locked_queue_yields_nothing(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        """ "Nothing free right now" is an empty list, not a wait and not an
        error — the caller polls again rather than blocking a worker."""
        statement = select(User).where(User.id.in_(rows)).order_by(User.id)

        async with sessions() as holder, sessions() as latecomer:
            assert len(await lock_rows(holder, statement)) == len(rows)

            assert (
                await asyncio.wait_for(
                    lock_rows(latecomer, statement, skip_locked=True),
                    timeout=BLOCK_TIMEOUT,
                )
                == []
            )

            await latecomer.rollback()
            await holder.rollback()

    async def test_nowait_reports_the_contention_instead(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        statement = select(User).where(User.id.in_(rows)).order_by(User.id)

        async with sessions() as holder, sessions() as impatient:
            await lock_rows(holder, statement)

            with pytest.raises(LockNotAvailableError):
                await asyncio.wait_for(
                    lock_rows(impatient, statement, nowait=True), timeout=BLOCK_TIMEOUT
                )

            await impatient.rollback()
            await holder.rollback()

    async def test_locked_rows_are_refreshed_too(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        """The same guarantee `lock_row` gives, which is why both go through
        one helper: a batch claim that returns cached copies is the queue
        worker acting on a job someone else already changed."""
        async with sessions() as reader:
            stale = await reader.get(User, rows[0])
            assert stale is not None
            assert stale.notification_webhook_url is None

            await change_url_elsewhere(sessions, rows[0], "https://example.test/batch")

            claimed = await lock_rows(
                reader, select(User).where(User.id == rows[0]), skip_locked=True
            )

            assert [row.id for row in claimed] == [rows[0]]
            assert claimed[0] is stale
            assert claimed[0].notification_webhook_url == "https://example.test/batch"
            await reader.rollback()

    async def test_nowait_and_skip_locked_together_are_refused(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        """Postgres rejects the combination; failing in Python names the reason."""
        async with sessions() as session:
            with pytest.raises(ValueError, match="mutually exclusive"):
                await lock_rows(session, select(User), nowait=True, skip_locked=True)


class TestLockTimeout:
    async def test_a_bounded_wait_gives_up_and_reports_55p03(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        """The middle ground: long enough to ride out normal contention, short
        enough that a request never parks on a lock indefinitely."""
        async with sessions() as holder, sessions() as patient:
            await lock_row(holder, User, rows[0])

            started = time.perf_counter()
            with pytest.raises(LockNotAvailableError):
                async with lock_timeout(patient, 0.25):
                    await asyncio.wait_for(
                        lock_row(patient, User, rows[0]), timeout=BLOCK_TIMEOUT
                    )
            elapsed = time.perf_counter() - started

            assert elapsed >= 0.2, "gave up before the timeout it was given"
            await patient.rollback()
            await holder.rollback()

    async def test_a_lock_available_within_the_window_is_taken(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        """The case `nowait` would have failed: the holder lets go in time."""
        async with sessions() as holder, sessions() as patient:
            await lock_row(holder, User, rows[0])

            async def release_shortly() -> None:
                await asyncio.sleep(SETTLE)
                await holder.commit()

            releasing = asyncio.create_task(release_shortly())
            async with lock_timeout(patient, 5.0):
                locked = await asyncio.wait_for(
                    lock_row(patient, User, rows[0]), timeout=BLOCK_TIMEOUT
                )
            await releasing

            assert locked is not None
            await patient.rollback()

    async def test_the_previous_setting_is_restored(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        async with sessions() as session:
            before = (
                await session.execute(text("SELECT current_setting('lock_timeout')"))
            ).scalar_one()

            async with lock_timeout(session, 0.5):
                inside = (
                    await session.execute(
                        text("SELECT current_setting('lock_timeout')")
                    )
                ).scalar_one()

            after = (
                await session.execute(text("SELECT current_setting('lock_timeout')"))
            ).scalar_one()

            assert inside == "500ms"
            assert after == before
            await session.rollback()

    async def test_sub_millisecond_waits_round_up_rather_than_to_zero(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        """Truncating 0.0001s to 0ms would hand Postgres its code for "wait
        forever", turning the tightest possible timeout into none at all."""
        async with sessions() as session:
            async with lock_timeout(session, 0.0001):
                inside = (
                    await session.execute(
                        text("SELECT current_setting('lock_timeout')")
                    )
                ).scalar_one()
            assert inside == "1ms"
            await session.rollback()

    async def test_zero_is_refused(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        async with sessions() as session:
            with pytest.raises(ValueError, match="wait indefinitely"):
                async with lock_timeout(session, 0):
                    pass  # pragma: no cover - the context manager never opens


class TestAbortedTransactions:
    async def test_a_failed_statement_poisons_the_whole_transaction(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        """The fact `src/locking/retry.py` is built around.

        This is why the deadlock retry cannot be `@retry` from
        `src/decorators`: without the rollback, attempt two does not re-run the
        work at all — it raises 25P02, an error about the *previous* failure,
        and every later attempt does the same.
        """
        async with sessions() as session:
            with pytest.raises(DBAPIError):
                await session.execute(text("SELECT 1 / 0"))

            with pytest.raises(DBAPIError) as excinfo:
                await session.execute(text("SELECT 1"))
            assert sqlstate(excinfo.value) == IN_FAILED_SQL_TRANSACTION

            await session.rollback()
            assert (await session.execute(text("SELECT 1"))).scalar_one() == 1


class TestDeadlockRetry:
    async def test_a_real_deadlock_is_survived_by_both_transactions(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        """Two transactions, two rows, opposite lock orders — a deadlock by
        construction. Postgres kills one; the retry re-runs it, and both end up
        committing their work.

        Both sides are wrapped rather than one, because *which* transaction
        Postgres chooses as the victim is not something a test can pin down.
        The assertion that survives that uncertainty is stronger anyway: both
        complete, and each row is updated exactly twice.
        """
        first, second = rows[0], rows[1]
        holding = {0: asyncio.Event(), 1: asyncio.Event()}
        go = asyncio.Event()
        attempts = {0: 0, 1: 0}

        @retry_on_deadlock(attempts=4, base_delay=0.05)
        async def touch_both(
            session: AsyncSession, index: int, head: uuid.UUID, tail: uuid.UUID
        ) -> None:
            attempts[index] += 1
            head_row = await lock_row(session, User, head)

            # Both parties reach here holding one lock; only then is either
            # allowed to reach for the second. On a retry these are already
            # set, so the re-run proceeds straight through.
            holding[index].set()
            await go.wait()

            tail_row = await lock_row(session, User, tail)
            assert head_row is not None and tail_row is not None

            url = f"https://example.test/{index}"
            head_row.notification_webhook_url = url
            tail_row.notification_webhook_url = url
            await session.commit()

        async def run(
            session: AsyncSession, index: int, head: uuid.UUID, tail: uuid.UUID
        ) -> None:
            """`create_task` wants a coroutine; the decorator returns an
            `Awaitable`, which is a weaker promise it will not accept."""
            await touch_both(session, index, head, tail)

        async with sessions() as one, sessions() as two:
            tasks: list[asyncio.Task[None]] = [
                asyncio.create_task(run(one, 0, first, second)),
                asyncio.create_task(run(two, 1, second, first)),
            ]
            await asyncio.wait_for(
                asyncio.gather(holding[0].wait(), holding[1].wait()),
                timeout=BLOCK_TIMEOUT,
            )
            go.set()

            await asyncio.wait_for(asyncio.gather(*tasks), timeout=BLOCK_TIMEOUT * 3)

        assert sum(attempts.values()) > 2, "no deadlock occurred; nothing was retried"

        # The correctness claim, independent of who lost. Each row started at
        # version 1 and was updated by both transactions: 3, not 4 (which would
        # mean a retry committed twice) and not 2 (a lost update).
        assert await version_of(sessions, first) == 3
        assert await version_of(sessions, second) == 3

    async def test_an_unretryable_failure_still_propagates(
        self, sessions: async_sessionmaker[AsyncSession], rows: list[uuid.UUID]
    ) -> None:
        """The loop must not turn a real defect into three of them."""
        calls = 0

        @retry_on_deadlock(attempts=3, base_delay=0.0)
        async def duplicate_email(session: AsyncSession, row_id: uuid.UUID) -> None:
            nonlocal calls
            calls += 1
            row = await lock_row(session, User, row_id)
            assert row is not None
            row.email = "taken@example.test"
            await session.commit()

        async with sessions() as blocker:
            existing = await blocker.get(User, rows[3])
            assert existing is not None
            existing.email = "taken@example.test"
            await blocker.commit()

        async with sessions() as session:
            with pytest.raises(DBAPIError) as excinfo:
                await duplicate_email(session, rows[0])
            assert sqlstate(excinfo.value) == "23505"
            assert not is_deadlock(excinfo.value)
            await session.rollback()

        assert calls == 1
