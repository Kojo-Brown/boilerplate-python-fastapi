"""The outbox against a real Postgres, because the guarantee is Postgres's.

Nothing in this file can be faked usefully. "The event row commits with the
state change" is a statement about a transaction; "two relays claim disjoint
batches" is a statement about `FOR UPDATE SKIP LOCKED`; "the schedule uses the
database's clock" is a statement about `now()`. An in-memory stand-in that
agreed with all three would be a reimplementation of Postgres, and the tests
would be measuring it rather than the code.

Skipped when `DATABASE_URL` names nothing reachable, and CI always has a
Postgres service, so every claim below is measured on every pull request.
Following `test_optimistic_concurrency_db.py`: reachability and schema are
checked separately, so a missing database skips and a broken one fails.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import Interval, func, literal, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from src.config import settings
from src.database import Base
from src.events.base import DomainEvent
from src.events.bus import EventBus
from src.events.catalog import UserRegistered
from src.models.outbox import LAST_ERROR_MAX_LENGTH, OutboxEvent
from src.models.user import User
from src.outbox.base import PendingEvent
from src.outbox.publisher import OutboxPublisher
from src.outbox.relay import OutboxRelay, RelayConfig
from src.outbox.store import SqlAlchemyOutboxBatch, session_batches

#: Long enough that a healthy server never trips it, short enough that a
#: genuinely stuck lock fails the test instead of the job.
BLOCK_TIMEOUT = 10.0


@pytest.fixture
async def engine() -> AsyncGenerator[AsyncEngine]:
    """An engine on `DATABASE_URL`, or a skip if there is nothing there.

    `NullPool` because the concurrency tests open two sessions that must be two
    *distinct* connections: a pooled engine handing the same one to both would
    serialise the contention being measured, and every claim would look
    disjoint for the wrong reason.
    """
    engine = create_async_engine(settings.DATABASE_URL, poolclass=NullPool)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except (OSError, SQLAlchemyError) as exc:
        await engine.dispose()
        pytest.skip(f"no usable Postgres at DATABASE_URL: {exc}")

    # A no-op in CI, where `alembic upgrade head` has already run — which is
    # what makes these tests a check on migration 0005 as well as on the
    # mapper. A column the migration forgot leaves the table already existing,
    # `create_all` skipping it, and everything below failing on it.
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine
    await engine.dispose()


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture(autouse=True)
async def empty_outbox(
    sessions: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[None]:
    """Start and finish with an empty table.

    The relay claims every ready row, so a leftover from an interrupted test
    would be delivered by the next one and its assertions would be about
    someone else's event. Cleaned both ways round for that reason.
    """
    async with sessions() as session:
        await session.execute(text("DELETE FROM outbox_events"))
        await session.commit()

    yield

    async with sessions() as session:
        await session.execute(text("DELETE FROM outbox_events"))
        await session.commit()


def a_registration(**overrides: object) -> UserRegistered:
    fields: dict[str, object] = {
        "user_id": str(uuid.uuid4()),
        "email": "new@example.test",
    }
    fields.update(overrides)
    return UserRegistered(**fields)  # type: ignore[arg-type]


async def rows_in(session: AsyncSession) -> list[OutboxEvent]:
    result = await session.execute(
        select(OutboxEvent).order_by(OutboxEvent.available_at, OutboxEvent.id)
    )
    return list(result.scalars().all())


async def _wait_until_empty(sessions: async_sessionmaker[AsyncSession]) -> None:
    """Poll until the table is empty. Bounded by the caller's `wait_for`."""
    while True:
        async with sessions() as session:
            if not await rows_in(session):
                return
        await asyncio.sleep(0.02)


async def stage(
    session: AsyncSession,
    *,
    event: DomainEvent | None = None,
    available_in: float = 0.0,
    attempts: int = 0,
) -> uuid.UUID:
    """Put one row in the table, optionally scheduled into the future.

    `available_at` is set from the *server's* clock even here, so a test that
    schedules a row for later is scheduling it by the same clock the claim
    compares against.
    """
    record = await OutboxPublisher(session).publish(
        event if event is not None else a_registration()
    )
    if available_in or attempts:
        row = await session.get(OutboxEvent, record.id)
        assert row is not None
        row.available_at = func.now() + literal(
            timedelta(seconds=available_in), Interval()
        )
        row.attempts = attempts
    await session.commit()
    return record.id


# --- the guarantee -------------------------------------------------------


class TestTheTransactionalGuarantee:
    """The one property the whole pattern exists for."""

    async def test_the_event_row_commits_with_the_state_change(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        async with sessions() as session:
            user = User(
                email=f"outbox-{uuid.uuid4()}@example.test",
                hashed_password="not-a-real-hash",
            )
            session.add(user)
            await OutboxPublisher(session).publish(
                a_registration(user_id=str(user.id), email=user.email)
            )
            await session.commit()
            user_id = user.id

        async with sessions() as session:
            assert await session.get(User, user_id) is not None
            (row,) = await rows_in(session)
            assert row.event_name == "user.registered"
            assert row.payload["email"] == user.email

            stored = await session.get(User, user_id)
            assert stored is not None
            await session.delete(stored)
            await session.commit()

    async def test_a_rollback_takes_the_event_with_it(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        """The failure the old ordering could not have: an event announcing a
        registration that never happened."""
        async with sessions() as session:
            user = User(
                email=f"outbox-{uuid.uuid4()}@example.test",
                hashed_password="not-a-real-hash",
            )
            session.add(user)
            await OutboxPublisher(session).publish(
                a_registration(user_id=str(user.id), email=user.email)
            )
            await session.flush()
            user_id = user.id
            await session.rollback()

        async with sessions() as session:
            assert await session.get(User, user_id) is None
            assert await rows_in(session) == []

    async def test_publishing_after_the_commit_writes_nothing(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        """The bug this ordering rule exists to prevent, demonstrated rather
        than described.

        A publish placed after `commit()` stages its row in a *fresh*
        transaction, and `get_db` closes the session without committing that
        one. No error is raised anywhere: the request succeeds, the user
        exists, and the notification is simply gone. `AuthService` publishes
        before its commit for this reason, and `test_auth_events.py` holds it
        there.
        """
        async with sessions() as session:
            user = User(
                email=f"outbox-{uuid.uuid4()}@example.test",
                hashed_password="not-a-real-hash",
            )
            session.add(user)
            await session.commit()
            user_id = user.id

            await OutboxPublisher(session).publish(a_registration())
            # No second commit — exactly what `get_db` does on the way out.

        async with sessions() as session:
            assert await session.get(User, user_id) is not None
            assert await rows_in(session) == []

            stored = await session.get(User, user_id)
            assert stored is not None
            await session.delete(stored)
            await session.commit()


# --- claiming ------------------------------------------------------------


class TestClaiming:
    async def test_a_fresh_row_is_immediately_claimable(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        async with sessions() as session:
            row_id = await stage(session)

        async with sessions() as session:
            claimed = await SqlAlchemyOutboxBatch(session).claim(limit=10)
            await session.rollback()

        assert [entry.id for entry in claimed] == [row_id]
        assert isinstance(claimed[0], PendingEvent)
        assert claimed[0].attempts == 0

    async def test_a_row_scheduled_for_later_is_not_claimed(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        async with sessions() as session:
            await stage(session, available_in=3600)

        async with sessions() as session:
            claimed = await SqlAlchemyOutboxBatch(session).claim(limit=10)
            await session.rollback()

        assert claimed == ()

    async def test_a_claim_takes_the_oldest_ready_rows_up_to_the_limit(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        async with sessions() as session:
            first = await stage(session, available_in=-30)
            second = await stage(session, available_in=-20)
            await stage(session, available_in=-10)

        async with sessions() as session:
            claimed = await SqlAlchemyOutboxBatch(session).claim(limit=2)
            await session.rollback()

        assert [entry.id for entry in claimed] == [first, second]

    async def test_two_relays_claim_disjoint_batches(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        """`SKIP LOCKED` is what lets more than one relay run at all. Without
        it the second claimer blocks until the first commits, and the outbox
        drains at the speed of one worker however many are deployed."""
        async with sessions() as session:
            for _ in range(4):
                await stage(session)

        async with sessions() as first, sessions() as second:
            mine = await asyncio.wait_for(
                SqlAlchemyOutboxBatch(first).claim(limit=2), BLOCK_TIMEOUT
            )
            # Still inside the first transaction, so its two rows are locked.
            theirs = await asyncio.wait_for(
                SqlAlchemyOutboxBatch(second).claim(limit=2), BLOCK_TIMEOUT
            )
            await first.rollback()
            await second.rollback()

        assert len(mine) == 2
        assert len(theirs) == 2
        assert {entry.id for entry in mine}.isdisjoint({e.id for e in theirs})

    async def test_a_rolled_back_claim_releases_its_rows(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        """Which is what makes killing a relay mid-batch a non-event: there is
        no lease to expire and no reaper to write."""
        async with sessions() as session:
            row_id = await stage(session)

        async with sessions() as session:
            await SqlAlchemyOutboxBatch(session).claim(limit=10)
            await session.rollback()

        async with sessions() as session:
            claimed = await SqlAlchemyOutboxBatch(session).claim(limit=10)
            await session.rollback()

        assert [entry.id for entry in claimed] == [row_id]

    async def test_a_claim_of_nothing_is_refused(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        """It would look exactly like an empty queue."""
        async with sessions() as session:
            with pytest.raises(ValueError, match="at least 1"):
                await SqlAlchemyOutboxBatch(session).claim(limit=0)


# --- finishing with a row ------------------------------------------------


class TestCompletingAndFailing:
    async def test_completing_deletes_the_row(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        async with sessions() as session:
            await stage(session)

        async with sessions() as session:
            batch = SqlAlchemyOutboxBatch(session)
            (entry,) = await batch.claim(limit=10)
            await batch.complete(entry)
            await session.commit()

        async with sessions() as session:
            assert await rows_in(session) == []

    async def test_failing_records_the_attempt_and_reschedules_the_row(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        async with sessions() as session:
            await stage(session)

        async with sessions() as session:
            batch = SqlAlchemyOutboxBatch(session)
            (entry,) = await batch.claim(limit=10)
            await batch.fail(entry, error="RuntimeError: nope", retry_in=120)
            await session.commit()

        async with sessions() as session:
            (row,) = await rows_in(session)
            assert row.attempts == 1
            assert row.last_error == "RuntimeError: nope"
            # The reschedule is computed by the server, so this comparison is
            # only approximately about *our* clock — a minute of slack either
            # way still proves the interval was applied.
            expected = datetime.now(UTC) + timedelta(seconds=120)
            assert abs((row.available_at - expected).total_seconds()) < 60

    async def test_a_failed_row_is_not_claimable_until_its_time(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        async with sessions() as session:
            await stage(session)

        async with sessions() as session:
            batch = SqlAlchemyOutboxBatch(session)
            (entry,) = await batch.claim(limit=10)
            await batch.fail(entry, error="RuntimeError: nope", retry_in=3600)
            await session.commit()

        async with sessions() as session:
            assert await SqlAlchemyOutboxBatch(session).claim(limit=10) == ()
            await session.rollback()

    async def test_attempts_accumulate_across_batches(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        """The counter lives in the row so that the backoff survives a relay
        restart. Two separate transactions, as two ticks would be."""
        async with sessions() as session:
            await stage(session)

        for _ in range(2):
            async with sessions() as session:
                batch = SqlAlchemyOutboxBatch(session)
                (entry,) = await batch.claim(limit=10)
                await batch.fail(entry, error="still broken", retry_in=-1)
                await session.commit()

        async with sessions() as session:
            (row,) = await rows_in(session)
            assert row.attempts == 2

    async def test_an_enormous_error_is_truncated_to_the_column(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        """Postgres raises on an over-long varchar rather than truncating, so
        without this a stack-trace-shaped error message would fail the *batch*
        — and take the delivery of every other event in it with it."""
        async with sessions() as session:
            await stage(session)

        async with sessions() as session:
            batch = SqlAlchemyOutboxBatch(session)
            (entry,) = await batch.claim(limit=10)
            await batch.fail(entry, error="x" * 5000, retry_in=1)
            await session.commit()

        async with sessions() as session:
            (row,) = await rows_in(session)
            assert row.last_error is not None
            assert len(row.last_error) == LAST_ERROR_MAX_LENGTH


# --- the relay, end to end -----------------------------------------------


class TestTheBatchScope:
    async def test_a_clean_exit_commits(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        async with sessions() as session:
            await stage(session)

        scope = session_batches(sessions)
        async with scope() as batch:
            (entry,) = await batch.claim(limit=10)
            await batch.complete(entry)

        async with sessions() as session:
            assert await rows_in(session) == []

    async def test_an_exception_rolls_the_whole_batch_back(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        """Which is what makes a relay that dies mid-batch harmless, and what
        makes delivery at-least-once rather than at-most-once: the completions
        it had already recorded go back with everything else, and the events
        are simply delivered again."""
        async with sessions() as session:
            row_id = await stage(session)

        scope = session_batches(sessions)
        with pytest.raises(RuntimeError, match="worker died"):
            async with scope() as batch:
                (entry,) = await batch.claim(limit=10)
                await batch.complete(entry)
                raise RuntimeError("worker died")

        async with sessions() as session:
            (row,) = await rows_in(session)
            assert row.id == row_id

    async def test_cancellation_rolls_back_too(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        """`BaseException` rather than `Exception` in the handler: a cancelled
        relay would otherwise leave the session for the garbage collector to
        close, with a transaction still open on the connection."""
        async with sessions() as session:
            await stage(session)

        scope = session_batches(sessions)
        with pytest.raises(asyncio.CancelledError):
            async with scope() as batch:
                (entry,) = await batch.claim(limit=10)
                await batch.complete(entry)
                raise asyncio.CancelledError

        async with sessions() as session:
            assert len(await rows_in(session)) == 1


class TestTheRelayAgainstPostgres:
    async def test_a_committed_event_reaches_its_subscriber(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        bus = EventBus()
        seen: list[DomainEvent] = []

        async def record(event: DomainEvent) -> None:
            seen.append(event)

        bus.subscribe(DomainEvent, record)
        published = a_registration(email="relayed@example.test")

        async with sessions() as session:
            await stage(session, event=published)

        relay = OutboxRelay(batches=session_batches(sessions), dispatcher=bus)
        result = await relay.drain_once()

        assert result.delivered == 1
        assert seen == [published]
        async with sessions() as session:
            assert await rows_in(session) == []

    async def test_a_failed_delivery_leaves_the_row_for_later(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        bus = EventBus()

        async def explodes(event: DomainEvent) -> None:
            raise RuntimeError("the mail queue is down")

        bus.subscribe(DomainEvent, explodes)

        async with sessions() as session:
            row_id = await stage(session)

        relay = OutboxRelay(
            batches=session_batches(sessions),
            dispatcher=bus,
            config=RelayConfig(retry_base_delay=60.0, jitter=False),
        )
        result = await relay.drain_once()

        assert result.failed == 1
        async with sessions() as session:
            (row,) = await rows_in(session)
            assert row.id == row_id
            assert row.attempts == 1
            assert row.last_error is not None
            assert "the mail queue is down" in row.last_error

    async def test_draining_twice_delivers_once(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        """The deletion is committed, so the second tick finds nothing. This is
        the whole reason `complete` shares the claim's transaction."""
        bus = EventBus()
        seen: list[DomainEvent] = []

        async def record(event: DomainEvent) -> None:
            seen.append(event)

        bus.subscribe(DomainEvent, record)

        async with sessions() as session:
            await stage(session)

        relay = OutboxRelay(batches=session_batches(sessions), dispatcher=bus)
        await relay.drain_once()
        second = await relay.drain_once()

        assert len(seen) == 1
        assert second.empty

    async def test_the_running_relay_drains_what_is_published_after_it_started(
        self, sessions: async_sessionmaker[AsyncSession]
    ) -> None:
        """The lifespan's arrangement, in miniature: a background loop over the
        application's own sessions, draining rows a request committed."""
        bus = EventBus()
        arrived = asyncio.Event()

        async def record(event: DomainEvent) -> None:
            arrived.set()

        bus.subscribe(DomainEvent, record)
        relay = OutboxRelay(
            batches=session_batches(sessions),
            dispatcher=bus,
            config=RelayConfig(poll_interval=0.05),
        )
        relay.start()
        try:
            async with sessions() as session:
                await stage(session)
            await asyncio.wait_for(arrived.wait(), BLOCK_TIMEOUT)
            # Waiting for the *row* rather than only for the subscriber: the
            # delete is committed after the dispatch returns, and stopping the
            # relay in between would cancel that transaction and roll the
            # deletion back — correctly, and confusingly for a test that had
            # already concluded the event was delivered.
            await asyncio.wait_for(_wait_until_empty(sessions), BLOCK_TIMEOUT)
        finally:
            await relay.stop()
