"""The deadlock-retry loop, measured without a database.

Everything here turns on *when* the loop rolls back and sleeps, which is
awkward to observe against a real server and trivial against a session that
only records. `test_locking_db.py` proves the same loop survives a genuine
deadlock; this file proves it does the right thing in the cases a live database
will not produce to order — a rollback that itself fails, a cancellation
arriving mid-retry, an exhausted budget.
"""

from __future__ import annotations

import asyncio
import random
from typing import cast

import pytest
from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.locking.errors import (
    DEADLOCK_DETECTED,
    LOCK_NOT_AVAILABLE,
    SERIALIZATION_FAILURE,
)
from src.locking.retry import (
    DeadlockRetryPolicy,
    retry_on_deadlock,
    run_with_deadlock_retry,
)


def pg_error(code: str) -> DBAPIError:
    """A driver error carrying `code`, wrapped the way SQLAlchemy wraps one."""

    class DriverError(Exception):
        def __init__(self) -> None:
            super().__init__(f"pg error {code}")
            self.sqlstate = code

    return OperationalError("UPDATE users SET ...", {}, DriverError())


class RecordingSession:
    """As much `AsyncSession` as the retry loop touches, which is `rollback`.

    A real session would work here too and would be worse: it would need a
    database, and the thing under test is the sequencing of rollback and sleep,
    not SQLAlchemy's.
    """

    def __init__(self, *, rollback_error: Exception | None = None) -> None:
        self.events: list[str] = []
        self._rollback_error = rollback_error

    @property
    def rollbacks(self) -> int:
        return self.events.count("rollback")

    async def rollback(self) -> None:
        self.events.append("rollback")
        if self._rollback_error is not None:
            raise self._rollback_error


def as_session(recorder: RecordingSession) -> AsyncSession:
    return cast(AsyncSession, recorder)


class Recorder:
    """Records the sleeps a policy asks for, without spending them."""

    def __init__(self, session: RecordingSession) -> None:
        self.delays: list[float] = []
        self._session = session

    async def sleep(self, delay: float) -> None:
        self._session.events.append("sleep")
        self.delays.append(delay)


@pytest.fixture
def session() -> RecordingSession:
    return RecordingSession()


@pytest.fixture
def clock(session: RecordingSession) -> Recorder:
    return Recorder(session)


def policy(
    clock: Recorder,
    *,
    attempts: int = 3,
    jitter: bool = False,
    **kwargs: object,
) -> DeadlockRetryPolicy:
    """A policy that never actually waits. Jitter off so delays are exact."""
    return retry_on_deadlock(
        attempts=attempts,
        jitter=jitter,
        asleep=clock.sleep,
        **kwargs,  # type: ignore[arg-type]
    )


class TestSuccessPath:
    async def test_a_call_that_works_is_left_alone(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        async def work(_: AsyncSession) -> str:
            return "committed"

        assert await policy(clock).run(as_session(session), work) == "committed"
        assert session.events == []

    async def test_arguments_reach_the_work_unchanged(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        async def work(_: AsyncSession, amount: int, *, note: str) -> str:
            return f"{amount}:{note}"

        result = await policy(clock).run(as_session(session), work, 7, note="rent")
        assert result == "7:rent"

    async def test_the_module_level_helper_uses_the_default_policy(
        self, session: RecordingSession
    ) -> None:
        """`run_with_deadlock_retry` is the no-configuration entry point."""
        calls = 0

        async def work(_: AsyncSession, tag: str) -> str:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise pg_error(DEADLOCK_DETECTED)
            return tag

        assert (
            await run_with_deadlock_retry(as_session(session), work, "done") == "done"
        )
        assert calls == 2
        assert session.rollbacks == 1


class TestRetrying:
    async def test_a_deadlock_is_re_run(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        attempts = 0

        async def work(_: AsyncSession) -> str:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise pg_error(DEADLOCK_DETECTED)
            return "third time"

        assert await policy(clock).run(as_session(session), work) == "third time"
        assert attempts == 3

    async def test_a_serialization_failure_is_re_run(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        """The failure a `SERIALIZABLE` transaction gets from `COMMIT` itself."""
        attempts = 0

        async def work(_: AsyncSession) -> str:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise pg_error(SERIALIZATION_FAILURE)
            return "ok"

        assert await policy(clock).run(as_session(session), work) == "ok"
        assert attempts == 2

    async def test_the_transaction_is_rolled_back_before_the_wait(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        """The whole reason this loop exists rather than `@retry`.

        Without the rollback the next attempt runs inside an aborted
        transaction and fails with 25P02 instead of retrying anything — see
        `test_locking_db.py`, which provokes that from a real server. The order
        matters as well as the fact: rolling back after the sleep would hold
        the losing transaction's locks for the whole backoff.
        """
        attempts = 0

        async def work(_: AsyncSession) -> str:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise pg_error(DEADLOCK_DETECTED)
            return "ok"

        await policy(clock).run(as_session(session), work)

        assert session.events == ["rollback", "sleep", "rollback", "sleep"]

    async def test_a_rollback_that_fails_does_not_stop_the_retry(
        self, clock: Recorder
    ) -> None:
        """A failed rollback means a dead connection, and the next attempt will
        say so in terms of the real problem. Raising from the cleanup would
        replace the deadlock with a less useful error."""
        broken = RecordingSession(rollback_error=SQLAlchemyError("connection gone"))
        attempts = 0

        async def work(_: AsyncSession) -> str:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise pg_error(DEADLOCK_DETECTED)
            return "recovered"

        result = await policy(Recorder(broken)).run(as_session(broken), work)

        assert result == "recovered"
        assert broken.rollbacks == 1


class TestGivingUp:
    async def test_the_original_exception_is_what_propagates(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        """No `RetryError` wrapper, for the same reason `@retry` has none: the
        exception's type is what the edge turns into a status code."""
        failure = pg_error(DEADLOCK_DETECTED)

        async def work(_: AsyncSession) -> None:
            raise failure

        with pytest.raises(DBAPIError) as excinfo:
            await policy(clock, attempts=3).run(as_session(session), work)

        assert excinfo.value is failure

    async def test_it_stops_after_the_configured_number_of_attempts(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        attempts = 0

        async def work(_: AsyncSession) -> None:
            nonlocal attempts
            attempts += 1
            raise pg_error(DEADLOCK_DETECTED)

        with pytest.raises(DBAPIError):
            await policy(clock, attempts=4).run(as_session(session), work)

        assert attempts == 4
        assert len(clock.delays) == 3

    async def test_the_final_failure_is_not_rolled_back(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        """Documented, deliberate: on exhaustion the session is left exactly as
        an unwrapped call would have left it, for the caller's existing error
        handling. Three attempts means two rollbacks, not three."""

        async def work(_: AsyncSession) -> None:
            raise pg_error(DEADLOCK_DETECTED)

        with pytest.raises(DBAPIError):
            await policy(clock, attempts=3).run(as_session(session), work)

        assert session.rollbacks == 2

    async def test_attempts_of_one_never_retries(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        attempts = 0

        async def work(_: AsyncSession) -> None:
            nonlocal attempts
            attempts += 1
            raise pg_error(DEADLOCK_DETECTED)

        with pytest.raises(DBAPIError):
            await policy(clock, attempts=1).run(as_session(session), work)

        assert attempts == 1
        assert session.events == []


class TestWhatIsNotRetried:
    async def test_a_durable_database_error_propagates_immediately(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        """A unique violation fails the same way however many times it runs."""
        attempts = 0

        async def work(_: AsyncSession) -> None:
            nonlocal attempts
            attempts += 1
            raise pg_error("23505")

        with pytest.raises(DBAPIError):
            await policy(clock).run(as_session(session), work)

        assert attempts == 1
        assert session.events == []

    async def test_a_refused_lock_is_not_retried_by_default(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        """`nowait` and `lock_timeout` are a caller saying "do not queue for
        this". A retry loop is a queue, so 55P03 stays out of the default set."""
        attempts = 0

        async def work(_: AsyncSession) -> None:
            nonlocal attempts
            attempts += 1
            raise pg_error(LOCK_NOT_AVAILABLE)

        with pytest.raises(DBAPIError):
            await policy(clock).run(as_session(session), work)

        assert attempts == 1

    async def test_an_ordinary_bug_propagates_immediately(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        attempts = 0

        async def work(_: AsyncSession) -> None:
            nonlocal attempts
            attempts += 1
            raise TypeError("wrong argument")

        with pytest.raises(TypeError):
            await policy(clock).run(as_session(session), work)

        assert attempts == 1
        assert session.events == []

    async def test_cancellation_is_never_retried(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        """The caller has stopped waiting. Re-running the transaction would
        hold locks for work nobody will read, and make shutdown hang."""
        attempts = 0

        async def work(_: AsyncSession) -> None:
            nonlocal attempts
            attempts += 1
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await policy(clock).run(as_session(session), work)

        assert attempts == 1
        assert session.events == []

    async def test_cancellation_wins_even_mid_retry(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        """A deadlock on attempt one, a cancellation on attempt two: the loop
        must stop, not treat the cancellation as another failure to absorb."""
        attempts = 0

        async def work(_: AsyncSession) -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise pg_error(DEADLOCK_DETECTED)
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await policy(clock, attempts=5).run(as_session(session), work)

        assert attempts == 2
        assert session.rollbacks == 1

    async def test_the_retryable_set_can_be_narrowed(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        attempts = 0

        async def work(_: AsyncSession) -> None:
            nonlocal attempts
            attempts += 1
            raise pg_error(SERIALIZATION_FAILURE)

        with pytest.raises(DBAPIError):
            await policy(clock, codes=frozenset({DEADLOCK_DETECTED})).run(
                as_session(session), work
            )

        assert attempts == 1


class TestBackoff:
    async def test_delays_double_and_are_capped(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        async def work(_: AsyncSession) -> None:
            raise pg_error(DEADLOCK_DETECTED)

        with pytest.raises(DBAPIError):
            await policy(clock, attempts=5, base_delay=0.1, max_delay=0.3).run(
                as_session(session), work
            )

        assert clock.delays == [0.1, 0.2, 0.3, 0.3]

    async def test_jitter_draws_from_below_the_ceiling(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        """Two transactions deadlocked against each other are released
        together; retrying in lockstep re-creates the same deadlock on the same
        rows, which is why this is on by default."""

        async def work(_: AsyncSession) -> None:
            raise pg_error(DEADLOCK_DETECTED)

        with pytest.raises(DBAPIError):
            await retry_on_deadlock(
                attempts=4,
                base_delay=0.1,
                max_delay=10.0,
                jitter=True,
                rng=random.Random(20260816),
                asleep=clock.sleep,
            ).run(as_session(session), work)

        assert len(clock.delays) == 3
        assert all(
            0.0 <= d <= ceiling
            for d, ceiling in zip(clock.delays, [0.1, 0.2, 0.4], strict=True)
        )
        # A seeded stream that happened to return its ceiling three times would
        # pass the bound above while jitter did nothing.
        assert len(set(clock.delays)) == 3


class TestPolicyValidation:
    """A bad policy fails at decoration time, not on the first deadlock."""

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"attempts": 0}, "attempts must be at least 1."),
            ({"base_delay": -1.0}, "base_delay must not be negative."),
            (
                {"base_delay": 2.0, "max_delay": 1.0},
                "max_delay must be at least base_delay.",
            ),
            ({"codes": frozenset()}, "codes must name at least one SQLSTATE"),
        ],
    )
    def test_rejects_an_unusable_policy(
        self, kwargs: dict[str, object], message: str
    ) -> None:
        with pytest.raises(ValueError, match=message):
            retry_on_deadlock(**kwargs)  # type: ignore[arg-type]


class TestDecorator:
    async def test_it_wraps_a_session_first_function(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        attempts = 0

        @retry_on_deadlock(attempts=3, jitter=False, asleep=clock.sleep)
        async def settle(_: AsyncSession, invoice: str) -> str:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise pg_error(DEADLOCK_DETECTED)
            return f"settled {invoice}"

        assert await settle(as_session(session), "inv-1") == "settled inv-1"
        assert attempts == 2
        assert session.rollbacks == 1

    async def test_keyword_arguments_survive_the_wrapper(
        self, session: RecordingSession, clock: Recorder
    ) -> None:
        @retry_on_deadlock(jitter=False, asleep=clock.sleep)
        async def settle(_: AsyncSession, invoice: str, *, force: bool = False) -> str:
            return f"{invoice}:{force}"

        assert await settle(as_session(session), "inv-2", force=True) == "inv-2:True"

    def test_the_wrapped_function_keeps_its_identity(self) -> None:
        """`functools.wraps`, so a traceback and `inspect.signature` both name
        the real function rather than `wrapper`."""
        import inspect

        @retry_on_deadlock()
        async def settle(session: AsyncSession, invoice: str) -> str:
            """Settle one invoice."""
            return invoice

        assert settle.__name__ == "settle"
        assert settle.__doc__ == "Settle one invoice."
        assert list(inspect.signature(settle).parameters) == ["session", "invoice"]
