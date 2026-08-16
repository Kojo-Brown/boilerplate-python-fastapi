"""SQLSTATE extraction and classification, with no database in sight.

The shapes exercised here are the ones a real driver produces — the codes and
attribute names were read off live asyncpg errors, and `test_locking_db.py`
asserts the same classifications against a Postgres that genuinely deadlocked.
These tests cover the branches that a live database will not reach on demand: a
driver that reports through `pgcode` instead, an exception with no SQLSTATE at
all, and the malformed values a defensive reader has to reject.
"""

from __future__ import annotations

import pytest
from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError

from src.exceptions import ConflictError
from src.locking.errors import (
    DEADLOCK_DETECTED,
    IN_FAILED_SQL_TRANSACTION,
    LOCK_NOT_AVAILABLE,
    RETRYABLE_SQLSTATES,
    SERIALIZATION_FAILURE,
    LockNotAvailableError,
    is_deadlock,
    is_lock_unavailable,
    is_retryable_conflict,
    is_serialization_failure,
    sqlstate,
)


class AsyncpgStyleError(Exception):
    """An asyncpg error: the code is on `sqlstate`."""

    def __init__(self, code: object) -> None:
        super().__init__(f"pg error {code}")
        self.sqlstate = code


class PsycopgStyleError(Exception):
    """A psycopg error: the code is on `pgcode` and there is no `sqlstate`."""

    def __init__(self, code: object) -> None:
        super().__init__(f"pg error {code}")
        self.pgcode = code


def wrapped(orig: Exception) -> DBAPIError:
    """`orig` as SQLAlchemy delivers it: hung off `DBAPIError.orig`."""
    return OperationalError("SELECT 1", {}, orig)


class TestSqlstate:
    def test_reads_it_from_a_wrapped_asyncpg_error(self) -> None:
        assert sqlstate(wrapped(AsyncpgStyleError("40P01"))) == "40P01"

    def test_reads_it_from_a_wrapped_psycopg_error(self) -> None:
        """The driver is swappable; the SQLSTATE is not."""
        assert sqlstate(wrapped(PsycopgStyleError("40P01"))) == "40P01"

    def test_reads_it_from_an_unwrapped_driver_error(self) -> None:
        assert sqlstate(AsyncpgStyleError("55P03")) == "55P03"

    def test_prefers_sqlstate_over_pgcode(self) -> None:
        """SQLAlchemy's asyncpg wrapper sets both, and they agree. If a driver
        ever disagrees with itself, the one named after the standard wins."""

        class Both(Exception):
            sqlstate = "40001"
            pgcode = "40P01"

        assert sqlstate(Both()) == SERIALIZATION_FAILURE

    def test_is_none_for_an_ordinary_exception(self) -> None:
        assert sqlstate(ValueError("not a database problem")) is None

    def test_is_none_for_a_sqlalchemy_error_with_no_driver_error(self) -> None:
        """`StaleDataError`, `NoResultFound` and friends never reach a driver."""
        assert sqlstate(SQLAlchemyError("mapper trouble")) is None

    @pytest.mark.parametrize("value", [None, 40001, "40P", "40P011", b"40P01"])
    def test_rejects_a_value_that_is_not_a_five_character_string(
        self, value: object
    ) -> None:
        """A code that is not a code must read as absent.

        The tempting `str(value)` would turn `None` into the string `"None"`,
        which matches no constant here — and so silently answers "not
        retryable" to every deadlock rather than failing where someone would
        notice.
        """
        assert sqlstate(wrapped(AsyncpgStyleError(value))) is None

    def test_does_not_loop_on_a_self_referential_orig(self) -> None:
        """A driver that points `orig` at itself must not hang the classifier."""

        class SelfReferential(Exception):
            pass

        exc = SelfReferential()
        exc.orig = exc  # type: ignore[attr-defined]
        assert sqlstate(exc) is None

    def test_gives_up_rather_than_walking_an_unbounded_chain(self) -> None:
        """Depth is bounded, so a pathological chain costs a fixed number of
        lookups instead of however many the driver felt like nesting."""

        class Link(Exception):
            def __init__(self) -> None:
                self.orig: Exception | None = None

        head = Link()
        node = head
        for _ in range(9):
            nxt = Link()
            node.orig = nxt
            node = nxt
        node.sqlstate = "40P01"  # type: ignore[attr-defined]

        assert sqlstate(head) is None


class TestClassification:
    def test_deadlock(self) -> None:
        exc = wrapped(AsyncpgStyleError(DEADLOCK_DETECTED))
        assert is_deadlock(exc)
        assert not is_serialization_failure(exc)
        assert not is_lock_unavailable(exc)

    def test_serialization_failure(self) -> None:
        exc = wrapped(AsyncpgStyleError(SERIALIZATION_FAILURE))
        assert is_serialization_failure(exc)
        assert not is_deadlock(exc)

    def test_lock_not_available(self) -> None:
        exc = wrapped(AsyncpgStyleError(LOCK_NOT_AVAILABLE))
        assert is_lock_unavailable(exc)
        assert not is_deadlock(exc)

    @pytest.mark.parametrize("code", sorted(RETRYABLE_SQLSTATES))
    def test_the_default_set_is_retryable(self, code: str) -> None:
        assert is_retryable_conflict(wrapped(AsyncpgStyleError(code)))

    @pytest.mark.parametrize(
        "code",
        [
            "23505",  # unique_violation — will fail identically forever
            "23503",  # foreign_key_violation
            "23514",  # check_violation
            LOCK_NOT_AVAILABLE,  # the caller asked not to wait
            IN_FAILED_SQL_TRANSACTION,  # a symptom of a missing rollback
        ],
    )
    def test_a_durable_failure_is_not_retryable(self, code: str) -> None:
        """Retrying any of these is at best a waste and at worst a hidden bug.

        25P02 is the interesting one: it means an earlier error left the
        transaction aborted, so retrying inside it can only produce 25P02
        again. It is what the rollback in `src/locking/retry.py` prevents, not
        something to spin on.
        """
        assert not is_retryable_conflict(wrapped(AsyncpgStyleError(code)))

    def test_a_non_database_exception_is_not_retryable(self) -> None:
        """A bug in the work is not a conflict, and waiting will not fix it."""
        assert not is_retryable_conflict(TypeError("wrong argument"))

    def test_the_retryable_set_can_be_narrowed(self) -> None:
        deadlocks_only = frozenset({DEADLOCK_DETECTED})
        serialization = wrapped(AsyncpgStyleError(SERIALIZATION_FAILURE))

        assert is_retryable_conflict(serialization)
        assert not is_retryable_conflict(serialization, codes=deadlocks_only)


class TestLockNotAvailableError:
    def test_is_a_409_with_its_own_code(self) -> None:
        exc = LockNotAvailableError()

        assert exc.status_code == 409
        assert exc.error_code == "LOCK_NOT_AVAILABLE"

    def test_is_a_conflict_so_the_edge_needs_no_special_case(self) -> None:
        """It renders through the existing `AppException` handler as a 409
        rather than escaping as an unhandled 500."""
        assert isinstance(LockNotAvailableError(), ConflictError)

    def test_carries_a_message_and_details(self) -> None:
        exc = LockNotAvailableError("held by another writer", details={"row": "1"})

        assert exc.message == "held by another writer"
        assert exc.details == {"row": "1"}

    def test_error_code_differs_from_a_durable_conflict(self) -> None:
        """The point of the subclass: "try again in a moment" and "this will
        never work" must not arrive as the same machine-readable code."""
        assert LockNotAvailableError().error_code != ConflictError().error_code
