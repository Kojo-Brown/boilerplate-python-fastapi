"""What a block of code asked the database, and what made it ask.

An N+1 is not "a lot of queries". It is a query count that **grows with the
number of rows already fetched**, and that distinction is the whole design of
this module: a test that asserts `len(log) == 3` pins an implementation detail
and breaks the next time somebody adds a legitimate statement, while a test
that measures the same block at two row counts and asserts the counts are equal
pins the property the name refers to. Prefer the second shape; see
`tests/test_n_plus_one.py`.

Two independent signals are recorded, because in an async application the
classic N+1 and its ugly cousin are caught by different things:

**Statements**, from the engine's `before_cursor_execute`. This is everything
that reaches the driver, whoever built it — ORM loads, Core statements, the
flush, and a hand-written `select()` inside a `for` loop, which is the N+1 that
no ORM feature protects you from and that no amount of `selectinload` will fix.
Repetition of one SQL *shape* is what betrays it.

**Relationship loads**, from the session's `do_orm_execute`. A statement
carrying `ORMExecuteState.is_relationship_load` was emitted to populate a
relationship rather than because anyone asked for it, and
`ORMExecuteState.lazy_loaded_from` separates the two ways that happens:
non-`None` means a *lazy* load fired for one already-loaded parent — the N+1 in
its textbook form — while `None` means an eager strategy issued its one extra
statement for the whole batch, which is `selectinload` doing its job. Counting
relationship loads alone would condemn both; the `lazy` flag is what makes the
signal usable.

Why both are needed, stated once: `assert_no_lazy_loads` catches a relationship
traversal and says which attribute, which a statement count cannot tell you.
`assert_no_repeated_statements` catches the loop, which the ORM never hears
about because every iteration is an ordinary top-level query. Neither subsumes
the other, and `assert_no_n_plus_one` runs both.

**Scope.** The listener attaches to the *engine* behind the session, so it sees
every statement that engine sends while the block is open, including any from
another session sharing it. That is deliberate — resolving a statement back to
its session from inside `before_cursor_execute` means asking the session for its
connection, which opens one when the answer is "none yet", and doing I/O there
is exactly the greenlet violation this file exists to help find. The tests give
each case its own engine, which makes the question moot.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import event
from sqlalchemy.engine import Connection, ExecutionContext
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import ORMExecuteState

#: How much of a statement is shown in a failure message. Long enough that the
#: table and the first columns are visible, short enough that ten of them are
#: still readable in pytest output.
EXCERPT_LENGTH = 120

_WHITESPACE = re.compile(r"\s+")


def _excerpt(sql: str) -> str:
    """`sql`, truncated to something that fits on a line of pytest output."""
    if len(sql) <= EXCERPT_LENGTH:
        return sql
    return f"{sql[:EXCERPT_LENGTH]}…"


class NPlusOneDetected(AssertionError):
    """Raised by the `assert_*` helpers below.

    An `AssertionError` subclass so pytest reports it as a failed assertion
    rather than an error, and a named one so a test can assert *which* check
    fired without matching on message text.
    """


@dataclass(frozen=True, slots=True)
class Statement:
    """One call into the driver."""

    #: The SQL as sent, with parameters still as placeholders. Two statements
    #: that differ only in their parameters are one shape, which is what makes
    #: repetition detectable at all.
    sql: str

    #: How many parameter sets went with it: 1 normally, and the batch size for
    #: an `executemany`. Kept separate from the statement count because the two
    #: answer different questions — SQLAlchemy's unit of work collapses N
    #: single-row UPDATEs into one `executemany`, so "how many round trips" and
    #: "how many rows were written" genuinely differ, and reporting only the
    #: first would make a batched write look like magic while reporting only
    #: the second would make it look like an N+1.
    parameter_sets: int

    executemany: bool

    @property
    def shape(self) -> str:
        """The statement with its whitespace collapsed, for grouping.

        SQLAlchemy renders the same construct identically every time, so this
        is a normalisation for readability rather than a fuzzy match: two
        shapes that compare equal really were the same statement.
        """
        return _WHITESPACE.sub(" ", self.sql).strip()

    @property
    def excerpt(self) -> str:
        return _excerpt(self.shape)


@dataclass(frozen=True, slots=True)
class RelationshipLoad:
    """A statement the ORM emitted to populate a relationship."""

    #: `Class.attribute`, e.g. `User.refresh_tokens`.
    path: str

    #: Whether this fired for a single already-loaded parent (a lazy load, and
    #: therefore one of N) rather than once for the whole batch.
    lazy: bool


@dataclass(eq=False)
class QueryLog:
    """Everything observed between entering and leaving `capture_queries`."""

    statements: list[Statement] = field(default_factory=list)
    relationship_loads: list[RelationshipLoad] = field(default_factory=list)

    def __len__(self) -> int:
        """The number of round trips — the number to assert on."""
        return len(self.statements)

    @property
    def parameter_sets(self) -> int:
        """Rows sent, counting each `executemany` batch by its size."""
        return sum(statement.parameter_sets for statement in self.statements)

    @property
    def lazy_loads(self) -> list[RelationshipLoad]:
        return [load for load in self.relationship_loads if load.lazy]

    @property
    def eager_loads(self) -> list[RelationshipLoad]:
        return [load for load in self.relationship_loads if not load.lazy]

    def shapes(self) -> Counter[str]:
        """How many times each distinct statement was sent."""
        return Counter(statement.shape for statement in self.statements)

    def repeated(self, limit: int = 1) -> list[tuple[str, int]]:
        """Shapes sent more than `limit` times, commonest first."""
        return [
            (shape, count)
            for shape, count in self.shapes().most_common()
            if count > limit
        ]

    def summary(self) -> str:
        """A readable dump, used in every failure message below.

        Both lists are grouped and counted rather than printed one entry per
        occurrence: the logs worth reading are the bad ones, and a thousand-row
        N+1 that answered with a thousand identical lines would bury its own
        diagnosis in pytest output.
        """
        lines = [
            f"{len(self.statements)} statement(s), "
            f"{self.parameter_sets} parameter set(s), "
            f"{len(self.lazy_loads)} lazy relationship load(s)"
        ]
        lines += [
            f"  {count}x {_excerpt(shape)}"
            for shape, count in self.shapes().most_common()
        ]
        loads = Counter(
            ("lazy" if load.lazy else "eager", load.path)
            for load in self.relationship_loads
        )
        lines += [
            f"  {count}x [{kind} relationship load] {path}"
            for (kind, path), count in loads.most_common()
        ]
        return "\n".join(lines)


def _relationship_name(state: ORMExecuteState) -> str:
    """`Class.attribute` for the relationship being loaded.

    The strategy path is a `PropRegistry` whose `prop` is the relationship
    itself, so the mapped class comes from the property rather than from the
    statement — which matters for an inherited relationship, where the entity
    in the statement is the subclass and the one that declared the attribute is
    not.
    """
    path = state.loader_strategy_path
    prop = getattr(path, "prop", None)
    if prop is None:  # pragma: no cover - a relationship load always has one
        return "<unknown relationship>"
    return f"{prop.parent.class_.__name__}.{prop.key}"


@contextmanager
def capture_queries(session: AsyncSession) -> Iterator[QueryLog]:
    """Record what `session`'s engine sends while the block runs.

    Synchronous rather than async because nothing in it awaits: the listeners
    are attached before the block and removed after it, and everything in
    between is the caller's own `await`s.

    The listeners are removed in a `finally`, so a failing assertion inside the
    block cannot leave a recorder attached to an engine that outlives it —
    which would otherwise show up as a later test's log filling with statements
    it never sent.
    """
    log = QueryLog()
    sync_session = session.sync_session
    engine = sync_session.get_bind().engine

    def _on_cursor_execute(
        conn: Connection,
        cursor: Any,
        statement: str,
        parameters: Sequence[Any] | dict[str, Any] | None,
        context: ExecutionContext | None,
        executemany: bool,
    ) -> None:
        sets = len(parameters) if executemany and parameters is not None else 1
        log.statements.append(
            Statement(sql=statement, parameter_sets=sets, executemany=executemany)
        )

    def _on_orm_execute(state: ORMExecuteState) -> None:
        if not state.is_relationship_load:
            return
        log.relationship_loads.append(
            RelationshipLoad(
                path=_relationship_name(state),
                # Set only when the load was triggered by touching the
                # attribute on one already-loaded instance. An eager strategy
                # runs for the whole batch and leaves it None.
                lazy=state.lazy_loaded_from is not None,
            )
        )

    event.listen(engine, "before_cursor_execute", _on_cursor_execute)
    event.listen(sync_session, "do_orm_execute", _on_orm_execute)
    try:
        yield log
    finally:
        event.remove(engine, "before_cursor_execute", _on_cursor_execute)
        event.remove(sync_session, "do_orm_execute", _on_orm_execute)


def assert_no_lazy_loads(log: QueryLog) -> None:
    """Fail if any relationship was loaded one parent at a time.

    This is the check that names the attribute, which is the hard part of
    fixing an N+1 — a statement count tells you there were nine queries, this
    tells you `User.refresh_tokens` was the reason and therefore where the
    loader option belongs.
    """
    lazy = log.lazy_loads
    if not lazy:
        return
    counts = Counter(load.path for load in lazy)
    offenders = ", ".join(f"{path} ({count}x)" for path, count in counts.most_common())
    raise NPlusOneDetected(
        f"lazy relationship load(s): {offenders}. "
        "Load it eagerly at the query — selectinload() for a collection, "
        "joinedload() for a many-to-one; see docs/n-plus-one.md.\n"
        f"{log.summary()}"
    )


def assert_no_repeated_statements(log: QueryLog, *, limit: int = 1) -> None:
    """Fail if one SQL shape was sent more than `limit` times.

    `limit` defaults to 1 because the interesting block is usually a single
    logical operation, where sending the same statement twice is already the
    beginning of the loop. Raise it deliberately, with a reason, for a block
    that legitimately repeats one statement a *fixed* number of times — and
    prefer measuring at two row counts instead, since a fixed repetition that
    the row count does not change is not an N+1 at all.
    """
    repeated = log.repeated(limit)
    if not repeated:
        return
    offenders = "\n".join(f"  {count}x {_excerpt(shape)}" for shape, count in repeated)
    raise NPlusOneDetected(
        f"statement shape(s) sent more than {limit}x:\n{offenders}\n"
        "If this is a loop issuing one query per row, hoist it into a single "
        "statement; see docs/n-plus-one.md.\n"
        f"{log.summary()}"
    )


def assert_no_n_plus_one(log: QueryLog, *, limit: int = 1) -> None:
    """Both checks, lazy loads first.

    Order matters for the message: a lazy load names the attribute to fix,
    while the repeated shape it also produces only names the SQL, so reporting
    the vaguer of the two first would bury the answer.
    """
    assert_no_lazy_loads(log)
    assert_no_repeated_statements(log, limit=limit)
