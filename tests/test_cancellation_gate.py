"""The fitness function for cancellation and task ownership across `src/`.

Two rules, each with an explicit exemption table, in the style of
`tests/test_immutability_gate.py`. The tables are the point: swallowing a
cancellation is occasionally right, and the way to say so is to name the
function here with a reason, not to leave it looking like the ordinary mistake
it resembles.

Both rules exist because the mistakes they catch are *silent*. A task nobody
owns does not raise; it is collected, or it fails and the exception is never
retrieved. A swallowed `CancelledError` does not raise either; the task keeps
running after everything above it has given up, and shutdown waits for
something that will never end — or does not wait, and truncates it. Neither
shows up in a test of the feature that contains it, and both are one-line
regressions.

This asserts nothing about whether the codebase *uses* cancellation well; the
per-module suites do that. It asserts that every place it is caught, somebody
decided.
"""

from __future__ import annotations

import ast
import pathlib
from collections.abc import Iterator
from typing import Final, NamedTuple

import pytest

import src

SRC = pathlib.Path(src.__file__).resolve().parent


class Site(NamedTuple):
    """One place in `src/` a rule looked at."""

    #: `module.Class.function`, the key an exemption is written against. The
    #: line number is deliberately not part of it: an exemption keyed by line
    #: would expire on the next unrelated edit above it.
    qualname: str
    module: str
    lineno: int
    source: str


# ---------------------------------------------------------------------------
# Rule 1 — every task has an owner
# ---------------------------------------------------------------------------

# Calls that hand back a task nobody else is holding. `TaskGroup.create_task`
# and `TaskScope.start_soon` are deliberately absent: those *are* the owner, so
# discarding what they return is the intended way to use them.
UNOWNED_TASK_STARTERS: Final[frozenset[str]] = frozenset(
    {
        "asyncio.create_task",
        "asyncio.ensure_future",
        "create_task",
        "ensure_future",
        "loop.create_task",
        "loop.ensure_future",
        "self._loop.create_task",
        "self._loop.ensure_future",
    }
)

# Discarded task starts allowed to stay, with the reason. Empty, and that is
# the interesting part: every task in `src/` is held by something.
UNOWNED_TASK_EXEMPTIONS: Final[dict[str, str]] = {}


# ---------------------------------------------------------------------------
# Rule 2 — a caught cancellation is re-raised
# ---------------------------------------------------------------------------

# Exception names whose handler will receive a `CancelledError`. `Exception` is
# not among them and that is not an oversight: `CancelledError` has inherited
# from `BaseException` since 3.8 precisely so that ordinary defensive handling
# cannot absorb it. A bare `except:` can, which is why `None` is matched too.
CANCELLATION_CATCHERS: Final[frozenset[str]] = frozenset(
    {
        "asyncio.CancelledError",
        "CancelledError",
        "BaseException",
        "exceptions.CancelledError",
    }
)

# Handlers that catch a cancellation and deliberately do not re-raise it.
SWALLOWED_CANCELLATION_EXEMPTIONS: Final[dict[str, str]] = {
    "src.outbox.relay.OutboxRelay.stop": (
        "The cancellation is this method's own — it called `task.cancel()` one "
        "line above and is awaiting the unwinding. Re-raising would cancel "
        "whoever is shutting the application down, which is the opposite of "
        "what asking a relay to stop should do."
    ),
    "src.kafka.runner.ConsumerRunner.stop": (
        "The same shape as `OutboxRelay.stop`, one line at a time: this method "
        "cancelled the consume loop itself and is awaiting the unwinding — "
        "which is what leaves the consumer group. Re-raising would cancel "
        "whoever is shutting the application down."
    ),
    "src.distributed_lock.lock.DistributedLock._stop_renewer": (
        "Same shape: the renewal task was cancelled by this method, so the "
        "`CancelledError` is the acknowledgement rather than a request. A "
        "cancellation aimed at the *caller* arrives on the caller's task and "
        "is unaffected."
    ),
    "src.structured.cancel._drain": (
        "The whole purpose of the module. The cancellation is absorbed so the "
        "cleanup underneath can finish, and it is returned to the caller — "
        "`protect` re-raises it, `finalize` re-arms it on the current task — "
        "so it is deferred by the length of the cleanup rather than dropped."
    ),
}


def _iter_source_files() -> Iterator[pathlib.Path]:
    return (
        path for path in sorted(SRC.rglob("*.py")) if "migrations" not in path.parts
    )


def _module_name(path: pathlib.Path) -> str:
    relative = path.relative_to(SRC.parent).with_suffix("")
    parts = [part for part in relative.parts if part != "__init__"]
    return ".".join(parts)


def _dotted(node: ast.expr) -> str | None:
    """Render `a.b.c` and `a.b.c()` as a dotted string, or `None` if it is not.

    Calls collapse to their callee so that `asyncio.get_running_loop().create_task`
    is not silently different from `loop.create_task`.
    """
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _dotted(node.value)
        return None if prefix is None else f"{prefix}.{node.attr}"
    if isinstance(node, ast.Call):
        callee = _dotted(node.func)
        if callee in {"asyncio.get_running_loop", "asyncio.get_event_loop"}:
            return "loop"
        return None
    return None


class _Visitor(ast.NodeVisitor):
    """Collects both rules' sites in one pass, tracking the enclosing qualname."""

    def __init__(self, module: str, source: str) -> None:
        self.module = module
        self._lines = source.splitlines()
        self._stack: list[str] = []
        self.unowned: list[Site] = []
        self.swallowed: list[Site] = []

    def _qualname(self) -> str:
        return ".".join([self.module, *self._stack])

    def _site(self, node: ast.AST) -> Site:
        lineno = getattr(node, "lineno", 0)
        return Site(
            qualname=self._qualname(),
            module=self.module,
            lineno=lineno,
            source=(
                self._lines[lineno - 1].strip()
                if 0 < lineno <= len(self._lines)
                else ""
            ),
        )

    def _enter(self, node: ast.AST, name: str) -> None:
        self._stack.append(name)
        self.generic_visit(node)
        self._stack.pop()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self._enter(node, node.name)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._enter(node, node.name)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._enter(node, node.name)

    def visit_Expr(self, node: ast.Expr) -> None:
        # An `Expr` is a statement whose value is thrown away. `await
        # something()` is an `Await` wrapping the call, so it is not matched —
        # awaiting is one way of owning the result.
        if isinstance(node.value, ast.Call):
            callee = _dotted(node.value.func)
            if callee in UNOWNED_TASK_STARTERS:
                self.unowned.append(self._site(node))
        self.generic_visit(node)

    def visit_ExceptHandler(self, node: ast.ExceptHandler) -> None:
        if _catches_cancellation(node) and not _contains_raise(node.body):
            self.swallowed.append(self._site(node))
        self.generic_visit(node)


def _catches_cancellation(handler: ast.ExceptHandler) -> bool:
    if handler.type is None:
        return True
    caught = (
        handler.type.elts if isinstance(handler.type, ast.Tuple) else [handler.type]
    )
    return any(_dotted(entry) in CANCELLATION_CATCHERS for entry in caught)


def _contains_raise(body: list[ast.stmt]) -> bool:
    """Whether any `raise` appears in the handler.

    Deliberately shallow about *which* exception and *under what condition*: a
    `raise` guarded by an `if` still counts, and encoding "re-raises on every
    path" would mean writing a reachability analysis to catch a mistake that
    has never been made. What is being defended against is the handler with no
    `raise` in it at all, which is what swallowing actually looks like.
    """
    return any(isinstance(node, ast.Raise) for stmt in body for node in ast.walk(stmt))


def _collect() -> tuple[list[Site], list[Site]]:
    unowned: list[Site] = []
    swallowed: list[Site] = []
    for path in _iter_source_files():
        source = path.read_text(encoding="utf-8")
        visitor = _Visitor(_module_name(path), source)
        visitor.visit(ast.parse(source, filename=str(path)))
        unowned.extend(visitor.unowned)
        swallowed.extend(visitor.swallowed)
    return unowned, swallowed


UNOWNED_SITES, SWALLOWED_SITES = _collect()


class TestEveryTaskHasAnOwner:
    def test_no_discarded_task_start(self) -> None:
        offenders = [
            f"{site.module}:{site.lineno}  {site.source}"
            for site in UNOWNED_SITES
            if site.qualname not in UNOWNED_TASK_EXEMPTIONS
        ]
        assert not offenders, (
            "Task started without an owner. The loop holds only a weak "
            "reference, so it can be collected mid-await, and its exception is "
            "never retrieved. Use `TaskScope` from `src/structured`, keep the "
            "returned task, or add the function to UNOWNED_TASK_EXEMPTIONS "
            "with a reason:\n  " + "\n  ".join(offenders)
        )

    def test_the_rule_can_actually_see_a_violation(self) -> None:
        """A gate nobody has watched fail is a gate nobody knows works."""
        source = "import asyncio\nasync def f():\n    asyncio.create_task(g())\n"
        visitor = _Visitor("probe", source)
        visitor.visit(ast.parse(source))

        assert [site.qualname for site in visitor.unowned] == ["probe.f"]

    def test_an_owned_task_is_not_flagged(self) -> None:
        source = (
            "import asyncio\n"
            "async def f():\n"
            "    task = asyncio.create_task(g())\n"
            "    await asyncio.create_task(h())\n"
            "    group.create_task(i())\n"
        )
        visitor = _Visitor("probe", source)
        visitor.visit(ast.parse(source))

        assert visitor.unowned == []


class TestCaughtCancellationIsReRaised:
    def test_no_handler_swallows_a_cancellation(self) -> None:
        offenders = [
            f"{site.qualname} ({site.module}:{site.lineno})"
            for site in SWALLOWED_SITES
            if site.qualname not in SWALLOWED_CANCELLATION_EXEMPTIONS
        ]
        assert not offenders, (
            "Cancellation caught and not re-raised. The task keeps running "
            "after everything above it has stopped waiting, so shutdown either "
            "hangs on it or truncates it. Re-raise, or add the function to "
            "SWALLOWED_CANCELLATION_EXEMPTIONS with a reason:\n  "
            + "\n  ".join(offenders)
        )

    @pytest.mark.parametrize(
        "clause",
        [
            "except asyncio.CancelledError:",
            "except CancelledError:",
            "except BaseException:",
            "except (ValueError, asyncio.CancelledError):",
            "except:",
        ],
    )
    def test_the_rule_can_actually_see_a_violation(self, clause: str) -> None:
        source = f"async def f():\n    try:\n        pass\n    {clause}\n        pass\n"
        visitor = _Visitor("probe", source)
        visitor.visit(ast.parse(source))

        assert [site.qualname for site in visitor.swallowed] == ["probe.f"]

    @pytest.mark.parametrize(
        "body",
        ["raise", "raise RuntimeError", "if x:\n            raise"],
    )
    def test_a_re_raising_handler_is_not_flagged(self, body: str) -> None:
        source = (
            f"async def f():\n    try:\n        pass\n"
            f"    except asyncio.CancelledError:\n        {body}\n"
        )
        visitor = _Visitor("probe", source)
        visitor.visit(ast.parse(source))

        assert visitor.swallowed == []

    def test_catching_plain_exception_is_not_a_cancellation_handler(self) -> None:
        """`CancelledError` is a `BaseException`, so this cannot catch it."""
        source = (
            "async def f():\n    try:\n        pass\n"
            "    except Exception:\n        pass\n"
        )
        visitor = _Visitor("probe", source)
        visitor.visit(ast.parse(source))

        assert visitor.swallowed == []


class TestTheExemptionTables:
    @pytest.mark.parametrize(
        ("table", "sites"),
        [
            (UNOWNED_TASK_EXEMPTIONS, UNOWNED_SITES),
            (SWALLOWED_CANCELLATION_EXEMPTIONS, SWALLOWED_SITES),
        ],
    )
    def test_every_exemption_is_still_needed(
        self, table: dict[str, str], sites: list[Site]
    ) -> None:
        """A stale exemption is a rule that stopped applying without anyone
        noticing, and the next real violation would inherit its licence."""
        present = {site.qualname for site in sites}
        assert not (set(table) - present), (
            "Exemption no longer matches anything in src/; delete it: "
            f"{sorted(set(table) - present)}"
        )

    @pytest.mark.parametrize(
        "table",
        [UNOWNED_TASK_EXEMPTIONS, SWALLOWED_CANCELLATION_EXEMPTIONS],
    )
    def test_every_exemption_gives_a_reason(self, table: dict[str, str]) -> None:
        assert not [name for name, reason in table.items() if len(reason.strip()) < 20]
