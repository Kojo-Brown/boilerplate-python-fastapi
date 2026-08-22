"""The fitness function for immutability across `src/`.

Three rules, each with an explicit exemption table. The tables are the point:
a mutable value object is sometimes right, and the way to say so is to name it
here with a reason, not to leave it looking like an oversight. A name absent
from a table fails, so an exemption cannot be acquired by accident.

None of this asserts that the codebase *uses* immutability well — that is what
the per-module suites are for. It asserts that the decision has been made
deliberately in every case, which is the property that decays silently.
"""

from __future__ import annotations

import ast
import dataclasses
import importlib
import pathlib
import pkgutil
from collections.abc import Iterator
from types import ModuleType
from typing import Any, Final

import pytest
from pydantic import BaseModel, ValidationError

import src

SRC = pathlib.Path(src.__file__).resolve().parent

# Container types that are mutable at runtime. A module-level `Final` name
# holding one of these is a shared, writable global no matter what the
# annotation says.
MUTABLE_CONTAINERS: Final[tuple[type, ...]] = (dict, list, set, bytearray)

# Module-level `Final` names allowed to hold a mutable container, with the
# reason. Empty today, and that is the interesting part: every constant in
# `src/` is now genuinely constant.
MUTABLE_CONSTANT_EXEMPTIONS: Final[dict[str, str]] = {}

# Dataclasses that carry state on purpose and therefore cannot be frozen.
UNFROZEN_DATACLASS_EXEMPTIONS: Final[dict[str, str]] = {
    "src.decorators.cache._Entry": (
        "One cached value and its expiry. It is storage that the cache "
        "rewrites in place, not a value the caller is handed."
    ),
    "src.distributed_lock.memory._Held": (
        "The in-memory lock backend's record of a held lease, mutated under "
        "the backend's own lock as the lease is renewed."
    ),
}

# Pydantic models allowed to stay mutable, with the reason.
UNFROZEN_MODEL_EXEMPTIONS: Final[dict[str, str]] = {}


def _iter_modules() -> Iterator[ModuleType]:
    """Import every module under `src/` so its objects can be inspected.

    Importing is required rather than incidental: a dataclass's `frozen` flag
    and a model's `model_config` exist only on the built class, and reading
    them from the AST would mean reimplementing decorator and inheritance
    resolution well enough to be trusted.
    """
    yield src
    for info in pkgutil.walk_packages(src.__path__, prefix="src."):
        yield importlib.import_module(info.name)


ALL_MODULES: Final[tuple[ModuleType, ...]] = tuple(_iter_modules())


def _qualified(obj: type) -> str:
    return f"{obj.__module__}.{obj.__qualname__}"


def _defined_here(obj: type, module: ModuleType) -> bool:
    """Whether `obj` was defined in `module` rather than imported into it.

    Without this every re-export is inspected once per importer, and a failure
    names whichever module happened to be walked first.
    """
    return getattr(obj, "__module__", None) == module.__name__


def _final_names(path: pathlib.Path) -> list[str]:
    """Module-level names annotated `Final`, read from the source.

    The AST rather than `typing.get_type_hints`: most modules here use
    `from __future__ import annotations`, so the runtime annotations are
    strings, and resolving them means every name in every module having to
    evaluate — including the ones only imported under `TYPE_CHECKING`, which
    by construction cannot.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: list[str] = []
    for node in tree.body:
        if not isinstance(node, ast.AnnAssign) or not isinstance(node.target, ast.Name):
            continue
        annotation = node.annotation
        head = annotation.value if isinstance(annotation, ast.Subscript) else annotation
        if isinstance(head, ast.Name) and head.id == "Final":
            names.append(node.target.id)
        elif isinstance(head, ast.Attribute) and head.attr == "Final":
            names.append(node.target.id)
    return names


def _module_path(module: ModuleType) -> pathlib.Path | None:
    filename = getattr(module, "__file__", None)
    return pathlib.Path(filename) if filename else None


# ---------------------------------------------------------------------------
# The problem being solved
# ---------------------------------------------------------------------------


class TestTheProblemBeingSolved:
    """What `Final` and `frozen=True` do not do, asserted against Python itself.

    These are the failures the rest of this file exists to prevent, written as
    tests so the justification for `FrozenDict` fails the build if it ever
    stops being true rather than sitting in a docstring going stale.
    """

    def test_final_does_not_prevent_mutating_the_constant(self) -> None:
        table: Final[dict[str, int]] = {"USD": 2}

        # Not a type error and not a runtime error: `Final` is about the name,
        # never about the object. At module scope this is every request the
        # process serves from then on.
        table["USD"] = 5

        assert table["USD"] == 5

    def test_frozen_dataclass_keeps_the_callers_container(self) -> None:
        @dataclasses.dataclass(frozen=True)
        class Value:
            data: dict[str, str]

        source = {"k": "original"}
        value = Value(data=source)

        source["k"] = "tampered"

        # The dataclass is frozen. The value object is not: nothing copied, so
        # whoever built the dict still owns what is inside it.
        assert value.data["k"] == "tampered"

    def test_frozen_pydantic_model_keeps_a_mutable_field(self) -> None:
        class Model(BaseModel):
            model_config = {"frozen": True}

            data: dict[str, str]

        model = Model(data={"k": "original"})

        with pytest.raises(ValueError, match="frozen"):
            model.data = {}  # type: ignore[misc]

        # Assignment to the field is refused; assignment *through* it is not.
        model.data["k"] = "tampered"
        assert model.data["k"] == "tampered"


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("module", ALL_MODULES, ids=lambda m: m.__name__)
def test_final_constants_are_not_mutable_containers(module: ModuleType) -> None:
    """A `Final` name must not hold a dict, list, set or bytearray.

    This is the rule that catches the regression the other two cannot: adding
    one entry to a status table as a plain dict literal looks exactly like the
    frozen version at the call site and reads the same in review.
    """
    path = _module_path(module)
    if path is None:
        pytest.skip("namespace package")

    offenders = []
    for name in _final_names(path):
        value = getattr(module, name, None)
        if not isinstance(value, MUTABLE_CONTAINERS):
            continue
        if f"{module.__name__}.{name}" in MUTABLE_CONSTANT_EXEMPTIONS:
            continue
        offenders.append(f"{name} is a {type(value).__name__}")

    assert not offenders, (
        f"{module.__name__} declares Final constants that can still be mutated: "
        f"{', '.join(offenders)}. Use FrozenDict, frozenset or a tuple, or add "
        f"the name to MUTABLE_CONSTANT_EXEMPTIONS with a reason."
    )


@pytest.mark.parametrize("module", ALL_MODULES, ids=lambda m: m.__name__)
def test_dataclasses_are_frozen(module: ModuleType) -> None:
    offenders = []
    for name in dir(module):
        obj = getattr(module, name)
        if not isinstance(obj, type) or not dataclasses.is_dataclass(obj):
            continue
        if not _defined_here(obj, module):
            continue
        if obj.__dataclass_params__.frozen:  # type: ignore[attr-defined]
            continue
        if _qualified(obj) in UNFROZEN_DATACLASS_EXEMPTIONS:
            continue
        offenders.append(_qualified(obj))

    assert not offenders, (
        f"Unfrozen dataclasses in {module.__name__}: {', '.join(offenders)}. "
        f"Add frozen=True, or record the name in "
        f"UNFROZEN_DATACLASS_EXEMPTIONS with the reason it holds state."
    )


@pytest.mark.parametrize("module", ALL_MODULES, ids=lambda m: m.__name__)
def test_pydantic_models_are_frozen(module: ModuleType) -> None:
    offenders = []
    for name in dir(module):
        obj = getattr(module, name)
        if not isinstance(obj, type) or not issubclass(obj, BaseModel):
            continue
        if obj is BaseModel or not _defined_here(obj, module):
            continue
        if obj.model_config.get("frozen"):
            continue
        if _qualified(obj) in UNFROZEN_MODEL_EXEMPTIONS:
            continue
        offenders.append(_qualified(obj))

    assert not offenders, (
        f"Unfrozen Pydantic models in {module.__name__}: {', '.join(offenders)}. "
        f"Add frozen=True to model_config, or record the name in "
        f"UNFROZEN_MODEL_EXEMPTIONS with a reason."
    )


def test_the_exemption_tables_have_no_stale_entries() -> None:
    """An exemption for something that no longer exists is a lie about the code.

    Without this the tables only ever grow: a class gets deleted or frozen, its
    entry stays, and the next reader believes there is still a mutable value
    object where there is not.
    """
    known: set[str] = set()
    for module in ALL_MODULES:
        path = _module_path(module)
        if path is not None:
            known.update(f"{module.__name__}.{n}" for n in _final_names(path))
        for name in dir(module):
            obj = getattr(module, name)
            if isinstance(obj, type) and _defined_here(obj, module):
                known.add(_qualified(obj))

    stale = sorted(
        (
            set(MUTABLE_CONSTANT_EXEMPTIONS)
            | set(UNFROZEN_DATACLASS_EXEMPTIONS)
            | set(UNFROZEN_MODEL_EXEMPTIONS)
        )
        - known
    )
    assert not stale, f"Exemptions naming things that no longer exist: {stale}"


def test_every_exemption_carries_a_reason() -> None:
    tables: dict[str, dict[str, str]] = {
        "MUTABLE_CONSTANT_EXEMPTIONS": MUTABLE_CONSTANT_EXEMPTIONS,
        "UNFROZEN_DATACLASS_EXEMPTIONS": UNFROZEN_DATACLASS_EXEMPTIONS,
        "UNFROZEN_MODEL_EXEMPTIONS": UNFROZEN_MODEL_EXEMPTIONS,
    }
    empty = [
        f"{table}[{key!r}]"
        for table, entries in tables.items()
        for key, reason in entries.items()
        if not reason.strip()
    ]
    assert not empty, f"Exemptions without a stated reason: {empty}"


def test_the_settings_singleton_cannot_be_written_to() -> None:
    """The one mutable global whose reach was the whole application.

    `settings` is imported by name in two dozen modules, and several
    subsystems build themselves from it once and cache the result — the
    `@cache`d `get_strategy`, the storage and payment registries, the
    idempotency and lock backends constructed in the lifespan. A write here
    used to take effect for whatever had not been built yet and for nothing
    else, which is a configuration change that appears to work.
    """
    from src.config import settings

    with pytest.raises(ValidationError):
        settings.PAYMENT_GATEWAY = "paypal"  # type: ignore[misc]


def test_a_validated_request_body_cannot_be_edited_in_place() -> None:
    """The handler-side half of freezing the schemas.

    By the time a handler runs, the raw body is gone: the model is the only
    record of what the client sent, so normalising it in place destroys the
    evidence. Deriving a new model says so at the line that does it.
    """
    from src.auth.schemas import RegisterRequest

    request = RegisterRequest(email="user@example.com", password="hunter2hunter2")

    with pytest.raises(ValidationError):
        request.email = "someone-else@example.com"  # type: ignore[misc]

    derived = request.model_copy(update={"email": "someone-else@example.com"})
    assert derived.email == "someone-else@example.com"
    assert request.email == "user@example.com"


def test_the_gate_actually_walks_the_package() -> None:
    """A guard against the walk silently returning nothing.

    Every parametrised test above passes vacuously if `ALL_MODULES` is empty,
    which is exactly what an import error inside `_iter_modules` would produce
    if it were ever caught and skipped.
    """
    names = {module.__name__ for module in ALL_MODULES}
    assert len(names) > 50
    for expected in ("src.config", "src.payments.base", "src.immutable", "src.main"):
        assert expected in names


def _find_class(qualified: str) -> Any:
    module_name, _, class_name = qualified.rpartition(".")
    return getattr(importlib.import_module(module_name), class_name)


@pytest.mark.parametrize("qualified", sorted(UNFROZEN_DATACLASS_EXEMPTIONS))
def test_exempt_dataclasses_are_private_to_their_subsystem(qualified: str) -> None:
    """An exempted mutable dataclass must not be part of a public contract.

    Being state rather than a value is a fine reason to stay mutable, and a
    poor reason to be handed to a caller. Each exempt class is either named
    privately or absent from its module's `__all__`.
    """
    obj = _find_class(qualified)
    module = importlib.import_module(obj.__module__)
    exported = getattr(module, "__all__", ())
    assert obj.__name__.startswith("_") or obj.__name__ not in exported
