"""Make mutmut's module keys agree with this project's `src` package.

Loaded only by mutmut, via `pytest_add_cli_args = ["-p", "mutmut_src_layout"]`
in `pyproject.toml`. A normal `pytest` run never imports this module.

**The conflict.** mutmut 3 assumes the src-*layout*: a directory named `src/`
that is an import root holding packages which are imported *without* a `src.`
prefix. It bakes that in twice — `mutmut.utils.format_utils.get_mutant_name`
calls `strip_prefix(module_name, prefix="src.")` when deriving a mutant key
from a file path, and `mutmut.stats.record_trampoline_hit` asserts outright
that a module name never starts with `src.`. Here `src` is not a layout, it *is*
the distribution's importable package: every module says `from src.foo import
bar` and `[tool.hatch.build.targets.wheel]` ships it under that name.

So the two halves of mutmut disagree about the same function. From the file
`src/pagination.py` it derives the key `pagination.x_build_page__mutmut_1`,
while the suite imports `src.pagination` and the trampoline reports a hit for
`src.pagination.build_page`. Nothing matches. mutmut fails loudly rather than
silently — first that assertion, and behind it a guard that exits with "tests
recorded trampoline hits but none match any mutant key" — which is the good
case: the bad one would be a green gate measuring nothing.

**The fix.** mutmut puts both `mutants/` and `mutants/src/` on `sys.path`, so
the mutated copy of `pagination.py` is reachable under two names: `pagination`,
which mutmut keys on, and `src.pagination`, which the suite asks for. Left
alone Python executes the file twice and produces two unrelated module objects,
each with its own trampoline state — the tests would exercise one while mutmut
activated mutants in the other. This finder collapses the two names onto one
module object, loading it under the name mutmut expects and binding the
`src.`-prefixed name to the same object.

**Why not something smaller.** Renaming the package is a 400-file change to
suit one dev tool. Putting the aliasing in `src/__init__.py` would ship import
machinery in the wheel to serve something that never runs in production. This
file is the smaller blast radius: it exists only inside a mutation run, and if
mutmut ever stops stripping the prefix, deleting it and the one `-p` argument
is the entire revert.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
from importlib.abc import Loader, MetaPathFinder
from importlib.machinery import ModuleSpec, PathFinder
from types import ModuleType
from typing import Any

_PREFIX = "src."


class _AliasLoader(Loader):
    """Bind an already-imported module to a second name without re-running it."""

    def __init__(self, target: ModuleType) -> None:
        self._target = target

    def create_module(self, spec: ModuleSpec) -> ModuleType:
        return self._target

    def exec_module(self, module: ModuleType) -> None:
        # Already executed under its unprefixed name. Running it again is the
        # one thing this whole file exists to prevent.
        return None


class _SrcAliasFinder(MetaPathFinder):
    """Resolve `src.foo.bar` to the same module object as `foo.bar`.

    `src` itself is left to the normal machinery: `src/__init__.py` is empty, so
    importing it is free and it carries no mutants to confuse.
    """

    def find_spec(
        self,
        fullname: str,
        path: Any = None,
        target: ModuleType | None = None,
    ) -> ModuleSpec | None:
        if not fullname.startswith(_PREFIX):
            return None

        stripped = fullname[len(_PREFIX) :]

        module = sys.modules.get(stripped)
        if module is None:
            module = self._load(stripped)
            if module is None:
                # Not aliasable — a genuinely missing submodule, say. Let the
                # rest of `sys.meta_path` answer so the ImportError names the
                # module the caller actually asked for.
                return None

        # Hand the prefixed name back to the import system to register, and do
        # not leave it in `sys.modules` here. CPython's `_find_spec` samples
        # `is_reload = name in sys.modules` *before* consulting the finders, and
        # if a finder has populated that name by the time it returns, it
        # discards the returned spec and uses `sys.modules[name].__spec__`
        # instead — which is the real file spec this module was built from. The
        # import system then loads it a second time through its own
        # `SourceFileLoader`, and the alias loader below is never called. That
        # is the same double execution `_load` guards against, arriving by a
        # completely different route, and it is why `_load` pre-registers the
        # prefixed name only for the duration of the module body.
        sys.modules.pop(fullname, None)
        return importlib.util.spec_from_loader(fullname, _AliasLoader(module))

    @staticmethod
    def _load(stripped: str) -> ModuleType | None:
        """Import `stripped`, registering *both* names before executing it.

        This is deliberately hand-rolled rather than `importlib.import_module`,
        and the reason is circular imports. `src/models/__init__.py` does
        `from src.models.outbox import OutboxEvent`, so loading `models` re-enters
        this finder for the parent name `src.models` while `models/__init__.py`
        is still running. Python's own machinery survives that because it puts a
        module into `sys.modules` *before* executing it, so a cycle sees the
        partially initialised object; `import_module` would do that for
        `models` but nothing would have claimed `src.models` yet, and the normal
        machinery would answer that name by building a second module object from
        the same file. That failure surfaces nowhere near its cause — as
        `sqlalchemy.exc.InvalidRequestError: Table 'outbox_events' is already
        defined for this MetaData instance`, from the second execution of a
        declarative model.

        So both names go into `sys.modules` up front, and a cycle arriving under
        either one finds the same half-built module.
        """
        parent_name, _, child = stripped.rpartition(".")
        parent: ModuleType | None = None
        search_path: Any = None

        if parent_name:
            parent = sys.modules.get(parent_name)
            if parent is None:
                parent = _SrcAliasFinder._load(parent_name)
                if parent is None:
                    return None

            # Executing the parent's `__init__.py` can import this very module
            # as a side effect — `src/models/__init__.py` imports every model
            # in the package, so resolving `models.user` loads `models`, which
            # loads `models.user`. CPython's own `_find_and_load_unlocked` has
            # this same re-check, under the comment "Crazy side-effects!".
            # Without it the module is executed a second time here, and for a
            # declarative model the second execution is the one that raises
            # `InvalidRequestError: Table '...' is already defined`.
            already = sys.modules.get(stripped)
            if already is not None:
                sys.modules[_PREFIX + stripped] = already
                return already

            search_path = getattr(parent, "__path__", None)
            if search_path is None:
                # The parent is a module, not a package, so `stripped` cannot
                # name anything.
                return None

        spec = PathFinder.find_spec(stripped, search_path)
        if spec is None or spec.loader is None:
            return None

        module = importlib.util.module_from_spec(spec)
        sys.modules[stripped] = module
        sys.modules[_PREFIX + stripped] = module
        try:
            spec.loader.exec_module(module)
        except BaseException:
            # Leaving a half-built module registered would make the next import
            # of it succeed and hand out something broken.
            sys.modules.pop(stripped, None)
            sys.modules.pop(_PREFIX + stripped, None)
            raise

        if parent is not None:
            # What the import system does after a successful submodule import;
            # `from src.models import user` reads the attribute, not sys.modules.
            setattr(parent, child, module)

        return module


def _install() -> None:
    if not any(isinstance(f, _SrcAliasFinder) for f in sys.meta_path):
        sys.meta_path.insert(0, _SrcAliasFinder())


# At import time, not from `pytest_configure`, and that is the subtlety of this
# file. `tests/conftest.py` imports `src.*` at module scope, and pytest loads
# the initial conftests in `pytest_load_initial_conftests` — which runs *before*
# `pytest_configure`. A finder installed from that hook would arrive to find
# every `src.*` module already in `sys.modules` under its prefixed name, alias
# nothing, and leave mutmut failing exactly as it does without this file. A `-p`
# plugin module, by contrast, is imported before the initial conftests are read.
#
# It also has to hold for the whole process rather than one pytest run: mutmut
# calls `pytest.main()` repeatedly in-process (stats collection, the clean
# baseline, then a run per mutant) and `sys.modules` persists across them, so
# whichever run imports a module first decides the name it keeps.
_install()
