"""Build gates and operational one-offs, importable as a package.

The file is here for one reason: `run_load_test.py` imports `smoke_start` for
the uvicorn-process handling they share, and without an `__init__.py` mypy
reaches the same file under two module names — `smoke_start` when it walks the
directory and `scripts.smoke_start` through that import — and refuses to check
anything at all. `tests/` already imports these modules as `scripts.*`
(`test_mutation_score_gate.py`, `test_tenancy_isolation.py`), so the package
name was the one already in use; this makes it the only one.

Nothing is exported here, and nothing should be: each module is run as
`uv run python scripts/<name>.py` and the package exists for the import graph.
"""
