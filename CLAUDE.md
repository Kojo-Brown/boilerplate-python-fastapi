# boilerplate-python-fastapi — Agent Instructions

## What this repo is
Production-grade FastAPI + Python 3.14 async API boilerplate. Spec-driven and PR-driven: one `SPEC.md` item per run.

## Your job (scheduled agent, every 4h)
1. `git checkout main && git pull --ff-only origin main`
2. Read `SPEC.md`, take the **first** `- [ ]` item. Phase 0 items always win.
3. `git checkout -b <type>/<kebab-slug>` (`feat`/`fix`/`chore`/`ci`/`docs`)
4. Implement it completely — source, types, tests, docs.
5. Run every gate locally; **all must pass** before pushing:
   ```
   uv sync
   uv run mypy .
   uv run ruff check .
   uv run pytest
   uv run python -c 'from src.main import app'
   uv run mutmut run && uv run mutmut export-cicd-stats
   uv run python scripts/check_mutation_score.py
   uv sync --group loadtest
   uv run python scripts/run_load_test.py
   uv run python scripts/check_load_budget.py
   ```
   The import check needs a `.env` (`cp .env.example .env`) — settings are
   validated at import time, so it fails without `DATABASE_URL` and `SECRET_KEY`.
   The mutation gate is the `mutation` CI job and blocks a merge like the rest, so
   it belongs in this list; it is ~45s because it is scoped to eight modules, and
   `mutmut run` exits 0 even when mutants survive, so the threshold is the
   separate script. See `docs/mutation-testing.md` before changing the threshold
   or the `only_mutate` list — and never lower either to turn a red build green.
   The load gate is the `load` CI job and blocks a merge like the rest, so it
   belongs in this list too. It is the last two lines because it is the slowest
   thing here that is not the suite — about seventy seconds, of which sixty are
   the run itself — and because it wants a Postgres and a Redis on
   `DATABASE_URL`/`REDIS_URL`, the same two the suite wants. `uv sync
   --group loadtest` is separate on purpose: locust registers a pytest plugin
   and no other gate installs it (see `[dependency-groups]` in
   `pyproject.toml`). `locust --headless` exits 0 at any latency whatsoever, so
   the ceilings are the separate script, exactly as with mutmut. Read
   `docs/load-testing.md` before changing a budget or adding a task — the two
   things that bite are that a task name is declared in
   `scripts/check_load_budget.py` and imported by the locustfile, so a name the
   budget table has never heard of fails the gate rather than going unmeasured;
   and that the budgets carry seven to twelvefold headroom over the worse of two
   measurements, the CI runner's and a developer machine's, because the job
   shares a runner with the server it is measuring — so a breach is a real
   regression and never something to turn green by raising a ceiling.
6. Commit, `git push -u origin <branch>`, then `gh pr create`.
7. `gh pr checks --watch` → **merge only if every check is green**:
   `gh pr merge --squash --delete-branch`
8. Pull main, mark the item `- [x]` in `SPEC.md`, update
   `../PROGRESS.md`, push as a `chore:` commit.

If a check fails, fix forward on the same branch. Never merge red. Never
weaken a test or lower a threshold to force green — if a gate is genuinely
wrong, change it deliberately and say why in the PR.

## Secrets
Never commit real credentials, tokens, keys, or `.env` files. Placeholders in
`.env.example` only; CI reads from the GitHub secret store. Test fixtures must
look obviously fake. Scan `git diff --cached` before every push.

## Conventions
- SQLAlchemy 2.0 async only — never a sync session
- Pydantic v2 models; `model_config` over class Config
- Dependencies via `Depends`, typed against Protocols so tests can override
- Routers under `/api/v1/`; errors go through the custom exception handler
- structlog everywhere, never bare `print` or `logging.getLogger` ad hoc

See `../ROUTINE.md` for the full workflow.
