# boilerplate-python-fastapi

> FastAPI 0.138 · Python 3.14 · SQLAlchemy 2.0 · PostgreSQL · Alembic · Pydantic v2

Async Python API starter with full auth, migrations, and DevOps.

## Stack

| Layer | Tech |
|-------|------|
| Framework | FastAPI 0.138 |
| Language | Python 3.14 |
| ORM | SQLAlchemy 2.0 (async) |
| Migrations | Alembic |
| Auth | JWT + OAuth 2.0 (python-jose, authlib) |
| Hashing | Argon2 (argon2-cffi) |
| Validation | Pydantic v2 |
| Package mgr | uv |
| Testing | Pytest + HTTPX |

## Quick Start

```bash
git clone https://github.com/Kojo-Brown/boilerplate-python-fastapi.git
cd boilerplate-python-fastapi
uv sync
cp .env.example .env
docker-compose up postgres -d
uv run alembic upgrade head
uv run fastapi dev src/main.py  # http://localhost:8000/docs
```

## Health probes

| Endpoint | Purpose | Touches Postgres |
|----------|---------|------------------|
| `GET /health` | Liveness — is the process alive? | No |
| `GET /health/ready` | Readiness — can it serve traffic? | Yes (`SELECT 1`) |

They are deliberately separate. Point a `livenessProbe` at `/health` and a
`readinessProbe` at `/health/ready`: if liveness queried the database, a brief
Postgres outage would restart every healthy replica instead of just draining
them. `/health/ready` returns `503` with
`{"status": "unavailable", "database": "unreachable"}` while the database is
down, and recovers on its own once it returns.

## Start-up smoke test

`uv run python scripts/smoke_start.py`

Boots the real `uvicorn src.main:app` process, waits for `/health`, requires
`/health/ready` to confirm a live `SELECT 1` against the configured Postgres,
then checks that SIGTERM shuts it down cleanly. CI runs this on every PR against
a Postgres service container, so a change that imports fine but cannot actually
start — a broken lifespan, a bad `DATABASE_URL`, an unmigrated schema — fails the
build instead of the deploy.

## Error contract

Every failure leaves the API through the handlers in `src/exception_handlers.py`
as `{"error", "message", "status"}`, plus `"details"` where there is something
to say. Handlers and routes never build that envelope themselves: code raises an
`AppException` subclass from `src/exceptions.py`, and the subclass carries the
status code, the machine-readable `error` code, and any headers that status
requires — so a 401 always arrives with its `WWW-Authenticate` challenge.

Keep rejections in the service layer and transport concerns at the edge:
a route that catches a domain error to relabel it is a route that will
eventually disagree with another route about the same condition.
[docs/solid.md](./docs/solid.md) records how that happened here and what it cost.

## Object storage
[docs/storage.md](./docs/storage.md) — the `StorageBackend` protocol, the S3,
local-disk and in-memory backends behind it, how `StorageFactory` chooses one
from `STORAGE_BACKEND`, and how to add a fourth without editing the factory.

## SOLID audit
[docs/solid.md](./docs/solid.md) — the audit of `src/` against each principle,
the refactors it produced, the findings deferred to later spec items, and the
two HTTP status codes it changed.

## Spec Progress
See [SPEC.md](./SPEC.md).
