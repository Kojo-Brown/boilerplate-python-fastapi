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

## Decorators
[docs/decorators.md](./docs/decorators.md) — `@cached`, `@retry` and `@timed`
from `src/decorators`, what each is and is not for, how they keep the signature
of what they wrap, and the order to stack them in.

## Domain events
[docs/events.md](./docs/events.md) — the async event bus in `src/events`, how a
subscriber is type-checked against the event it observes, why publishing waits
for its subscribers, and what happens when one of them fails.

## Transactional outbox
[docs/outbox.md](./docs/outbox.md) — why publishing either before or after the
commit loses something, the event row written in the same transaction as the
state change, the relay that claims committed rows with
`FOR UPDATE SKIP LOCKED` and dispatches them, why payloads are JSON scalars
only and refused at publish time, and what at-least-once delivery asks of a
subscriber.

## Payments
[docs/payments.md](./docs/payments.md) — the `PaymentGateway` protocol, the
Stripe and PayPal adapters behind it, the table of provider differences each
one absorbs, why amounts are integer minor units with a currency exponent
table, and how `pending` differs from both success and failure.

## Dependency injection
[docs/dependency-injection.md](./docs/dependency-injection.md) — the protocols
`AuthService` depends on instead of repositories and a session, the providers in
`src/dependencies.py` that supply them, why all three share one session per
request, and how to override any of it in a test.

## Idempotent requests
[docs/idempotency.md](./docs/idempotency.md) — the `Idempotency-Key` middleware,
what a client sends and gets back, why a reused key with a different payload is
a 422 and a concurrent one a 409, which responses are deliberately *not* stored,
why reservations and completed records have different TTLs, and what
idempotency still does not give you.

## Optimistic concurrency
[docs/optimistic-concurrency.md](./docs/optimistic-concurrency.md) — the lost
update and why isolation levels do not fix it, the `version_id_col` on `User`,
the `ETag` and `If-Match` protocol on `/api/v1/users/me`, why the precondition
is checked both in the service and in the UPDATE's WHERE clause, and how to put
the same guard on another resource.

## Pessimistic locking
[docs/pessimistic-locking.md](./docs/pessimistic-locking.md) — `lock_row`,
`lock_rows` and `lock_timeout` over `SELECT ... FOR UPDATE`, when to prefer this
to optimistic concurrency and when not to, the four lock modes and which one
quietly blocks foreign-key inserts, the stale-copy trap an ORM sets under every
row lock, and the deadlock-retry wrapper — including why `@retry` from
`src/decorators` cannot do that job.

## Distributed locking
[docs/distributed-locking.md](./docs/distributed-locking.md) — leases and
fencing tokens in `src/distributed_lock`, why a lock alone cannot stop a paused
holder from writing and what the token does about it, the `async with` API with
its bounded wait and optional renewal, and the two Redis keys per lock —
including the counter that must never be evicted.

## Parallel execution
[docs/parallel-execution.md](./docs/parallel-execution.md) — the two reasons a
handler stops the event loop and why their fixes are opposites: `CpuPool` in
`src/parallel/cpu.py` for compute (why `spawn` rather than `fork` is a
correctness question, why a deadline has to be armed inside the worker to get
the slot back, admission control instead of an unbounded queue, and recovery
from an OOM-killed child), and `gather_bounded` in `src/parallel/io.py` for
IO — including the two things wrong with plain `asyncio.gather`, both of which
are asserted against `asyncio` itself in the test suite.

## SOLID audit
[docs/solid.md](./docs/solid.md) — the audit of `src/` against each principle,
the refactors it produced, the findings it deferred to later spec items and how
each was closed, and the two HTTP status codes it changed.

## Spec Progress
See [SPEC.md](./SPEC.md).
