# Integration testing against real Postgres and Redis

Fourteen modules in `tests/` talk to a real server rather than a fake, because
what they assert cannot be asserted against anything else:

| Module(s) | The claim that needs a real server |
| --- | --- |
| `test_tenancy_db.py` | A row-level security *policy* isolates tenants — asked as `rls_probe`, a role the policy applies to, because a superuser bypasses every policy |
| `test_locking_db.py` | `SELECT … FOR UPDATE` actually blocks a second transaction |
| `test_optimistic_concurrency_db.py` | A version column's `UPDATE … WHERE version = ?` loses the race it is supposed to lose |
| `test_outbox_db.py`, `test_streaming_db.py` | Statements parse, and a server-side cursor streams rather than buffering |
| `test_encryption_db.py` | The ciphertext in the column is what the key ring produced |
| `test_observability_db.py` | SQLAlchemy instrumentation emits spans for real queries |
| `test_idempotency_*.py`, `test_distributed_lock_*.py`, `test_webhook_replay_redis.py`, `test_redis_streams_*.py` | `SET NX` is atomic, and a Lua script runs as one unit |

A mocked query layer accepts any statement, so none of these is a question a
fake can answer. They are integration tests in the strict sense: the thing under
test is the interaction.

## The problem this solves

Every one of those modules used to skip when nothing was listening on
`DATABASE_URL` or `REDIS_URL`, and CI supplied service containers. So the claims
were measured on every pull request — and nowhere else.

That is not a tidy arrangement with a small gap. It means the gate `CLAUDE.md`
tells a contributor to run locally is a *different, smaller* suite than the gate
that blocks a merge, and the difference is invisible: a run ending
`121 passed, 195 skipped` looks like a pass. The Phase 0 note in `SPEC.md`
records what that cost once already — `test_distributed_lock_redis.py` was red
locally for weeks while CI was green, because one test built its own Redis
client without the skip guard its siblings had, and nobody's local run ever got
far enough to notice.

Measured on this repository before the change, on a machine with Docker and no
servers running:

```
121 passed, 195 skipped        # the fourteen modules, 316 tests
```

and after:

```
316 passed
```

## How it works

`tests/containers.py` starts the servers itself. `tests/conftest.py` calls
`provision()` once, at import, and it returns having pointed `DATABASE_URL` and
`REDIS_URL` at containers it started — or having decided not to.

Every run says which, as a header line, because a silent decision is what made
the gap above expensive:

```
integration backend: start postgres, start redis
containers started: postgres, redis
```

### `INTEGRATION_BACKEND`

| Value | Behaviour | Who sets it |
| --- | --- | --- |
| `auto` (default) | Reuse a reachable server; otherwise start containers if Docker answers; otherwise change nothing and let the per-test guards skip | a developer, implicitly |
| `external` | Use `DATABASE_URL` and `REDIS_URL` exactly as given, and never touch Docker | the `test` and `mutation` CI jobs |
| `testcontainers` | Always start containers, and **fail** rather than skip when Docker is unavailable | the `integration` CI job |

An unrecognised value raises. `testcontainers` exists to convert a silent skip
into a failure, and a typo that quietly fell back to `auto` would hand that
back — the `integration` job would become a slower copy of `test`, green either
way.

The three rows are not alternatives to choose between; each is exercised on
every pull request. `external` is what keeps the `test` job talking to the
services its workflow declares, so adding all of this changed nothing about it.
`testcontainers` is what proves the container path still works, in a job with no
services at all. `auto` is what a developer gets without choosing anything.

### Why both CI jobs

The `test` job keeps its service containers rather than being converted. Three
of its steps — `alembic upgrade head`, the import check, and
`scripts/smoke_start.py`, which boots a real uvicorn and requires
`/health/ready` to round-trip a `SELECT 1` — run *before* pytest and outside it.
Testcontainers containers exist only for the length of the pytest process, so
those steps have nothing to connect to. It also needs a Kafka broker, which is
out of scope here.

The `integration` job therefore declares no services and runs the fourteen
modules under `INTEGRATION_BACKEND=testcontainers`. It is the job that would
have caught the Redis regression above.

### Why not the library's own container classes

`pyproject.toml` depends on `testcontainers` with **no extras**, and
`tests/containers.py` builds both services from the core `DockerContainer`.

- `testcontainers[redis]` requires `redis>=7`. This project resolves to
  `redis==6.4.0`, because `celery[redis]` pulls `kombu[redis]`, which caps it at
  `<6.5`. The extra is unsatisfiable here — `uv sync` fails to resolve rather
  than installing anything.
- `testcontainers[postgres]` declares no requirements at all, and
  `PostgresContainer` reaches for a *sync* driver to probe readiness. `asyncpg`
  is the only driver this project declares.

So readiness is probed with `asyncpg` and `redis.asyncio`, the clients the
application itself uses. That is the better probe regardless of the packaging:
it proves the driver can complete a handshake and round-trip `SELECT 1`, where a
TCP connect proves only that something holds the port — and for Postgres the
window between those two facts is `initdb`, which is most of a cold start.

`port_is_listening` *is* a plain TCP connect, and the asymmetry is deliberate.
There the question is "has a human already provided a server?", where a listener
is the honest answer; a handshake would make `auto` depend on credentials.

### Images

`postgres:17` and `redis:7-alpine`, matching the services the `test` job
declares. Overridable with `INTEGRATION_POSTGRES_IMAGE` and
`INTEGRATION_REDIS_IMAGE`.

Matching CI matters more than the size of the download, and `postgres:17-alpine`
is the specific thing not to reach for: alpine is musl, and musl's `strcoll`
orders text differently from glibc's, so a suite asserting on `ORDER BY` would
disagree with CI for a reason nobody would think to look for.

### Schema

A started Postgres gets `alembic upgrade head`, in a subprocess.

`create_all` is not an alternative. `test_tenancy_db.py` asserts on
`FORCE ROW LEVEL SECURITY` and the policies, which exist only in migration
`0008`, and it fails loudly rather than skipping when they are absent —
correctly, because a database without them turns its every assertion into a
tautology.

A subprocess rather than `alembic.command.upgrade` because `alembic/env.py`
reads `settings.DATABASE_URL` off the already-imported singleton, and calls
`fileConfig(...)`, which reconfigures the root logger — `tests/conftest.py`
already spends a 40-line fixture working around that class of clobbering. The
subprocess inherits the environment, leaves this process's logging alone, and is
the same command CI runs.

### The mutation gate

`mutmut` drives `pytest.main()` in this process, so `provision()` returns early
when `mutmut` is in `sys.modules`. The gate's seven selected test files reach no
Postgres or Redis — that is why the `mutation` job declares no services and
finishes in minutes — so starting a database for it would add a container and a
migration to a run that cannot touch either, and would make a gate needing no
Docker refuse to start without one.

The job also sets `INTEGRATION_BACKEND=external`. The `sys.modules` check is
what makes a local `uv run mutmut run` agree with it when nobody remembers to.

## Adding a suite

Three things bite:

1. **Read the URL late.** `src.config` builds its settings singleton at import
   time. Take `settings.DATABASE_URL` inside a fixture, as the existing modules
   do; a module-level copy is read before `provision()` has published anything.
2. **Keep the reachability guard, and keep it in a fixture.** Skip on a
   connection failure, and fail on anything after it — a connection error is an
   environment without a database, and everything later is a defect. Put the
   guard in a fixture rather than restating it, which is precisely what the
   distributed-lock regression came down to: one test built its own client
   inline and inherited no guard.
3. **Namespace Redis keys per test** (`f"test-thing:{uuid4()}"`) rather than
   calling `FLUSHDB`. Under `auto` the Redis may be a developer's own.

## Running it

```bash
uv run pytest                             # auto: containers if Docker, skips if not
INTEGRATION_BACKEND=testcontainers uv run pytest   # containers, or fail
INTEGRATION_BACKEND=external uv run pytest         # DATABASE_URL / REDIS_URL as given
```

Containers are stopped on interpreter exit. Testcontainers' own Ryuk reaper is
the backstop for a run killed outright; `TESTCONTAINERS_RYUK_DISABLED=true` is
the library's knob if an environment cannot run it.
