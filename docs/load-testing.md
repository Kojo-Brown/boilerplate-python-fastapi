# Load testing with a latency budget in CI

Four read paths, driven by locust for a minute on every pull request, with a
95th-percentile ceiling on each that fails the build when it is exceeded.

| File | What it is |
| --- | --- |
| `loadtest/locustfile.py` | The profile: four tasks, each asserting its own status code |
| `scripts/run_load_test.py` | Boots the app, mints the run's credentials, drives locust, writes the CSV |
| `scripts/check_load_budget.py` | The gate: the budgets, and everything that is an error rather than a breach |
| `tests/test_load_budget_gate.py` | 43 cases on the gate, because a gate that cannot fail is worse than no gate |
| `.github/workflows/ci.yml` → `load` | The job, between `integration` and `mutation` |

Run the whole thing exactly as CI does:

```
uv sync --group loadtest
uv run python scripts/run_load_test.py
uv run python scripts/check_load_budget.py
```

It needs a Postgres and a Redis on `DATABASE_URL` and `REDIS_URL` — the same
two the rest of `CLAUDE.md`'s gates want — and takes about seventy seconds.

## Why the measurement and the gate are two commands

`locust --headless` exits non-zero when a *request* failed and exits 0 at any
response time whatsoever. It reports latency; it does not gate on it. So the
budget lives in a second process that reads the CSV, which is the same split as
`mutmut run` and `scripts/check_mutation_score.py` and is there for the same
reason: the measuring tool's exit code answers a different question from the
one that should block a merge.

Both halves are one command each and both run locally. That is not a
convenience. This repository has already paid once for a gate only CI could
run — `tests/test_distributed_lock_redis.py` was red locally for weeks while CI
was green (see the Phase 0 note in `SPEC.md` and `docs/integration-testing.md`)
— and a load test is the easiest gate in the world to end up in that state,
because the obvious implementation is eight lines of `run:` in a workflow that
exist nowhere else.

## The profile

Twenty paced users against one uvicorn worker for sixty seconds, ≈155 req/s.

| Request name | What it exercises |
| --- | --- |
| `GET /health` | Nothing but the stack: six middlewares, routing, the handler. The floor the rest are read against |
| `GET /health/ready` | A live round trip to every configured dependency — `SELECT 1` on the async engine, a Redis `PING` |
| `GET /api/v1/users/me` | The real read path: bearer decode, the tenant `set_config` statement, the row fetch, an encrypted column decrypted, the entity tag, serialisation |
| `GET /api/v1/users/me (If-None-Match)` | The same, answered 304 from the validator. The gap to the row above is what conditional GET costs |

Three decisions in that table are worth their reasoning.

**`between(0.05, 0.2)` rather than no wait.** A zero-wait user is a saturation
test: it pushes until something queues, and the latency it reports is the depth
of that queue rather than the service's response time. Paced users make the
arrival rate a property of `--users`, so the number the gate reads is a response
time. Twenty users at ~155 req/s against a single worker answering in single-digit
milliseconds is nowhere near the knee — deliberately. Finding the knee is a
different exercise and wants a ramp, not a ceiling.

**`FastHttpUser`, not `HttpUser`.** The requests-based client costs several
times more CPU per request, and on a shared runner that cost lands *inside* the
measurement: the client would be reporting its own overhead as the service's
p95.

**Every task asserts its own status.** This is the part that is easy to leave
out and expensive to leave out. A 401 is fast and a 404 is faster; a budget on
latency alone passes with flying colours against a run where nothing was
authorised, which is the failure a load test is most likely to actually have.
The conditional task requires **304** and not "2xx or 304", because a 200 there
means the tag it was given no longer describes the profile and the task has
quietly become a duplicate of the one above it.

### No writes, and what it would take to add them

`POST /auth/register` and `POST /auth/login` are rate-limited to five a minute
(`src/auth/router.py`) and every virtual user shares one source address, so a
sustained write load would measure `slowapi` rejecting it. `PATCH
/api/v1/users/me` would be worse: it requires `If-Match` against the current
version and bumps that version on success, so concurrent users would spend the
run losing preconditions to each other and the gate would be reading the latency
of 412s.

A write profile is a worthwhile separate item. It needs an account per virtual
user, seeded out of band rather than through the rate-limited endpoint, and its
own budget — `PATCH` does an `UPDATE … WHERE version = ?` and a commit, so it
belongs nowhere near the ceilings below.

### The one login, and where the token goes

`scripts/run_load_test.py` registers and logs in one account before locust
starts, and hands the access token and the profile's entity tag to it in the
environment. A login costs an argon2 verification by design (~75ms, see
`src/auth/password.py`) on top of being rate-limited, so it can be neither a
task nor an `on_start`.

The account is `loadtest@example.com` with the password
`loadtest-not-a-real-password` — a reserved domain (RFC 2606) and a password
that says what it is. Nothing there is a credential: it is created through the
public API against whatever database the run points at. A 409 from the register
is success, since the account outlives one run; a 401 from the login that
follows is not, and means the address exists with some other password. The token
itself is minted per run, lives in one subprocess's environment, and is never
printed.

## Budgets

`BUDGETS` in `scripts/check_load_budget.py`:

| Request name | p95 on CI | worst p95 locally | Budget | Request floor |
| --- | --- | --- | --- | --- |
| `GET /health` | 1ms | 2ms | 25ms | 400 |
| `GET /health/ready` | 2ms | 5ms | 40ms | 400 |
| `GET /api/v1/users/me` | 5ms | 11ms | 75ms | 1200 |
| `GET /api/v1/users/me (If-None-Match)` | 5ms | 11ms | 75ms | 1200 |

Both columns come from the command at the top of this file. **CI**
(`ubuntu-latest`, Postgres 17 and Redis as service containers): 9,233 requests,
0 failures, aggregate p95 4ms. **Locally**, worst of four runs on a four-core
machine with Postgres 16 and Redis on localhost, ~9,100 requests each, 0
failures, aggregate p95 between 7 and 10ms. The runner is the faster of the two,
which is worth knowing before reading a red job as "CI is slow today"; the local
column is the worst of several runs rather than one, because a developer's
machine is also doing other things and one sample would understate the spread.

**These are regression bounds, not an SLO.** Each ceiling is seven to twelve
times the worse of the two numbers beside it, and the multiple is what the
hardware spread buys rather than a round number somebody liked. The tail on the
runner reaches about 90ms at the 99.9th percentile, so single slow requests do
happen and a gate must not fire on a handful of them; but for a p95 to move past
these ceilings, one request in twenty has to be slow, which is a change and not
a mood.

What the budgets catch is the change that moves a response time by a *multiple*:
an N+1 query added to a read path (`docs/n-plus-one.md`), a dependency check
made serial, a middleware that now hashes something per request. What they
deliberately do not catch is a 30% drift, which on this hardware cannot be told
apart from the runner's mood. A gate that fires on noise gets switched off, and
then catches nothing at all.

**The two 304 and 200 ceilings are equal, and that is the interesting one.**
The tempting version budgets the revalidation *below* the unconditional read,
because `docs/conditional-get.md` claims revalidation is cheaper. At this
payload it is not cheaper in time: a profile is about 190 bytes, so the saving
is bytes on the wire, and the two measure within a millisecond of each other on
both machines — sometimes the 304 is the slower of the pair. A ceiling encoding
"the 304 must be faster" would be gating on noise. Re-measure before changing that, on a resource
whose representation is large enough for the claim to be about milliseconds.

### Re-measuring, and changing a budget

Run the two commands at the top of this file; the gate prints the table whether
it passes or fails. On hardware slower than the numbers above, pass
`--headroom 2.0` rather than editing `BUDGETS` — the flag multiplies every
ceiling, refuses a value below 1.0 so it can only loosen, and leaves the
committed numbers as the thing CI gates on.

Lowering a budget to turn a red build green is the one move this gate exists to
prevent. If a change genuinely makes an endpoint slower and that is acceptable,
raise the ceiling in the same commit, with the measurement and the reason, and
say so in the pull request.

## What is an error rather than a breach

Three checks beyond the percentile, each closing a way for a load test to pass
having measured nothing:

- **A budgeted name absent from the stats.** A task renamed or deleted would
  otherwise silently stop being measured and the run would still be green. This
  is why the four request names are declared in
  `scripts/check_load_budget.py` and *imported* by the locustfile rather than
  the other way round — the import direction is forced (the gate must be
  importable without locust, since the `test` job does not install the group)
  but it is also the right way round: a name is only worth recording if
  something gates on it.
- **A name in the stats with no budget.** Adding a task is then a deliberate
  edit to `BUDGETS` with a number to justify, rather than a row nobody gates on.
- **A floor on the request count per name.** A p95 over thirty samples is not a
  p95. A profile edited to the point where one path is barely exercised fails
  rather than reporting a confident number about it.

And one that sounds like a detail and is not: locust writes `N/A` for every
percentile of a name it recorded no response for, and the gate refuses it
explicitly. Read as a zero, a path that answered nothing would post the best
number in the table.

Failures must be zero outright. Every task asserts its own status, so a failure
here is a 401, a 503 or a 304-that-was-a-200 — in each case the run measured
something other than what it claims to, and such a response is usually *faster*
than the one it replaced.

## The server's log must not go to a pipe

`scripts/run_load_test.py` reuses `scripts/smoke_start.py`'s `serve` and
`wait_for_liveness` rather than keeping a second copy of uvicorn process
management, and passes `output=` a file.

That argument exists because of a bug worth recording. uvicorn writes an
access-log line per request, about 70 bytes of it; a pipe nobody reads holds
64 KiB before the *writer* blocks, so the budget is roughly 900 requests and a
run of this profile writes 640 KiB. The first version of this script took
`serve`'s default pipe, and partway through the run the server stopped
answering, ignored SIGTERM and had to be killed — with no error anywhere and
nothing in the summary to say the run had been truncated. A smoke test making
three requests never sees it, which is why the parameter has a default that is
wrong for every caller but that one, and a docstring saying so.

The log is kept rather than discarded: it is the only account of what the
application did during the run, and the `load` job uploads it with the CSVs.

## Interpreting a red `load` job

| Message | What it means |
| --- | --- |
| `p95 …ms exceeds …ms` | A real regression, or the runner was loaded. Re-run once; if it repeats, it is the change |
| `requests failed` | Read `build/load/load_failures.csv`. Usually a 401 (the bootstrap broke) or a 503 (`/health/ready` lost a dependency) |
| `below the floor of N` | The server could not keep up with a paced 155 req/s, or the profile's weights changed |
| `has no budget` | A task was added. Add its ceiling to `BUDGETS` and a row to the table above |
| `absent from the stats` | A task was renamed or removed. Rename the constant in `scripts/check_load_budget.py` |
| `no response times (95%='N/A')` | locust recorded the name but no response. The task raised before sending |
| `ignored SIGTERM` | A lifespan shutdown hook is hanging. `build/load/load_server.log` has the last thing it did |

## Related

- `docs/conditional-get.md` — the 304 protocol the fourth task measures
- `docs/health.md` — what `/health/ready` actually probes
- `docs/n-plus-one.md` — the regression these budgets are most likely to catch
- `docs/integration-testing.md` — the other gate that has to run in both places
- `docs/mutation-testing.md` — the same measure-then-gate split
