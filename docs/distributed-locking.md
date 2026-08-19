# Distributed locking with fencing tokens

`src/distributed_lock/` gives one critical section at a time across processes,
machines and deploys, for work the database cannot serialise on its own.

```python
from src.distributed_lock import DistributedLock

async with DistributedLock(backend, f"rebuild:{report_id}", ttl_seconds=60) as lease:
    await rebuild(report_id, fence=lease.token)
```

Two things in that snippet carry the design. The `async with` is what guarantees
the lock is given back on every path out of the body. The `fence=lease.token` is
what makes the lock *safe*, and it is not optional — see
[Why a lock is not enough](#why-a-lock-is-not-enough).

## Which lock do I want?

| Situation | Use |
| --- | --- |
| A read-modify-write of a row, in a transaction you control | `src/locking` (`SELECT … FOR UPDATE`) |
| A user editing a resource through the API, conflicts rare | `src/concurrency` (ETag / `If-Match`) |
| A retried HTTP request that must not execute twice | `src/idempotency` (`Idempotency-Key`) |
| A section spanning something the database cannot see | this package |

Reach for this one only when the last row is true: a scheduled job that must not
run twice across replicas, a call to a third-party API with no idempotency key
of its own, a rebuild of an object in S3. Anything Postgres can already
serialise should be serialised by Postgres — a row lock and the write it
protects sit inside one transaction, and that closes a window this package can
only narrow.

## Why a lock is not enough

Every acquisition here is a **lease**: it expires on its own after
`ttl_seconds`, whether or not the holder is still alive. Without that, a worker
killed mid-section would lock the name forever, because nobody is left to run
the release.

The cost of that guarantee is the central fact about distributed locks:

> A lease can expire while its holder still believes it holds one.

A stop-the-world GC pause, an event loop blocked by CPU work, an over-long
database call, a network partition — any of them can put more wall time between
"acquire returned" and "the write lands" than the TTL allows. The holder finds
out late, or never.

```
holder A   ──acquire(t=1)──▶ [ ····· paused ····· ] ──write──▶ ✗ rejected
lease A                      ├────── expires ──────┤
holder B                                ──acquire(t=2)──▶ ──write──▶ ✓
```

Renewal narrows that window; it does not close it, because the pause can land
between the last successful renewal and the write. **No lock server can fix
this**, Redlock included: a quorum of lock servers still cannot bound a client's
pause.

What fixes it is moving the check to where the damage would happen. Every
acquisition mints a **fencing token** — a strictly increasing integer, never
reused for that name. The resource remembers the highest token it has accepted
and refuses anything lower, so the paused holder's write is rejected no matter
what it believes about the lock.

In SQL that is one clause:

```sql
UPDATE jobs
   SET state = 'done', fence = :token
 WHERE id = :id AND fence < :token
```

`rowcount == 0` means you were fenced out. For a resource whose "last accepted
token" is not in the row being written, `require_fence(token, last_accepted,
resource=…)` is the same check as a function — though prefer the SQL where you
can, since one statement that both checks and writes has no window between the
two.

If the resource cannot be fenced at all — a third-party API with no version or
idempotency parameter — say so at the call site. The lock still cuts the
probability of a double execution enormously; it does not eliminate it, and code
that reads as though it does will be trusted more than it deserves.

## Acquisition does not queue by default

`wait_timeout` defaults to `0`: a held name raises `LockUnavailableError` (409)
straight away. This mirrors `nowait` in `src/locking/rows.py` — a retry loop
*is* a queue, and an unbounded one turns one slow holder into every worker in
the pool blocked behind it.

```python
# Wait up to two seconds, then give up.
async with DistributedLock(backend, name, wait_timeout=2.0):
    ...
```

The wait is full-jitter exponential backoff via `backoff_delay` in
`src/decorators/base.py` — the same policy as the two retry loops elsewhere in
this codebase. Jitter matters more here than usual: everyone waiting was woken
by the same release, and un-jittered backoff marches them back in step.

Keep `wait_timeout` below whatever timeout sits above the call. A request that
waits 30 seconds for a lock and is then abandoned by the client has held a
worker for nothing.

## Choosing a TTL, and renewing

The TTL is a trade, not a maximum:

* too short, and a slow section loses its lease mid-flight;
* too long, and a killed worker blocks the name for that long.

Start from the p99 duration of the body plus a margin. For a section whose
duration is genuinely unpredictable, keep the TTL short and renew:

```python
async with DistributedLock(backend, name, ttl_seconds=30, renew_interval=10):
    ...
```

A background task extends the lease every `renew_interval` seconds. Use roughly
a third of the TTL, which leaves room for one failed attempt. The token does not
change across renewals — it identifies the *lease*, not the renewal, so a
resource that has accepted writes under it keeps accepting them.

Renewal is a convenience. It is not what makes the write safe; the token is.

## Losing the lease

Exit releases the lease only if the store still says it is yours — compared and
deleted in one Lua script, because an unconditional `DEL` after your lease
expired would release whoever came next, and a third caller would walk into the
section they are still running.

If the store reports the lease already expired or already reassigned, the body
ran outside the protection it asked for, and:

* a body that returned normally raises `LockLostError` (409) on the way out;
* a body that raised propagates its own exception — the lock's opinion is the
  less interesting of the two;
* a release that could not reach the store raises nothing. That is not evidence
  the lease had ended, and the name frees itself when the TTL runs out.

`LockLostError` is a report, not a protection. By the time it is raised the
section has already run. What limits the damage is the fencing token; what the
exception does is stop you reporting success for work that may have been fenced
out.

## Backends

| Backend | When |
| --- | --- |
| `RedisLockBackend` | Everything with more than one process. The default. |
| `InMemoryLockBackend` | Tests, and single-process development runs. |

The in-memory backend coordinates callers inside one process and nothing beyond
it: two uvicorn workers would each hold their own dictionary, both would acquire
the same name at the same instant, and both would hand out the same fencing
tokens to different holders. The factory logs a warning when it is selected
outside a test or development environment.

Select with `DISTRIBUTED_LOCK_BACKEND`; both are exercised by the same contract
suite in `tests/test_distributed_lock_contract.py`, and the Redis leg runs
against a real server in CI.

## Operating the Redis backend

Two keys per lock name:

```
{namespace}:lock:{name}    "{token}:{owner}", with a PX expiry
{namespace}:fence:{name}   the token counter, INCRed per acquisition, no TTL
```

**The counter must not be evictable.** If it disappears, the next `INCR` returns
1 and the store starts issuing tokens a resource has already accepted and moved
past — at which point the fencing check silently stops rejecting the writers it
exists to reject. Concretely:

* `maxmemory-policy` must not be an `allkeys-*` policy: those evict keys with no
  expiry set, which is exactly what the counters are. Use `noeviction`, or a
  `volatile-*` policy.
* A failover to a replica that had not received the last `INCR`s replays tokens
  for the same reason, because Redis replication is asynchronous. This is a
  property of the deployment rather than a bug to fix. If your locks guard
  something where a replayed token would be costly, keep the counters on a
  server with AOF `appendfsync everysec` or better, and expect to accept a small
  window regardless.
* `SCRIPT FLUSH`, a restart, or a failover to a server that never saw the
  scripts is handled: `register_script` retries with a full `EVAL`, so the first
  call after one is slower rather than failing.

Sharing one Redis with Celery and the idempotency store is fine — the namespaces
keep the keys apart — but the eviction policy above applies to the whole
instance. `DISTRIBUTED_LOCK_REDIS_URL` points the locks at a different instance
or database when that is not acceptable.

## Configuration

| Setting | Default | Meaning |
| --- | --- | --- |
| `DISTRIBUTED_LOCK_BACKEND` | `redis` | `redis` or `memory` |
| `DISTRIBUTED_LOCK_REDIS_URL` | *(empty)* | Overrides `REDIS_URL` for locks |
| `DISTRIBUTED_LOCK_NAMESPACE` | `dlock` | Key prefix |
| `DISTRIBUTED_LOCK_TTL_SECONDS` | `30.0` | Default lease length |

There is deliberately no fail-open switch, unlike `IDEMPOTENCY_FAIL_OPEN`. A
section that runs while its coordination is unreachable is precisely the
concurrent execution the lock was taken to prevent, so an unreachable store is
`LockBackendUnavailableError` (503) and the section does not run.

## What this package does not do

* **No Redlock.** A single store is the coordination point. The multi-master
  quorum does not solve the pause problem, so the fencing token carries the
  safety argument and the store only keeps contention cheap.
* **No reentrancy.** A second acquisition by the same owner is refused. Handing
  it out would let an inner block's release free the outer block's lock, and the
  outer section would run on believing otherwise.
* **No route consumes it yet.** Nothing in this schema has a job queue or an
  external resource that needs cross-process exclusion, and a lock on a route
  that does not need one is a worked example pretending to be a requirement. The
  transactional outbox later in Phase 7 is the first plausible consumer.
  `LockBackendDep` in `src/dependencies.py` is wired and waiting.
