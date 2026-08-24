# Pessimistic locking

`src/locking` holds two things that are only useful together: row locks
(`SELECT ... FOR UPDATE` and its weaker relatives) and a wrapper that re-runs a
transaction Postgres killed to break a deadlock. Locking without the retry
leaves an error nobody handles; retrying without the locks retries a race it
never had to lose.

This is the counterpart to [`optimistic-concurrency.md`](./optimistic-concurrency.md),
which solves the same lost-update problem the other way round.

## Which one to reach for

|                        | Optimistic (`src/concurrency`)              | Pessimistic (`src/locking`)                 |
| ---------------------- | ------------------------------------------- | ------------------------------------------- |
| Concurrent writers     | both proceed, the loser is rejected          | the second one waits                        |
| Cost when uncontended  | one integer column                           | a lock held for the rest of the transaction |
| Cost when contended    | a whole attempt discarded                    | a wait                                      |
| Failure the client sees| `412`, "re-read and retry"                   | usually none — it waited                    |
| Spans HTTP requests    | yes — the tag survives a round trip          | no — a lock dies with its transaction       |

Default to optimistic for anything a user edits through an API. Conflicts are
rare there, a lock cannot span the think-time between `GET` and `PATCH`, and
blocking one request on another user's transaction is worse than asking one of
them to re-read.

Reach for pessimistic locking when:

- **the decision depends on the row you are about to change** — "is the balance
  still enough?", "is there stock left?". Retrying is not the same as never
  having gone wrong, and a version check tells you afterwards;
- **conflicts are the normal case**, not the exception, so discarding whole
  attempts costs more than waiting;
- **the work between read and write is expensive** enough that redoing it hurts;
- **several rows move together** and you need them all still true at commit.

## Taking a lock

```python
from src.locking import LockMode, lock_row, lock_rows, lock_timeout

# One row, by primary key. Blocks until whoever holds it commits.
account = await lock_row(session, Account, account_id, mode=LockMode.NO_KEY_UPDATE)

# Fail immediately rather than queue.
account = await lock_row(session, Account, account_id, nowait=True)

# Wait, but not forever.
async with lock_timeout(session, 2.0):
    account = await lock_row(session, Account, account_id)

# A work queue: each worker walks away with a disjoint batch.
jobs = await lock_rows(
    session,
    select(Job).where(Job.state == "pending").order_by(Job.id).limit(10),
    skip_locked=True,
)
```

`lock_row` returns `None` only for "no such row" — never for "someone else has
it", which raises. `skip_locked` is offered on `lock_rows` and not on `lock_row`
for that reason: on a single row it would collapse *gone* and *busy* into the
same `None`.

There is no unlock call because Postgres has none. `COMMIT` and `ROLLBACK` are
the only releases, so a transaction that takes a lock and then does something
slow holds it for the whole of that something.

### Lock modes

| `LockMode`      | SQL                  | Excludes writers | Blocks foreign-key references |
| --------------- | -------------------- | ---------------- | ----------------------------- |
| `UPDATE`        | `FOR UPDATE`         | yes              | **yes**                       |
| `NO_KEY_UPDATE` | `FOR NO KEY UPDATE`  | yes              | no                            |
| `SHARE`         | `FOR SHARE`          | yes              | no                            |
| `KEY_SHARE`     | `FOR KEY SHARE`      | no               | no                            |

The third column is the one that surprises people. Postgres takes a `KEY SHARE`
lock on a row implicitly whenever another table's row is inserted referencing
it, and `FOR UPDATE` conflicts with that — so locking a `users` row `FOR UPDATE`
blocks inserts into `refresh_tokens`, invisibly, from code that never mentions
either. `NO_KEY_UPDATE` is what a plain `UPDATE` of a non-key column takes
anyway and is the better default for a read-modify-write of an ordinary column.

`SHARE` deserves one warning: several holders that each try to upgrade to
`UPDATE` deadlock, by construction.

### `nowait` versus `lock_timeout`

Both surface as `LockNotAvailableError` — a `ConflictError` subclass, so it
reaches the edge as a **409** with `error_code: "LOCK_NOT_AVAILABLE"` rather
than a 500. The distinct code matters: a client can tell "someone is editing
this, try again in a moment" from the 409s that will fail identically forever,
like a duplicate email.

`nowait` is for when queueing is itself wrong — a "claim this" button that
should say *someone else got there first* rather than spin. `lock_timeout` is
usually the better choice on a request path: real contention clears in
milliseconds, so a short wait succeeds where `nowait` would have failed, and the
bound still keeps a request from parking on a lock until the client gives up.

`lock_timeout` bounds *waiting for a lock* only. A slow query that is not
blocked is `statement_timeout`, a different setting.

## The stale-copy trap

The single most dangerous thing about pessimistic locking through an ORM:

```python
user = await session.get(User, user_id)          # version 1
...                                              # someone else commits
row = (await session.execute(                    # takes the lock, and...
    select(User).where(User.id == user_id).with_for_update()
)).scalar_one()
assert row is user                               # ...returns the *cached* copy
```

The lock is genuinely held and the row genuinely re-read, but the identity map
wins over the result set, so the attributes the caller reads are the ones from
before the lock existed. That is the exact race the lock was for, failing
silently, in code that looks correct. `test_locking_db.py` demonstrates it
rather than describing it.

`lock_row` and `lock_rows` both run with `populate_existing=True`, which
overwrites the in-memory instance with what the locked read returned. They share
one helper so the two cannot drift into disagreeing about it.

The corollary is a rule: **take the lock before you modify the row.**
`populate_existing` overwrites unflushed changes along with everything else —
and a modification decided on before the lock was held was decided on stale data
anyway.

`Session.get(..., with_for_update=...)` is the obvious alternative and is not
used. It routes through the refresh path, which turns on version checking, and
`User` carries a `version_id_col` for optimistic concurrency — so a pessimistic
reader whose copy has moved on gets `StaleDataError` instead of the current row
it asked for. Right answer for an optimistic refresh, wrong one here.

## Deadlocks

Two transactions that lock the same rows in opposite orders will deadlock. This
is not a bug to be fixed at the call site; with concurrent writers and more than
one row it is a scheduling outcome. Postgres detects it after `deadlock_timeout`
(1s by default), kills one transaction with SQLSTATE **40P01**, and lets the
other commit.

Two responses, and you want both:

**Prevent what you can.** Lock in a consistent order — by primary key is the
usual one — so two opposite transfers queue instead of deadlocking:

```python
for account_id in sorted((source_id, destination_id)):
    await lock_row(session, Account, account_id)
```

**Retry what is left.** Ordering discipline breaks down across features that do
not know about each other, so the net still has to be there.

```python
from src.locking import retry_on_deadlock

@retry_on_deadlock(attempts=3)
async def transfer(
    session: AsyncSession, source: uuid.UUID, destination: uuid.UUID, amount: int
) -> None:
    for account_id in sorted((source, destination)):
        await lock_row(session, Account, account_id)
    ...
    await session.commit()
```

The decorated function must take an `AsyncSession` as its first parameter. For a
closure rather than a named function, `run_with_deadlock_retry(session, work,
...)` does the same with default policy.

### Why this is not `@retry` from `src/decorators`

Same shape, and it would be wrong. **A Postgres transaction is dead after any
error**, and re-running a statement inside it does not reproduce the original
failure — it raises SQLSTATE **25P02**, "current transaction is aborted,
commands ignored until end of transaction block". So attempt two would not retry
the deadlock; it would raise something new, unrelated and confusing, and attempt
three would do it again.

The retry is only sound with a `ROLLBACK` in between, which is a fact about
database sessions that a general-purpose call retrier has no business knowing.
Hence the second, narrower loop. The backoff *policy* is not duplicated: both
call `backoff_delay` in `src/decorators/base.py`.

`test_locking_db.py` provokes the 25P02 from a real server rather than taking
the paragraph above on trust.

### The contract the wrapper cannot enforce

Three obligations, all on the caller:

1. **Re-read everything, every attempt.** The rollback undoes the reads as well
   as the writes, so a row loaded before the first attempt describes a world
   that no longer exists — and was the stale copy that lost. Take identifiers
   as arguments and load rows inside the callable.
2. **Own the commit.** A serialisation failure under `REPEATABLE READ` or
   `SERIALIZABLE` (SQLSTATE 40001) is raised by `COMMIT` itself, so a caller
   that commits after the wrapper returns has put the failure it wanted retried
   outside the loop.
3. **Do not nest it in a wider transaction you care about.** The rollback
   between attempts rewinds everything, not just this unit of work.

### What is and is not retried

Retried by default: **40P01** (deadlock) and **40001** (serialisation failure).
Both failed because of *another* transaction, so the identical request may well
succeed.

Not retried: everything else. A unique violation will fail the same way forever;
`asyncio.CancelledError` means the caller stopped waiting, and re-running a
transaction on its behalf holds locks for work nobody will read; **55P03** is
what `nowait` and `lock_timeout` raise, and retrying it turns a caller's
explicit "do not queue" into a queue. It can be added through `codes=` if you
really want it, and usually you do not.

On exhaustion the **original** exception propagates — no `RetryError` wrapper,
for the same reason `@retry` has none: the exception's type is what the edge
turns into a status code. The session is left aborted, exactly as an unwrapped
call would have left it.

Backoff is full-jitter, and the jitter is load-bearing here in a way it is not
elsewhere: two transactions deadlocked against each other are released
together, so retrying in lockstep re-creates the same deadlock on the same rows.

## What is not here

**No route uses this yet.** Nothing in the current schema has a balance, a stock
count, or a queue — the rows this API writes are single-owner profile rows, and
`/api/v1/users/me` is correctly served by optimistic concurrency. Wiring a lock
into a route that does not need one would be a worked example pretending to be a
requirement. The library is complete and tested, and its first genuine consumer
is the transactional outbox relay (`src/outbox/store.py`, `docs/outbox.md`),
which claims rows with exactly the `FOR UPDATE SKIP LOCKED` batch pattern
above — `lock_rows` itself, not a second copy of it.

**No advisory locks.** `pg_advisory_xact_lock` is the right tool for mutual
exclusion over something that is not a row — a nightly job, a per-tenant
critical section — and is a different enough idea to belong in its own module.
The Redis distributed lock is the next SPEC item and covers the cross-process
case.

**No `of=` on the lock clause.** `FOR UPDATE OF t` matters when a statement
joins several tables and should lock only one; both functions here lock the
entity they select. A caller needing it can pass a statement that already
carries its own `with_for_update` — but note that `lock_rows` will replace it.
