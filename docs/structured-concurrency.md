# Structured concurrency: ownership, budgets, and cleanup that finishes

`asyncio` gives you three ways to lose work, and all three are silent. A task
nobody holds can be collected mid-await. A per-call timeout does not add up to
a request budget. A `finally:` on a cancelled task is itself cancelled, so the
release that has to happen is exactly the `await` that gets cut.

`src/structured` closes each of them, one module apiece.

| Problem | What `asyncio` gives you | What this gives you |
| --- | --- | --- |
| the task nobody owns | `asyncio.create_task` | `TaskScope` (`scope.py`) |
| timeouts that do not compose | `asyncio.timeout` | `deadline()` (`deadline.py`) |
| cleanup cut by cancellation | `asyncio.shield` | `protect` / `finalize` (`cancel.py`) |

The package is named `structured` and not `concurrency` because
`src/concurrency` is already the *other* meaning of that word: `If-Match`,
ETags and `version_id_col`, which are about two writers racing at one row
rather than two coroutines racing on one loop.

## 1. Every task has an owner

```python
asyncio.create_task(send_the_thing())
```

Three things are wrong with that line and none of them raise.

**Nobody owns the task.** The loop keeps only a weak reference, so a task with
no strong reference anywhere can be garbage-collected mid-await and simply
stop. That is documented behaviour, and it is why `create_task`'s own
documentation tells you to keep the return value. What you see is work that
sometimes did not happen.

**Nobody reads the exception.** A task that raises and is never awaited logs
`Task exception was never retrieved` from a `__del__`, at collection time,
detached from the request that started it. It is not raised, not counted, and
not attributable.

**It outlives its context.** It borrows the `ContextVar`s of whoever created
it, so its logs carry a stale request id, its session may already be closed,
and shutdown does not wait for it.

```python
async with TaskScope("app", on_exit=WhenScopeExits.CANCEL) as scope:
    scope.start_soon(relay.run, name="drain")
    scope.start_soon(partial(renew, lease), name="renew")
    yield                       # serve requests
# both children are cancelled, unwound, and finished here
```

### The two exit rules

| | What exiting the block does | Use for |
| --- | --- | --- |
| `WAIT` (default) | waits for every child | work the block exists to complete |
| `CANCEL` | cancels every child, *then* waits | anything that runs until told to stop |

`WAIT` is `asyncio.TaskGroup`'s rule. `CANCEL` exists because half of what a
server runs in the background never finishes on its own: put a `while True:`
relay loop in a plain `TaskGroup` and `__aexit__` blocks forever, so shutdown
hangs, SIGTERM escalates to SIGKILL, and the in-flight batch is truncated
rather than rolled back.

The waiting in `CANCEL` is not politeness. `task.cancel()` only *schedules* a
`CancelledError`; the coroutine has to be resumed for it to be delivered and
for the `finally:` that rolls back a transaction or releases a lock to run.

### What it adds over `TaskGroup`, and what it does not replace

`TaskGroup` is used underneath rather than reimplemented, because the parts
that look simple are the parts that are not: cancelling siblings when a child
fails, collecting the results into a `BaseExceptionGroup`, and the `uncancel()`
bookkeeping that lets an enclosing `asyncio.timeout` tell its own expiry from
someone else's cancellation.

On top of it: the `CANCEL` rule; factories rather than coroutines, so a child
the scope never starts cannot emit `RuntimeWarning: coroutine ... was never
awaited` from the collector; names on every child, because in a hung process
`asyncio.all_tasks()` is the only evidence there is; and a real error for
starting a task in a closed scope.

**Failures still arrive as an `ExceptionGroup`** — caught with `except*`. Single
-child groups are deliberately *not* unwrapped: that would make the type of the
exception depend on how many children happened to fail, so the handler that
worked in testing breaks the day two fail together. The one exception is the
body's own exception, which `TaskGroup` also wraps; `raise ValueError` inside a
scope comes back out as `ValueError`, because a `with` block should hand back
the exception you raised in it. That unwrapping is keyed on object identity, so
it can never reach a child's failure.

A crashing daemon taking the scope down with it is intended. The alternative is
a process that keeps serving with its relay dead.

## 2. Budgets, not timeouts

A handler with a five-second budget makes three upstream calls, each given a
five-second timeout. No individual number is wrong and the handler can take
fifteen seconds. Per-call timeouts bound a *call*; what the client waiting on a
socket cares about is the *request*.

```python
async with deadline(30, name="request"):
    async with deadline(10, name="stripe"):   # arms its own timer
        ...
    async with deadline(60, name="report"):   # clamped to what "request" has left
        ...
```

A nested scope can only ever lower the ceiling. One that asks for longer than
the enclosing budget has left silently gets the remainder — a timeout that
could raise the ceiling would not be a budget.

### Which scope expired

When a nested scope is clamped it does **not** arm a timer of its own. Two
timers set to the same instant would both fire and both cancel the same task,
and which won the race would pick the error message — for a distinction that
matters, since "the request budget ran out" and "the payment gateway was slow"
have different fixes. The enclosing scope owns the instant, arms it, and names
itself in `DeadlineExceeded.scope`.

`DeadlineExceeded` is **not** a `TimeoutError`, and that is deliberate.
`TimeoutError` is what a socket read and an `httpx` call raise, so a handler
catching it to retry an upstream would otherwise catch the enclosing budget
expiring and retry inside a scope with no time left. The two mean opposite
things: one says "that call failed, try another", the other says "stop".
Because they are distinct, `src/decorators/retry.py` needs no special case.

A `TimeoutError` raised by the body itself is passed through untouched — a slow
socket is not a spent budget.

### Handing the budget to something that only understands numbers

```python
response = await client.post(url, timeout=clamp_to_deadline(5.0))
```

An `httpx` timeout is a number, and a number cannot know what is left. Passing
a flat five seconds when the request has 300ms of budget spends 4.7 seconds
producing an answer nobody is waiting for. `clamp_to_deadline` raises rather
than returning zero on a spent budget, because most clients read a non-positive
timeout as "no timeout" and would wait forever at precisely the wrong moment.

### Loop time, not `time.monotonic`

`Deadline.expires_at` comes from `loop.time()`. Under `uvloop` — which this
application runs on — that clock is libuv's, and its epoch is *not*
`time.monotonic()`'s. A deadline built from `time.monotonic()` and handed to
`asyncio.timeout_at` is wrong by the difference between two arbitrary epochs:
usually far in the past, so the scope expires immediately. It fails on uvloop
and passes on the default event loop, which is the worst way for it to fail.

A `Deadline` therefore belongs to the loop that created it, and `remaining()`
needs a running loop.

### What a deadline does not bound

Anything that never awaits. A scope around a tight CPU loop expires and nothing
happens until the loop yields — which is what `src/parallel/cpu.py` and its
in-worker `SIGALRM` deadline exist for.

## 3. Cleanup that finishes

A client disconnects, Starlette cancels the handler, and the `CancelledError`
surfaces at whatever it was awaiting. Correct and desirable — until the
unwinding reaches a `finally:` that has to release an idempotency reservation
or roll a transaction back. Those awaits are on the cancelled task too.

The failure needs *two* cancellations to appear, which is why it survives
review: the first is delivered and caught, the cleanup starts, and it takes a
second — a shutdown draining its tasks, a `TaskGroup` aborting, a
`gather_bounded` cancelling siblings — to cut it. So it never happens in
development and happens under load.

### Why `asyncio.shield` is not the answer

```python
await asyncio.shield(release())
```

`shield` protects the inner coroutine. It does not protect the *await*, which
raises `CancelledError` immediately, so the caller carries on unwinding and
`release()` keeps running with nobody holding it — the unowned task from part 1.
`shield` converts "cleanup that got cancelled" into "cleanup that may or may not
happen", which is harder to see, not better.

What is needed is to keep waiting: absorb the cancellations aimed at us, let the
cleanup finish, then honour them.

### `protect` or `finalize`

They differ in one decision — what to do with a cancellation that arrived while
the cleanup was running — and it depends on where the call sits.

`protect` **re-raises** it, after the work has finished. Use it where the
protected call is the work.

`finalize` **re-arms** it on the current task and never raises, swallowing the
cleanup's own failures into the log. Use it in an `except:` or `finally:`,
where raising would replace the exception being unwound with an artefact of the
cleanup — turning a 500 anyone could debug into a bare `CancelledError`.
Re-arming means the cancellation is delivered at the next `await` and the task
still ends cancelled: deferred by the length of the cleanup, not discarded.

```python
try:
    await self.app(scope, receive, capturing_send)
except BaseException:
    await finalize(
        partial(self._release, full_key),
        name="idempotency-release",
        timeout=RELEASE_TIMEOUT_SECONDS,
    )
    raise
```

That is `src/middleware/idempotency.py`, and it is the one `await` in that
middleware that runs on a task somebody has already cancelled. Without it a
second cancellation leaves the reservation held, answering every retry with 409
until its TTL runs out —
`test_the_reservation_is_released_despite_a_second_cancellation` fails if the
`finalize` is removed.

`timeout` bounds the *protection*, not the work's usefulness. Without one, a
cleanup that hangs holds shutdown open until the supervisor sends SIGKILL,
which truncates every other shutdown step that had not run yet.

Neither function swallows a cancellation. The one place `CancelledError` is
caught without an immediate re-raise is `_drain`, which returns it to the
caller — and that is on the exemption table below, with that reason.

## The fitness function

`tests/test_cancellation_gate.py` walks the AST of every module under `src/`
and fails on:

1. **A discarded task start** — `asyncio.create_task(...)` or `ensure_future(...)`
   as a bare statement. `TaskGroup.create_task` and `TaskScope.start_soon` are
   not matched: those *are* the owner.
2. **A caught cancellation with no `raise` in the handler** — including
   `except BaseException:` and a bare `except:`, both of which catch
   `CancelledError`. `except Exception:` is not matched, because since 3.8
   `CancelledError` inherits from `BaseException` precisely so it cannot.

Each rule has an exemption table keyed by `module.Class.function`, and an entry
needs a written reason. A stale entry fails too, so an exemption cannot outlive
the code it was written for. The task-ownership table is empty today.

The three current cancellation exemptions are `OutboxRelay.stop`,
`DistributedLock._stop_renewer` and `structured.cancel._drain`. The first two
are the same shape: the cancellation is the method's *own*, requested one line
above, so re-raising it would cancel whoever asked for the shutdown.

## What is not here

**Adoption in the lifespan.** `OutboxRelay.start`/`stop` and the distributed
lock's renewer each hand-roll create-cancel-await-swallow, and both are what
`TaskScope` is for. Converting them means restructuring `src/main.py`'s
lifespan, whose teardown order is load-bearing and documented in place — the
relay must stop before the bus is cleared, or its last batch is delivered to
nothing and deleted. That is a separate change with its own risk, not a
sub-clause of this one.

**A request-wide budget on every route.** `deadline()` composes with an ASGI
middleware that opens one scope per request, but *what* the budget should be is
a deployment question (it has to exceed the slowest legitimate request and sit
under the load balancer's own timeout), and a wrong default fails requests that
used to work.

**A nursery-style `start()` that waits for readiness.** `start_soon` schedules
and returns. A child that has to be serving before the caller proceeds needs an
`asyncio.Event` it sets itself; wiring that into the scope would mean guessing
what "ready" means for every kind of child.
