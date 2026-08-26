# Parallel execution: CPU-bound offload and bounded IO fan-out

The event loop is one thread. A request handler that stops making progress
stops *the server* making progress — not just its own response, but every
other request in flight, the health check the orchestrator is about to give up
on, and the keep-alives.

There are exactly two reasons a handler stops, and they have opposite fixes.
Applying either fix to the other problem makes things worse.

| The handler is… | Thread is | Fix | Module |
| --- | --- | --- | --- |
| waiting on IO | idle | overlap the waits, with a bound | `src/parallel/io.py` |
| computing | busy | move the work out of the process | `src/parallel/cpu.py` |
| running for *seconds* | either | don't do it in a request at all | `src/tasks/` (Celery) |

The third row is the one most often got wrong in the other direction. A pool
slot held for eight seconds is a slot the next request cannot have, and a
client holding a socket open for eight seconds is a client that has probably
already timed out. That work belongs in a queue with a job id, not here.

## Why a thread is not the answer for compute

`run_in_threadpool` is FastAPI's answer for a *blocking* call — a synchronous
database driver, `open()`, `subprocess.wait()` — and it is the right one,
because those release the GIL while they wait.

Pure-Python compute does not. A thread parsing a CSV holds the interpreter lock
in exactly the way a coroutine parsing a CSV does; all the thread adds is a
switch every few milliseconds, so the loop gets slivers of the interpreter
instead of none of it. Slightly better tail latency, same throughput collapse,
plus a second place for the bug to hide.

The work has to leave the process. That is what `CpuPool` is for.

Worth knowing which of your dependencies release the GIL, because the answer
changes the tool: `hashlib`, `zlib`, `argon2`, most of `numpy`'s array
operations and every well-written C extension release it and are therefore
*thread* candidates. Password hashing in particular belongs in a thread, not in
`CpuPool`.

## CPU-bound work: `CpuPool`

```python
from src.dependencies import CpuPoolDep
from src.parallel import CpuTaskTimeoutError

# Module level. It has to be importable by name in the child — see "pickling".
def render_thumbnail(image: bytes, width: int) -> bytes:
    ...

@router.post("/thumbnails")
async def create_thumbnail(pool: CpuPoolDep, image: bytes) -> Response:
    data = await pool.run(render_thumbnail, image, 320, timeout=2.0)
    return Response(content=data, media_type="image/webp")
```

One pool per server process, started and stopped by the lifespan in
`src/main.py`. `CpuPoolDep` hands a handler the pool, not a result: what to run
and how long to allow it are properties of the work, not of the pool.

### `spawn`, never `fork`

`CpuPool` refuses `start_method="fork"`, and `Settings` refuses it one level
earlier by typing the field as a `Literal["spawn", "forkserver"]`. This is not a
preference.

A forked child is a memory copy of the parent, and the parent is an async web
server. It inherits the SQLAlchemy pool's *open sockets*, the Redis client's,
and the loop's epoll set — descriptors whose other end belongs to the parent.
Two processes then read and write one TCP stream, and a Postgres connection
with two writers does not fail cleanly: it interleaves protocol frames and
hands one request's rows to another.

`fork` in a process that has threads is also undefined behaviour at the POSIX
level. Only the calling thread survives, so any lock held by a thread that did
not survive is held forever by nobody. uvicorn runs threads. CPython 3.12 warns
about this; 3.14 changed the Linux default to `forkserver` because of it.

**Consequence of `spawn`:** the child re-imports the parent's `__main__`. Under
uvicorn that is a module of uvicorn's and harmless. In a standalone script it is
your own file, so a script that builds a pool at import time will spawn itself
recursively until multiprocessing refuses with a `RuntimeError` naming
`freeze_support`. The fix is the usual `if __name__ == "__main__":` guard.

### Pickling: the callable travels by name

Pickle does not send code. It records a function's module and qualified name and
looks it up again in the child. So the target must be a module-level function,
or a method of a picklable object — never a lambda, a closure, or a function
defined inside another function, no matter how trivial.

`run` checks this eagerly and raises `NotOffloadableError` at the call site.
Without that check, the mistake surfaces as an `AttributeError` raised on the
executor's internal queue-management thread, with a traceback through
`concurrent.futures.process` and no mention of the code that made it.

Arguments and return values are pickled too, and that cost is real: sending a
50 MB array to a worker and back is two copies through a pipe. If the payload is
large, send a *reference* — an object-storage key, a row id — and let the worker
fetch it.

### Timeouts stop the worker, not just the wait

This is the part most implementations get wrong. `asyncio.wait_for` around an
executor future cancels **the wait, never the work**. The worker keeps
computing, holding a slot in a pool that has a fixed number of them, while the
parent reports a timeout to the client. A few of those and the pool has no
capacity left, having told nobody why.

So the deadline is armed **inside the worker** with `signal.setitimer`, and the
handler raises `WorkerDeadline` — which derives from `BaseException`
specifically so that ordinary `except Exception:` blocks in the workload cannot
swallow it. `CpuPool.run` translates it to `CpuTaskTimeoutError` (504), and the
slot comes back.

There is deliberately no second, parent-side timer racing it. A parent-side wait
is measured from *submission*, so it includes time spent queued behind other
calls and would fire first under load — reporting a timeout for a call that had
not started. `timeout` is therefore a budget for the call, not for the wait;
queue time is bounded separately, by admission control.

Two limits, neither of which is worked around because neither can be:

- **Windows.** No `setitimer`. There `run` falls back to a parent-side
  `wait_for`, which bounds the request and leaks the slot.
  `CpuPool.deadline_enforced` tells you which regime you are in.
- **C extensions holding the GIL.** Signal handlers run at bytecode boundaries.
  A tight `numpy` loop or a regex in catastrophic backtracking will overrun its
  deadline and nothing here will stop it. For attacker-influenced input to such
  a library, a pool is not enough — the work needs its own killable process, or
  a limit inside the library. What bounds the request in that case is the
  timeout at the edge (uvicorn, the ingress), which is the right layer for an
  end-to-end bound and not this module's to impose.

`details["enforced_by"]` on the raised `CpuTaskTimeoutError` says which fired:
`"worker"` means the slot came back, `"wait"` means it did not.

### Admission control instead of an unbounded queue

`ProcessPoolExecutor`'s work queue is unbounded. Submissions that cannot run yet
sit in the *parent's* memory as pickled payloads, so an endpoint that offloads a
megabyte of input grows the parent's heap without limit under a spike — while
every one of those requests waits on a socket that has likely already given up.

`CpuPool` admits `max_workers * (1 + queue_depth_per_worker)` calls and refuses
the rest with `CpuPoolOverloadedError` (503, `Retry-After: 1`). Shedding load
where it can still be reported beats an OOM twenty seconds later. Nothing ran,
so a retry costs the pool nothing it has not already spent.

### Recovery from a dead worker

A `ProcessPoolExecutor` whose child was killed — the OOM killer is the realistic
case — raises `BrokenProcessPool` for **every subsequent submission, forever**.
There is no reset. Without recovery, one OOM takes the endpoint down until the
pod restarts.

`CpuPool` detects the break, replaces the executor, and raises
`CpuPoolUnavailableError` (503) to the requests caught in it. `max_tasks_per_child`
(100 by default) is the preventive half: a worker is retired after a set number
of calls, so a slow leak in a third-party decoder is capped by the recycle
rather than by the container's memory limit.

### Sizing

`CPU_POOL_MAX_WORKERS=0` derives the count from this process's real CPU
allowance minus one — leaving the loop a core, because a pool that saturates
every core takes back the responsiveness offloading was for.

"Real allowance" matters: `os.cpu_count()` reports the *machine's* cores, so a
pod limited to 2 CPUs on a 64-core node sees 64 and would spawn 64 interpreters
to fight over two cores' worth of quota. `os.process_cpu_count()` (3.13+) and
`os.sched_getaffinity` report what the process may actually use.

**uvicorn's `--workers` multiplies this.** Four server workers with four pool
workers each is sixteen child processes plus four parents. Size against the
container's limit, not the node's.

## IO-bound work: `gather_bounded`

```python
from functools import partial
from src.parallel import WhenOneFails, gather_bounded, map_bounded

results = await map_bounded(fetch_profile, user_ids, limit=8)

results = await gather_bounded(
    (partial(fetch, url) for url in urls),
    limit=8,
    when_one_fails=WhenOneFails.RUN_ALL,
)
```

### The two things wrong with plain `asyncio.gather`

**It is unbounded.** `gather(*(fetch(u) for u in urls))` over ten thousand URLs
opens ten thousand sockets at once. The upstream sees a thundering herd, the
process runs out of file descriptors, and the connection pool underneath
silently serialises everything behind its own limit while ten thousand tasks sit
in the loop's ready queue making the scheduler slower.

**It leaks siblings on failure.** `gather(..., return_exceptions=False)`
propagates the first exception to the awaiting coroutine and **does not cancel
the other tasks**. A handler that fans out five calls, has one fail, and returns
a 502 leaves four requests in flight against the upstream — writing results
nowhere, holding connections, logging exceptions from a request that finished
minutes ago. Under load that is a slow leak with no obvious cause.

Both are asserted directly in `tests/test_parallel_io.py::TestTheProblemBeingSolved`,
against plain `asyncio.gather`, so the justification for this module fails the
build if it ever stops being true.

### Factories, not coroutines

`gather_bounded` takes callables that *return* awaitables. A coroutine object
exists the moment you write `fetch(url)`, so passing coroutines means
constructing all ten thousand up front — the memory this function exists to
bound. And a coroutine never awaited (because a fail-fast run cancelled the
batch before reaching it) emits `RuntimeWarning: coroutine ... was never
awaited` from the garbage collector, at a point in the log unrelated to the
failure.

`map_bounded` exists because building factories by hand is where the mistake
gets made: `(fn(item) for item in items)` looks like a generator of factories
and is a generator of coroutines.

### Failure policy

`WhenOneFails.CANCEL_REST` (the default) cancels the outstanding items, **awaits
them**, and re-raises the first exception. The awaiting is the load-bearing
part: `task.cancel()` only schedules a `CancelledError`; the coroutine has to be
resumed for its `finally` blocks — the ones releasing connections — to run.

`WhenOneFails.RUN_ALL` lets everything finish and returns exceptions in place.
Use it when items are independently useful (a fan-out of notifications, a batch
import) and one bad row must not discard ninety-nine good ones. `partition_results`
splits the outcome, keeping input indices so the caller can report *which* items
failed.

Cancellation from outside propagates either way: if the caller is cancelled by a
client disconnect or an enclosing `wait_for`, every outstanding item is
cancelled and drained before `CancelledError` leaves.

### Results are in input order

Never completion order. That is what makes the result safe to zip back against
whatever produced the inputs — a bug that only appears once one call is slower
than the others.

### A semaphore bounds concurrency, not rate

Eight concurrent calls that each take 10ms is 800 requests per second at an
upstream that may only permit 100. When the upstream publishes a *rate*, that
needs a token bucket as well. The semaphore only keeps this process from opening
more sockets than it can afford.

### Share the semaphore when the resource is shared

`limit=8` inside a handler that fifty concurrent requests are running is four
hundred sockets — and 8 is the number the engineer reading that line will reason
about. Per-call limits do not compose. `get_outbound_semaphore()`
(`OUTBOUND_CONCURRENCY_LIMIT`, default 20) is one process-wide bound to pass as
`semaphore=` wherever the thing being protected is a resource rather than a
batch.

### Retries compose on the inside

`src/decorators/retry.py` wraps a single call. Put it inside the slot —
`partial(retry(...)(fetch), url)` — so an attempt that fails on its own is
retried within its slot rather than failing the whole batch.

## Configuration

| Setting | Default | Notes |
| --- | --- | --- |
| `CPU_POOL_MAX_WORKERS` | `0` | 0 = derive from the CPU allowance, minus one |
| `CPU_POOL_MAX_TASKS_PER_CHILD` | `100` | Retire a worker after N calls; caps leaks |
| `CPU_POOL_QUEUE_DEPTH_PER_WORKER` | `4` | Past `workers * (1 + depth)`, offloads get 503 |
| `CPU_POOL_START_METHOD` | `spawn` | `spawn` or `forkserver`; `fork` is rejected |
| `OUTBOUND_CONCURRENCY_LIMIT` | `20` | Process-wide ceiling for shared fan-outs |

## Errors

| Exception | Status | Meaning |
| --- | --- | --- |
| `CpuTaskTimeoutError` | 504 | Ran out of time. `details["enforced_by"]` is `worker` or `wait` |
| `CpuPoolOverloadedError` | 503 | Refused at the door; nothing ran, retry is cheap |
| `CpuPoolUnavailableError` | 503 | Pool not started, shutting down, or replacing dead workers |
| `NotOffloadableError` | 500 | A lambda or closure was passed to `run` — a programming error |
| `WorkerDeadline` | — | Internal, worker-side only. Never reaches a caller |

## Relationship to the rest of the codebase

- **`src/tasks/`** — Celery, for work measured in seconds. `CpuPool` is for tens
  of milliseconds to a couple of seconds, inside one request.
- **`src/locking/`, `src/distributed_lock/`** — concurrency *control*. This
  module is concurrency *capacity*. A fan-out that mutates shared state needs
  both.
- **`src/decorators/retry.py`** — composes inside a `gather_bounded` slot.
- **`src/structured/`** — the same work seen from the other side. This module
  answers "how many at once"; that one answers "for how long, who owns the
  task, and what runs when the caller gives up". `gather_bounded` already
  cancels and drains its siblings; what it cannot supply is a budget spanning
  the whole handler, which is `deadline()`.
