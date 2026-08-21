"""Getting work off the event loop, in the two ways that are not the same.

The event loop is one thread, and a request handler that stops making progress
stops *the server* making progress. There are exactly two reasons a handler
stops, they have opposite fixes, and applying either fix to the other problem
makes things worse:

**It is waiting.** An HTTP call, a query, a file read. The thread is idle and
the fix is to overlap the waits — but overlapping them without a bound is how a
handler opens ten thousand sockets, and `asyncio.gather` leaves the siblings of
a failed call running with nobody awaiting them. `io.py` is the bounded,
cleaned-up version: `gather_bounded` and `map_bounded`.

**It is computing.** Parsing, resizing, hashing, compressing. The thread is
busy, so overlapping buys nothing, and moving it to a thread buys nothing
either — the GIL means a compute-bound thread takes the interpreter away from
the loop exactly as a compute-bound coroutine does. The work has to leave the
process. `cpu.py` is a `ProcessPoolExecutor` with the sharp edges of running one
underneath an async server made explicit: `spawn` rather than `fork`, deadlines
that stop the worker rather than only the wait, admission control instead of an
unbounded queue, and recovery from an OOM-killed child.

The third case — work that is neither, because it is *long* — belongs in
neither. Anything measured in seconds is a Celery task (`src/tasks/`), not a
pool slot held while a client waits on a socket.

See `docs/parallel-execution.md`.
"""

from src.parallel.cpu import (
    CAN_ENFORCE_WORKER_DEADLINE,
    DEFAULT_MAX_TASKS_PER_CHILD,
    DEFAULT_QUEUE_DEPTH_PER_WORKER,
    CpuPool,
    DeadlinedCpuPool,
    default_workers,
    ensure_offloadable,
    supported_start_methods,
)
from src.parallel.errors import (
    CpuPoolOverloadedError,
    CpuPoolUnavailableError,
    CpuTaskTimeoutError,
    NotOffloadableError,
    WorkerDeadline,
)
from src.parallel.factory import (
    build_cpu_pool,
    get_cpu_pool,
    get_outbound_semaphore,
)
from src.parallel.io import (
    AwaitableFactory,
    WhenOneFails,
    gather_bounded,
    map_bounded,
    partition_results,
)

__all__ = [
    "CAN_ENFORCE_WORKER_DEADLINE",
    "DEFAULT_MAX_TASKS_PER_CHILD",
    "DEFAULT_QUEUE_DEPTH_PER_WORKER",
    "AwaitableFactory",
    "CpuPool",
    "CpuPoolOverloadedError",
    "CpuPoolUnavailableError",
    "CpuTaskTimeoutError",
    "DeadlinedCpuPool",
    "NotOffloadableError",
    "WhenOneFails",
    "WorkerDeadline",
    "build_cpu_pool",
    "default_workers",
    "ensure_offloadable",
    "gather_bounded",
    "get_cpu_pool",
    "get_outbound_semaphore",
    "map_bounded",
    "partition_results",
    "supported_start_methods",
]
