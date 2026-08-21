"""The process-wide CPU pool, and the shared semaphores that bound fan-out.

Separate from `cpu.py` so that module stays free of the settings object and can
be constructed with explicit arguments in a test — the same split as
`src/distributed_lock/factory.py`.
"""

from __future__ import annotations

import asyncio
from functools import lru_cache

from src.config import Settings, settings
from src.parallel.cpu import CpuPool


@lru_cache(maxsize=1)
def get_cpu_pool() -> CpuPool:
    """The pool for this server process.

    Cached because it owns worker processes: a pool per request would spawn and
    tear down interpreters per call, which costs more than the work being
    offloaded and would eventually exhaust the process table. The lifespan
    starts and stops this one instance.

    Not started here. Building a `CpuPool` allocates nothing but a semaphore, so
    importing this module — which `src/dependencies.py` does, at import time —
    must not have the side effect of creating an executor. Call
    `get_cpu_pool.cache_clear()` after changing settings in a test.
    """
    return build_cpu_pool()


def build_cpu_pool(config: Settings | None = None) -> CpuPool:
    """A new, unstarted pool from configuration."""
    resolved = config if config is not None else settings
    return CpuPool(
        max_workers=resolved.CPU_POOL_MAX_WORKERS or None,
        max_tasks_per_child=resolved.CPU_POOL_MAX_TASKS_PER_CHILD or None,
        queue_depth_per_worker=resolved.CPU_POOL_QUEUE_DEPTH_PER_WORKER,
        start_method=resolved.CPU_POOL_START_METHOD,
    )


@lru_cache(maxsize=1)
def get_outbound_semaphore() -> asyncio.Semaphore:
    """One bound on concurrent outbound HTTP, shared by every fan-out.

    Per-call limits do not compose: `limit=8` inside a handler that fifty
    concurrent requests are running is four hundred sockets, and the number
    written at the call site is the one an engineer will reason about. A
    process-wide semaphore is the only place the real ceiling can live.

    Safe to cache across the process despite belonging to no loop: since Python
    3.10 `asyncio.Semaphore` no longer binds a loop at construction, and it
    binds waiters to the running loop only while they wait. A process runs one
    loop; a test suite that runs several must call
    `get_outbound_semaphore.cache_clear()` between them, or pass its own.
    """
    return asyncio.Semaphore(settings.OUTBOUND_CONCURRENCY_LIMIT)
