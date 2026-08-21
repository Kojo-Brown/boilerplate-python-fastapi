"""Pool construction from settings, and the wiring into the app."""

from __future__ import annotations

import asyncio

import pytest

from src.config import Settings
from src.parallel.cpu import CpuPool, default_workers
from src.parallel.factory import (
    build_cpu_pool,
    get_cpu_pool,
    get_outbound_semaphore,
)


def make_settings(**overrides: object) -> Settings:
    base = {
        "DATABASE_URL": "postgresql+asyncpg://fake:fake@localhost/fake",
        "SECRET_KEY": "not-a-real-key-for-tests-only",
    }
    return Settings(**{**base, **overrides})  # type: ignore[arg-type]


class TestBuildCpuPool:
    def test_reads_the_worker_count_from_settings(self) -> None:
        pool = build_cpu_pool(make_settings(CPU_POOL_MAX_WORKERS=3))
        assert pool.max_workers == 3

    def test_zero_workers_means_derive_the_count(self) -> None:
        # 0 rather than None because pydantic-settings reads an int from the
        # environment and "" is not one. The sentinel has to be a valid int.
        pool = build_cpu_pool(make_settings(CPU_POOL_MAX_WORKERS=0))
        assert pool.max_workers == default_workers()

    def test_queue_depth_shapes_capacity(self) -> None:
        pool = build_cpu_pool(
            make_settings(CPU_POOL_MAX_WORKERS=2, CPU_POOL_QUEUE_DEPTH_PER_WORKER=3)
        )
        assert pool.capacity == 8

    def test_a_queue_depth_of_zero_admits_only_what_can_run(self) -> None:
        pool = build_cpu_pool(
            make_settings(CPU_POOL_MAX_WORKERS=2, CPU_POOL_QUEUE_DEPTH_PER_WORKER=0)
        )
        assert pool.capacity == 2

    def test_forkserver_is_accepted(self) -> None:
        pool = build_cpu_pool(make_settings(CPU_POOL_START_METHOD="forkserver"))
        assert pool.running is False

    def test_settings_reject_fork_before_the_pool_ever_sees_it(self) -> None:
        # The `Literal` in `Settings` is the first line of defence: a deployment
        # that sets CPU_POOL_START_METHOD=fork fails at start-up, not at the
        # first offload.
        with pytest.raises(ValueError):
            make_settings(CPU_POOL_START_METHOD="fork")

    def test_the_pool_is_not_started_by_building_it(self) -> None:
        # Importing `src.dependencies` imports this module, so construction
        # must not have the side effect of creating an executor.
        assert build_cpu_pool(make_settings()).running is False


class TestGetCpuPool:
    def test_is_cached_so_every_caller_shares_one_pool(self) -> None:
        get_cpu_pool.cache_clear()
        try:
            assert get_cpu_pool() is get_cpu_pool()
        finally:
            get_cpu_pool.cache_clear()

    def test_cache_clear_yields_a_fresh_pool(self) -> None:
        get_cpu_pool.cache_clear()
        first = get_cpu_pool()
        get_cpu_pool.cache_clear()
        try:
            assert get_cpu_pool() is not first
        finally:
            get_cpu_pool.cache_clear()

    def test_returns_an_unstarted_pool(self) -> None:
        get_cpu_pool.cache_clear()
        try:
            assert isinstance(get_cpu_pool(), CpuPool)
            assert get_cpu_pool().running is False
        finally:
            get_cpu_pool.cache_clear()


class TestOutboundSemaphore:
    def test_is_cached_so_the_bound_is_process_wide(self) -> None:
        get_outbound_semaphore.cache_clear()
        try:
            assert get_outbound_semaphore() is get_outbound_semaphore()
        finally:
            get_outbound_semaphore.cache_clear()

    async def test_bounds_at_the_configured_limit(self) -> None:
        get_outbound_semaphore.cache_clear()
        try:
            semaphore = get_outbound_semaphore()
            acquired = 0
            for _ in range(200):
                if semaphore.locked():
                    break
                await semaphore.acquire()
                acquired += 1
            assert semaphore.locked() is True
            assert acquired == 20
        finally:
            get_outbound_semaphore.cache_clear()

    def test_is_an_asyncio_semaphore_built_outside_a_loop(self) -> None:
        # Safe since 3.10, which stopped binding a loop at construction. Stated
        # as a test because the old behaviour is still what most references
        # describe, and reverting to a per-loop factory would be a regression
        # nobody would notice until two loops shared a process.
        get_outbound_semaphore.cache_clear()
        try:
            assert isinstance(get_outbound_semaphore(), asyncio.Semaphore)
        finally:
            get_outbound_semaphore.cache_clear()


class TestAppWiring:
    async def test_the_lifespan_starts_and_stops_the_pool(self) -> None:
        # The pool owns child processes, so start-up and shutdown have to be
        # driven by something. Left out of the lifespan it would still work —
        # `start` is idempotent and the first request would build it — but
        # nothing would ever tear it down, and a reload would orphan the
        # workers.
        from src.main import app, lifespan

        get_cpu_pool.cache_clear()
        try:
            pool = get_cpu_pool()
            assert pool.running is False
            async with lifespan(app):
                assert pool.running is True
            assert pool.running is False
        finally:
            get_cpu_pool.cache_clear()

    def test_the_dependency_alias_resolves_to_the_shared_pool(self) -> None:
        from typing import get_args

        from src.dependencies import CpuPoolDep

        get_cpu_pool.cache_clear()
        try:
            # `Annotated[CpuPool, Depends(get_cpu_pool)]`. FastAPI resolves an
            # override by the *callable* inside `Depends`, so that is what a
            # test would target and what this alias has to carry.
            annotated_type, depends = get_args(CpuPoolDep)
            assert annotated_type is CpuPool
            assert depends.dependency is get_cpu_pool
        finally:
            get_cpu_pool.cache_clear()
