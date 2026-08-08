"""`@cached` — hits, expiry, eviction, single-flight and invalidation.

Time is injected everywhere, so expiry is asserted by moving a fake clock
rather than by sleeping. The single-flight tests use `asyncio.Event` to pin the
interleaving instead of racing real tasks and hoping.
"""

from __future__ import annotations

import asyncio
import inspect

import pytest

from src.decorators import UncacheableArgumentError, cached, make_key, signature_key


class FakeClock:
    """A monotonic clock a test can move."""

    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


async def test_second_call_is_served_from_the_cache() -> None:
    calls = 0

    @cached(ttl=60, clock=FakeClock())
    async def load(key: str) -> str:
        nonlocal calls
        calls += 1
        return f"value-{key}"

    assert await load("a") == "value-a"
    assert await load("a") == "value-a"
    assert calls == 1

    info = load.cache_info()
    assert (info.hits, info.misses, info.size) == (1, 1, 1)
    assert info.hit_rate == 0.5


async def test_distinct_arguments_are_distinct_entries() -> None:
    calls = 0

    @cached(ttl=60, clock=FakeClock())
    async def load(key: str) -> str:
        nonlocal calls
        calls += 1
        return key

    await load("a")
    await load("b")
    await load("a")

    assert calls == 2
    assert load.cache_info().size == 2


async def test_an_entry_expires_after_its_ttl() -> None:
    clock = FakeClock()
    calls = 0

    @cached(ttl=30, clock=clock)
    async def load() -> int:
        nonlocal calls
        calls += 1
        return calls

    assert await load() == 1
    clock.advance(29)
    assert await load() == 1

    clock.advance(2)
    assert await load() == 2
    assert load.cache_info().expirations == 1


async def test_the_least_recently_used_entry_is_evicted_first() -> None:
    @cached(ttl=60, maxsize=2, clock=FakeClock())
    async def load(key: str) -> str:
        return key

    await load("a")
    await load("b")
    await load("a")  # refreshes "a", leaving "b" as the oldest
    await load("c")

    info = load.cache_info()
    assert info.size == 2
    assert info.evictions == 1

    await load("b")  # evicted, so this misses
    assert load.cache_info().misses == 4


async def test_an_exception_is_not_cached() -> None:
    calls = 0

    @cached(ttl=60, clock=FakeClock())
    async def load() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("cold start")
        return "warm"

    with pytest.raises(RuntimeError):
        await load()

    assert await load() == "warm"
    assert load.cache_info().size == 1


async def test_concurrent_misses_collapse_into_one_call() -> None:
    """The stampede guard: N waiters on a cold key must produce one call."""
    calls = 0
    release = asyncio.Event()

    @cached(ttl=60, clock=FakeClock())
    async def load(key: str) -> str:
        nonlocal calls
        calls += 1
        await release.wait()
        return f"value-{key}"

    waiters = [asyncio.create_task(load("a")) for _ in range(5)]
    await asyncio.sleep(0)  # let every waiter reach the lock
    release.set()

    assert await asyncio.gather(*waiters) == ["value-a"] * 5
    assert calls == 1

    info = load.cache_info()
    assert (info.hits, info.misses) == (4, 1)


async def test_different_keys_do_not_block_each_other() -> None:
    """Single-flight is per key; a slow "a" must not hold up "b"."""
    started: list[str] = []
    release_a = asyncio.Event()

    @cached(ttl=60, clock=FakeClock())
    async def load(key: str) -> str:
        started.append(key)
        if key == "a":
            await release_a.wait()
        return key

    task_a = asyncio.create_task(load("a"))
    await asyncio.sleep(0)
    assert await load("b") == "b"

    release_a.set()
    assert await task_a == "a"
    assert started == ["a", "b"]


async def test_locks_are_released_once_a_key_goes_idle() -> None:
    """Otherwise a wide key space leaks one `asyncio.Lock` per key seen."""

    @cached(ttl=60, maxsize=64, clock=FakeClock())
    async def load(key: int) -> int:
        return key

    for key in range(20):
        await load(key)

    assert load._locks == {}
    assert load._waiting == {}


async def test_a_failed_miss_releases_the_lock_for_the_next_caller() -> None:
    calls = 0

    @cached(ttl=60, clock=FakeClock())
    async def load() -> str:
        nonlocal calls
        calls += 1
        raise RuntimeError("still cold")

    for _ in range(3):
        with pytest.raises(RuntimeError):
            await load()

    assert calls == 3
    assert load._locks == {}


async def test_cache_invalidate_drops_one_entry() -> None:
    calls = 0

    @cached(ttl=60, clock=FakeClock())
    async def load(key: str) -> str:
        nonlocal calls
        calls += 1
        return key

    await load("a")
    await load("b")

    assert load.cache_invalidate("a") is True
    assert load.cache_invalidate("a") is False

    await load("a")
    await load("b")
    assert calls == 3


async def test_cache_clear_empties_entries_and_counters() -> None:
    @cached(ttl=60, clock=FakeClock())
    async def load(key: str) -> str:
        return key

    await load("a")
    await load("a")
    load.cache_clear()

    info = load.cache_info()
    assert (info.hits, info.misses, info.size) == (0, 0, 0)
    assert info.hit_rate == 0.0


def test_sync_functions_are_cached_too() -> None:
    calls = 0

    @cached(ttl=60, clock=FakeClock())
    def load(key: str) -> str:
        nonlocal calls
        calls += 1
        return key

    assert load("a") == "a"
    assert load("a") == "a"
    assert calls == 1


async def test_signature_is_preserved_at_runtime() -> None:
    async def work(value: int, *, flag: bool = False) -> str:
        return f"{value}{flag}"

    decorated = cached(ttl=60)(work)

    assert inspect.signature(decorated) == inspect.signature(work)
    assert decorated.__name__ == "work"
    assert decorated.__doc__ == work.__doc__
    assert decorated.__wrapped__ is work


async def test_unhashable_arguments_raise_a_named_error() -> None:
    @cached(ttl=60, clock=FakeClock())
    async def load(payload: object) -> str:
        return str(payload)

    with pytest.raises(UncacheableArgumentError, match="hashable"):
        await load({"not": "hashable"})


def test_make_key_separates_positional_from_keyword() -> None:
    assert make_key((1,), {}) != make_key((), {"value": 1})
    assert make_key((), {"a": 1, "b": 2}) == make_key((), {"b": 2, "a": 1})


async def test_default_key_treats_call_shapes_as_distinct() -> None:
    """Documented cost of the fast key: a duplicate entry, never a wrong value."""
    calls = 0

    @cached(ttl=60, clock=FakeClock())
    async def load(key: str) -> str:
        nonlocal calls
        calls += 1
        return key

    await load("a")
    await load(key="a")

    assert calls == 2


async def test_signature_key_collapses_equivalent_call_shapes() -> None:
    calls = 0

    async def load(key: str, *, upper: bool = False) -> str:
        nonlocal calls
        calls += 1
        return key.upper() if upper else key

    decorated = cached(ttl=60, clock=FakeClock(), key=signature_key(load))(load)

    assert await decorated("a") == "a"
    assert await decorated(key="a") == "a"
    assert await decorated("a", upper=False) == "a"
    assert calls == 1

    assert await decorated("a", upper=True) == "A"
    assert calls == 2


async def test_signature_key_orders_var_keyword_arguments() -> None:
    calls = 0

    async def load(**options: int) -> int:
        nonlocal calls
        calls += 1
        return sum(options.values())

    decorated = cached(ttl=60, clock=FakeClock(), key=signature_key(load))(load)

    assert await decorated(a=1, b=2) == 3
    assert await decorated(b=2, a=1) == 3
    assert calls == 1


def test_signature_key_rejects_a_call_the_function_could_not_accept() -> None:
    def load(key: str) -> str:
        return key

    build = signature_key(load)
    with pytest.raises(UncacheableArgumentError, match="signature"):
        build((), {"wrong": "name"})


def test_signature_key_rejects_unhashable_bound_arguments() -> None:
    def load(payload: object) -> str:
        return str(payload)

    build = signature_key(load)
    with pytest.raises(UncacheableArgumentError, match="hashable"):
        build(([1, 2],), {})


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"ttl": 0}, "ttl"),
        ({"ttl": -1}, "ttl"),
        ({"ttl": 60, "maxsize": 0}, "maxsize"),
    ],
)
def test_an_unusable_cache_is_rejected_at_decoration(
    kwargs: dict[str, int], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        cached(**kwargs)
