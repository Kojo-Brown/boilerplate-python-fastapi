"""`@cached` — an in-process TTL + LRU memo for expensive calls.

Scope, stated up front so nobody reaches for this expecting Redis: the store is
a dict inside one worker process. Two Uvicorn workers keep two copies, a
deploy empties both, and nothing here invalidates across machines. That makes
it right for things that are expensive, read-mostly and tolerant of being
briefly stale in one process — a JWKS document, a feature-flag snapshot, a
config row read on every request — and wrong for anything a user expects to see
change immediately after they change it.

What it does that `functools.lru_cache` does not:

- **Entries expire.** `lru_cache` holds a value until eviction pressure removes
  it, which for a small key space is never.
- **Coroutine functions work.** Wrapping an `async def` in `lru_cache` caches
  the *coroutine object*, and awaiting the second hit raises
  `RuntimeError: cannot reuse already awaited coroutine`.
- **Concurrent misses collapse.** The async wrapper holds a per-key lock across
  the miss, so N simultaneous callers of a cold key produce one underlying
  call, not N. Without that, a cache in front of a slow dependency stampedes it
  exactly when it is already struggling.
- **Failures are not cached.** An exception propagates and leaves the key cold.

Two limitations worth knowing before use:

- **Do not decorate a method.** `self` becomes part of the key, so the cache
  holds a strong reference to every instance it has seen and they stop being
  collected. Cache a module-level function and pass the fields it needs.
- **The counters are not thread-safe.** Reads and writes interleave safely
  under one event loop, which is how this application runs. Driving the sync
  wrapper from a thread pool can lose a `hits` increment — the stats drift,
  the cache does not corrupt.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Hashable, Mapping
from dataclasses import dataclass
from typing import Any, Final, ParamSpec, TypeVar, overload

import structlog

from src.decorators.base import DEFAULT_CLOCK, Clock, is_async_callable

logger = structlog.get_logger(__name__)

P = ParamSpec("P")
R = TypeVar("R")

#: Turns a call's arguments into something hashable. See `make_key`.
KeyBuilder = Callable[[tuple[object, ...], Mapping[str, object]], Hashable]

# Separates positional from keyword arguments in the default key so that
# `f(1)` and `f(x=1)` cannot collide by accident.
_KWD_MARK: Final = object()


class UncacheableArgumentError(TypeError):
    """Raised when a call's arguments cannot form a cache key.

    A `TypeError` rather than an `AppException` on purpose: passing a dict to a
    cached function is a defect in the calling code, not a condition a client
    can provoke or a handler should render as a tidy JSON error. It reaches the
    generic 500 handler, which is the correct outcome for a bug.
    """


def make_key(args: tuple[object, ...], kwargs: Mapping[str, object]) -> Hashable:
    """Default `KeyBuilder`: the literal call shape.

    Fast — a tuple build and one `hash` — but it keys on *how* the function was
    called, not on the arguments it ended up with. `f(1)` and `f(x=1)` are
    different keys, and so are `f(1)` and `f(1, flag=False)` when `flag`
    already defaults to `False`. That costs a duplicate entry, never a wrong
    answer. Pass `key=signature_key(f)` when the call sites vary and the
    duplication matters.
    """
    key: tuple[object, ...] = args
    if kwargs:
        key = (*key, _KWD_MARK, *sorted(kwargs.items()))
    try:
        hash(key)
    except TypeError as exc:
        raise UncacheableArgumentError(
            "Cache key arguments must be hashable; "
            f"got {[type(arg).__name__ for arg in (*args, *kwargs.values())]}. "
            "Pass a scalar, or supply key= to build one."
        ) from exc
    return key


def signature_key(func: Callable[..., object]) -> KeyBuilder:
    """`KeyBuilder` that keys on the *bound* arguments, defaults included.

    Binds each call against the real signature first, so `f(1)`, `f(x=1)` and
    `f(1, flag=False)` (with `flag=False` as the default) all land on one entry.
    The cost is an `inspect.BoundArguments` per call — a few microseconds, which
    is noise in front of anything worth caching and is not in front of anything
    that is not.
    """
    signature = inspect.signature(func)

    def build(args: tuple[object, ...], kwargs: Mapping[str, object]) -> Hashable:
        try:
            bound = signature.bind(*args, **kwargs)
        except TypeError as exc:
            # The call would not have been valid anyway; let the failure name
            # the real problem instead of surfacing as a cache-key error.
            raise UncacheableArgumentError(
                f"Arguments do not match the signature of {func.__qualname__}: {exc}"
            ) from exc
        bound.apply_defaults()

        items: list[tuple[str, object]] = []
        for name, value in bound.arguments.items():
            if signature.parameters[name].kind is inspect.Parameter.VAR_KEYWORD:
                # `**kwargs` arrives as a dict, which is unhashable; ordering it
                # also makes f(a=1, b=2) and f(b=2, a=1) the same key.
                items.append((name, tuple(sorted(value.items()))))
            else:
                items.append((name, value))

        key = tuple(items)
        try:
            hash(key)
        except TypeError as exc:
            raise UncacheableArgumentError(
                f"Cache key arguments for {func.__qualname__} must be hashable; "
                f"got {dict(bound.arguments)!r}."
            ) from exc
        return key

    return build


@dataclass(frozen=True, slots=True)
class CacheInfo:
    """A snapshot of one cache's counters. Returned by `cache_info()`."""

    hits: int
    misses: int
    evictions: int
    expirations: int
    size: int
    maxsize: int
    ttl: float

    @property
    def hit_rate(self) -> float:
        """Hits as a fraction of lookups, or 0.0 before the first lookup."""
        lookups = self.hits + self.misses
        return self.hits / lookups if lookups else 0.0


@dataclass
class _Entry[R]:
    value: R
    expires_at: float


class _TTLStore[R]:
    """Bounded LRU map whose entries also expire. Not part of the public API.

    Recency is `OrderedDict` insertion order, refreshed on every hit. Expiry is
    lazy — an entry is only noticed to be stale when it is looked up, or when
    eviction reaches it — because a sweeper task would cost a background
    coroutine per cache to reclaim memory that `maxsize` already bounds.
    """

    def __init__(self, *, maxsize: int, ttl: float, clock: Clock) -> None:
        self._entries: OrderedDict[Hashable, _Entry[R]] = OrderedDict()
        self._maxsize = maxsize
        self._ttl = ttl
        self._clock = clock
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._expirations = 0

    def peek(self, key: Hashable) -> _Entry[R] | None:
        """Return a live entry, or None. Records nothing — see `hit`/`miss`."""
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.expires_at <= self._clock():
            del self._entries[key]
            self._expirations += 1
            return None
        self._entries.move_to_end(key)
        return entry

    def hit(self) -> None:
        self._hits += 1

    def miss(self) -> None:
        self._misses += 1

    def set(self, key: Hashable, value: R) -> None:
        if key not in self._entries and len(self._entries) >= self._maxsize:
            self._entries.popitem(last=False)
            self._evictions += 1
        self._entries[key] = _Entry(value=value, expires_at=self._clock() + self._ttl)
        self._entries.move_to_end(key)

    def invalidate(self, key: Hashable) -> bool:
        return self._entries.pop(key, None) is not None

    def clear(self) -> None:
        self._entries.clear()
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._expirations = 0

    def info(self) -> CacheInfo:
        return CacheInfo(
            hits=self._hits,
            misses=self._misses,
            evictions=self._evictions,
            expirations=self._expirations,
            size=len(self._entries),
            maxsize=self._maxsize,
            ttl=self._ttl,
        )


class _CacheAPI[**P, R]:
    """The bookkeeping both wrappers share, minus the call itself.

    Never instantiated directly; `__call__` exists so that this class is a
    callable as far as the type checker is concerned, which is what lets
    `functools.update_wrapper` treat an instance as the wrapper it is.
    """

    def __init__(
        self,
        func: Callable[..., Any],
        *,
        maxsize: int,
        ttl: float,
        clock: Clock,
        key: KeyBuilder,
    ) -> None:
        self._func = func
        self._key = key
        self._store: _TTLStore[R] = _TTLStore(maxsize=maxsize, ttl=ttl, clock=clock)
        # Copies __name__, __doc__, __qualname__ and __wrapped__ onto this
        # instance. `inspect.signature` follows __wrapped__, so FastAPI, pytest
        # fixtures and `help()` still see the signature of the real function —
        # the static half of that guarantee is the ParamSpec on __call__.
        functools.update_wrapper(self, func)

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> Any:
        raise NotImplementedError

    def cache_info(self) -> CacheInfo:
        """Counters since the last `cache_clear()`."""
        return self._store.info()

    def cache_clear(self) -> None:
        """Drop every entry and reset the counters."""
        self._store.clear()

    def cache_invalidate(self, *args: P.args, **kwargs: P.kwargs) -> bool:
        """Drop the entry for one specific call. True if there was one.

        Takes the same arguments as the function, so invalidating after a write
        reads as the call it undoes:

            await get_settings(tenant_id)
            get_settings.cache_invalidate(tenant_id)
        """
        return self._store.invalidate(self._key(args, kwargs))


class CachedFunction[**P, R](_CacheAPI[P, R]):
    """A synchronous function plus its cache. Returned by `cached()`."""

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        key = self._key(args, kwargs)
        entry = self._store.peek(key)
        if entry is not None:
            self._store.hit()
            return entry.value

        self._store.miss()
        value: R = self._func(*args, **kwargs)
        self._store.set(key, value)
        return value


class AsyncCachedFunction[**P, R](_CacheAPI[P, R]):
    """A coroutine function plus its cache. Returned by `cached()`.

    Misses are single-flighted: the first caller for a cold key holds a lock
    while it computes, and everyone who arrives meanwhile waits and then reads
    the value the first one stored. The locks are refcounted and dropped once
    idle, so a large key space does not leave a lock per key behind.
    """

    def __init__(
        self,
        func: Callable[..., Any],
        *,
        maxsize: int,
        ttl: float,
        clock: Clock,
        key: KeyBuilder,
    ) -> None:
        super().__init__(func, maxsize=maxsize, ttl=ttl, clock=clock, key=key)
        self._locks: dict[Hashable, asyncio.Lock] = {}
        self._waiting: dict[Hashable, int] = {}

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        key = self._key(args, kwargs)
        entry = self._store.peek(key)
        if entry is not None:
            self._store.hit()
            return entry.value

        lock = self._acquire_slot(key)
        try:
            async with lock:
                # Filled while we queued: the caller ahead of us has already
                # done this exact call, so this is a hit that merely arrived
                # late, not a second miss.
                entry = self._store.peek(key)
                if entry is not None:
                    self._store.hit()
                    return entry.value

                self._store.miss()
                value: R = await self._func(*args, **kwargs)
                self._store.set(key, value)
                return value
        finally:
            self._release_slot(key)

    def _acquire_slot(self, key: Hashable) -> asyncio.Lock:
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        self._waiting[key] = self._waiting.get(key, 0) + 1
        return lock

    def _release_slot(self, key: Hashable) -> None:
        remaining = self._waiting[key] - 1
        if remaining > 0:
            self._waiting[key] = remaining
            return
        del self._waiting[key]
        self._locks.pop(key, None)

    def cache_clear(self) -> None:
        """Drop every entry and reset the counters.

        The in-flight locks are left alone deliberately: a caller currently
        computing a miss still has to release the lock it holds, and clearing
        the map underneath it would let a second caller start the same work
        against a lock nobody is holding.
        """
        super().cache_clear()


class CacheDecorator:
    """Applies a cache to one function. Returned by `cached()`."""

    def __init__(
        self,
        *,
        ttl: float,
        maxsize: int,
        clock: Clock,
        key: KeyBuilder | None,
    ) -> None:
        if ttl <= 0:
            raise ValueError("ttl must be greater than 0 seconds.")
        if maxsize < 1:
            raise ValueError("maxsize must be at least 1.")
        self._ttl = ttl
        self._maxsize = maxsize
        self._clock = clock
        self._key = key

    # The two overloads genuinely overlap, and the order resolves it the way the
    # runtime does. A plain `def` that *returns* an awaitable matches both;
    # `inspect.iscoroutinefunction` says no, so it gets a `CachedFunction` while
    # this annotation promises an `AsyncCachedFunction`. Flipping the order,
    # which is what mypy suggests, would mistype every `async def` instead — a
    # far commoner shape — so the narrow case is accepted and named here rather
    # than traded for the broad one. Declare such a function `async def` if you
    # hit it.
    @overload
    def __call__(  # type: ignore[overload-overlap]
        self, func: Callable[P, Awaitable[R]]
    ) -> AsyncCachedFunction[P, R]: ...

    @overload
    def __call__(self, func: Callable[P, R]) -> CachedFunction[P, R]: ...

    # The overloads above are the checked contract; `Any` here only keeps the
    # implementation compatible with both of them.
    def __call__(self, func: Callable[..., Any]) -> Any:
        key = self._key if self._key is not None else make_key

        if not is_async_callable(func):
            return CachedFunction(
                func, maxsize=self._maxsize, ttl=self._ttl, clock=self._clock, key=key
            )

        wrapper: AsyncCachedFunction[..., Any] = AsyncCachedFunction(
            func, maxsize=self._maxsize, ttl=self._ttl, clock=self._clock, key=key
        )
        # An instance whose `__call__` is `async def` is not itself a coroutine
        # function, so `inspect.iscoroutinefunction` would say no. FastAPI reads
        # exactly that to decide whether a dependency is awaited or shipped to a
        # thread pool, and a cached dependency in a thread pool returns an
        # un-awaited coroutine. The mark is the supported way to say otherwise.
        inspect.markcoroutinefunction(wrapper)
        return wrapper


def cached(
    *,
    ttl: float,
    maxsize: int = 128,
    clock: Clock = DEFAULT_CLOCK,
    key: KeyBuilder | None = None,
) -> CacheDecorator:
    """Memoise the decorated function in this process for `ttl` seconds.

    Works on `async def` and plain `def` alike and preserves the signature of
    what it wraps. The returned object also carries `cache_info()`,
    `cache_clear()` and `cache_invalidate(*args, **kwargs)`.

    Read the module docstring before using this on anything user-facing: the
    cache is per-process, so "I saved it and it did not change" is a real
    failure mode for a value one worker has cached and another has not.

    Args:
        ttl: Seconds an entry stays fresh. Required — there is no default,
            because the right one depends entirely on what is being cached and
            an inherited guess is how a cache becomes a stale-data bug.
        maxsize: Entry ceiling; the least recently used entry is evicted on
            overflow. Bound it to what the key space actually is.
        clock: Expiry clock, `time.monotonic` by default. Pass a controllable
            one to test expiry without sleeping.
        key: Builds the cache key from `(args, kwargs)`. Defaults to `make_key`;
            pass `signature_key(func)` to key on bound arguments instead.

    Raises:
        ValueError: `ttl` is not positive or `maxsize` is below 1. Raised at
            decoration time, so the mistake surfaces at import.

    Example:
        >>> @cached(ttl=300, maxsize=1)
        ... async def jwks() -> dict[str, object]:
        ...     response = await client.get(JWKS_URL)
        ...     return response.json()
        >>>
        >>> jwks.cache_info().hit_rate
        0.0
    """
    return CacheDecorator(ttl=ttl, maxsize=maxsize, clock=clock, key=key)
