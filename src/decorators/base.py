"""Shared plumbing for the decorators in this package.

Nothing here imports the settings object, SQLAlchemy or FastAPI. A decorator
that reached for `settings` would be undecidable at import time — the module
that applies it runs before the environment is necessarily loaded — and would
make "cache this for 30 seconds" untestable without a config file. Every knob
is therefore an argument at the decoration site, and every source of
non-determinism (the clock, sleeping, the jitter RNG) is injectable so a test
can pin it.

## Why every decorator here is a class

`@timed` and `@timed()` cannot both be given a precise type: the two-form
version needs an implementation signature loose enough to accept a function
*or* nothing, which erases the `ParamSpec` link between what goes in and what
comes out. Requiring the parentheses — `@timed()`, `@retry()`, `@cached()` —
buys back a fully checked signature on every public entry point, so the cost is
one pair of brackets.

The object the factory returns is a small class with an overloaded `__call__`
rather than a closure, because an overload cannot be attached to a value. The
first overload matches coroutine functions and the second everything else,
which is also the order the runtime dispatch checks them in.
"""

from __future__ import annotations

import functools
import inspect
import random
import time
from collections.abc import Awaitable, Callable
from typing import Final

#: Returns a monotonically increasing number of seconds. `time.monotonic` for
#: expiry decisions, `time.perf_counter` for measuring a duration — never
#: `time.time`, which a clock adjustment can move backwards.
Clock = Callable[[], float]

#: `asyncio.sleep`, or a test double that records the delay without spending it.
AsyncSleeper = Callable[[float], Awaitable[None]]

#: `time.sleep`, or a test double. Blocking — see `retry` for when that is a bug.
SyncSleeper = Callable[[float], None]

DEFAULT_CLOCK: Clock = time.monotonic

DEFAULT_TIMER: Clock = time.perf_counter


def is_async_callable(obj: object) -> bool:
    """Whether `obj(...)` has to be awaited.

    `inspect.iscoroutinefunction` answers this for an `async def` and nothing
    else. A callable *object* whose `__call__` is a coroutine function — which
    is exactly what `@cached` returns — reads as synchronous to it, and the
    failure mode is silent rather than loud: stacking `@retry` on `@cached`
    would take the synchronous branch, hand back the coroutine without awaiting
    it, and retry nothing, because the call cannot fail inside a `try` block it
    never entered. So each decorator asks this instead.
    """
    unwrapped = obj
    while isinstance(unwrapped, functools.partial):
        unwrapped = unwrapped.func

    if inspect.iscoroutinefunction(unwrapped):
        return True

    call = getattr(unwrapped, "__call__", None)
    return call is not None and inspect.iscoroutinefunction(call)


def default_event_name(func: object) -> str:
    """`module.qualname` for anything callable, partials and objects included.

    Reading `func.__qualname__` directly is fine for an `async def` and an
    `AttributeError` for a `functools.partial`, which turns "decorate this" into
    a crash at import time. Partials are unwrapped so the name points at what
    actually runs, and a callable object falls back to its type — a stable,
    recognisable event name beats a stack trace.
    """
    target = func
    while isinstance(target, functools.partial):
        target = target.func

    module = getattr(target, "__module__", None) or type(target).__module__
    qualname = getattr(target, "__qualname__", None) or type(target).__qualname__
    return f"{module}.{qualname}"


# `base_delay * 2 ** (attempt - 1)` is an int power, and `0.1 * 2 ** 2000`
# raises OverflowError rather than returning inf. Capping the exponent keeps a
# misconfigured `attempts` from turning into a crash inside the backoff maths;
# 2**32 seconds already exceeds any sane `max_delay`, so the cap never bites in
# a configuration that was going to work anyway.
MAX_BACKOFF_EXPONENT: Final[int] = 32

#: Shared jitter source. Module-level so the default is one stream rather than
#: a new `Random` per decoration; tests pass a seeded instance instead.
DEFAULT_RNG: Final[random.Random] = random.Random()


def backoff_delay(
    attempt: int,
    *,
    base_delay: float,
    max_delay: float,
    jitter: bool,
    rng: random.Random,
) -> float:
    """Full-jitter exponential backoff for the wait after `attempt`.

    The ceiling doubles per attempt and is clamped by `max_delay`; with jitter
    on, the actual wait is drawn uniformly from `[0, ceiling]`. Full jitter
    rather than the ceiling itself because the failure being retried is usually
    shared — a database that just fell over disappoints every worker at once,
    and a deadlock is by definition a fight between concurrent transactions —
    so un-jittered backoff marches the losers back in step and the retry storm
    lands as one spike instead of spreading out. For deadlocks specifically it
    is worse than a spike: two transactions that retry in lockstep can deadlock
    against each other again on the same pair of rows.

    Lives here rather than in either caller because there are two retry loops
    in this codebase — `src/decorators/retry.py` for transient call failures
    and `src/locking/retry.py` for database conflicts — and a backoff policy
    that drifts between them is a difference nobody would notice until a
    production incident made one of them behave unlike the other.

    `attempt` is 1-based: the wait after the first failure uses `base_delay`.
    """
    exponent = min(attempt - 1, MAX_BACKOFF_EXPONENT)
    ceiling = min(max_delay, base_delay * (2.0**exponent))
    if not jitter:
        return ceiling
    return rng.uniform(0.0, ceiling)


def duration_ms(seconds: float) -> float:
    """Convert a `Clock` delta to milliseconds, rounded for log readability.

    Three decimals keeps microsecond resolution — enough to see a cache hit —
    without emitting seventeen significant figures of float noise into a log
    aggregator that will index every one of them.
    """
    return round(seconds * 1000, 3)
