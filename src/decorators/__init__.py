"""Cross-cutting decorators: `@cached`, `@retry`, `@timed`.

Three concerns that every one of caching, retrying and measuring would
otherwise smear across the body of the function that has the real work in it.
Each is applied at the decoration site, configured there and nowhere else, and
each preserves the signature of what it wraps — `ParamSpec` for the type
checker, `functools.update_wrapper` for anything reading `__name__` or
`inspect.signature` at runtime, so a wrong argument is still a type error and
FastAPI still sees the parameters it needs to build a route.

They compose, and the order is not arbitrary. Reading bottom-up — the order the
decorators actually apply:

    @timed(event="rates.fetch")           # 3. total, including every retry
    @retry(attempts=3, on=httpx.TransportError)
    @cached(ttl=60)                       # 1. innermost: a hit skips both
    async def fetch_rates(base: str) -> Rates: ...

`@cached` innermost so a hit costs nothing, `@retry` around it so failures are
never stored, `@timed` outermost so the recorded duration is the latency the
caller actually experienced. Put `@timed` innermost instead and it times one
attempt out of three; put `@cached` outermost and it caches the retry loop,
which is fine right up to the point where it caches a failure.

See `docs/decorators.md` for when each is the wrong tool.
"""

from src.decorators.base import AsyncSleeper, Clock, SyncSleeper
from src.decorators.cache import (
    AsyncCachedFunction,
    CacheDecorator,
    CachedFunction,
    CacheInfo,
    KeyBuilder,
    UncacheableArgumentError,
    cached,
    make_key,
    signature_key,
)
from src.decorators.retry import (
    ExceptionTypes,
    RetryDecorator,
    RetryPredicate,
    retry,
)
from src.decorators.timing import TimedDecorator, timed

__all__ = [
    "AsyncCachedFunction",
    "AsyncSleeper",
    "CacheDecorator",
    "CacheInfo",
    "CachedFunction",
    "Clock",
    "ExceptionTypes",
    "KeyBuilder",
    "RetryDecorator",
    "RetryPredicate",
    "SyncSleeper",
    "TimedDecorator",
    "UncacheableArgumentError",
    "cached",
    "make_key",
    "retry",
    "signature_key",
    "timed",
]
