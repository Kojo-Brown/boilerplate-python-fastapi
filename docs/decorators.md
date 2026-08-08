# Decorators: `@cached`, `@retry`, `@timed`

Three cross-cutting concerns that would otherwise be hand-written inside every
function that needs them. All three live in `src/decorators`, take their whole
configuration at the decoration site, and preserve the signature of what they
wrap.

```python
from src.decorators import cached, retry, timed
```

## Signature preservation

Each decorator is generic over a `ParamSpec`, so the type checker still sees
the parameters and return type of the underlying function:

```python
@cached(ttl=60)
async def get_plan(tenant_id: str) -> Plan: ...

await get_plan(42)          # error: Argument 1 has incompatible type "int"
plan: Plan = await get_plan("acme")   # inferred as Plan, not Any
```

At runtime `functools.update_wrapper` copies `__name__`, `__doc__`,
`__qualname__` and `__wrapped__`, so `inspect.signature` — which FastAPI uses
to build a route's parameters, and pytest uses to resolve fixtures — resolves
through the wrapper to the real function.

Every decorator is a **factory that must be called**: `@timed()`, not `@timed`.
Supporting both forms would need an implementation signature loose enough to
accept either a function or nothing, and that erases the `ParamSpec` link
between input and output. One pair of brackets buys a fully checked signature.

Each works on `async def` and plain `def` alike; the dispatch is
`inspect.iscoroutinefunction` at decoration time, so there is no per-call cost
to supporting both.

## `@timed`

Emits one structlog event per call with a `duration_ms` field.

```python
@timed(event="db.users.by_email", slow_after=0.25)
async def get_by_email(email: str) -> User | None: ...
```

| Argument | Meaning |
|---|---|
| `event` | Log event prefix. Defaults to `module.qualname`; `.duration` is appended. |
| `slow_after` | Seconds above which a *successful* call is logged at warning with `slow=True`. |
| `timer` | Elapsed-time source, `time.perf_counter` by default. Injectable for tests. |

Failures are timed too and logged at warning with `outcome="error"`;
cancellation gets `outcome="cancelled"`, because a client that hung up is not
an incident. The exception always propagates unchanged.

This is not a metrics client. It puts the number in the log record and leaves
turning that into a histogram to whatever ships the logs, which keeps a
Prometheus or OTel dependency out of the boilerplate.

## `@retry`

Re-runs a call that failed for a reason that might not recur, with full-jitter
exponential backoff.

```python
@retry(attempts=4, on=httpx.TransportError, base_delay=0.2, max_delay=5.0)
async def fetch_rates(base: str) -> Rates: ...
```

| Argument | Meaning |
|---|---|
| `attempts` | Total calls, not extra ones. `attempts=3` is one try and two retries. |
| `on` | Exception types worth retrying. Narrow it — the `Exception` default is deliberately too broad for production. |
| `give_up_on` | Checked first, wins over `on`. Carves a durable failure out of a retryable family. |
| `should_retry` | Predicate for when the type is not enough (e.g. a 503 body). Never called on the last attempt. |
| `base_delay` / `max_delay` | Backoff floor and ceiling in seconds. |
| `jitter` / `rng` | Full jitter on by default; pass a seeded `random.Random` to make delays reproducible. |
| `sleep` / `asleep` | Injectable sleepers. |

Two behaviours worth knowing:

- **The original exception propagates.** There is no `RetryError` wrapper,
  because this API derives status codes from exception types — wrapping a
  `ConflictError` would turn a 409 into a 500 after the third attempt. That
  attempts happened is in the logs, not in the exception type.
- **Cancellation is never retried,** even with `on=BaseException`. Retrying
  through an `asyncio.CancelledError` keeps work alive that nothing awaits and
  makes shutdown hang.

Backoff is *full jitter*: the wait is drawn from `[0, min(max_delay,
base_delay * 2**n)]`. The failure being retried is usually shared — a database
that falls over disappoints every worker at once — and an un-jittered curve
marches them all back in step, so the retry storm lands as one spike.

⚠️ Retrying is only safe for operations that can be repeated without doubling
their effect. The decorator cannot check that; you have to. And note the
synchronous form sleeps with `time.sleep`, which blocks the event loop: use it
in Celery tasks, CLI entry points and startup checks, and decorate the
coroutine everywhere else.

## `@cached`

An in-process TTL + LRU memo.

```python
@cached(ttl=300, maxsize=1)
async def jwks() -> dict[str, object]:
    return (await client.get(JWKS_URL)).json()
```

| Argument | Meaning |
|---|---|
| `ttl` | Seconds an entry stays fresh. Required — no default, because an inherited guess is how a cache becomes a stale-data bug. |
| `maxsize` | Entry ceiling; least recently used is evicted on overflow. Default 128. |
| `clock` | Expiry clock, `time.monotonic` by default. Injectable so expiry can be tested without sleeping. |
| `key` | Builds the cache key from `(args, kwargs)`. Defaults to `make_key`. |

The wrapper also carries `cache_info()`, `cache_clear()` and
`cache_invalidate(*args, **kwargs)` — the last takes the same arguments as the
function, so invalidating after a write reads as the call it undoes.

### Why not `functools.lru_cache`

- Entries here **expire**; `lru_cache` holds a value until eviction pressure
  removes it, which for a small key space is never.
- `lru_cache` on an `async def` caches the *coroutine object*, and the second
  hit raises `RuntimeError: cannot reuse already awaited coroutine`.
- Concurrent misses **collapse**: the async wrapper holds a per-key lock across
  the miss, so N simultaneous callers of a cold key produce one underlying
  call. Without that, a cache in front of a slow dependency stampedes it
  exactly when it is already struggling. The locks are refcounted and dropped
  once a key goes idle.
- Exceptions are **not** cached; a failed miss leaves the key cold.

### Cache keys

`make_key` — the default — keys on the literal call shape. It is fast, but
`f(1)` and `f(x=1)` are different entries, and so are `f(1)` and
`f(1, flag=False)` when `flag` already defaults to `False`. That costs a
duplicate entry, never a wrong answer.

`signature_key(func)` binds each call against the real signature first, so all
three of those collapse into one entry, at the cost of an
`inspect.BoundArguments` per call:

```python
decorated = cached(ttl=60, key=signature_key(load))(load)
```

### Limits

- **Per-process.** Two Uvicorn workers keep two copies and a deploy empties
  both. Right for expensive, read-mostly, briefly-stale-is-fine values — a
  JWKS document, a feature-flag snapshot, a config row read on every request.
  Wrong for anything a user expects to see change immediately after they change
  it. Cross-process invalidation needs Redis, not this.
- **Do not decorate a method.** `self` lands in the key, so the cache pins
  every instance it has seen. Cache a module-level function and pass the fields
  it needs.
- **The counters are not thread-safe.** They interleave safely under one event
  loop, which is how this app runs; driving the sync wrapper from a thread pool
  can lose a `hits` increment. The stats drift, the cache does not corrupt.

## Composition order

Reading bottom-up, in the order the decorators apply:

```python
@timed(event="rates.fetch")                    # 3. total, including every retry
@retry(attempts=3, on=httpx.TransportError)    # 2. only failures reach here
@cached(ttl=60)                                # 1. a hit skips both
async def fetch_rates(base: str) -> Rates: ...
```

`@cached` innermost so a hit costs nothing; `@retry` around it so failures are
never stored; `@timed` outermost so the recorded duration is the latency the
caller actually experienced. Put `@timed` innermost and it times one attempt
out of three. Put `@cached` outermost and it caches the retry loop, which is
fine right up to the point where it caches a failure.

Two things to know when stacking:

- **`cache_info()` and friends do not survive an outer decorator.** `@retry`
  and `@timed` return plain functions, so the name a stack binds cannot reach
  the cache API. Build the stack by hand and keep the middle reference when you
  need it:

  ```python
  inner = cached(ttl=60)(fetch_rates)
  fetch_rates = timed(event="rates.fetch")(retry(attempts=3)(inner))
  inner.cache_invalidate("usd")
  ```

- **Awaitability is detected structurally.** `@cached` returns a callable
  object whose `__call__` is `async def`, which `inspect.iscoroutinefunction`
  alone would call synchronous — so `@retry` and `@timed` use
  `is_async_callable`, and `@cached` marks its async wrapper with
  `inspect.markcoroutinefunction` so FastAPI awaits a cached dependency instead
  of sending it to a thread pool.
