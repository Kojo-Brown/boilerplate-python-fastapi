# Bulkheads: bounding what one slow dependency can take from you

The circuit breaker in `docs/resilience.md` stops calling a dependency that has
**failed**. This is about the one that has not.

A dependency that starts answering in four seconds instead of forty
milliseconds raises no error, logs nothing, trips no breaker, and satisfies
every health check pointed at it. What it does is multiply its share of this
process by a hundred. Each of those calls is holding a connection, an
`asyncio` task, a database session the handler above it opened, and a client
socket waiting on the answer. Nothing is broken, and the application stops
serving anything — including every request that has no dependency on the slow
thing at all.

A bulkhead is the ship-building metaphor taken literally: the hull is divided
so a breach floods one compartment rather than the vessel. The hull here is this
process's capacity, and a compartment is one origin's share of it.

```
 requests ─▶ ┌───────────── Bulkhead("https://api.slow") ─────────────┐
             │  in flight: ████████████████████ 20/20                 │
             │  queued:    ███████ 7/40  ── waited > 1s ──▶ refuse     │ ──▶ send
             └───────────────────────────────────────────────────────┘
             ┌───────────── Bulkhead("https://api.fast") ─────────────┐
             │  in flight: ██ 2/20         (untouched)                │ ──▶ send
             └───────────────────────────────────────────────────────┘
```

## Using it

Nothing. Every client built by `resilient_async_client` already has one:

```python
from src.resilience import resilient_async_client

client = resilient_async_client(base_url="https://api.example.test")
```

To tune the compartment:

```python
from src.resilience import BulkheadConfig, BulkheadRegistry, resilient_async_client

client = resilient_async_client(
    base_url="https://api.example.test",
    bulkheads=BulkheadRegistry(
        config=BulkheadConfig(
            limit=8,                 # calls in flight to this origin
            max_queue=16,            # calls allowed to wait for a slot
            acquire_timeout=0.5,     # how long one waits
            execution_timeout=20.0,  # hard ceiling on one attempt
        )
    ),
)
```

The registry is per origin and shared, exactly like `CircuitBreakerRegistry` —
which matters more here than it does there. Two `httpx.AsyncClient`s built for
the same dependency have two connection pools, so without a shared registry
they would be entitled to twice the concurrency the limit names, for no reason
anybody chose.

## What gets refused, and what it is called

| Situation | Raised | Retried? | Counts against the breaker? |
| --- | --- | --- | --- |
| Compartment full, queue full | `BulkheadFullError(QUEUE_FULL)` | no | no |
| Queued, wait expired | `BulkheadFullError(ACQUIRE_TIMEOUT)` | no | no |
| Request budget already spent | `BulkheadFullError(NO_BUDGET)` | no | no |
| Attempt exceeded the hard timeout | `BulkheadTimeoutError` | idempotent only | **yes** |

The split down the middle of that table is the design.

`BulkheadFullError` is **this process** out of capacity. The dependency was
never asked and may be in perfect health — merely popular. Counting it against
the breaker would open a circuit on a remote service because of a local
shortage, and retrying it would add another waiter to the queue that is already
the problem. It is an `httpx.TransportError`, so every caller that already
handles "could not reach the dependency" handles it, but deliberately **not**
an `httpx.TimeoutException`: that would say the far end failed to answer, and
would make `request_was_sent` treat a request that provably never left as
possibly-delivered — which is exactly what stops a `POST` being repeated.

`BulkheadTimeoutError` is **the dependency** failing to answer, so it is
counted, and it is retried on the same terms as a read timeout.

## The hard timeout, and why httpx's own is not one

`httpx.Timeout` is per *phase* — connect, write, read, pool — and every phase
timeout **restarts whenever a byte moves**. `read=10.0` bounds the wait for the
next read, not the exchange. A far end that dribbles one header line every nine
seconds never trips it and holds a connection, a slot and a task for as long as
it cares to. And `resilient_async_client(timeout=None)` is legal, because it is
legal in httpx, and then there is no phase timeout at all.

`BulkheadConfig.execution_timeout` is the ceiling that cannot be reset. Without
one, a handful of hung calls hold slots for the life of the process and the
limit stops being a limit — a compartment that can only ever fill is worse than
no compartment, because it fails closed on a dependency that has since
recovered.

It bounds the request through to the response **headers**, which is the span
`handle_async_request` owns. See the known limits below for what that leaves.

## Where it sits, and why the order is what it is

```
ResilientTransport   retry loop, with the whole policy
  └── breaker.acquire()          synchronous: admits or raises, never waits
        └── BulkheadTransport    async: may wait up to acquire_timeout
              └── AsyncHTTPTransport
```

**The bulkhead is inside the retry loop**, so a slot is taken per attempt and
given back before the backoff. A slot held across a sleep is capacity spent
doing nothing, at the exact moment the dependency has none to spare. The
consequence — a retry that arrives to find the compartment full is refused — is
correct rather than unfortunate: when a dependency is saturated, a retry is
precisely the traffic worth shedding.

**The breaker is outside the bulkhead**, because the free test goes first.
`CircuitBreaker.acquire` admits or raises in the same tick; `Bulkhead.acquire`
can wait. Reversed, every call to a dependency already known to be down would
queue for a slot before being told the circuit is open — spending the
compartment, and the caller's time, on a decision that was available for free.

## The queue

A compartment with no queue rejects the instant it is full, which turns an
ordinary burst into a wave of errors. A compartment with an unbounded queue is
worse: a dependency that stops answering collects tasks forever, and a
concurrency limit becomes a memory limit. So there is a queue, it is bounded,
and the wait is short.

Two properties are worth stating because they are easy to get wrong and silent
when you do:

* **The slot is handed to the waiter at the head**, not released for whoever
  wakes up first. A woken waiter that finds the slot taken has to queue again,
  and under sustained load it can do that indefinitely while later arrivals
  walk past it.
* **A new arrival never steps over the queue.** The fast path checks that
  nobody is waiting, not only that a slot is free. Without that, a busy
  compartment starves its own queue the way an unfair lock does, and the
  acquire timeout goes from a rare event to the common one.

`asyncio.Semaphore` gives neither the queue bound nor the hand-off, and binds
itself to the first event loop that contends on it — which a process-wide
registry outlives. `src/resilience/bulkhead.py` explains that choice at the
point where it is made.

## Interaction with the request deadline

`Bulkhead.acquire` consults `current_deadline()` before queueing, and there are
three answers:

* **No enclosing scope** — the acquire timeout stands.
* **Budget already spent** — refused immediately as `NO_BUDGET`, because there
  is no wait worth starting.
* **Budget shorter than the acquire timeout** — no timer of our own is armed,
  and the enclosing scope is left to expire and name itself. This is the rule
  `deadline()` already applies to its own nesting: two timers set to the same
  instant both fire, both cancel the same task, and which one wins decides the
  error message — for a distinction that matters, since "the request budget ran
  out" and "the compartment for this dependency is full" have different fixes.

## Observing it

```python
from src.resilience import DEFAULT_BULKHEADS

DEFAULT_BULKHEADS.stats()
# {"https://api.stripe.com": BulkheadStats(limit=20, in_flight=3, queued=0, rejected=0)}
```

Every refusal logs `bulkhead.rejected` with the origin, the reason, and how
long the call waited. A rising `queued` is the early warning the breaker cannot
give you: it means a dependency is slowing down while still answering
correctly.

## Known limits, written down rather than left to be found

* **The hard timeout does not cover the response body.** It bounds the request
  through to the headers, because that is the span the transport owns —
  `asyncio.timeout` cannot be held across the return, and holding one across a
  `yield` in an async generator is unsafe for reasons the asyncio docs give.
  The *slot*, however, is held until the body is closed, so a dependency that
  drips its body still consumes the compartment and still gets shed; what it
  does not get is cut off. An enclosing `deadline()` is what bounds that.
* **A response that is never closed never returns its slot.** The slot is bound
  to the body, so code that takes a `stream=True` response and abandons it
  leaks one unit of capacity — the same property, and the same fix, as httpx's
  own connection pool. `async with client.stream(...)` is the fix.
* **Compartment state is per process**, exactly as breaker state is. Five
  replicas allow five times the limit between them, which is usually what you
  want from a per-process resource bound and is not what you want if the
  dependency published a global concurrency quota. Sharing it would mean a
  round trip to Redis on the admission path of every outbound call.
* **It is not a rate limiter.** A compartment bounds calls *in flight*, not
  calls *per second*: twenty concurrent calls that each take 10ms is 2,000
  requests per second at a dependency that may allow 100. `src/parallel/io.py`
  draws the same distinction for `gather_bounded`. When an upstream publishes a
  rate, that needs a token bucket as well.
* **The connection pool is still unbounded.** `resilient_async_client` passes
  `httpx.Limits()` when the caller names no limits, and a bare `Limits()`
  leaves `max_connections` as `None` — no ceiling at all; the familiar 100 is
  `httpx.Client`'s own default, replaced wholesale by anything passed
  explicitly. So the compartment is now the only thing bounding outbound
  concurrency in this codebase. Left that way deliberately: a per-dependency
  bound that sheds immediately with a typed error naming the origin is strictly
  better than a process-wide one that sheds after a full pool timeout with a
  `PoolTimeout`, and a cap underneath would only decide which of the two a
  caller hits first. `is_local_shortage` treats both identically for exactly
  that reason.
* **No metrics.** `BulkheadRegistry.stats()` is a snapshot suitable for a
  health endpoint, and every refusal is logged. Turning `in_flight`, `queued`
  and `rejected` into gauges and counters belongs with the Prometheus item.
* **Nothing bounds concurrency to Postgres, Redis or Kafka this way.** The
  compartments here are per HTTP origin, because that is where the transport
  seam is. SQLAlchemy's pool and the Redis client's pool have their own limits,
  and unifying them behind one admission layer is a different design.
