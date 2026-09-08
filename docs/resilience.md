# Outbound HTTP: retry with jitter, and a circuit breaker per dependency

Every call out of this process to something it does not control can fail in two
ways that look identical from the call site and want opposite treatment.

A **transient** failure is one where the same request, sent again in a moment,
succeeds: a connection refused by an instance that has already been drained, a
504 from a gateway waiting on a cold cache, a 429. Retrying is the whole fix.

A **sustained** failure is one where the same request, sent again in a moment,
fails again, and again, for minutes. Retrying is not a fix, it is a load
generator aimed at a service that is already down — and worse, every retried
request is a connection from this process's pool, held for the length of a
timeout, that a request to a *healthy* dependency now cannot have. That is how
one slow third party takes an application down: not by being slow, but by
consuming everything that waits on it.

The two mechanisms in `src/resilience` divide exactly along that line. Retry
handles the first. The circuit breaker exists to notice that this is no longer
the first, and to stop.

```
             ┌──────────────── ResilientTransport ───────────────┐
 client  ──▶ │  breaker.acquire() ─▶ send ─▶ classify ─▶ retry?  │ ──▶ network
             └──────────────────────────────────────────────────┘
                       │                          │
                  refuses when            full-jitter backoff,
                  the circuit is           or `Retry-After`
                  open for this origin
```

## Using it

```python
from src.resilience import resilient_async_client

client = resilient_async_client(base_url="https://api.example.test")

async with client:
    response = await client.get("/v1/things")
```

That is the whole integration. It is an ordinary `httpx.AsyncClient` — the
policy lives in a transport underneath it, so `base_url`, headers, cookies,
redirects, the connection pool and event hooks all behave exactly as httpx
documents them. Both payment adapters build their client this way; anything
else in this codebase that talks to a service it does not own should too.

To tune it:

```python
from src.resilience import CircuitBreakerConfig, CircuitBreakerRegistry, RetryPolicy

client = resilient_async_client(
    base_url="https://api.example.test",
    timeout=5.0,                                   # per attempt, see below
    retry=RetryPolicy(attempts=4, base_delay=0.2, max_delay=10.0),
    breakers=CircuitBreakerRegistry(
        config=CircuitBreakerConfig(failure_threshold=10, reset_timeout=60.0)
    ),
)
```

## What counts as a failure

One predicate, in `base.py`, answers both "is this worth retrying?" and "does
this count towards opening the circuit?", because a failure that is not worth
retrying is not evidence of an outage either.

| Outcome | Failure? | Why |
| --- | --- | --- |
| 2xx, 3xx | no | |
| 4xx other than 429 | **no** | We asked for the wrong thing. A wave of 404s is this application's bug, and opening a circuit on it would turn that bug into an outage for every other caller of the dependency. |
| 429 | yes | The far end is shedding load — the state a breaker exists to stop making worse. |
| 5xx | yes | A server fault by definition. |
| 501, 505 | **no** | Permanent protocol answers. They will say the same thing in ten minutes. |
| `ConnectError`, `ReadTimeout`, `WriteError`, `RemoteProtocolError`… | yes | The dependency could not be reached or could not finish. |
| `PoolTimeout` | **no** | *This process* ran out of connections. The far end may be in perfect health; counting it opens a circuit on a remote service because of a local shortage, and retrying it queues another waiter on a pool that is already full. |

## What may be retried

Retrying is safe when the request definitely was not processed, or when
processing it twice is harmless. Those are two different facts, and this
transport knows the first one better than the caller does.

* **A connect-phase failure proves the request never arrived.** `ConnectError`
  and `ConnectTimeout` mean no connection existed to carry it, so *any* method
  may be repeated — including a `POST`. Treating these like every other failure
  gives up free retries on the most common failure during a rolling deploy.
* **Everything else leaves the outcome unknown.** A read timeout, a write
  error, a connection dropped mid-response, and every response status all
  happened with bytes on the wire. Only an idempotent method
  (`GET`/`HEAD`/`PUT`/`DELETE`/`OPTIONS`/`TRACE`) is repeated. A read timeout
  is the failure *most* likely to mean the far end is slow rather than absent,
  and so most likely to have worked — repeating a `POST` there is a second
  charge.
* **An idempotency key opts a `POST` back in.** A request carrying
  `Idempotency-Key` or `PayPal-Request-Id` is retried whatever its method,
  because the caller has told the far end how to deduplicate it. This is a
  claim the transport cannot verify — a header the receiver ignores is just a
  header — so the set of header names is configurable and defaults to the two
  this codebase already sends.
* **A streamed body is never retried.** A request built from an async generator
  carries a stream that the first attempt consumes; sending it again transmits
  zero bytes, *successfully*, and the far end stores the empty version. The
  stream is inspected before the first attempt (`is_replayable`), because a
  transport underneath is entitled to buffer the body as it sends it, which
  would make a one-shot stream look replayable by the time the retry decision
  is taken.

## The backoff

Full-jitter exponential backoff, through the same `backoff_delay` as every
other retry loop in this codebase — see `src/decorators/base.py` for why the
wait is drawn from `[0, ceiling]` rather than being the ceiling.

A `Retry-After` on the response wins over the computed backoff, with a jitter
draw from `[0, base_delay)` added **on top**: the header is a floor the server
asked for, so waking before it is disobedience, and waking at exactly it puts
every rate-limited client back on the wire in the same millisecond. A
`Retry-After` longer than `max_retry_after` (30s by default) ends the retrying
and returns the response — a far end asking for an hour is not asking to be
retried inside this request.

## The breaker

One breaker per origin (`scheme://host:port`), because one dependency being
down is not a reason to stop calling a different one: a process-wide breaker
turns a Stripe outage into a PayPal outage. Per *origin* rather than per URL
for the mirror-image reason — a breaker keyed by path never sees enough traffic
on one endpoint to trip, and the thing that fails is a server.

```
            failures in window >= threshold
   CLOSED ─────────────────────────────────▶ OPEN
      ▲                                        │
      │ success_threshold probes succeed       │ reset_timeout elapses
      │                                        ▼
      └──────────────── HALF_OPEN ◀────────────┘
                            │
                            └── a probe fails ──▶ OPEN (timer restarted)
```

Three decisions worth stating:

**Failures are counted in a bounded window, not consecutively.** Under a
consecutive-failure rule, any interleaved success resets the count — so a
dependency failing half its calls, which is an unambiguous outage to anyone
looking at a dashboard, never opens the circuit at all.

**Half-open admits one probe, not everything.** The point of the state is to
spend a single request finding out. A half-open circuit that admits every
waiting caller is a thundering herd aimed at a service that has just come back
up, and it will knock it over again.

**Closing takes more than one success**, because a restarting dependency can
answer one request correctly and fall over on the next. Closing also clears the
window, or the first failure after recovery would re-open the circuit however
healthy the dependency now is.

### `CircuitOpenError` is an `httpx.TransportError`

This is the load-bearing type decision in the package. Every existing caller of
an outbound client here already answers "the dependency could not be reached"
by catching `httpx.TransportError` and translating it into this application's
own 502 or 503 — `StripeGateway._request`, `PayPalGateway._fetch_token`, the
webhook notifier. An open circuit *is* that answer, arrived at without spending
a socket to find out. Given an exception type of its own it would escape every
one of those handlers and surface as an unhandled 500, turning a deliberately
handled outage into an internal error at exactly the moment the breaker is
supposed to be making things better.

It carries `origin` and `retry_after` so a caller can put a real number in a
`Retry-After` header of its own instead of inventing one.

## Timeouts, budgets, and what this does not do

**`timeout` is per attempt.** That is the only thing a transport-level timeout
can be. Three attempts against a 15-second timeout is a 45-second request,
which is not what the caller who wrote `timeout=15` had in mind, and no total
budget lives in this transport.

The bound belongs to the caller, and this codebase already has one. An
enclosing `deadline()` from `src/structured/deadline.py` is consulted before
every backoff: a retry that would not fit in the remaining budget is not
started, and the failure is returned now rather than 4.7 seconds into a wait
nobody will be around for.

```python
async with deadline(2.0, name="checkout"):
    await gateway.charge(request)   # retries only while the budget allows
```

With no enclosing deadline there is nothing to consult and the attempts run to
exhaustion. Wrapping the request path in a deadline is the fix, not a knob
here.

**Cancellation is never retried and never counted.** `asyncio.CancelledError`
means the caller stopped caring; retrying through it keeps work alive that
nothing is waiting for. It also releases the half-open probe slot rather than
recording an outcome, so a cancelled probe does not strand a circuit in a state
where nothing may pass and nothing will ever report.

## Known limits, written down rather than left to be found

* **Breaker state is per process.** Five replicas hold five independent
  opinions about whether a dependency is up, and each spends its own probe to
  find out. Sharing it would mean a round trip to Redis on the admission path
  of every outbound call — which is a network call to decide whether to make a
  network call — and a shared breaker one replica can open for all of them.
  `CircuitBreakerRegistry` is the seam if that trade is ever worth making.
* **Nothing bounds concurrency per dependency.** The breaker limits calls once
  a dependency has *failed*; it does nothing about a dependency that is merely
  slow, which will still fill the connection pool with waiters. That is a
  bulkhead, and it is the next item in `SPEC.md`. `PoolTimeout` is deliberately
  left un-retried and uncounted here so it stays visible as the local
  exhaustion it is.
* **No metrics.** `CircuitBreakerRegistry.states()` is a snapshot suitable for
  a health endpoint, and the transport logs `http.retry_scheduled`,
  `http.retry_exhausted`, `http.retry_declined` and the breaker logs
  `circuit.opened` / `circuit.half_open` / `circuit.closed`. Turning those into
  counters belongs with the Prometheus item.
* **The webhook notifier keeps its own retry ladder.** `src/notifications/webhook.py`
  retries at the delivery level, where it can see the receiver's status codes
  and its own attempt budget. Handing it a resilient client as well would
  multiply the two ladders — three attempts each is nine requests to an
  endpoint that asked for one — so it is deliberately left alone. Consolidating
  the two is a change to that module's contract, not a wiring change.
