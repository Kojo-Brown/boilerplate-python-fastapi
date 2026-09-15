# Liveness and readiness probes

Two endpoints, answering two different questions, with two different
consequences when they say no.

| | Question | Failing answer | What an orchestrator does |
| --- | --- | --- | --- |
| `GET /health` | Is this process alive? | never, unless the process is gone | restarts the container |
| `GET /health/ready` | Can it serve traffic? | `503` while a required dependency is unreachable | stops routing to it, leaves it running |

Collapsing the two is the classic operational bug, and it fails in the
direction that hurts: point a `livenessProbe` at something that checks Postgres,
lose Postgres for thirty seconds, and the orchestrator kills and restarts
**every** replica — a recoverable dependency blip escalated into an outage by
the monitoring, with a thundering herd of cold processes reconnecting at the
other end.

So `/health` touches nothing. It returns `{"status": "ok"}` if the process is
running its event loop, and that is the entire contract.

The code is `src/health/`: `router.py` (the endpoints), `base.py` (what a check
is), `registry.py` (how checks are run), `checks.py` (Postgres and Redis) and
`wiring.py` (which checks *this* configuration needs).

## What readiness actually checks

```jsonc
// GET /health/ready  → 200
{
  "status": "ready",
  "checks": {
    "database": {
      "status": "ok",
      "criticality": "required",
      "duration_ms": 1.271,
      "description": "postgresql, via the application connection pool",
      "error": null
    },
    "redis": {
      "status": "ok",
      "criticality": "required",
      "duration_ms": 0.42,
      "description": "redis, used by celery, distributed-lock, idempotency",
      "error": null
    }
  }
}
```

Every check is a real round trip — `SELECT 1` and `PING`, the cheapest thing
each protocol offers. A probe must not read a table or touch application state:
it runs several times a minute on every replica forever, and what it has to
prove is that a connection can be opened and an answer can come back.

### Three statuses, two status codes

| `status` | HTTP | Meaning |
| --- | --- | --- |
| `ready` | 200 | Every check passed. |
| `degraded` | **200** | Something `optional` is unreachable. The process can still serve requests. |
| `unavailable` | 503 | Something `required` is unreachable. |

`degraded` being a 200 is the point of the whole design. A readiness probe is
not a report card — it is an instruction to a load balancer, and the only thing
that should ever stop traffic is an inability to serve it. A process pulled out
of rotation because a background stream consumer's Redis is down is one that
stopped serving requests it could have served. **Alert on the body; route on the
status code.**

### Criticality comes from your configuration, not from ours

Nothing in `src/health/` believes a dependency is important in the abstract.
`wiring.py` reads what this deployment has switched on:

| Dependency | Criticality | Why |
| --- | --- | --- |
| Postgres | `required` | Every route that is not a probe reads or writes it. |
| Celery broker | `required` | `src/auth/router.py` enqueues `send_welcome_email_task` inside the register handler, so an unreachable broker is a failed registration. |
| Idempotency store | `IDEMPOTENCY_FAIL_OPEN` decides | Fail-open off: every request with an `Idempotency-Key` is refused while the store is down. Fail-open on: those requests are served without deduplication, which is a degradation you chose. |
| Distributed lock backend | `required` | A handler that cannot take a lock fails. |
| Redis Streams | `optional` | A background consumer; nothing in the request path touches it. |
| Anything set to `memory` | *no check* | There is nothing to round-trip, and a check that cannot fail is noise. |

Kafka is deliberately not probed. Events leave this application through the
transactional outbox (`docs/outbox.md`): a request commits a row and returns,
and the relay delivers it afterwards. An unreachable broker is delivery lag, not
an inability to serve traffic — and failing readiness on it would take a healthy
API out of rotation for a queue behaving exactly as designed. If you add a route
that publishes synchronously through `MessagePublisherDep`, add a `required`
check for it (see [Adding a check](#adding-a-check)).

### One server, one check

The three `*_REDIS_URL` settings all fall back to `REDIS_URL`, so the ordinary
deployment has one Redis wearing four hats. Checks are grouped by the URL they
resolve to: one server means one check named `redis`, and the `description`
names its users. Split any subsystem onto its own server and it gets its own
check, named after its users — `redis:redis-streams` — because the response is
unauthenticated and a URL is not something to put in it.

## Configuration

| Setting | Default | |
| --- | --- | --- |
| `HEALTH_CHECK_TIMEOUT_SECONDS` | `2.0` | Ceiling on one probe. Also the probe client's Redis socket timeout. |
| `HEALTH_CACHE_TTL_SECONDS` | `1.0` | How long one run's results are served to everyone. `0` disables caching. |

Checks run **concurrently**, so the endpoint costs the slowest dependency rather
than the sum of them, and the timeout very nearly bounds the whole response.

The cache is not a micro-optimisation. A readiness endpoint is polled by the
kubelet, by every load balancer in front of it, and by whatever else has been
pointed at it, all multiplied by the replica count and aimed at a single
Postgres — and the multiplication peaks exactly when that Postgres is already
struggling. The TTL makes probe load a function of time instead of of how many
probers exist, and concurrent callers that miss the cache share one run rather
than starting one each. Failures are cached the same way, for the same reason.

Keep the TTL below the shortest `periodSeconds` polling this process and every
poll still sees a fresh answer.

## Kubernetes

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8000
  periodSeconds: 10
  timeoutSeconds: 1        # it touches nothing; 1s is generous
  failureThreshold: 3

readinessProbe:
  httpGet:
    path: /health/ready
    port: 8000
  periodSeconds: 10
  # Above HEALTH_CHECK_TIMEOUT_SECONDS, with room for the response itself.
  # Kubernetes defaults this to 1, which would cut the probe off before it
  # could produce the 503 that names the failing dependency.
  timeoutSeconds: 3
  failureThreshold: 2
```

There is no `startupProbe`, and it is not an omission. A startup probe exists to
keep a liveness probe from killing a process that is still booting — but uvicorn
binds its listening socket only *after* the lifespan's start-up completes, so
during that window there is no socket to probe and every connection is refused,
which every orchestrator already reads as "not up yet". An endpoint could not be
reached in the only window where it would mean anything.

The `HEALTHCHECK` in the `Dockerfile` points at `/health`, not `/health/ready`,
and that is deliberate too: a container healthcheck is a statement about the
container, and a process marked unhealthy because its database is down is a
process something will eventually restart for a reason restarting cannot fix.
Readiness is the orchestrator's business, above the container.

Two more things worth setting on the deployment rather than here:

* **`terminationGracePeriodSeconds`** above your longest request. Readiness
  going false does not drain in-flight requests; the grace period does.
* **Do not expose `/health/ready` publicly.** It carries no URL, no credential
  and no driver message — failures are reported as an exception *type* name,
  because asyncpg and redis-py both quote the connection string in theirs — but
  it does enumerate which dependencies this service has.

## What a failure looks like

```jsonc
// GET /health/ready  → 503
{
  "status": "unavailable",
  "checks": {
    "database": { "status": "ok", "criticality": "required", "duration_ms": 3.42, "description": "postgresql, via the application connection pool", "error": null },
    "redis": { "status": "failed", "criticality": "required", "duration_ms": 1.03, "description": "redis, used by celery, distributed-lock, idempotency", "error": "ConnectionError" }
  }
}
```

| `error` | Usually means |
| --- | --- |
| `ConnectionRefusedError` | Nothing is listening. Wrong host or port, or the server is down. |
| `ConnectionError` | redis-py could not reach or keep the connection. |
| `TimeoutError` | The check did not finish inside `HEALTH_CHECK_TIMEOUT_SECONDS` — a wedged or badly overloaded dependency, which is the case a probe without a timeout would hang on forever. |
| `OperationalError` | Postgres answered and refused: authentication, a missing database, or connection limits. |

The full driver message — which is where the useful detail lives, and also the
credentials — goes to the log, as `health.check_failed` or
`health.check_timed_out`, with the check name and its criticality.

## What these probes do *not* tell you

The Redis check answers *is the server reachable*, on a dedicated client of its
own; it does not measure the pools in `src/idempotency` or
`src/distributed_lock`. Pool exhaustion is a saturation signal and belongs on a
dashboard (`docs/metrics.md`), not in a readiness probe — the response to
overload must not be to remove the busiest replicas from the load balancer and
hand their traffic to the rest.

The database check does go through the application's engine, because a pool that
cannot hand out a connection is a process that cannot serve a request. The
asymmetry is deliberate; `src/health/checks.py` argues both halves.

## Adding a check

Anything matching the `HealthCheck` protocol can be registered:

```python
from src.health.base import Criticality


class SearchIndexCheck:
    """`GET /_cluster/health` against the search cluster."""

    def __init__(self, client: SearchClient) -> None:
        self._client = client

    @property
    def name(self) -> str:
        return "search"

    @property
    def criticality(self) -> Criticality:
        # Reads fall back to Postgres, so an unreachable index degrades
        # this service rather than stopping it.
        return "optional"

    @property
    def description(self) -> str:
        return "opensearch, used by /api/v1/users search"

    async def probe(self) -> None:
        # Raise anything to fail; the registry times, catches and redacts.
        await self._client.cluster_health()
```

Register it in `build_registry` (`src/health/wiring.py`). Three rules:

1. **Raise to fail.** Do not catch and return a status — the registry times the
   call, converts the exception to a type name and logs the message.
2. **Be cancellable.** Every probe runs under `asyncio.timeout`, so it must be
   interruptible at its await points and release what it holds on the way out.
   A probe that blocks the event loop cannot be timed out at all, which is why
   the Redis client also carries a driver-level socket timeout.
3. **Say `optional` unless a request genuinely cannot be served.** The cost of
   getting this wrong is an outage caused by a dependency that was only ever
   degraded.

Give the check an `async def aclose(self)` if it owns a client; the lifespan
calls `HealthRegistry.aclose()` and skips checks that have none.

## Tests

| File | |
| --- | --- |
| `tests/test_health.py` | The endpoints: status codes, body shape, `Cache-Control`, no leaked driver detail. |
| `tests/test_health_registry.py` | Concurrency, timeouts, caching and coalescing, cancellation. |
| `tests/test_health_checks.py` | `SELECT 1` and `PING`, including a real engine pointed at a closed port. |
| `tests/test_health_wiring.py` | Which checks a configuration produces, and how critical each is. |
| `scripts/smoke_start.py` | The whole path against a real Postgres in CI: boot uvicorn, require `ready`, SIGTERM. |
