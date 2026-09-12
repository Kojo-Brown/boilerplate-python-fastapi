# Observability: traces, metrics and logs

`src/observability/` configures the OpenTelemetry SDK for this service: a span
per request, a span per outbound call and per SQL statement, HTTP metrics, log
records carrying the trace they happened in, and W3C context propagation
joining the three across process boundaries.

Everything is off unless `OTEL_ENABLED=true`. The one exception is trace
correlation on log lines, which is in the structlog chain unconditionally and
costs a context read — see [Logs](#logs) for why that is not a setting.

## Turning it on

```bash
OTEL_ENABLED=true
OTEL_SERVICE_NAME=orders-api
OTEL_EXPORTER=otlp
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318
```

`OTEL_EXPORTER_OTLP_ENDPOINT` is the **base** URL: `/v1/traces`, `/v1/metrics`
and `/v1/logs` are appended to it. Port 4318 is OTLP over HTTP/protobuf, which
is what this service speaks; 4317 is gRPC and will answer nothing useful. A
base URL used where a full one was expected is the commonest OTLP
misconfiguration there is — the collector answers 404 per batch and everything
else about the process looks healthy — which is why `signal_endpoint` builds
the full URL in one place and refuses an empty endpoint rather than falling
back to localhost.

Three exporters:

| `OTEL_EXPORTER` | What happens |
| --- | --- |
| `otlp` | Batched export to the collector at `OTEL_EXPORTER_OTLP_ENDPOINT`. |
| `console` | Spans and records printed to stdout as they end. For a local look at what would be sent. |
| `none` | Providers are built and record normally, with nothing attached to send them. What the tests use. |

A hosted collector's API key goes in `OTEL_EXPORTER_OTLP_HEADERS` as
`key=value` pairs, from the environment like every other credential here.

## Local collector

There is no collector in `docker-compose.yml` — it would be a fourth service
running by default for something switched off by default. To run one:

```bash
docker run --rm -p 4318:4318 -p 55679:55679 \
  -v "$PWD/otel-collector.yaml:/etc/otelcol/config.yaml" \
  otel/opentelemetry-collector:latest
```

with a config that accepts OTLP/HTTP and prints what arrives:

```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
exporters:
  debug:
    verbosity: detailed
service:
  pipelines:
    traces:  {receivers: [otlp], exporters: [debug]}
    metrics: {receivers: [otlp], exporters: [debug]}
    logs:    {receivers: [otlp], exporters: [debug]}
```

For a quick look without any of that, `OTEL_EXPORTER=console`.

## Traces

### Sampling

`OTEL_TRACES_SAMPLER_RATIO` is a fraction, not a percentage, and it applies
**only to traces this process starts**. The sampler is
`ParentBased(TraceIdRatioBased(ratio))`: a decision that arrived in a
`traceparent` is honoured as it stands, because that is what the `sampled` flag
in the W3C header is for. So with an upstream that samples everything, lowering
the ratio here changes nothing — and that is correct. A bare ratio sampler
would re-decide per span and produce traces with holes in the middle, which is
worse than no trace at all, since the missing span is usually where the latency
was.

The ratio sampler is deterministic in the trace id, so two services configured
at the same ratio agree about the same trace rather than each keeping a
different tenth of it.

### What produces spans

- **Inbound HTTP** — one `SERVER` span per request, named for the route
  template (`GET /api/v1/users/{user_id}`, never the interpolated path, which
  would make every id its own operation in the backend). The `traceparent` on
  the request becomes its parent.
- **Outbound HTTP** — one `CLIENT` span per attempt. The instrumentation wraps
  `AsyncHTTPTransport.handle_async_request`, the real transport at the bottom
  of the stack `resilient_async_client` builds, so it sits *inside* the retry
  and bulkhead transports: three attempts are three spans, each with its own
  `traceparent`, which is also what the far end sees. A retried call looks
  like a retried call rather than one slow request.
- **SQL** — one span per statement, from the engine passed to
  `configure_observability`.

`/health` and `/health/ready` produce no spans (`OTEL_EXCLUDED_URLS`). A
liveness probe every second is the highest-rate endpoint most services have and
says nothing about a user's request; leaving it in skews every latency
aggregate towards an endpoint that does no work.

The ASGI instrumentation's per-message `receive`/`send` child spans are
switched off, and not by a setting: this API streams exports and serves SSE,
where a span per `send` is a span per chunk, and one large download would fill
the export queue on its own.

## Metrics

Metrics are not sampled and cannot be — an aggregate assembled from a tenth of
the requests is not a tenth as accurate, it is wrong. The knob is
`OTEL_METRIC_EXPORT_INTERVAL_SECONDS`, which is both the cost (one export per
interval, whatever the traffic) and the resolution (a burst shorter than the
interval shows up in the totals and not in the shape).

What is emitted today is what the instrumentation libraries emit — HTTP server
and client duration histograms. Application metrics and a Prometheus scrape
endpoint are the next item in `SPEC.md`; `build_meter_provider` is the provider
they will hang off.

## Logs

Two separate things, and only the second costs anything.

**Correlation** stamps `trace_id` and `span_id` onto every structlog event
emitted inside a recording span, in the W3C spelling (32 and 16 lower-case hex
digits) that a backend joins on. It is unconditional, because logs whose
correlation depends on a setting are logs nobody trusts to be correlated.
Outside a span nothing is added at all — an absent field says "this happened
outside a trace", where a zeroed id would say the opposite to every query that
looks for it.

```json
{"event": "request.completed", "status_code": 200, "duration_ms": 12.4,
 "request_id": "0f9c…", "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
 "span_id": "00f067aa0ba902b7"}
```

`request_id` stays: it is this service's own handle on one request, echoed to
the client in `X-Request-ID`, and it survives a deployment where the collector
is down.

**Export** mirrors each event into the OTLP logs pipeline when
`OTEL_LOGS_ENABLED` and the SDK are on. It is a structlog processor rather than
the SDK's `LoggingHandler` on the root logger, because this application's logs
do not go through `logging` at all — `configure_logging` uses
`PrintLoggerFactory` — so a handler would faithfully export the handful of
records that libraries emit and none of this service's own. The event string
becomes the record body, everything bound to it becomes an attribute, and the
current span is read from the ambient context, so a record and the span it
happened in agree without either being told about the other.

The forwarder never raises: it sits in the chain of every `logger.info` in this
codebase, including the ones inside exception handlers, and an export failure
must not become a failed request. stdout remains the log of record.

## W3C context propagation

The propagators are installed globally as a composite of trace context and
baggage. `traceparent` carries the trace identity and the sampling decision;
`baggage` carries what the caller attached to the request (a tenant, a
feature-flag cohort) and is the only way for that to survive a hop.

HTTP is covered in both directions by the instrumentation. **Messages are not**,
because they carry headers of their own — `(name, bytes)` pairs rather than a
string map — and a context that is not injected at the publish is a trace that
ends at the producer plus an unrelated one that starts at the consumer. Two
helpers cover that seam:

```python
from src.observability import extract_trace_context, inject_trace_context

# Producer: stamp the current context onto the record's headers.
headers = inject_trace_context(headers)
await publisher.publish("orders.events", value=body, headers=headers)

# Consumer: run the handler's span under the context the record carried.
context = extract_trace_context(record.headers)
with tracer.start_as_current_span("orders.events process", context=context):
    await handle(record)
```

Three details the obvious `dict(headers)` implementation gets wrong:

- **Duplicate names are legal.** `MessageHeaderGetter` returns every match, in
  order, and lets the propagator decide what that means — which keeps the
  messaging side consistent with the HTTP side, where the same propagator reads
  the same duplicates. Collapsing to a mapping would quietly resolve it the
  other way.
- **Injecting twice replaces.** A record that is republished — the retry ladder
  in `src/dlq` does exactly this — must not accumulate a `traceparent` per hop.
- **A header value need not be text.** Undecodable bytes yield no context
  rather than a leniently decoded one that parses to nonsense.

### Not wired: the publishers

`inject_trace_context` is **not** called inside `KafkaMessagePublisher.publish`
or the in-memory publisher, and that is deliberate rather than unfinished. The
retry ladder in `src/dlq` already forwards the `traceparent` a record arrived
with, so injecting at every publish would overwrite the producing trace with
the replaying one, and "which of those two should a redelivered record belong
to" is a question with two defensible answers — probably a span *link* rather
than a parent. Until that is decided, the helpers are explicit at the call
site, where the caller knows which trace the record belongs to.

## When it is configured

`configure_observability` runs at **import** in `src/main.py`, not in the
lifespan, and that is not a style preference. Instrumenting a FastAPI app
patches `build_middleware_stack`, and Starlette builds that stack on the app's
*first* ASGI call — which is the lifespan message itself. An app instrumented
from inside its own lifespan therefore serves every request through a stack
assembled before the middleware existed: no server spans at all, no warning,
and nothing else out of place. `tests/test_observability_http.py` pins both
halves of that.

Nothing is built, imported or started while `OTEL_ENABLED` is false, so the
import stays free of side effects. The teardown stays in the lifespan, where a
shutdown belongs — only one of the two has to happen before the first request.

## Shutdown

`shutdown_observability` unwinds the instrumentation first, then flushes and
closes each provider. Both halves matter:

- The batch processors hold spans and log records in this process's memory and
  nowhere else. A process that exits without flushing loses the telemetry from
  the last few seconds of its life, which during a rolling deploy or an OOM
  kill is exactly the telemetry somebody is about to go looking for.
- The flush is bounded by `OTEL_SHUTDOWN_TIMEOUT_SECONDS`. Telemetry must never
  be the reason a SIGTERM misses its grace period and the container is killed,
  so a collector that has stopped answering costs at most that per signal and
  loses what was queued.

## Configuration reference

| Setting | Default | Notes |
| --- | --- | --- |
| `OTEL_ENABLED` | `false` | Nothing is built, imported or started when off. |
| `OTEL_SERVICE_NAME` | `boilerplate-python-fastapi` | Backends group by this. Two services sharing it are one service with confusing latency. |
| `OTEL_SERVICE_VERSION` | `0.1.0` | |
| `OTEL_EXPORTER` | `otlp` | `otlp` \| `console` \| `none`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | Base URL; signal paths are appended. |
| `OTEL_EXPORTER_OTLP_HEADERS` | — | `key=value,key=value`. Where the API key goes. |
| `OTEL_EXPORTER_OTLP_TIMEOUT_SECONDS` | `10.0` | |
| `OTEL_TRACES_ENABLED` | `true` | |
| `OTEL_METRICS_ENABLED` | `true` | |
| `OTEL_LOGS_ENABLED` | `true` | Export only; correlation is always on. |
| `OTEL_TRACES_SAMPLER_RATIO` | `1.0` | Fraction, for traces this process starts. |
| `OTEL_METRIC_EXPORT_INTERVAL_SECONDS` | `60.0` | Cost and resolution both. |
| `OTEL_BATCH_SCHEDULE_DELAY_SECONDS` | `5.0` | |
| `OTEL_BATCH_MAX_QUEUE_SIZE` | `2048` | Bounded: sustained export failure drops telemetry rather than the process. |
| `OTEL_SHUTDOWN_TIMEOUT_SECONDS` | `5.0` | Per signal. |
| `OTEL_EXCLUDED_URLS` | `health,health/ready` | Regular expressions against the path. |
| `OTEL_INSTRUMENT_SQLALCHEMY` | `true` | |
| `OTEL_INSTRUMENT_HTTPX` | `true` | |
| `OTEL_SEMCONV_STABILITY_OPT_IN` | `http` | Stable HTTP attribute names. `http/dup` emits both during a dashboard migration. |

`OTEL_SERVICE_NAME`, `OTEL_EXPORTER_OTLP_ENDPOINT` and
`OTEL_EXPORTER_OTLP_HEADERS` are the SDK's own environment variables, read here
and passed explicitly to the exporters rather than left for the SDK to pick up,
so that `Settings` stays the single description of how this process is
configured. `OTEL_SEMCONV_STABILITY_OPT_IN` is the one that has to go the other
way: the instrumentation libraries read it from the environment at their first
`instrument()` call and offer no argument for it, so
`apply_semconv_stability` exports it — a value already in the environment wins.

## Using it from your own code

```python
from opentelemetry import trace

tracer = trace.get_tracer(__name__)

async def reconcile(order_id: str) -> None:
    with tracer.start_as_current_span("orders.reconcile") as span:
        span.set_attribute("order.id", order_id)
        ...
```

`get_tracer` on the global provider is a no-op tracer when the SDK is off, so
this costs nothing in a deployment that has not turned telemetry on, and needs
no guard.

For a process that is not the API — a Celery worker, a dedicated outbox relay
— call `configure_observability(settings)` with no `app`, and
`shutdown_observability` on the way out.

## What is deliberately not here

- **No collector in `docker-compose.yml`**, for the reason above.
- **No Prometheus endpoint and no RED metrics.** The next `SPEC.md` item.
- **No span links.** Which is what a redelivered message probably wants; see
  [Not wired: the publishers](#not-wired-the-publishers).
- **No tail sampling.** Head sampling is a per-process decision; keeping "all
  traces that contain an error" means a collector that buffers a whole trace
  before deciding, which is a deployment concern rather than an application
  one.
- **No Redis, Celery or Kafka instrumentation.** Each is another library
  pinning another client version, and the span that matters at those seams —
  the consumer's own — is the one `extract_trace_context` gives you.
