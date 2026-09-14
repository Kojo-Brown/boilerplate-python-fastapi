# RED metrics and the Prometheus scrape endpoint

Three numbers describe a request-driven service: how many requests arrive
(**rate**), how many fail (**errors**), and how long they take (**duration**).
This service records all three, exposes them at `GET /metrics` in the
Prometheus exposition format, and ships the Grafana dashboard that reads them
in [`dashboards/grafana/red.json`](../dashboards/grafana/red.json).

The code is `src/observability/red.py` (the metric contract) and
`src/observability/prometheus.py` (the endpoint). Both hang off the
`MeterProvider` built in `src/observability/metrics.py`, which
[docs/observability.md](./observability.md) describes.

## Turning it on

```bash
OTEL_ENABLED=true
OTEL_METRICS_ENABLED=true
PROMETHEUS_ENABLED=true      # the default
```

`PROMETHEUS_ENABLED` alone is not enough, and the 503 you get says so. A pull
reader reads instruments off the `MeterProvider`, and there is no provider
unless the SDK is on.

| Setting | Default | |
| --- | --- | --- |
| `PROMETHEUS_ENABLED` | `true` | Attach the pull reader. Costs nothing until scraped. |
| `PROMETHEUS_METRICS_PATH` | `/metrics` | Change it and change `OTEL_EXCLUDED_URLS` to match. |
| `PROMETHEUS_SCRAPE_TOKEN` | — | Empty means unauthenticated. Set it to require `Authorization: Bearer <token>`. |

`PROMETHEUS_ENABLED` is on by default while the rest of telemetry is off,
which looks inconsistent until you look at what each costs. An OTLP exporter
that is on with nothing to export to spends a thread, a queue and a failing
HTTP request every schedule delay. A pull reader spends nothing at all until
somebody GETs the path: no thread, no queue, no outbound connection.

### Three shapes of deployment

| | `OTEL_EXPORTER` | `PROMETHEUS_ENABLED` | |
| --- | --- | --- | --- |
| Push to a collector | `otlp` | `false` | Metrics leave over OTLP with traces and logs. |
| Scrape only | `none` | `true` | Collected in-process, pulled out. No collector anywhere. |
| Both | `otlp` | `true` | One recording, two readers. Useful during a migration. |

The middle row is worth naming because it is not obvious: `OTEL_EXPORTER=none`
does not mean "metrics off". A `MeterProvider` with no push exporter still
records every instrument — see `build_meter_provider` — so "none" plus a scrape
registry is a complete, sensible production setup.

## What is exposed

| Metric | Type | |
| --- | --- | --- |
| `http_server_request_duration_seconds` | histogram | RED, all three of it. |
| `http_server_active_requests` | gauge | Requests in flight. |
| `http_server_response_body_size_bytes` | histogram | Egress. Not part of RED. |
| `target_info` | gauge | `service_name`, `service_version`, `service_instance_id`. |
| `process_*`, `python_gc_*` | | CPU, RSS, file descriptors, GC. |

Labels on the duration histogram: `http_request_method`, `http_route`,
`http_response_status_code`, `error_type`.

### Why there is no hand-rolled middleware

A Prometheus histogram carries its own `_count`, so one duration histogram is
all three letters of RED: rate is `rate(..._count[5m])`, errors is the same
filtered on `http_response_status_code`, duration is the buckets. The ASGI
instrumentation already records exactly that histogram per request. A second
instrument measuring the same thing would double the series and produce two
numbers that disagree at the edges of a scrape interval.

What the instrumentation does *not* decide is which attributes are worth
keeping and where the bucket boundaries are. Those are decided in
`src/observability/red.py`, as SDK views, and they are the reason a dashboard
can be checked in at all.

### Cardinality, which is the thing that goes wrong

Every label combination is a time series, and a histogram is roughly seventeen
of them per combination. Two rules keep that bounded here:

- **The route label is the template, not the path.** `/api/v1/users/{user_id}`,
  not `/api/v1/users/7`. The instrumentation resolves this from the router, so
  a new series appears when somebody adds an endpoint rather than when somebody
  sends a request. A request that matched no route carries an empty
  `http_route` and they all collapse into one series, which is what stops a URL
  scanner from writing a series per URL it tries.
- **Constant labels are dropped.** `url.scheme` and `network.protocol.version`
  never vary within a deployment. A view drops them before aggregation, so the
  series is never created rather than created and ignored.

`http_server_active_requests` is reduced further, to the method alone: it is a
gauge, one series per label set at every instant, and "is the server saturated"
does not need a per-route answer.

### The bucket boundaries are part of the contract

```
0.005 0.01 0.025 0.05 0.075 0.1 0.25 0.5 0.75 1 2.5 5 7.5 10   (seconds)
```

`histogram_quantile` interpolates *within* a bucket, so these decide how wrong
a p99 can be — and a p99 above 10s saturates at the `+Inf` bucket and reads as
10s. They currently match what the semantic conventions recommend, which means
pinning them in `RED_DURATION_BUCKETS` changes nothing today. That is the
point: a contrib upgrade that revises the default cannot silently move the
numbers on a dashboard this repository ships and claims is correct.

Change them for a service whose latencies live somewhere else — an API where
everything is under 20ms wants boundaries under 20ms — and change the dashboard
with them.

## Scraping it

```yaml
scrape_configs:
  - job_name: boilerplate-python-fastapi
    metrics_path: /metrics
    scrape_interval: 15s
    static_configs:
      - targets: ["api:8000"]
```

In Kubernetes, the equivalent annotations or a `ServiceMonitor`; the `job` and
`instance` labels the dashboard groups by are attached by Prometheus from the
scrape configuration, not by this service.

With `PROMETHEUS_SCRAPE_TOKEN` set:

```yaml
    authorization:
      type: Bearer
      credentials_file: /etc/prometheus/secrets/scrape-token
```

The token is compared with `secrets.compare_digest`. A plain `==` on a secret
leaks its prefix through response timing, and this endpoint is by design
reachable by anything that can reach the service.

### One process, one registry

The registry lives in this process's memory. Four uvicorn workers behind one
port are four independent sets of counters, and a scrape reaches one of them at
random — so a counter appears to jump backwards and `rate()` reads it as a
reset. Run one worker per container and scale with replicas; Prometheus is
built to sum across targets. If you must fork, `prometheus_client` has a
multiprocess collector driven by `PROMETHEUS_MULTIPROC_DIR`, which is not wired
up here because it changes the meaning of every gauge in the exposition.

### The endpoint does not measure itself

`metrics` is in `OTEL_EXCLUDED_URLS` alongside the health probes. At one scrape
every fifteen seconds it would otherwise be among the highest-rate routes on
the dashboard, and it measures the monitoring rather than the service. If you
change `PROMETHEUS_METRICS_PATH`, change that too — there is a test asserting
the two defaults agree.

## The dashboard

Import `dashboards/grafana/red.json`. It asks which Prometheus datasource to
use rather than pinning one, and has `job` and `route` variables.

Top row: rate, 5xx share, p99, requests in flight. Then rate by route,
responses by status code, the latency quantiles together, and p99 by route.
The bottom row is CPU, resident memory and open file descriptors, which are
what turn "latency went up" into a cause: flat CPU points at a dependency,
pinned CPU at saturation, memory that only climbs at a leak.

Errors are 5xx only. A client sending bad input is not this service failing,
and folding 4xx in makes a scanner hitting bad URLs look like an outage. The
per-status panel is there for when the distinction matters — a wave of 401s and
a wave of 422s are different incidents.

`tests/test_metrics_dashboard.py` parses every query in that file and checks
each metric and label against a real scrape of a real instrumented app. Rename
a metric and the test names it; the usual fate of a checked-in dashboard is to
read "No data" for a year and this is the thing that prevents it.

## Adding a metric of your own

```python
from opentelemetry import metrics

meter = metrics.get_meter(__name__)
orders_placed = meter.create_counter(
    "orders.placed", unit="{order}", description="Orders accepted."
)

orders_placed.add(1, {"payment.gateway": gateway})
```

`get_meter` on the global provider returns a no-op when the SDK is off, so this
costs nothing in a deployment that has not turned telemetry on and needs no
guard. It appears in the scrape as `orders_placed_total`.

Before you add the attribute, ask what bounds it. A gateway name is bounded by
the codebase; a customer id is bounded by your growth, and is how a Prometheus
falls over.

## What is deliberately not here

- **No Prometheus or Grafana in `docker-compose.yml`**, for the same reason
  there is no collector in it: two more services running by default for
  something switched off by default. The scrape config above is the whole of
  what you need.
- **No exemplars.** Linking a histogram bucket to a trace id is the natural
  next step and the exporter supports it, but it needs a Prometheus started
  with `--enable-feature=exemplar-storage` and a Grafana datasource configured
  to follow them, which is deployment rather than application.
- **No alerting rules.** Burn-rate alerts against an error budget are their own
  `SPEC.md` item, and they belong with the SLO definition rather than with the
  instrument.
- **No database or queue metrics.** SQLAlchemy and Kafka are instrumented for
  *traces*; a metric per statement is a different cardinality question and the
  span already answers "which query was slow".
