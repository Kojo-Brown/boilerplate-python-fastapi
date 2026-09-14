# Dashboards

Grafana dashboards checked in as JSON, importable as they stand: each asks
which Prometheus datasource to use rather than pinning one, so nothing has to
be edited before an import.

| File | |
| --- | --- |
| [`grafana/red.json`](./grafana/red.json) | Rate, errors and duration for the HTTP server, plus the process numbers that explain them. |

Every query in every file here is parsed by `tests/test_metrics_dashboard.py`
and checked against a real scrape of a real instrumented app, so a renamed
metric fails a test rather than quietly producing a panel that reads "No data".

See [docs/metrics.md](../docs/metrics.md) for what the service exposes and how
to scrape it.
