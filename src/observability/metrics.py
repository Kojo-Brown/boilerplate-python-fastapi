"""The `MeterProvider`, and the one thing metrics do not share with traces.

Traces are sampled: a span that is not recorded costs nothing and the ones that
survive still describe the shape of the traffic. Metrics are not, and cannot
be — an aggregate assembled from a tenth of the requests is not a tenth as
accurate, it is wrong, because the thing being asked of it (a rate, a
percentile) is a property of the population. So every instrument here records
every call, and the knob is the *export interval* rather than a sample rate:
the cost of metrics is one export per interval regardless of traffic, which is
why `OTEL_METRIC_EXPORT_INTERVAL_SECONDS` is the setting that matters and why
metrics can be turned off on their own.

`PeriodicExportingMetricReader` runs that export on its own thread. Its
`export_interval_millis` is also the *resolution* of anything derived from
these series: a burst shorter than the interval is visible in the totals and
invisible in the shape.

What is emitted is what the instrumentation libraries emit —
`http.server.request.duration` from the ASGI instrumentation, the client
equivalent from httpx — reshaped into the RED contract by the views in
`src/observability/red.py`, which is where the reasoning about attributes and
bucket boundaries lives.

A provider can carry more than one reader, and this one does when a scrape
registry is passed: the periodic push above and the pull reader in
`src/observability/prometheus.py` read the same instruments, so recording
happens once whichever way the numbers leave the process. That is also why
`OTEL_EXPORTER=none` is not the same as "metrics off" — see `build_meter_provider`.
"""

from __future__ import annotations

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    MetricExporter,
    MetricReader,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource
from prometheus_client import CollectorRegistry

from src.config import Settings
from src.observability.exporters import parse_otlp_headers, signal_endpoint
from src.observability.prometheus import build_prometheus_reader
from src.observability.red import red_views


def build_metric_exporter(settings: Settings) -> MetricExporter | None:
    """The exporter named by `OTEL_EXPORTER`, or `None` for "none"."""
    if settings.OTEL_EXPORTER == "none":
        return None
    if settings.OTEL_EXPORTER == "console":
        return ConsoleMetricExporter()
    # Imported lazily, for the reason given in `tracing.build_span_exporter`.
    from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
        OTLPMetricExporter,
    )

    return OTLPMetricExporter(
        endpoint=signal_endpoint(settings.OTEL_EXPORTER_OTLP_ENDPOINT, "metrics"),
        headers=parse_otlp_headers(settings.OTEL_EXPORTER_OTLP_HEADERS),
        timeout=int(settings.OTEL_EXPORTER_OTLP_TIMEOUT_SECONDS),
    )


def build_meter_provider(
    settings: Settings,
    resource: Resource,
    *,
    scrape_registry: CollectorRegistry | None = None,
) -> MeterProvider:
    """A provider with a periodic reader, a pull reader, both, or neither.

    A provider with no readers is not inert the way a `TracerProvider` with no
    processors is — instruments still aggregate in memory, bounded by their
    cardinality — so "none" here means "collected and never sent", which is
    what a test wants when it attaches an `InMemoryMetricReader` of its own,
    and what a scrape-only deployment wants when it passes a `scrape_registry`:
    `OTEL_EXPORTER=none` with `PROMETHEUS_ENABLED=true` collects everything and
    pushes nothing, waiting to be pulled.

    Args:
        settings: Configuration; decides the push exporter and the interval.
        resource: The service identity every series carries. It reaches a
            Prometheus scrape as the `target_info` metric rather than as labels.
        scrape_registry: When given, a `PrometheusMetricReader` is attached to
            it. The caller owns the registry because the endpoint that renders
            it needs the same object — see `src/observability/prometheus.py`.

    Returns:
        A provider carrying the RED views, so the attribute sets and bucket
        boundaries are identical on both paths out.
    """
    exporter = build_metric_exporter(settings)
    readers: list[MetricReader] = []
    if exporter is not None:
        readers.append(
            PeriodicExportingMetricReader(
                exporter,
                export_interval_millis=(
                    settings.OTEL_METRIC_EXPORT_INTERVAL_SECONDS * 1000
                ),
            )
        )
    if scrape_registry is not None:
        readers.append(build_prometheus_reader(scrape_registry))
    return MeterProvider(resource=resource, metric_readers=readers, views=red_views())


__all__ = ["build_meter_provider", "build_metric_exporter"]
