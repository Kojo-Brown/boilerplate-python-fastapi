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

What is emitted is, for now, what the instrumentation libraries emit —
`http.server.request.duration` from the ASGI instrumentation, the client
equivalent from httpx. Hand-rolled application metrics (a RED dashboard, a
Prometheus scrape endpoint) are the next item in SPEC.md and deliberately not
here: this module is the provider they will hang off.
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

from src.config import Settings
from src.observability.exporters import parse_otlp_headers, signal_endpoint


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


def build_meter_provider(settings: Settings, resource: Resource) -> MeterProvider:
    """A provider with a periodic reader, or with no reader under "none".

    A provider with no readers is not inert the way a `TracerProvider` with no
    processors is — instruments still aggregate in memory, bounded by their
    cardinality — so "none" here means "collected and never sent", which is
    what a test wants when it attaches an `InMemoryMetricReader` of its own.
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
    return MeterProvider(resource=resource, metric_readers=readers)


__all__ = ["build_meter_provider", "build_metric_exporter"]
