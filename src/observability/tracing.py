"""The `TracerProvider`: what is sampled, how it leaves, and when.

Three decisions live here, and each of them is a bug taken the other way.

**The sampler is `ParentBased(TraceIdRatioBased(ratio))`, never the ratio on
its own.** A bare ratio sampler re-decides per span, so a trace that this
service is part of gets sampled at each hop independently: the caller's span is
kept, ours is dropped, the database span below it is kept, and what arrives at
the backend is a trace with a hole in the middle — which is worse than no trace
at all, because the missing span is where the latency was. `ParentBased`
honours the decision that arrived in the `traceparent` (that is what the
`sampled` flag in the W3C header is *for*) and consults the ratio only when
this process is the one starting the trace. The consequence worth saying out
loud: with an upstream sampling at 100%, `OTEL_TRACES_SAMPLER_RATIO` here
changes nothing at all, and that is correct.

**The ratio sampler is deterministic in the trace id, not random.** Two
processes configured with the same ratio make the *same* decision about the
same trace, which is what keeps a distributed trace whole when the root is a
service that does not propagate a decision.

**Batch for a collector, simple for the console.** `BatchSpanProcessor` exports
from its own thread on a schedule, so a request never waits on the collector;
its queue is bounded, and a sustained export failure drops spans rather than
growing the heap. `SimpleSpanProcessor` exports inline on `end()`, which is
exactly wrong for a network exporter and exactly right for a console one, where
the "export" is a `print` and the point is to see the span at the moment it
ends rather than five seconds later.

The `"none"` exporter builds a provider with no processor at all. That is not a
degenerate case: it is what the tests use, because a provider with no exporter
still records spans, and a test that attaches its own in-memory processor sees
everything a collector would have.
"""

from __future__ import annotations

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    SpanExporter,
)
from opentelemetry.sdk.trace.sampling import (
    ALWAYS_ON,
    ParentBased,
    Sampler,
    TraceIdRatioBased,
)

from src.config import Settings
from src.observability.exporters import (
    export_batch_size,
    parse_otlp_headers,
    signal_endpoint,
)


def build_sampler(ratio: float) -> Sampler:
    """`ParentBased` around the ratio, with the two ends short-circuited.

    A ratio of 1.0 is `ALWAYS_ON` rather than `TraceIdRatioBased(1.0)` so that
    the common case does no arithmetic per trace, and `ParentBased(ALWAYS_ON)`
    still defers to a parent that says "not sampled" — which matters, because
    "sample everything I start" and "override what upstream decided" are
    different requests and only the first is being made here.

    Raises:
        ValueError: For a ratio outside [0, 1]. The SDK clamps instead, which
            turns a typo like `50` (meaning 50%) into "sample everything"
            without saying so.
    """
    if not 0.0 <= ratio <= 1.0:
        raise ValueError(
            f"OTEL_TRACES_SAMPLER_RATIO must be between 0.0 and 1.0, got {ratio!r}. "
            "It is a fraction, not a percentage."
        )
    if ratio == 1.0:
        return ParentBased(ALWAYS_ON)
    return ParentBased(TraceIdRatioBased(ratio))


def build_span_exporter(settings: Settings) -> SpanExporter | None:
    """The exporter named by `OTEL_EXPORTER`, or `None` for "none"."""
    if settings.OTEL_EXPORTER == "none":
        return None
    if settings.OTEL_EXPORTER == "console":
        return ConsoleSpanExporter()
    # Imported here rather than at module scope: the OTLP exporter pulls in
    # `requests` and the protobuf runtime, which a deployment exporting to the
    # console — or to nothing, as under pytest — has no reason to load.
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    return OTLPSpanExporter(
        endpoint=signal_endpoint(settings.OTEL_EXPORTER_OTLP_ENDPOINT, "traces"),
        headers=parse_otlp_headers(settings.OTEL_EXPORTER_OTLP_HEADERS),
        timeout=settings.OTEL_EXPORTER_OTLP_TIMEOUT_SECONDS,
    )


def build_tracer_provider(settings: Settings, resource: Resource) -> TracerProvider:
    """A provider ready to record, with the exporter wired if there is one.

    Not registered as the global provider here. Building and installing are
    separate so that a test can hold a provider of its own without touching
    process-wide state; `configure_observability` does the installing.
    """
    provider = TracerProvider(
        resource=resource,
        sampler=build_sampler(settings.OTEL_TRACES_SAMPLER_RATIO),
    )
    exporter = build_span_exporter(settings)
    if exporter is None:
        return provider
    if settings.OTEL_EXPORTER == "console":
        provider.add_span_processor(SimpleSpanProcessor(exporter))
    else:
        provider.add_span_processor(
            BatchSpanProcessor(
                exporter,
                max_queue_size=settings.OTEL_BATCH_MAX_QUEUE_SIZE,
                schedule_delay_millis=settings.OTEL_BATCH_SCHEDULE_DELAY_SECONDS * 1000,
                max_export_batch_size=export_batch_size(
                    settings.OTEL_BATCH_MAX_QUEUE_SIZE
                ),
            )
        )
    return provider


__all__ = ["build_sampler", "build_span_exporter", "build_tracer_provider"]
