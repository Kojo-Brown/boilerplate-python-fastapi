"""OpenTelemetry: traces, metrics and logs, joined by W3C context propagation.

The three signals are one system rather than three, and what joins them is the
context. A request arrives carrying a `traceparent`; the ASGI instrumentation
extracts it and opens a server span underneath it; every log line emitted while
that span is current is stamped with its ids; every outbound httpx call and
every statement the session runs becomes a child span; and every record this
service publishes to a broker carries the context onwards, so the work that
happens a second later in another process is still the same trace.

Usage in an application:

    from src.observability import configure_observability, shutdown_observability

    handle = configure_observability(settings, app=app, engine=engine)
    ...
    shutdown_observability(handle, settings)

Usage at a messaging boundary, where there is no HTTP header to ride on:

    from src.observability import extract_trace_context, inject_trace_context

    headers = inject_trace_context(headers)              # producer
    context = extract_trace_context(record.headers)      # consumer

Everything is off unless `OTEL_ENABLED` is true, and each signal can be turned
off on its own. The one exception is `add_trace_correlation`, which is in the
logging chain unconditionally and costs a context read — see
`src/observability/logs.py` for why that is not a setting.

See `docs/observability.md` for the collector side and for what is deliberately
not here.
"""

from src.observability.instrumentation import (
    apply_semconv_stability,
    instrument_fastapi,
    instrument_httpx,
    instrument_sqlalchemy,
    uninstrument_fastapi,
    uninstrument_httpx,
    uninstrument_sqlalchemy,
)
from src.observability.logs import (
    LogForwarder,
    add_trace_correlation,
    build_logger_provider,
    log_forwarder,
    structlog_processors,
)
from src.observability.metrics import build_meter_provider
from src.observability.propagation import (
    MessageHeaders,
    configure_propagation,
    extract_trace_context,
    inject_trace_context,
)
from src.observability.resource import build_resource
from src.observability.setup import (
    Observability,
    configure_observability,
    shutdown_observability,
)
from src.observability.tracing import build_tracer_provider

__all__ = [
    "LogForwarder",
    "MessageHeaders",
    "Observability",
    "add_trace_correlation",
    "apply_semconv_stability",
    "build_logger_provider",
    "build_meter_provider",
    "build_resource",
    "build_tracer_provider",
    "configure_observability",
    "configure_propagation",
    "extract_trace_context",
    "inject_trace_context",
    "instrument_fastapi",
    "instrument_httpx",
    "instrument_sqlalchemy",
    "log_forwarder",
    "shutdown_observability",
    "structlog_processors",
    "uninstrument_fastapi",
    "uninstrument_httpx",
    "uninstrument_sqlalchemy",
]
