"""Logs: correlated with traces always, exported over OTLP when asked.

Two separate things, and only the second costs anything.

**Correlation** is `add_trace_correlation`, a structlog processor that stamps
`trace_id` and `span_id` onto every event emitted inside a recording span. It
is unconditional — in the chain whether or not the SDK is configured — because
it is three attribute reads on a context object that is already in memory, and
because logs whose correlation depends on a setting are logs nobody trusts to
be correlated. The ids are written in the W3C spelling (32 and 16 lower-case
hex digits, zero-padded) rather than as integers, since that is the form a
backend joins on and the form that appears in a `traceparent`.

**Export** is `log_forwarder`, a processor that also emits each event into the
OpenTelemetry logs pipeline. It is bound to a `Logger` by
`configure_observability` and is a single `is None` check until then, which is
what lets it sit permanently in the chain that `configure_logging` builds
without ordering the two configuration steps against each other.

Why a structlog processor rather than the SDK's `LoggingHandler` on the root
logger, which is the usual wiring: this application's logs do not go through
`logging`. `configure_logging` uses `PrintLoggerFactory`, so structlog writes
JSON to stdout itself and the stdlib root logger sees none of it. A
`LoggingHandler` would faithfully export the handful of records that libraries
emit through `logging` and none of this service's own, which is the worst
possible half to have.

The emitted record keeps the event's own shape: the `event` string is the log
body, everything else bound to it is an attribute, and the severity comes from
structlog's level. The current span is picked up from the ambient context by
the SDK, so a record and the span it happened in agree without either being
told about the other.

`opentelemetry.sdk._logs` is spelled with a leading underscore upstream. The
logs *API* is stable and the SDK module is what every OTLP logs exporter in the
ecosystem imports; the name is a versioning artefact of the signal reaching
stability after the others, not a warning to stay out. It is imported in
exactly this module so that a rename is one file to change.
"""

from __future__ import annotations

import sys
import time
from typing import Final

import structlog
from opentelemetry._logs import Logger as OTelLogger
from opentelemetry._logs import SeverityNumber
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import (
    BatchLogRecordProcessor,
    ConsoleLogRecordExporter,
    LogRecordExporter,
    SimpleLogRecordProcessor,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import format_span_id, format_trace_id, get_current_span
from structlog.types import EventDict, WrappedLogger

from src.config import Settings
from src.immutable import FrozenDict
from src.observability.exporters import (
    export_batch_size,
    parse_otlp_headers,
    signal_endpoint,
)

#: structlog level name to OTLP severity. `warn` and `exception` are aliases
#: structlog itself normalises, and are mapped anyway: a custom processor
#: upstream of this one is allowed to set `level` to whatever it likes.
SEVERITY_BY_LEVEL: Final[FrozenDict[str, SeverityNumber]] = FrozenDict(
    {
        "notset": SeverityNumber.UNSPECIFIED,
        "debug": SeverityNumber.DEBUG,
        "info": SeverityNumber.INFO,
        "warn": SeverityNumber.WARN,
        "warning": SeverityNumber.WARN,
        "error": SeverityNumber.ERROR,
        "exception": SeverityNumber.ERROR,
        "critical": SeverityNumber.FATAL,
        "fatal": SeverityNumber.FATAL,
    }
)

#: Keys that become part of the record proper rather than one of its
#: attributes. `trace_id` and `span_id` are here because the SDK sets them on
#: the record from the active context, so repeating them as attributes would
#: store every id twice and let the two disagree.
_STRUCTURAL_KEYS: Final[frozenset[str]] = frozenset(
    {"event", "level", "timestamp", "exc_info", "trace_id", "span_id"}
)

#: Attribute values OTLP can carry without a lossy conversion.
AttributeValue = str | bool | int | float


def add_trace_correlation(
    _logger: WrappedLogger, _method_name: str, event_dict: EventDict
) -> EventDict:
    """Stamp the active trace and span ids onto a log event.

    Nothing is added outside a span, rather than a pair of zeroed ids: an
    absent key says "this happened outside a trace", while
    `00000000000000000000000000000000` says "this happened in a trace" to
    every query that looks for the field.
    """
    span_context = get_current_span().get_span_context()
    if not span_context.is_valid:
        return event_dict
    event_dict["trace_id"] = format_trace_id(span_context.trace_id)
    event_dict["span_id"] = format_span_id(span_context.span_id)
    return event_dict


def _attribute_value(value: object) -> AttributeValue:
    """One event field, as something OTLP can carry.

    Anything that is not a primitive is rendered with `repr`, which keeps a
    `UUID`, a `datetime` or a dataclass readable in the backend instead of
    dropping the field.
    """
    if isinstance(value, str | bool | int | float):
        return value
    return repr(value)


def _exception_from(event_dict: EventDict) -> BaseException | None:
    """The exception a `log.exception(...)` call is reporting, if any.

    structlog's `set_exc_info` puts `exc_info=True` in the dict and leaves the
    lookup to the renderer, so `True` has to be resolved against the exception
    currently being handled. A tuple or an exception instance may also arrive,
    since both are legal to pass to `exc_info=`.
    """
    exc_info = event_dict.get("exc_info")
    if exc_info is None or exc_info is False:
        return None
    if isinstance(exc_info, BaseException):
        return exc_info
    if isinstance(exc_info, tuple):
        exception = exc_info[1]
        return exception if isinstance(exception, BaseException) else None
    current = sys.exc_info()[1]
    return current


class LogForwarder:
    """structlog processor that mirrors events into the OTel logs pipeline.

    A bound object rather than a closure so that the binding can be undone:
    `shutdown_observability` unbinds it, and a test that configures a provider
    of its own leaves no forwarder pointing at a shut-down exporter behind.

    Never raises. A telemetry pipeline that can fail a log call is a telemetry
    pipeline that can fail a request — the forwarder sits in the chain of every
    `logger.info` in this codebase, including the ones inside exception
    handlers — so an export failure is swallowed and the event still reaches
    stdout, which is the log of record.
    """

    def __init__(self) -> None:
        self._logger: OTelLogger | None = None

    @property
    def bound(self) -> bool:
        return self._logger is not None

    def bind(self, logger: OTelLogger) -> None:
        self._logger = logger

    def unbind(self) -> None:
        self._logger = None

    def __call__(
        self, _logger: WrappedLogger, method_name: str, event_dict: EventDict
    ) -> EventDict:
        otel_logger = self._logger
        if otel_logger is None:
            return event_dict
        try:
            self._emit(otel_logger, method_name, event_dict)
        except Exception:  # pragma: no cover - defensive, see the class docstring
            pass
        return event_dict

    def _emit(
        self, otel_logger: OTelLogger, method_name: str, event_dict: EventDict
    ) -> None:
        level = str(event_dict.get("level", method_name)).lower()
        attributes: dict[str, AttributeValue] = {
            key: _attribute_value(value)
            for key, value in event_dict.items()
            if key not in _STRUCTURAL_KEYS
        }
        otel_logger.emit(
            # No `context=`: the SDK reads the ambient one, which is the same
            # span `add_trace_correlation` just read a few processors earlier.
            timestamp=time.time_ns(),
            severity_number=SEVERITY_BY_LEVEL.get(level, SeverityNumber.UNSPECIFIED),
            severity_text=level.upper(),
            body=_attribute_value(event_dict.get("event", "")),
            attributes=attributes,
            exception=_exception_from(event_dict),
        )


#: The process-wide forwarder. `configure_logging` puts it in the processor
#: chain; `configure_observability` gives it somewhere to send.
log_forwarder: Final[LogForwarder] = LogForwarder()


def build_log_exporter(settings: Settings) -> LogRecordExporter | None:
    """The exporter named by `OTEL_EXPORTER`, or `None` for "none"."""
    if settings.OTEL_EXPORTER == "none":
        return None
    if settings.OTEL_EXPORTER == "console":
        return ConsoleLogRecordExporter()
    # Imported lazily, for the reason given in `tracing.build_span_exporter`.
    from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter

    return OTLPLogExporter(
        endpoint=signal_endpoint(settings.OTEL_EXPORTER_OTLP_ENDPOINT, "logs"),
        headers=parse_otlp_headers(settings.OTEL_EXPORTER_OTLP_HEADERS),
        timeout=settings.OTEL_EXPORTER_OTLP_TIMEOUT_SECONDS,
    )


def build_logger_provider(settings: Settings, resource: Resource) -> LoggerProvider:
    """A provider wired to the configured exporter, batching where it matters.

    Batch for OTLP and simple for the console, the same split and for the same
    reason as `tracing.build_tracer_provider`: exporting a log record inline
    over the network would put the collector's latency inside every
    `logger.info` call in a request handler.
    """
    provider = LoggerProvider(resource=resource)
    exporter = build_log_exporter(settings)
    if exporter is None:
        return provider
    if settings.OTEL_EXPORTER == "console":
        provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    else:
        provider.add_log_record_processor(
            BatchLogRecordProcessor(
                exporter,
                max_queue_size=settings.OTEL_BATCH_MAX_QUEUE_SIZE,
                schedule_delay_millis=settings.OTEL_BATCH_SCHEDULE_DELAY_SECONDS * 1000,
                max_export_batch_size=export_batch_size(
                    settings.OTEL_BATCH_MAX_QUEUE_SIZE
                ),
            )
        )
    return provider


def structlog_processors() -> list[structlog.types.Processor]:
    """The two processors `configure_logging` inserts before the renderer.

    Order matters: correlation first, so that the ids it adds are in the dict
    the renderer writes to stdout, and the forwarder second, which skips them
    because the record carries them structurally.
    """
    return [add_trace_correlation, log_forwarder]


__all__ = [
    "SEVERITY_BY_LEVEL",
    "LogForwarder",
    "add_trace_correlation",
    "build_log_exporter",
    "build_logger_provider",
    "log_forwarder",
    "structlog_processors",
]
