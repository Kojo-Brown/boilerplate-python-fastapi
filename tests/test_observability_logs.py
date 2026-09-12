"""Trace correlation on every log line, and the bridge into the OTLP pipeline.

Two properties are load-bearing and both are about what happens when the SDK is
*not* configured, which is the state this application ships in: correlation adds
nothing outside a span, and the forwarder is inert until something binds it.
"""

from collections.abc import Iterator

import pytest
import structlog
from opentelemetry._logs import LogRecord, SeverityNumber
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import (
    InMemoryLogRecordExporter,
    SimpleLogRecordProcessor,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import format_span_id, format_trace_id, use_span
from structlog.types import EventDict

from src.observability.logs import (
    SEVERITY_BY_LEVEL,
    LogForwarder,
    add_trace_correlation,
    log_forwarder,
    structlog_processors,
)


def an_event(**fields: object) -> EventDict:
    event: EventDict = {"event": "something.happened", "level": "info"}
    event.update(fields)
    return event


@pytest.fixture
def tracing() -> Iterator[tuple[TracerProvider, InMemorySpanExporter]]:
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    yield provider, exporter
    provider.shutdown()


@pytest.fixture
def logs() -> Iterator[tuple[LoggerProvider, InMemoryLogRecordExporter]]:
    provider = LoggerProvider()
    # The SDK ships this constructor untyped.
    exporter = InMemoryLogRecordExporter()  # type: ignore[no-untyped-call]
    provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
    yield provider, exporter
    provider.shutdown()


class TestTraceCorrelation:
    def test_nothing_is_added_outside_a_span(self) -> None:
        # An absent key says "outside a trace"; a zeroed id would say "inside
        # one" to every query that looks for the field.
        event = add_trace_correlation(None, "info", an_event())
        assert "trace_id" not in event
        assert "span_id" not in event

    def test_the_active_span_is_stamped_on(
        self, tracing: tuple[TracerProvider, InMemorySpanExporter]
    ) -> None:
        provider, _ = tracing
        span = provider.get_tracer(__name__).start_span("work")
        with use_span(span, end_on_exit=True):
            event = add_trace_correlation(None, "info", an_event())
        context = span.get_span_context()
        assert event["trace_id"] == format_trace_id(context.trace_id)
        assert event["span_id"] == format_span_id(context.span_id)

    def test_the_ids_are_in_the_w3c_spelling(
        self, tracing: tuple[TracerProvider, InMemorySpanExporter]
    ) -> None:
        # Zero-padded lower-case hex, 32 and 16 wide — the form that appears in
        # a traceparent and the form a backend joins logs to traces on.
        provider, _ = tracing
        with use_span(
            provider.get_tracer(__name__).start_span("work"), end_on_exit=True
        ):
            event = add_trace_correlation(None, "info", an_event())
        assert len(str(event["trace_id"])) == 32
        assert len(str(event["span_id"])) == 16
        assert str(event["trace_id"]) == str(event["trace_id"]).lower()
        int(str(event["trace_id"]), 16)

    def test_the_event_is_otherwise_untouched(
        self, tracing: tuple[TracerProvider, InMemorySpanExporter]
    ) -> None:
        provider, _ = tracing
        with use_span(
            provider.get_tracer(__name__).start_span("work"), end_on_exit=True
        ):
            event = add_trace_correlation(None, "info", an_event(request_id="abc"))
        assert event["event"] == "something.happened"
        assert event["request_id"] == "abc"


class TestTheForwarderWhenUnbound:
    def test_it_passes_the_event_through(self) -> None:
        forwarder = LogForwarder()
        assert not forwarder.bound
        event = an_event()
        assert forwarder(None, "info", event) is event

    def test_binding_and_unbinding(
        self, logs: tuple[LoggerProvider, InMemoryLogRecordExporter]
    ) -> None:
        provider, _ = logs
        forwarder = LogForwarder()
        forwarder.bind(provider.get_logger("t"))
        assert forwarder.bound
        forwarder.unbind()
        assert not forwarder.bound


class TestTheForwarderWhenBound:
    @pytest.fixture(autouse=True)
    def bound(
        self, logs: tuple[LoggerProvider, InMemoryLogRecordExporter]
    ) -> Iterator[None]:
        provider, exporter = logs
        self.exporter = exporter
        self.forwarder = LogForwarder()
        self.forwarder.bind(provider.get_logger("test"))
        yield
        self.forwarder.unbind()

    def emitted(self) -> LogRecord:
        records = self.exporter.get_finished_logs()
        assert len(records) == 1
        return records[0].log_record

    def test_the_event_becomes_the_body(self) -> None:
        self.forwarder(None, "info", an_event())
        assert self.emitted().body == "something.happened"

    def test_the_event_is_returned_unchanged_for_the_renderer(self) -> None:
        event = an_event()
        assert self.forwarder(None, "info", event) is event

    def test_bound_fields_become_attributes(self) -> None:
        self.forwarder(None, "info", an_event(request_id="abc", status_code=200))
        attributes = dict(self.emitted().attributes or {})
        assert attributes["request_id"] == "abc"
        assert attributes["status_code"] == 200

    def test_the_structural_fields_are_not_repeated_as_attributes(self) -> None:
        # trace_id and span_id live on the record itself; duplicating them
        # stores every id twice and lets the two disagree.
        self.forwarder(
            None,
            "info",
            an_event(timestamp="2026-01-01T00:00:00Z", trace_id="a" * 32),
        )
        attributes = dict(self.emitted().attributes or {})
        assert "event" not in attributes
        assert "level" not in attributes
        assert "timestamp" not in attributes
        assert "trace_id" not in attributes

    def test_a_non_primitive_field_is_kept_as_its_repr(self) -> None:
        import uuid

        identifier = uuid.uuid4()
        self.forwarder(None, "info", an_event(user=identifier))
        attributes = dict(self.emitted().attributes or {})
        assert attributes["user"] == repr(identifier)

    @pytest.mark.parametrize(
        ("level", "expected"),
        [
            ("debug", SeverityNumber.DEBUG),
            ("info", SeverityNumber.INFO),
            ("warning", SeverityNumber.WARN),
            ("error", SeverityNumber.ERROR),
            ("critical", SeverityNumber.FATAL),
        ],
    )
    def test_the_severity_follows_the_structlog_level(
        self, level: str, expected: SeverityNumber
    ) -> None:
        self.forwarder(None, level, an_event(level=level))
        record = self.emitted()
        assert record.severity_number is expected
        assert record.severity_text == level.upper()

    def test_an_unknown_level_is_unspecified_rather_than_an_error(self) -> None:
        self.forwarder(None, "info", an_event(level="chatty"))
        assert self.emitted().severity_number is SeverityNumber.UNSPECIFIED

    def test_the_method_name_is_used_when_no_level_was_added(self) -> None:
        event: EventDict = {"event": "e"}
        self.forwarder(None, "warning", event)
        assert self.emitted().severity_number is SeverityNumber.WARN

    def test_the_handled_exception_is_attached(self) -> None:
        # structlog's set_exc_info leaves `exc_info=True` behind, so the
        # exception has to be resolved from the one being handled.
        try:
            raise ValueError("boom")
        except ValueError:
            self.forwarder(None, "error", an_event(level="error", exc_info=True))
        attributes = dict(self.emitted().attributes or {})
        assert attributes["exception.type"] == "ValueError"
        assert attributes["exception.message"] == "boom"

    def test_an_exception_instance_is_attached(self) -> None:
        self.forwarder(
            None, "error", an_event(level="error", exc_info=RuntimeError("nope"))
        )
        attributes = dict(self.emitted().attributes or {})
        assert attributes["exception.type"] == "RuntimeError"

    def test_an_exc_info_tuple_is_attached(self) -> None:
        error = RuntimeError("nope")
        self.forwarder(
            None,
            "error",
            an_event(level="error", exc_info=(RuntimeError, error, None)),
        )
        attributes = dict(self.emitted().attributes or {})
        assert attributes["exception.type"] == "RuntimeError"

    def test_exc_info_false_attaches_nothing(self) -> None:
        try:
            raise ValueError("boom")
        except ValueError:
            self.forwarder(None, "info", an_event(exc_info=False))
        attributes = dict(self.emitted().attributes or {})
        assert "exception.type" not in attributes

    def test_exc_info_true_with_nothing_being_handled_attaches_nothing(self) -> None:
        self.forwarder(None, "info", an_event(exc_info=True))
        attributes = dict(self.emitted().attributes or {})
        assert "exception.type" not in attributes

    def test_a_tuple_that_holds_no_exception_attaches_nothing(self) -> None:
        self.forwarder(None, "info", an_event(exc_info=(None, None, None)))
        attributes = dict(self.emitted().attributes or {})
        assert "exception.type" not in attributes

    def test_a_non_string_event_is_carried_as_its_repr(self) -> None:
        self.forwarder(None, "info", {"event": {"nested": "mapping"}, "level": "info"})
        assert self.emitted().body == repr({"nested": "mapping"})

    def test_the_record_carries_the_active_span(
        self, tracing: tuple[TracerProvider, InMemorySpanExporter]
    ) -> None:
        provider, _ = tracing
        span = provider.get_tracer(__name__).start_span("work")
        with use_span(span, end_on_exit=True):
            self.forwarder(None, "info", an_event())
        record = self.emitted()
        assert record.trace_id == span.get_span_context().trace_id
        assert record.span_id == span.get_span_context().span_id


class TestTheProcessorChain:
    def test_correlation_runs_before_the_forwarder(self) -> None:
        processors = structlog_processors()
        assert processors[0] is add_trace_correlation
        assert processors[1] is log_forwarder

    def test_configure_logging_installs_both(self) -> None:
        from src.logging_config import configure_logging

        configure_logging("INFO")
        chain = structlog.get_config()["processors"]
        assert add_trace_correlation in chain
        assert log_forwarder in chain
        # The renderer stays last: the forwarder reads the event dict, so it
        # has to run while there still is one.
        assert chain.index(log_forwarder) == len(chain) - 2

    def test_every_structlog_level_has_a_severity(self) -> None:
        for level in ("debug", "info", "warning", "error", "critical", "exception"):
            assert level in SEVERITY_BY_LEVEL
