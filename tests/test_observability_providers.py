"""The three providers, the sampler, and the resource they all carry.

These build objects rather than exercise them end to end — the end-to-end pass
is `test_observability_http.py`. What is asserted here is the wiring that is
invisible at runtime and expensive to get wrong: that a sampling decision
arriving in a `traceparent` wins over the local ratio, that the console
exporter is attached to a *simple* processor and the OTLP one to a batch
processor, and that a provider built for "none" records without exporting.
"""

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
)
from opentelemetry.sdk.trace.sampling import Decision, ParentBased
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    TraceFlags,
    set_span_in_context,
)

from src.config import Settings
from src.observability.logs import build_log_exporter, build_logger_provider
from src.observability.metrics import build_meter_provider, build_metric_exporter
from src.observability.resource import build_resource
from src.observability.tracing import (
    build_sampler,
    build_span_exporter,
    build_tracer_provider,
)

TRACE_ID = 0x4BF92F3577B34DA6A3CE929D0E0E4736
SPAN_ID = 0x00F067AA0BA902B7


def a_settings(**overrides: object) -> Settings:
    """Settings with the SDK on and nothing exported, plus overrides.

    Built explicitly rather than by mutating the global `settings`, which is
    frozen for the reasons given in `src/config.py`.
    """
    base: dict[str, object] = {
        "DATABASE_URL": "postgresql+asyncpg://fake:fake@localhost/fake",
        "SECRET_KEY": "test-secret-key-not-a-real-one",
        "ENVIRONMENT": "test",
        "OTEL_ENABLED": True,
        "OTEL_EXPORTER": "none",
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


def parent_context(*, sampled: bool):  # type: ignore[no-untyped-def]
    """A remote parent span context with the W3C `sampled` flag set or clear."""
    return set_span_in_context(
        NonRecordingSpan(
            SpanContext(
                trace_id=TRACE_ID,
                span_id=SPAN_ID,
                is_remote=True,
                trace_flags=TraceFlags(TraceFlags.SAMPLED if sampled else 0x00),
            )
        )
    )


class TestResource:
    def test_it_carries_the_service_identity(self) -> None:
        resource = build_resource(
            a_settings(OTEL_SERVICE_NAME="orders-api", OTEL_SERVICE_VERSION="2.3.1")
        )
        assert resource.attributes["service.name"] == "orders-api"
        assert resource.attributes["service.version"] == "2.3.1"

    def test_the_environment_is_the_one_the_process_believes_it_is_in(self) -> None:
        resource = build_resource(a_settings(ENVIRONMENT="staging"))
        assert resource.attributes["deployment.environment"] == "staging"

    def test_the_sdk_detected_attributes_survive_the_merge(self) -> None:
        # `Resource.create` merges over the detectors; losing them would take
        # `telemetry.sdk.*` with it, which is how a backend identifies the
        # producer.
        resource = build_resource(a_settings())
        assert "telemetry.sdk.name" in resource.attributes


class TestSampler:
    def test_a_full_ratio_samples_a_trace_this_process_starts(self) -> None:
        sampler = build_sampler(1.0)
        result = sampler.should_sample(None, TRACE_ID, "GET /users")
        assert result.decision is Decision.RECORD_AND_SAMPLE

    def test_a_zero_ratio_records_nothing_it_starts(self) -> None:
        sampler = build_sampler(0.0)
        result = sampler.should_sample(None, TRACE_ID, "GET /users")
        assert result.decision is Decision.DROP

    def test_it_is_parent_based_at_every_ratio(self) -> None:
        assert isinstance(build_sampler(1.0), ParentBased)
        assert isinstance(build_sampler(0.25), ParentBased)

    def test_an_upstream_yes_wins_over_a_zero_ratio(self) -> None:
        # The whole point of ParentBased: sampling locally at 0% must not punch
        # a hole in the middle of a trace somebody upstream decided to keep.
        sampler = build_sampler(0.0)
        result = sampler.should_sample(
            parent_context(sampled=True), TRACE_ID, "GET /users"
        )
        assert result.decision is Decision.RECORD_AND_SAMPLE

    def test_an_upstream_no_wins_over_a_full_ratio(self) -> None:
        sampler = build_sampler(1.0)
        result = sampler.should_sample(
            parent_context(sampled=False), TRACE_ID, "GET /users"
        )
        assert result.decision is Decision.DROP

    def test_the_decision_is_deterministic_in_the_trace_id(self) -> None:
        # Two processes at the same ratio must agree about the same trace, or
        # a distributed trace is sampled into fragments.
        first = build_sampler(0.5)
        second = build_sampler(0.5)
        for trace_id in (TRACE_ID, TRACE_ID + 1, TRACE_ID + 2):
            assert (
                first.should_sample(None, trace_id, "op").decision
                is second.should_sample(None, trace_id, "op").decision
            )

    @pytest.mark.parametrize("ratio", [-0.1, 1.5, 50.0])
    def test_a_ratio_outside_the_unit_interval_is_refused(self, ratio: float) -> None:
        # The SDK clamps, which turns "50" meaning 50% into "sample
        # everything" without a word.
        with pytest.raises(ValueError, match="between 0.0 and 1.0"):
            build_sampler(ratio)


class TestTracerProvider:
    def test_none_builds_a_provider_with_no_exporter(self) -> None:
        assert build_span_exporter(a_settings(OTEL_EXPORTER="none")) is None
        provider = build_tracer_provider(a_settings(), build_resource(a_settings()))
        assert isinstance(provider, TracerProvider)
        provider.shutdown()

    def test_a_provider_with_no_exporter_still_records(self) -> None:
        # This is what makes "none" the right mode for tests: a processor the
        # test attaches sees everything a collector would have.
        settings = a_settings()
        provider = build_tracer_provider(settings, build_resource(settings))
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        exporter = InMemorySpanExporter()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        with provider.get_tracer(__name__).start_as_current_span("work"):
            pass
        assert [span.name for span in exporter.get_finished_spans()] == ["work"]
        provider.shutdown()

    def test_the_console_exporter_is_attached_to_a_simple_processor(self) -> None:
        settings = a_settings(OTEL_EXPORTER="console")
        assert isinstance(build_span_exporter(settings), ConsoleSpanExporter)
        provider = build_tracer_provider(settings, build_resource(settings))
        processors = provider._active_span_processor._span_processors
        assert [type(p) for p in processors] == [SimpleSpanProcessor]
        provider.shutdown()

    def test_the_otlp_exporter_is_attached_to_a_batch_processor(self) -> None:
        settings = a_settings(
            OTEL_EXPORTER="otlp",
            OTEL_EXPORTER_OTLP_ENDPOINT="http://collector.test:4318",
            OTEL_EXPORTER_OTLP_HEADERS="api-key=fake-collector-key",
        )
        provider = build_tracer_provider(settings, build_resource(settings))
        processors = provider._active_span_processor._span_processors
        assert [type(p) for p in processors] == [BatchSpanProcessor]
        provider.shutdown()

    def test_the_otlp_exporter_is_pointed_at_the_traces_path(self) -> None:
        exporter = build_span_exporter(
            a_settings(
                OTEL_EXPORTER="otlp",
                OTEL_EXPORTER_OTLP_ENDPOINT="http://collector.test:4318",
                OTEL_EXPORTER_OTLP_HEADERS="api-key=fake-collector-key",
            )
        )
        assert exporter is not None
        # Reaching into the exporter's privates on purpose: the endpoint it
        # was built with is not otherwise observable, and getting it wrong is
        # a 404 per batch with nothing else out of place.
        assert getattr(exporter, "_endpoint") == "http://collector.test:4318/v1/traces"
        assert getattr(exporter, "_headers")["api-key"] == "fake-collector-key"
        exporter.shutdown()

    def test_the_resource_reaches_the_spans(self) -> None:
        settings = a_settings(OTEL_SERVICE_NAME="orders-api")
        provider = build_tracer_provider(settings, build_resource(settings))
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
            InMemorySpanExporter,
        )

        exporter = InMemorySpanExporter()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        with provider.get_tracer(__name__).start_as_current_span("work"):
            pass
        span = exporter.get_finished_spans()[0]
        assert span.resource.attributes["service.name"] == "orders-api"
        provider.shutdown()


class TestMeterProvider:
    def test_none_builds_a_provider_with_no_reader(self) -> None:
        settings = a_settings()
        assert build_metric_exporter(settings) is None
        provider = build_meter_provider(settings, build_resource(settings))
        assert isinstance(provider, MeterProvider)
        assert list(provider._metric_readers) == []
        provider.shutdown()

    def test_a_provider_with_no_reader_still_records(self) -> None:
        from opentelemetry.sdk.metrics.export import InMemoryMetricReader

        settings = a_settings()
        reader = InMemoryMetricReader()
        provider = MeterProvider(
            resource=build_resource(settings), metric_readers=[reader]
        )
        provider.get_meter(__name__).create_counter("things").add(3)
        data = reader.get_metrics_data()
        assert data is not None
        provider.shutdown()

    def test_console_gets_a_periodic_reader(self) -> None:
        settings = a_settings(OTEL_EXPORTER="console")
        assert isinstance(build_metric_exporter(settings), ConsoleMetricExporter)
        provider = build_meter_provider(settings, build_resource(settings))
        readers = list(provider._metric_readers)
        assert [type(r) for r in readers] == [PeriodicExportingMetricReader]
        provider.shutdown()

    def test_otlp_reader_uses_the_metrics_path_and_the_interval(self) -> None:
        settings = a_settings(
            OTEL_EXPORTER="otlp",
            OTEL_EXPORTER_OTLP_ENDPOINT="http://collector.test:4318",
            OTEL_METRIC_EXPORT_INTERVAL_SECONDS=15.0,
        )
        provider = build_meter_provider(settings, build_resource(settings))
        reader = next(iter(provider._metric_readers))
        assert isinstance(reader, PeriodicExportingMetricReader)
        assert reader._export_interval_millis == 15000
        assert (
            getattr(reader._exporter, "_endpoint")
            == "http://collector.test:4318/v1/metrics"
        )
        provider.shutdown()


class TestLoggerProvider:
    def test_none_builds_a_provider_with_no_processor(self) -> None:
        settings = a_settings()
        assert build_log_exporter(settings) is None
        provider = build_logger_provider(settings, build_resource(settings))
        assert provider._multi_log_record_processor._log_record_processors == ()
        provider.shutdown()

    def test_console_gets_a_simple_processor(self) -> None:
        settings = a_settings(OTEL_EXPORTER="console")
        provider = build_logger_provider(settings, build_resource(settings))
        processors = provider._multi_log_record_processor._log_record_processors
        assert len(processors) == 1
        assert type(processors[0]).__name__ == "SimpleLogRecordProcessor"
        provider.shutdown()

    def test_otlp_gets_a_batch_processor_on_the_logs_path(self) -> None:
        settings = a_settings(
            OTEL_EXPORTER="otlp",
            OTEL_EXPORTER_OTLP_ENDPOINT="http://collector.test:4318",
        )
        provider = build_logger_provider(settings, build_resource(settings))
        processors = provider._multi_log_record_processor._log_record_processors
        assert len(processors) == 1
        assert type(processors[0]).__name__ == "BatchLogRecordProcessor"
        provider.shutdown()

    def test_the_resource_reaches_the_records(self) -> None:
        from opentelemetry.sdk._logs.export import (
            InMemoryLogRecordExporter,
            SimpleLogRecordProcessor,
        )

        settings = a_settings(OTEL_SERVICE_NAME="orders-api")
        provider = build_logger_provider(settings, build_resource(settings))
        # The SDK ships this constructor untyped.
        exporter = InMemoryLogRecordExporter()  # type: ignore[no-untyped-call]
        provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
        provider.get_logger("t").emit(body="hello")
        record = exporter.get_finished_logs()[0]
        assert record.resource.attributes["service.name"] == "orders-api"
        provider.shutdown()


class TestBatchTuning:
    def test_the_queue_bound_and_delay_reach_the_span_processor(self) -> None:
        settings = a_settings(
            OTEL_EXPORTER="otlp",
            OTEL_EXPORTER_OTLP_ENDPOINT="http://collector.test:4318",
            OTEL_BATCH_MAX_QUEUE_SIZE=64,
            OTEL_BATCH_SCHEDULE_DELAY_SECONDS=2.0,
        )
        provider = build_tracer_provider(settings, build_resource(settings))
        processor = provider._active_span_processor._span_processors[0]
        batch = getattr(processor, "_batch_processor")
        assert batch._max_queue_size == 64
        assert batch._schedule_delay_millis == 2000
        provider.shutdown()
