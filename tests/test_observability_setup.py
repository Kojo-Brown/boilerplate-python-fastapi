"""`configure_observability` and its inverse.

Everything here runs with `install_globally=False`. The OTel API refuses to
replace a provider once one is installed, so a suite that installed globally
would leave the first test's provider serving every later one — see the module
docstring in `src/observability/setup.py`.

The teardown is as much of the subject as the tests are: instrumentors are
process-wide singletons, and a test that leaves httpx or SQLAlchemy patched
hands the next one a provider that has already been shut down.
"""

from collections.abc import Iterator

import pytest
from fastapi import FastAPI
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy.ext.asyncio import create_async_engine

from src.config import Settings
from src.observability.logs import log_forwarder
from src.observability.setup import (
    Observability,
    configure_observability,
    shutdown_observability,
)

FAKE_DATABASE_URL = "postgresql+asyncpg://fake:fake@localhost/fake"


def a_settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "DATABASE_URL": FAKE_DATABASE_URL,
        "SECRET_KEY": "test-secret-key-not-a-real-one",
        "ENVIRONMENT": "test",
        "OTEL_ENABLED": True,
        "OTEL_EXPORTER": "none",
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


@pytest.fixture
def app() -> FastAPI:
    """A throwaway app, so nothing instruments `src.main.app` by accident."""
    return FastAPI()


@pytest.fixture(autouse=True)
def leave_no_instrumentation_behind() -> Iterator[None]:
    yield
    from src.observability.instrumentation import (
        uninstrument_httpx,
        uninstrument_sqlalchemy,
    )

    uninstrument_httpx()
    uninstrument_sqlalchemy()
    log_forwarder.unbind()


class TestDisabled:
    def test_it_builds_nothing(self, app: FastAPI) -> None:
        handle = configure_observability(
            a_settings(OTEL_ENABLED=False), app=app, install_globally=False
        )
        assert handle == Observability()
        assert not handle.enabled

    def test_no_middleware_is_added_to_the_app(self, app: FastAPI) -> None:
        configure_observability(
            a_settings(OTEL_ENABLED=False), app=app, install_globally=False
        )
        assert not getattr(app, "_is_instrumented_by_opentelemetry", False)

    def test_the_log_forwarder_stays_unbound(self, app: FastAPI) -> None:
        configure_observability(
            a_settings(OTEL_ENABLED=False), app=app, install_globally=False
        )
        assert not log_forwarder.bound

    def test_shutting_down_an_empty_handle_does_nothing(self) -> None:
        shutdown_observability(Observability(), a_settings())


class TestEnabled:
    def test_all_three_providers_are_built(self, app: FastAPI) -> None:
        handle = configure_observability(a_settings(), app=app, install_globally=False)
        assert handle.tracer_provider is not None
        assert handle.meter_provider is not None
        assert handle.logger_provider is not None
        assert handle.enabled
        shutdown_observability(handle, a_settings())

    def test_the_app_is_instrumented(self, app: FastAPI) -> None:
        handle = configure_observability(a_settings(), app=app, install_globally=False)
        assert getattr(app, "_is_instrumented_by_opentelemetry", False)
        shutdown_observability(handle, a_settings())

    def test_the_log_forwarder_is_bound_to_the_provider(self, app: FastAPI) -> None:
        handle = configure_observability(a_settings(), app=app, install_globally=False)
        assert log_forwarder.bound
        shutdown_observability(handle, a_settings())

    def test_httpx_is_instrumented_by_default(self, app: FastAPI) -> None:
        handle = configure_observability(a_settings(), app=app, install_globally=False)
        assert handle.httpx_instrumented
        shutdown_observability(handle, a_settings())

    def test_httpx_instrumentation_can_be_switched_off(self, app: FastAPI) -> None:
        handle = configure_observability(
            a_settings(OTEL_INSTRUMENT_HTTPX=False), app=app, install_globally=False
        )
        assert not handle.httpx_instrumented
        shutdown_observability(handle, a_settings())

    def test_sqlalchemy_is_instrumented_when_an_engine_is_given(
        self, app: FastAPI
    ) -> None:
        engine = create_async_engine(FAKE_DATABASE_URL)
        handle = configure_observability(
            a_settings(), app=app, engine=engine, install_globally=False
        )
        assert handle.sqlalchemy_instrumented
        shutdown_observability(handle, a_settings())

    def test_sqlalchemy_is_skipped_without_an_engine(self, app: FastAPI) -> None:
        # A worker process has traces and no engine of its own to hand over.
        handle = configure_observability(a_settings(), app=app, install_globally=False)
        assert not handle.sqlalchemy_instrumented
        shutdown_observability(handle, a_settings())

    def test_sqlalchemy_instrumentation_can_be_switched_off(self, app: FastAPI) -> None:
        engine = create_async_engine(FAKE_DATABASE_URL)
        handle = configure_observability(
            a_settings(OTEL_INSTRUMENT_SQLALCHEMY=False),
            app=app,
            engine=engine,
            install_globally=False,
        )
        assert not handle.sqlalchemy_instrumented
        shutdown_observability(handle, a_settings())

    def test_an_app_is_optional(self) -> None:
        # The relay and the Celery worker configure telemetry with no server.
        handle = configure_observability(a_settings(), install_globally=False)
        assert handle.app is None
        assert handle.tracer_provider is not None
        shutdown_observability(handle, a_settings())

    def test_the_global_propagator_is_installed(self, app: FastAPI) -> None:
        from opentelemetry.propagate import get_global_textmap

        handle = configure_observability(a_settings(), app=app, install_globally=False)
        assert {"traceparent", "baggage"} <= set(get_global_textmap().fields)
        shutdown_observability(handle, a_settings())


class TestSignalsIndividually:
    @pytest.mark.parametrize(
        ("setting", "attribute"),
        [
            ("OTEL_TRACES_ENABLED", "tracer_provider"),
            ("OTEL_METRICS_ENABLED", "meter_provider"),
            ("OTEL_LOGS_ENABLED", "logger_provider"),
        ],
    )
    def test_one_signal_off_leaves_the_others_on(
        self, app: FastAPI, setting: str, attribute: str
    ) -> None:
        handle = configure_observability(
            a_settings(**{setting: False}), app=app, install_globally=False
        )
        assert getattr(handle, attribute) is None
        assert handle.enabled
        shutdown_observability(handle, a_settings())

    def test_logs_off_leaves_the_forwarder_unbound(self, app: FastAPI) -> None:
        handle = configure_observability(
            a_settings(OTEL_LOGS_ENABLED=False), app=app, install_globally=False
        )
        assert not log_forwarder.bound
        shutdown_observability(handle, a_settings())


class TestInstallingGlobally:
    """The default path, exercised without actually taking the one-way door.

    `set_tracer_provider` refuses to replace a provider that is already
    installed, so a test that really installed one would serve it to every
    later test in the process and then shut it down. The three installs are
    intercepted instead, which asserts exactly what the branch does: each
    provider that was built is handed to its own registration function.
    """

    def test_each_built_provider_is_registered(
        self, app: FastAPI, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from opentelemetry import metrics as metrics_api
        from opentelemetry import trace as trace_api

        from src.observability import setup as setup_module

        installed: dict[str, object] = {}
        monkeypatch.setattr(
            trace_api,
            "set_tracer_provider",
            lambda provider: installed.__setitem__("traces", provider),
        )
        monkeypatch.setattr(
            metrics_api,
            "set_meter_provider",
            lambda provider: installed.__setitem__("metrics", provider),
        )
        monkeypatch.setattr(
            setup_module,
            "set_logger_provider",
            lambda provider: installed.__setitem__("logs", provider),
        )

        handle = configure_observability(a_settings(), app=app)
        try:
            assert installed["traces"] is handle.tracer_provider
            assert installed["metrics"] is handle.meter_provider
            assert installed["logs"] is handle.logger_provider
        finally:
            shutdown_observability(handle, a_settings())

    def test_a_signal_that_was_not_built_is_not_registered(
        self, app: FastAPI, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from opentelemetry import metrics as metrics_api
        from opentelemetry import trace as trace_api

        from src.observability import setup as setup_module

        installed: list[str] = []
        monkeypatch.setattr(
            trace_api, "set_tracer_provider", lambda _: installed.append("traces")
        )
        monkeypatch.setattr(
            metrics_api, "set_meter_provider", lambda _: installed.append("metrics")
        )
        monkeypatch.setattr(
            setup_module, "set_logger_provider", lambda _: installed.append("logs")
        )

        handle = configure_observability(
            a_settings(OTEL_TRACES_ENABLED=False, OTEL_LOGS_ENABLED=False), app=app
        )
        try:
            assert installed == ["metrics"]
        finally:
            shutdown_observability(handle, a_settings())


class TestSemanticConventions:
    def test_the_stable_http_attribute_names_are_opted_into(self, app: FastAPI) -> None:
        # The instrumentations read this from the environment once and cache
        # it, so exporting it is the only way a setting can reach them.
        import os

        from src.observability.instrumentation import SEMCONV_STABILITY_ENV_VAR

        original = os.environ.pop(SEMCONV_STABILITY_ENV_VAR, None)
        try:
            handle = configure_observability(
                a_settings(), app=app, install_globally=False
            )
            assert os.environ[SEMCONV_STABILITY_ENV_VAR] == "http"
            shutdown_observability(handle, a_settings())
        finally:
            os.environ.pop(SEMCONV_STABILITY_ENV_VAR, None)
            if original is not None:
                os.environ[SEMCONV_STABILITY_ENV_VAR] = original

    def test_a_value_already_in_the_environment_wins(self, app: FastAPI) -> None:
        import os

        from src.observability.instrumentation import SEMCONV_STABILITY_ENV_VAR

        original = os.environ.get(SEMCONV_STABILITY_ENV_VAR)
        os.environ[SEMCONV_STABILITY_ENV_VAR] = "http/dup"
        try:
            handle = configure_observability(
                a_settings(), app=app, install_globally=False
            )
            assert os.environ[SEMCONV_STABILITY_ENV_VAR] == "http/dup"
            shutdown_observability(handle, a_settings())
        finally:
            os.environ.pop(SEMCONV_STABILITY_ENV_VAR, None)
            if original is not None:
                os.environ[SEMCONV_STABILITY_ENV_VAR] = original

    def test_an_empty_setting_exports_nothing(self, app: FastAPI) -> None:
        import os

        from src.observability.instrumentation import (
            SEMCONV_STABILITY_ENV_VAR,
            apply_semconv_stability,
        )

        original = os.environ.pop(SEMCONV_STABILITY_ENV_VAR, None)
        try:
            apply_semconv_stability(a_settings(OTEL_SEMCONV_STABILITY_OPT_IN=""))
            assert SEMCONV_STABILITY_ENV_VAR not in os.environ
        finally:
            if original is not None:
                os.environ[SEMCONV_STABILITY_ENV_VAR] = original


class TestShutdown:
    def test_it_drains_what_the_batch_processor_is_still_holding(
        self, app: FastAPI
    ) -> None:
        # The property that matters at SIGTERM: a span recorded a moment ago
        # is in this process's memory and nowhere else until something flushes.
        handle = configure_observability(a_settings(), app=app, install_globally=False)
        assert handle.tracer_provider is not None
        exporter = InMemorySpanExporter()
        handle.tracer_provider.add_span_processor(
            BatchSpanProcessor(exporter, schedule_delay_millis=60_000)
        )
        with handle.tracer_provider.get_tracer(__name__).start_as_current_span("work"):
            pass
        assert exporter.get_finished_spans() == ()

        shutdown_observability(handle, a_settings())
        assert [span.name for span in exporter.get_finished_spans()] == ["work"]

    def test_it_removes_the_middleware_from_the_app(self, app: FastAPI) -> None:
        handle = configure_observability(a_settings(), app=app, install_globally=False)
        shutdown_observability(handle, a_settings())
        assert not getattr(app, "_is_instrumented_by_opentelemetry", False)

    def test_it_unbinds_the_log_forwarder(self, app: FastAPI) -> None:
        handle = configure_observability(a_settings(), app=app, install_globally=False)
        shutdown_observability(handle, a_settings())
        assert not log_forwarder.bound

    def test_it_unwinds_the_sqlalchemy_instrumentation(self, app: FastAPI) -> None:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

        engine = create_async_engine(FAKE_DATABASE_URL)
        handle = configure_observability(
            a_settings(), app=app, engine=engine, install_globally=False
        )
        shutdown_observability(handle, a_settings())
        assert not SQLAlchemyInstrumentor().is_instrumented_by_opentelemetry

    def test_it_unwinds_the_httpx_instrumentation(self, app: FastAPI) -> None:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        handle = configure_observability(a_settings(), app=app, install_globally=False)
        shutdown_observability(handle, a_settings())
        assert not HTTPXClientInstrumentor().is_instrumented_by_opentelemetry

    def test_the_timeout_falls_back_without_settings(self, app: FastAPI) -> None:
        # A caller that has lost its Settings must still be able to drain.
        handle = configure_observability(a_settings(), app=app, install_globally=False)
        shutdown_observability(handle)
        assert not log_forwarder.bound
