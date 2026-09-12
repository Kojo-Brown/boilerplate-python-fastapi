"""Turning the whole thing on, and — the harder half — turning it off again.

`configure_observability` is the only function the application calls. It builds
the three providers, installs them, instruments the seams, and hands back a
handle; `shutdown_observability` takes that handle and drains what is buffered
before the process exits.

**Shutdown is not tidiness.** The batch processors hold spans and log records
in this process's memory and nowhere else, so a process that exits without
flushing loses precisely the telemetry from the last few seconds of its life —
which, during a rolling deploy or an OOM kill, is the telemetry somebody is
about to go looking for. It is also bounded: `OTEL_SHUTDOWN_TIMEOUT_SECONDS`
caps the wait, because telemetry must never be the reason a SIGTERM misses its
grace period and the pod is killed.

**Installing globally is a one-way door, so it is a parameter.** The OTel API
refuses to replace a provider that is already installed — a second
`set_tracer_provider` logs a warning and keeps the first — which is correct for
an application (two providers means two halves of the traffic in two places)
and unusable for a test suite. `install_globally=False` builds and wires
everything exactly as production does, against providers the caller holds,
which is how the tests here exercise this function rather than a paraphrase of
it.
"""

from __future__ import annotations

from dataclasses import dataclass

import structlog
from fastapi import FastAPI
from opentelemetry import metrics as metrics_api
from opentelemetry import trace as trace_api
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.trace import TracerProvider
from sqlalchemy.ext.asyncio import AsyncEngine

from src.config import Settings
from src.observability.instrumentation import (
    apply_semconv_stability,
    instrument_fastapi,
    instrument_httpx,
    instrument_sqlalchemy,
    uninstrument_fastapi,
    uninstrument_httpx,
    uninstrument_sqlalchemy,
)
from src.observability.logs import build_logger_provider, log_forwarder
from src.observability.metrics import build_meter_provider
from src.observability.propagation import configure_propagation
from src.observability.resource import build_resource
from src.observability.tracing import build_tracer_provider

logger = structlog.get_logger(__name__)

#: The instrumentation library name every span this package opens is scoped to.
INSTRUMENTATION_NAME = "src.observability"


@dataclass(frozen=True, slots=True)
class Observability:
    """What was built, so that it can be flushed and unwound.

    Every field is optional because every signal can be switched off on its
    own, and because the disabled case — `OTEL_ENABLED=false`, the default —
    is this dataclass with nothing in it rather than a `None` the caller has to
    check for.
    """

    tracer_provider: TracerProvider | None = None
    meter_provider: MeterProvider | None = None
    logger_provider: LoggerProvider | None = None
    #: Held so shutdown can remove the ASGI middleware from the same app.
    app: FastAPI | None = None
    httpx_instrumented: bool = False
    sqlalchemy_instrumented: bool = False

    @property
    def enabled(self) -> bool:
        return any(
            (
                self.tracer_provider is not None,
                self.meter_provider is not None,
                self.logger_provider is not None,
            )
        )


def configure_observability(
    settings: Settings,
    *,
    app: FastAPI | None = None,
    engine: AsyncEngine | None = None,
    install_globally: bool = True,
) -> Observability:
    """Build the providers, install them, instrument the seams.

    Args:
        settings: Configuration. `OTEL_ENABLED=false` returns an empty handle
            without importing an exporter or starting a thread.
        app: The FastAPI app to add server spans to. `None` skips it, which is
            what a Celery worker or a relay process wants — those have traces
            and no HTTP server.
        engine: The async engine to add statement spans to.
        install_globally: Register the providers as the process-wide ones. See
            the module docstring.

    Returns:
        A handle for `shutdown_observability`. Never raises for a
        misconfiguration of a signal that is switched off.
    """
    if not settings.OTEL_ENABLED:
        return Observability()

    # First, and unconditionally: a propagator is what reads the `traceparent`
    # off an inbound request, and the instrumentation installed below asks the
    # *global* one for it.
    configure_propagation()

    # Before any instrumentor runs: they read the attribute-naming mode from
    # the environment at their first `instrument()` call and cache it.
    apply_semconv_stability(settings)

    resource = build_resource(settings)

    tracer_provider = (
        build_tracer_provider(settings, resource)
        if settings.OTEL_TRACES_ENABLED
        else None
    )
    meter_provider = (
        build_meter_provider(settings, resource)
        if settings.OTEL_METRICS_ENABLED
        else None
    )
    logger_provider = (
        build_logger_provider(settings, resource)
        if settings.OTEL_LOGS_ENABLED
        else None
    )

    if install_globally:
        if tracer_provider is not None:
            trace_api.set_tracer_provider(tracer_provider)
        if meter_provider is not None:
            metrics_api.set_meter_provider(meter_provider)
        if logger_provider is not None:
            set_logger_provider(logger_provider)

    if logger_provider is not None:
        log_forwarder.bind(logger_provider.get_logger(INSTRUMENTATION_NAME))

    if app is not None:
        instrument_fastapi(
            app,
            settings,
            tracer_provider=tracer_provider,
            meter_provider=meter_provider,
        )
    httpx_instrumented = settings.OTEL_INSTRUMENT_HTTPX
    if httpx_instrumented:
        instrument_httpx(tracer_provider=tracer_provider)
    sqlalchemy_instrumented = settings.OTEL_INSTRUMENT_SQLALCHEMY and engine is not None
    if sqlalchemy_instrumented and engine is not None:
        instrument_sqlalchemy(engine, tracer_provider=tracer_provider)

    logger.info(
        "observability.configured",
        service=settings.OTEL_SERVICE_NAME,
        exporter=settings.OTEL_EXPORTER,
        traces=tracer_provider is not None,
        metrics=meter_provider is not None,
        logs=logger_provider is not None,
        sampler_ratio=settings.OTEL_TRACES_SAMPLER_RATIO,
    )
    return Observability(
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
        logger_provider=logger_provider,
        app=app,
        httpx_instrumented=httpx_instrumented,
        sqlalchemy_instrumented=sqlalchemy_instrumented,
    )


def shutdown_observability(
    handle: Observability, settings: Settings | None = None
) -> None:
    """Unwind the instrumentation, then flush and close the providers.

    In that order, and the order is the point: instrumentation still running
    while a provider is shutting down produces spans that arrive after the last
    export and are dropped without a trace of their own. The forwarder is
    unbound for the same reason — a `logger.info` during the rest of the
    lifespan's teardown would otherwise be handed to a closed pipeline.

    Each provider is flushed with a timeout and then shut down. A collector
    that has stopped answering delays this by at most
    `OTEL_SHUTDOWN_TIMEOUT_SECONDS` per signal and loses what was queued, which
    is the right trade against holding a draining process open.
    """
    timeout_seconds = (
        settings.OTEL_SHUTDOWN_TIMEOUT_SECONDS if settings is not None else 5.0
    )
    timeout_millis = int(timeout_seconds * 1000)

    if handle.app is not None:
        uninstrument_fastapi(handle.app)
    if handle.httpx_instrumented:
        uninstrument_httpx()
    if handle.sqlalchemy_instrumented:
        uninstrument_sqlalchemy()

    log_forwarder.unbind()

    if handle.tracer_provider is not None:
        handle.tracer_provider.force_flush(timeout_millis)
        handle.tracer_provider.shutdown()
    if handle.meter_provider is not None:
        # `MeterProvider.shutdown` takes the timeout itself and flushes on the
        # way out, so there is no separate force_flush here: calling both would
        # spend the budget twice on a collector that is already not answering.
        handle.meter_provider.shutdown(timeout_millis=timeout_millis)
    if handle.logger_provider is not None:
        handle.logger_provider.force_flush(timeout_millis)
        handle.logger_provider.shutdown()


__all__ = [
    "INSTRUMENTATION_NAME",
    "Observability",
    "configure_observability",
    "shutdown_observability",
]
