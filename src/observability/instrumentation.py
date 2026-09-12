"""Where the spans actually come from: ASGI in, httpx out, SQLAlchemy below.

Three seams, chosen because between them they cover every boundary a request
crosses in this application and because none of them can be forgotten by the
next person to add a route or a client.

**FastAPI** is the inbound half of W3C propagation. The ASGI instrumentation
extracts the `traceparent` from the request headers before the handler runs, so
the server span it opens is a *child* of the caller's span rather than the root
of a second trace. It is applied to the app object rather than globally, which
matters for the tests: `TestClient(app)` on an instrumented app is instrumented,
and an app built in another test is not.

**httpx** is the outbound half, and where it attaches is worth knowing before
reading a trace from this service. It wraps `AsyncHTTPTransport
.handle_async_request` — the real transport at the bottom of the stack — and
not `AsyncClient.send`. Every outbound call here goes through
`resilient_async_client`, which wraps a `RetryTransport` around a
`BulkheadTransport` around that one, so the client span is *inside* the retry
loop: three attempts are three spans, each with its own `traceparent`, which is
also what the far end sees. That is the more useful shape — a retried call
looks like a retried call rather than one slow request — and it is the reason
this is the instrumentation library's business rather than a fourth transport
of our own, which would have had to pick a position in that stack and would
have been wrong for one of the two readings.

**SQLAlchemy** turns the slowest part of most handlers from a gap in the trace
into a span per statement. It is instrumented per engine, and the engine here
is an `AsyncEngine`, whose `sync_engine` attribute is the object the events
fire on — passing the async wrapper instruments nothing and reports no error.

Each is guarded by a setting, and `uninstrument` exists for all three, because
instrumentors are process-wide singletons: a test that instruments and does not
undo it leaves every later test emitting spans into a provider that has been
shut down.

`apply_semconv_stability` is the fourth thing here and the odd one: the HTTP
attribute names the three instrumentations emit are chosen by an environment
variable and by nothing else, so honouring a setting means putting it there.
"""

from __future__ import annotations

import os
from typing import Final

import structlog
from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.metrics import MeterProvider
from opentelemetry.trace import TracerProvider
from sqlalchemy.ext.asyncio import AsyncEngine

from src.config import Settings

logger = structlog.get_logger(__name__)

#: The environment variable the instrumentation libraries read to decide
#: between the stable HTTP semantic conventions and the superseded ones.
SEMCONV_STABILITY_ENV_VAR: Final[str] = "OTEL_SEMCONV_STABILITY_OPT_IN"


def apply_semconv_stability(settings: Settings) -> None:
    """Put `OTEL_SEMCONV_STABILITY_OPT_IN` where the instrumentations look.

    They read it from the environment once, at the first `instrument()` call,
    and cache the answer for the life of the process — there is no argument and
    no provider to pass it through. So a setting that decides whether spans say
    `http.request.method` or `http.method` can only be honoured by exporting
    it, which is done here rather than in the modules that instrument so that
    the one process-environment write in this package has a name.

    An explicit value already in the environment wins: it is the same variable,
    and a deployment that set it meant it.
    """
    if settings.OTEL_SEMCONV_STABILITY_OPT_IN:
        os.environ.setdefault(
            SEMCONV_STABILITY_ENV_VAR, settings.OTEL_SEMCONV_STABILITY_OPT_IN
        )


def instrument_fastapi(
    app: FastAPI,
    settings: Settings,
    *,
    tracer_provider: TracerProvider | None = None,
    meter_provider: MeterProvider | None = None,
) -> None:
    """Server spans and HTTP metrics for one app, minus the probe endpoints.

    `OTEL_EXCLUDED_URLS` keeps `/health` out of both. A liveness probe is the
    highest-rate endpoint most services have and says nothing about a user's
    request; leaving it in costs a span per second per replica forever, and
    skews every latency aggregate towards an endpoint that does no work.
    """
    FastAPIInstrumentor.instrument_app(
        app,
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
        excluded_urls=settings.OTEL_EXCLUDED_URLS or None,
        # The ASGI instrumentation can open a child span per `receive` and per
        # `send` message. Off, and not a setting: this API streams exports
        # (`src/api/v1/exports.py`) and serves SSE, where a span per `send` is
        # a span per chunk — a single large download would fill the batch
        # queue on its own and evict everything else waiting in it. The one
        # thing those spans carry that the server span does not is
        # time-to-first-byte, which is worth a purpose-built metric rather
        # than an unbounded number of spans.
        exclude_spans=["receive", "send"],
    )


def uninstrument_fastapi(app: FastAPI) -> None:
    """Remove the ASGI middleware again. Safe on an uninstrumented app."""
    if getattr(app, "_is_instrumented_by_opentelemetry", False):
        FastAPIInstrumentor.uninstrument_app(app)


def instrument_httpx(*, tracer_provider: TracerProvider | None = None) -> None:
    """Client spans for every `httpx` client, including ones built later.

    Global rather than per client: `HTTPXClientInstrumentor().instrument()`
    patches the class, so a client constructed by a module that has never heard
    of this package — `src/notifications/webhook.py` builds its own — is
    instrumented too. That is the property worth having, since the failure mode
    of a per-client approach is a hole in the trace at exactly the dependency
    somebody forgot to route through the factory.
    """
    HTTPXClientInstrumentor().instrument(tracer_provider=tracer_provider)


def uninstrument_httpx() -> None:
    """Unpatch httpx. Safe when it was never patched."""
    HTTPXClientInstrumentor().uninstrument()


def instrument_sqlalchemy(
    engine: AsyncEngine, *, tracer_provider: TracerProvider | None = None
) -> None:
    """A span per statement on one engine.

    `engine.sync_engine` is deliberate and is the whole subtlety of this
    function: SQLAlchemy's asyncio layer is a façade over a synchronous engine,
    the `before_cursor_execute` events fire on that inner object, and handing
    the outer one to the instrumentor is accepted in silence and produces no
    spans at all.
    """
    SQLAlchemyInstrumentor().instrument(
        engine=engine.sync_engine, tracer_provider=tracer_provider
    )


def uninstrument_sqlalchemy() -> None:
    """Detach the SQLAlchemy event listeners. Safe when never attached."""
    SQLAlchemyInstrumentor().uninstrument()


__all__ = [
    "SEMCONV_STABILITY_ENV_VAR",
    "apply_semconv_stability",
    "instrument_fastapi",
    "instrument_httpx",
    "instrument_sqlalchemy",
    "uninstrument_fastapi",
    "uninstrument_httpx",
    "uninstrument_sqlalchemy",
]
