"""End to end: a request carrying a `traceparent` through to the spans it made.

The unit tests next door assert the wiring. This one asserts the thing the
wiring is for — that an inbound W3C context becomes the parent of this
service's work, that the work it does downstream continues the same trace
rather than starting a second one, and that the log lines emitted in between
name the span they happened in.

A throwaway app is used rather than `src.main.app`: instrumentation attaches to
an app object for the life of the process, and the rest of the suite shares
that one.
"""

import asyncio
import logging
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager

import httpx
import pytest
import structlog
import uvicorn
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient
from opentelemetry.sdk.trace import ReadableSpan
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import Tracer, format_span_id, format_trace_id
from structlog.types import EventDict, WrappedLogger

from src.config import Settings
from src.observability.logs import log_forwarder
from src.observability.setup import (
    Observability,
    configure_observability,
    shutdown_observability,
)

TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
TRACE_ID_HEX = "4bf92f3577b34da6a3ce929d0e0e4736"
UPSTREAM_SPAN_ID_HEX = "00f067aa0ba902b7"


def a_settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "DATABASE_URL": "postgresql+asyncpg://fake:fake@localhost/fake",
        "SECRET_KEY": "test-secret-key-not-a-real-one",
        "ENVIRONMENT": "test",
        "OTEL_ENABLED": True,
        "OTEL_EXPORTER": "none",
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


@pytest.fixture
def captured_logs() -> Iterator[list[dict[str, object]]]:
    """structlog's own capture, so the correlation fields can be read back.

    The level and the caching are set explicitly rather than inherited.
    `configure_logging` installs a *filtering* bound logger built from
    `LOG_LEVEL` — WARNING in CI — and caches it on the first call, so a
    fixture that replaced only the processor chain would capture nothing and
    fail depending on which test ran first.
    """
    entries: list[dict[str, object]] = []
    original = structlog.get_config()

    def capture(_logger: WrappedLogger, _name: str, event_dict: EventDict) -> str:
        entries.append(dict(event_dict))
        return ""

    from src.observability.logs import add_trace_correlation

    structlog.configure(
        processors=[add_trace_correlation, capture],
        wrapper_class=structlog.make_filtering_bound_logger(logging.NOTSET),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
    yield entries
    structlog.configure(**original)


@pytest.fixture
def traced_app() -> Iterator[tuple[FastAPI, InMemorySpanExporter]]:
    """An app with server spans going into an in-memory exporter."""
    app = FastAPI()

    @app.get("/things")
    async def read_things() -> dict[str, str]:
        structlog.get_logger(__name__).info("handler.ran")
        return {"status": "ok"}

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "alive"}

    @app.get("/boom")
    async def boom() -> dict[str, str]:
        raise RuntimeError("handler exploded")

    settings = a_settings()
    handle = configure_observability(settings, app=app, install_globally=False)
    assert handle.tracer_provider is not None
    exporter = InMemorySpanExporter()
    handle.tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
    app.state.observability = handle
    yield app, exporter
    shutdown_observability(handle, settings)


@pytest.fixture
async def echo_origin() -> AsyncIterator[str]:
    """A uvicorn on an ephemeral port that hands back the headers it received.

    Uninstrumented on purpose: it stands in for somebody else's service, and
    what is asserted is what this process put on the wire.
    """
    echo = FastAPI()

    @echo.get("/echo")
    async def echo_headers(request: Request) -> dict[str, str]:
        return dict(request.headers)

    config = uvicorn.Config(
        echo, host="127.0.0.1", port=0, log_level="warning", lifespan="off"
    )
    instance = uvicorn.Server(config)
    serving = asyncio.ensure_future(instance.serve())
    try:
        while not instance.started:
            await asyncio.sleep(0.01)
        port = instance.servers[0].sockets[0].getsockname()[1]
        yield f"http://127.0.0.1:{port}"
    finally:
        instance.should_exit = True
        await asyncio.wait_for(serving, timeout=5.0)


@pytest.fixture
async def client(
    traced_app: tuple[FastAPI, InMemorySpanExporter],
) -> AsyncIterator[AsyncClient]:
    app, _ = traced_app
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as async_client:
        yield async_client


def exporter_tracer(traced: tuple[FastAPI, InMemorySpanExporter]) -> Tracer:
    """A tracer on the same provider the fixture exports from."""
    app, _ = traced
    handle: Observability = app.state.observability
    assert handle.tracer_provider is not None
    return handle.tracer_provider.get_tracer(__name__)


def server_span(exporter: InMemorySpanExporter) -> ReadableSpan:
    spans = [
        span for span in exporter.get_finished_spans() if span.kind.name == "SERVER"
    ]
    assert len(spans) == 1, [span.name for span in exporter.get_finished_spans()]
    return spans[0]


class TestServerSpans:
    async def test_a_request_produces_a_server_span(
        self, client: AsyncClient, traced_app: tuple[FastAPI, InMemorySpanExporter]
    ) -> None:
        _, exporter = traced_app
        response = await client.get("/things")
        assert response.status_code == 200
        span = server_span(exporter)
        assert span.attributes is not None
        assert span.attributes["http.request.method"] == "GET"

    async def test_the_route_template_names_the_span_not_the_url(
        self, client: AsyncClient, traced_app: tuple[FastAPI, InMemorySpanExporter]
    ) -> None:
        # A span named after the raw path makes every distinct id its own
        # operation, which is how a trace backend's cardinality explodes.
        _, exporter = traced_app
        await client.get("/things")
        assert server_span(exporter).name == "GET /things"

    async def test_an_inbound_traceparent_becomes_the_parent(
        self, client: AsyncClient, traced_app: tuple[FastAPI, InMemorySpanExporter]
    ) -> None:
        _, exporter = traced_app
        await client.get("/things", headers={"traceparent": TRACEPARENT})
        span = server_span(exporter)
        assert format_trace_id(span.context.trace_id) == TRACE_ID_HEX
        assert span.parent is not None
        assert format_span_id(span.parent.span_id) == UPSTREAM_SPAN_ID_HEX

    async def test_a_request_without_one_starts_its_own_trace(
        self, client: AsyncClient, traced_app: tuple[FastAPI, InMemorySpanExporter]
    ) -> None:
        _, exporter = traced_app
        await client.get("/things")
        assert server_span(exporter).parent is None

    async def test_two_requests_are_two_traces(
        self, client: AsyncClient, traced_app: tuple[FastAPI, InMemorySpanExporter]
    ) -> None:
        _, exporter = traced_app
        await client.get("/things")
        await client.get("/things")
        trace_ids = {span.context.trace_id for span in exporter.get_finished_spans()}
        assert len(trace_ids) == 2

    async def test_the_health_endpoint_is_excluded(
        self, client: AsyncClient, traced_app: tuple[FastAPI, InMemorySpanExporter]
    ) -> None:
        # A liveness probe every second would otherwise be the highest-volume
        # span in the system and the least interesting.
        _, exporter = traced_app
        response = await client.get("/health")
        assert response.status_code == 200
        assert exporter.get_finished_spans() == ()

    async def test_a_failing_handler_leaves_a_failed_span(
        self, client: AsyncClient, traced_app: tuple[FastAPI, InMemorySpanExporter]
    ) -> None:
        with pytest.raises(RuntimeError, match="handler exploded"):
            await client.get("/boom")
        _, exporter = traced_app
        span = server_span(exporter)
        assert span.status.is_ok is False
        assert any(event.name == "exception" for event in span.events)

    async def test_no_span_is_left_open_per_request(
        self, client: AsyncClient, traced_app: tuple[FastAPI, InMemorySpanExporter]
    ) -> None:
        # One ended server span per request, and nothing accumulating: the
        # ASGI send/receive child spans are switched off deliberately, because
        # a streaming response would otherwise produce one per chunk.
        _, exporter = traced_app
        await client.get("/things")
        await client.get("/things")
        assert len(exporter.get_finished_spans()) == 2


class TestOutboundPropagation:
    """The other half of W3C propagation, against a real socket.

    A real server rather than `httpx.MockTransport`, because of where the
    instrumentation attaches: it wraps `AsyncHTTPTransport.handle_async_request`
    — the transport that actually opens a connection — so a mock transport
    substituted for it is, correctly, not instrumented at all. Asserting
    against one would have asserted nothing about what leaves this process.
    """

    async def test_an_outbound_call_carries_the_context_onwards(
        self,
        echo_origin: str,
        traced_app: tuple[FastAPI, InMemorySpanExporter],
    ) -> None:
        _, exporter = traced_app
        tracer = exporter_tracer(traced_app)
        with tracer.start_as_current_span("caller"):
            async with httpx.AsyncClient() as outbound:
                response = await outbound.get(f"{echo_origin}/echo")

        received = response.json()
        assert "traceparent" in received
        client_spans = [
            span for span in exporter.get_finished_spans() if span.kind.name == "CLIENT"
        ]
        assert len(client_spans) == 1
        version, trace_id, span_id, _ = received["traceparent"].split("-")
        assert version == "00"
        assert trace_id == format_trace_id(client_spans[0].context.trace_id)
        assert span_id == format_span_id(client_spans[0].context.span_id)

    async def test_the_outbound_span_continues_the_inbound_trace(
        self,
        client: AsyncClient,
        echo_origin: str,
        traced_app: tuple[FastAPI, InMemorySpanExporter],
    ) -> None:
        # One trace from the caller, through this service, to the dependency:
        # the property that a `traceparent` on the way in and a `traceparent`
        # on the way out are the same trace.
        app, exporter = traced_app

        @app.get("/fanout")
        async def fanout() -> dict[str, str]:
            async with httpx.AsyncClient() as outbound:
                forwarded = await outbound.get(f"{echo_origin}/echo")
            return {"traceparent": forwarded.json().get("traceparent", "")}

        response = await client.get("/fanout", headers={"traceparent": TRACEPARENT})

        assert response.json()["traceparent"].split("-")[1] == TRACE_ID_HEX
        assert {
            format_trace_id(span.context.trace_id)
            for span in exporter.get_finished_spans()
        } == {TRACE_ID_HEX}

    async def test_the_client_span_is_a_child_of_the_server_span(
        self,
        client: AsyncClient,
        echo_origin: str,
        traced_app: tuple[FastAPI, InMemorySpanExporter],
    ) -> None:
        app, exporter = traced_app

        @app.get("/fanout")
        async def fanout() -> dict[str, str]:
            async with httpx.AsyncClient() as outbound:
                await outbound.get(f"{echo_origin}/echo")
            return {"status": "ok"}

        await client.get("/fanout")

        spans = {span.kind.name: span for span in exporter.get_finished_spans()}
        assert spans["CLIENT"].parent is not None
        assert spans["CLIENT"].parent.span_id == spans["SERVER"].context.span_id


class TestLogCorrelation:
    async def test_a_log_line_from_a_handler_names_its_span(
        self,
        client: AsyncClient,
        traced_app: tuple[FastAPI, InMemorySpanExporter],
        captured_logs: list[dict[str, object]],
    ) -> None:
        _, exporter = traced_app
        await client.get("/things", headers={"traceparent": TRACEPARENT})

        handler_logs = [
            entry for entry in captured_logs if entry.get("event") == "handler.ran"
        ]
        assert len(handler_logs) == 1
        span = server_span(exporter)
        assert handler_logs[0]["trace_id"] == TRACE_ID_HEX
        assert handler_logs[0]["span_id"] == format_span_id(span.context.span_id)

    async def test_a_log_line_outside_a_request_has_no_trace_fields(
        self, captured_logs: list[dict[str, object]]
    ) -> None:
        structlog.get_logger(__name__).info("startup.ran")
        assert captured_logs[-1].get("trace_id") is None


class TestDisabledEndToEnd:
    async def test_nothing_is_recorded_when_the_sdk_is_off(self) -> None:
        app = FastAPI()

        @app.get("/things")
        async def read_things() -> dict[str, str]:
            return {"status": "ok"}

        handle = configure_observability(
            a_settings(OTEL_ENABLED=False), app=app, install_globally=False
        )
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as async_client:
            response = await async_client.get("/things")
        assert response.status_code == 200
        assert not handle.enabled
        assert not log_forwarder.bound
        shutdown_observability(handle, a_settings())


class TestWhenInstrumentationHappens:
    """Why `src/main.py` configures telemetry at import and not in the lifespan.

    Instrumenting a FastAPI app patches `build_middleware_stack`, and Starlette
    builds that stack on the app's *first* ASGI call — which is the lifespan
    message. Instrumenting from inside the lifespan is therefore too late: the
    app serves every request through a stack assembled before the middleware
    existed, and emits no server spans while reporting no error. These two
    tests pin both halves, because the failure has no other symptom.
    """

    def test_instrumented_before_the_first_call_produces_spans(self) -> None:
        app = FastAPI()

        @app.get("/things")
        async def read_things() -> dict[str, str]:
            return {"status": "ok"}

        settings = a_settings()
        handle = configure_observability(settings, app=app, install_globally=False)
        assert handle.tracer_provider is not None
        exporter = InMemorySpanExporter()
        handle.tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
        try:
            # The context manager runs the lifespan, as a deployment does.
            with TestClient(app) as client:
                assert client.get("/things").status_code == 200
        finally:
            shutdown_observability(handle, settings)

        assert [span.name for span in exporter.get_finished_spans()] == ["GET /things"]

    def test_instrumented_inside_the_lifespan_produces_none(self) -> None:
        settings = a_settings()
        captured: list[InMemorySpanExporter] = []

        @asynccontextmanager
        async def lifespan(app: FastAPI) -> AsyncIterator[None]:
            handle = configure_observability(settings, app=app, install_globally=False)
            assert handle.tracer_provider is not None
            exporter = InMemorySpanExporter()
            handle.tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
            captured.append(exporter)
            yield
            shutdown_observability(handle, settings)

        app = FastAPI(lifespan=lifespan)

        @app.get("/things")
        async def read_things() -> dict[str, str]:
            return {"status": "ok"}

        with TestClient(app) as client:
            assert client.get("/things").status_code == 200

        # No error, no warning, no span. If a future version of the
        # instrumentation makes this work, this test fails and the comment in
        # `src/main.py` can be relaxed.
        assert captured[0].get_finished_spans() == ()
