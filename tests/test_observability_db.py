"""SQLAlchemy spans, against a real Postgres.

Skipped when `DATABASE_URL` names nothing reachable, and CI always has one.
Against a stub session this would assert nothing: the instrumentation hangs off
the `before_cursor_execute` events of the *synchronous* engine underneath the
async façade, and the failure it exists to catch — instrumenting the wrapper
instead, which the SDK accepts in silence — looks identical until a statement
actually executes.
"""

from collections.abc import AsyncIterator

import pytest
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from src.config import Settings, settings
from src.observability.setup import configure_observability, shutdown_observability


def a_settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "DATABASE_URL": settings.DATABASE_URL,
        "SECRET_KEY": "test-secret-key-not-a-real-one",
        "ENVIRONMENT": "test",
        "OTEL_ENABLED": True,
        "OTEL_EXPORTER": "none",
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


@pytest.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    """An engine on `DATABASE_URL`, or a skip if there is nothing there."""
    engine = create_async_engine(settings.DATABASE_URL, poolclass=NullPool)
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except (OSError, SQLAlchemyError) as exc:
        await engine.dispose()
        pytest.skip(f"no usable Postgres at DATABASE_URL: {exc}")
    yield engine
    await engine.dispose()


class TestStatementSpans:
    async def test_a_statement_becomes_a_span(self, engine: AsyncEngine) -> None:
        configured = a_settings()
        handle = configure_observability(
            configured, engine=engine, install_globally=False
        )
        assert handle.tracer_provider is not None
        exporter = InMemorySpanExporter()
        handle.tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
        try:
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        finally:
            shutdown_observability(handle, configured)

        spans = [
            span for span in exporter.get_finished_spans() if span.kind.name == "CLIENT"
        ]
        assert spans, [span.name for span in exporter.get_finished_spans()]
        assert any("SELECT" in span.name for span in spans)

    async def test_nothing_is_recorded_once_it_is_uninstrumented(
        self, engine: AsyncEngine
    ) -> None:
        # The other half of the same wiring: an engine still emitting into a
        # provider that has been shut down is how a suite poisons itself.
        configured = a_settings()
        handle = configure_observability(
            configured, engine=engine, install_globally=False
        )
        assert handle.tracer_provider is not None
        exporter = InMemorySpanExporter()
        handle.tracer_provider.add_span_processor(SimpleSpanProcessor(exporter))
        shutdown_observability(handle, configured)

        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        assert exporter.get_finished_spans() == ()
