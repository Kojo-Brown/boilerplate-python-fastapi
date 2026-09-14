"""The views: which labels survive, and which bucket boundaries a p99 rests on.

These are the two decisions `src/observability/red.py` makes on top of what the
instrumentation emits, and both are invisible at runtime — a dropped attribute
looks exactly like an attribute that was never set, and a changed boundary
looks exactly like a latency that moved. So they are asserted against the
readings themselves, through an `InMemoryMetricReader` on a provider built the
way `build_meter_provider` builds it.

The attribute assertions are written as "this one is gone, these are here"
rather than as an equality against the full set, because the instrumentation is
free to add attributes in a minor release and only the ones named here are part
of the contract the dashboard queries.
"""

from collections.abc import Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    Gauge,
    Histogram,
    HistogramDataPoint,
    InMemoryMetricReader,
    Metric,
    NumberDataPoint,
    Sum,
)
from prometheus_client import generate_latest

from src.config import Settings
from src.observability.instrumentation import apply_semconv_stability
from src.observability.metrics import build_meter_provider
from src.observability.prometheus import build_scrape_registry
from src.observability.red import (
    INSTRUMENT_ACTIVE_REQUESTS,
    INSTRUMENT_REQUEST_DURATION,
    PROMETHEUS_DURATION_LABELS,
    RED_DURATION_BUCKETS,
    red_views,
)
from src.observability.resource import build_resource


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
def readings() -> Iterator[list[Metric]]:
    """Serve a few requests through the real views, then hand back the metrics.

    The app is instrumented directly rather than through
    `configure_observability`, because what is under test is the provider's
    view configuration and nothing else — no propagation, no exporters, no
    process-wide state to unwind beyond the instrumentation itself.
    """
    settings = a_settings()
    # The instrumentation reads the attribute-naming mode from the environment
    # at its first `instrument()` call and caches it process-wide; `conftest.py`
    # pins it so that is deterministic, and this states the dependency.
    apply_semconv_stability(settings)
    reader = InMemoryMetricReader()
    provider = MeterProvider(
        resource=build_resource(settings),
        metric_readers=[reader],
        views=red_views(),
    )
    app = FastAPI()

    @app.get("/items/{item_id}")
    def read_item(item_id: int) -> dict[str, int]:
        return {"id": item_id}

    FastAPIInstrumentor.instrument_app(app, meter_provider=provider)
    try:
        client = TestClient(app)
        client.get("/items/1")
        client.get("/items/2")
        data = reader.get_metrics_data()
        assert data is not None
        yield [
            metric
            for resource_metrics in data.resource_metrics
            for scope_metrics in resource_metrics.scope_metrics
            for metric in scope_metrics.metrics
        ]
    finally:
        FastAPIInstrumentor.uninstrument_app(app)


def one(metrics: list[Metric], name: str) -> Metric:
    matching = [metric for metric in metrics if metric.name == name]
    assert matching, f"{name} was not recorded; got {[m.name for m in metrics]}"
    return matching[0]


def points(metric: Metric) -> list[HistogramDataPoint | NumberDataPoint]:
    """The data points, narrowed.

    `Metric.data` is a union of the aggregations an instrument can carry and
    each has its own point type, so the narrowing has to happen somewhere; here
    rather than in every test.
    """
    data = metric.data
    assert isinstance(data, Histogram | Sum | Gauge)
    return list(data.data_points)


def attributes_of(metric: Metric) -> set[str]:
    """The attribute *keys* on the first point, which is what a view decides."""
    return set(points(metric)[0].attributes or {})


def histogram_points(metric: Metric) -> list[HistogramDataPoint]:
    found = [point for point in points(metric) if isinstance(point, HistogramDataPoint)]
    assert found, f"{metric.name} is not a histogram"
    return found


class TestTheDurationHistogram:
    def test_it_keeps_the_attributes_the_dashboard_groups_by(
        self, readings: list[Metric]
    ) -> None:
        attributes = attributes_of(one(readings, INSTRUMENT_REQUEST_DURATION))
        assert {
            "http.request.method",
            "http.route",
            "http.response.status_code",
        } <= attributes

    def test_the_constant_attributes_are_dropped(self, readings: list[Metric]) -> None:
        """`url.scheme` and `network.protocol.version` never vary in a
        deployment, and a label that never varies is still carried on every one
        of the ~17 series a histogram costs per route and status."""
        attributes = attributes_of(one(readings, INSTRUMENT_REQUEST_DURATION))
        assert "url.scheme" not in attributes
        assert "network.protocol.version" not in attributes

    def test_the_boundaries_are_the_ones_the_dashboard_was_drawn_against(
        self, readings: list[Metric]
    ) -> None:
        """`histogram_quantile` interpolates inside a bucket, so a boundary
        that moves moves the p99 on a dashboard this repository ships."""
        point = histogram_points(one(readings, INSTRUMENT_REQUEST_DURATION))[0]
        assert tuple(point.explicit_bounds) == RED_DURATION_BUCKETS

    def test_the_boundaries_are_sorted_and_positive(self) -> None:
        """A Prometheus histogram with an unsorted `le` is not a histogram."""
        assert list(RED_DURATION_BUCKETS) == sorted(RED_DURATION_BUCKETS)
        assert len(set(RED_DURATION_BUCKETS)) == len(RED_DURATION_BUCKETS)
        assert all(boundary > 0 for boundary in RED_DURATION_BUCKETS)

    def test_the_route_is_the_template_rather_than_the_path(
        self, readings: list[Metric]
    ) -> None:
        """Two requests to two ids are one series, not two.

        This is the difference between a bounded label and an unbounded one,
        and it is the single most expensive thing to get wrong in a Prometheus
        setup — `/items/1` and `/items/2` as separate series means the series
        count grows with traffic rather than with the codebase.
        """
        recorded = histogram_points(one(readings, INSTRUMENT_REQUEST_DURATION))
        assert len(recorded) == 1
        assert (recorded[0].attributes or {})["http.route"] == "/items/{item_id}"
        assert recorded[0].count == 2


class TestActiveRequests:
    def test_it_is_reduced_to_the_method(self, readings: list[Metric]) -> None:
        """A gauge per route answers no question the histogram cannot."""
        attributes = attributes_of(one(readings, INSTRUMENT_ACTIVE_REQUESTS))
        assert attributes == {"http.request.method"}


class TestThePrometheusSpellings:
    def test_the_label_names_are_the_attribute_names_with_dots_replaced(
        self,
    ) -> None:
        """Written down because the dashboard queries them by that spelling."""
        assert PROMETHEUS_DURATION_LABELS == {
            "http_request_method",
            "http_route",
            "http_response_status_code",
            "error_type",
        }


class TestTheProviderCarriesTheViews:
    """`build_meter_provider` is where the views are attached, and a view that
    is written but never attached is invisible in exactly the way this whole
    module exists to catch. Asserted through the scrape path, since that is the
    one the dashboard reads."""

    def test_the_views_reach_the_prometheus_reader(self) -> None:
        settings = a_settings()
        registry = build_scrape_registry()
        provider = build_meter_provider(
            settings, build_resource(settings), scrape_registry=registry
        )
        try:
            histogram = provider.get_meter("test").create_histogram(
                INSTRUMENT_REQUEST_DURATION, unit="s"
            )
            histogram.record(0.3, {"http.route": "/x", "url.scheme": "http"})
            body = generate_latest(registry).decode()
        finally:
            provider.shutdown()

        assert 'http_route="/x"' in body
        assert "url_scheme" not in body
        # The boundaries reach the exposition as `le` labels, which is the form
        # `histogram_quantile` actually reads.
        for boundary in RED_DURATION_BUCKETS:
            assert f'le="{boundary}"' in body

    def test_a_scrape_only_provider_has_exactly_one_reader(self) -> None:
        """`OTEL_EXPORTER=none` plus a registry: collected, pulled, not pushed."""
        settings = a_settings(OTEL_EXPORTER="none")
        provider = build_meter_provider(
            settings, build_resource(settings), scrape_registry=build_scrape_registry()
        )
        try:
            assert len(provider._metric_readers) == 1
        finally:
            provider.shutdown()

    def test_without_a_registry_nothing_is_scrapable(self) -> None:
        settings = a_settings(OTEL_EXPORTER="none")
        provider = build_meter_provider(settings, build_resource(settings))
        try:
            assert len(provider._metric_readers) == 0
        finally:
            provider.shutdown()
