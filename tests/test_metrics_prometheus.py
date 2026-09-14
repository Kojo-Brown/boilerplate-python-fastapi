"""The scrape endpoint: what it serves, what it refuses, and when it is 503.

A throwaway app rather than `src.main.app`, for the reason given in
`test_observability_http.py`: instrumenting an app attaches middleware for the
life of the process and the rest of the suite shares that one.

Every test here drives the real `configure_observability` with
`install_globally=False` — the providers are built and wired exactly as
production builds them, they are simply not registered as the process-wide
ones. What is asserted is therefore the wiring itself rather than a paraphrase
of it, and the teardown matters as much as the assertion: `metrics_exposition`
is process-wide state, so a test that binds and does not unbind leaves every
later test scraping a reader whose provider has been shut down.
"""

from collections.abc import Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from prometheus_client import CONTENT_TYPE_LATEST

from src.config import Settings
from src.observability.prometheus import (
    UNAVAILABLE_BODY,
    MetricsExposition,
    build_metrics_router,
    build_scrape_registry,
    metrics_exposition,
)
from src.observability.setup import (
    Observability,
    configure_observability,
    shutdown_observability,
)

#: Obviously fake, and only ever compared against itself.
SCRAPE_TOKEN = "test-scrape-token-not-a-real-one"


def a_settings(**overrides: object) -> Settings:
    """Settings with telemetry on, nothing pushed, and scraping available.

    `OTEL_EXPORTER="none"` with `PROMETHEUS_ENABLED=True` is the scrape-only
    deployment, which is also the one a test can run without a collector.
    """
    base: dict[str, object] = {
        "DATABASE_URL": "postgresql+asyncpg://fake:fake@localhost/fake",
        "SECRET_KEY": "test-secret-key-not-a-real-one",
        "ENVIRONMENT": "test",
        "OTEL_ENABLED": True,
        "OTEL_EXPORTER": "none",
        "PROMETHEUS_ENABLED": True,
        # Stated rather than inherited. `Settings` reads `.env` when one
        # exists, so a test that relied on the shipped default would pass or
        # fail depending on whether the developer running it had copied
        # `.env.example` — which is exactly what CI does. The default itself is
        # asserted separately, off the field rather than off an instance.
        "OTEL_EXCLUDED_URLS": "health,health/ready,metrics",
        # Both are process-wide monkey-patches rather than per-object, and
        # neither is what these tests are about.
        "OTEL_INSTRUMENT_HTTPX": False,
        "OTEL_INSTRUMENT_SQLALCHEMY": False,
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


def an_app(settings: Settings) -> FastAPI:
    """A tiny app with one good route, one failing route and `/metrics`."""
    app = FastAPI()

    @app.get("/items/{item_id}")
    def read_item(item_id: int) -> dict[str, int]:
        return {"id": item_id}

    @app.get("/boom")
    def boom() -> dict[str, str]:
        raise RuntimeError("deliberate")

    app.include_router(build_metrics_router(settings))
    return app


@pytest.fixture
def unbound() -> Iterator[None]:
    """Guarantee the process-wide exposition is unbound before and after."""
    metrics_exposition.unbind()
    yield
    metrics_exposition.unbind()


@pytest.fixture
def configured(unbound: None) -> Iterator[tuple[TestClient, Observability]]:
    """A configured app and its handle, unwound afterwards."""
    settings = a_settings()
    app = an_app(settings)
    handle = configure_observability(settings, app=app, install_globally=False)
    client = TestClient(app, raise_server_exceptions=False)
    yield client, handle
    shutdown_observability(handle, settings)


class TestScrapeRegistry:
    def test_it_carries_the_process_and_interpreter_collectors(self) -> None:
        registry = build_scrape_registry()
        names = {
            sample.name for metric in registry.collect() for sample in metric.samples
        }
        assert "process_start_time_seconds" in names
        assert "python_info" in names

    def test_each_call_is_a_separate_registry(self) -> None:
        """The whole reason not to use `prometheus_client.REGISTRY`.

        Registering the same collector into the global registry twice raises
        `Duplicated timeseries`, which would make a second `MeterProvider` — a
        reload's, or the next test's — an error rather than an isolated object.
        """
        first, second = build_scrape_registry(), build_scrape_registry()
        assert first is not second


class TestExposition:
    def test_it_renders_nothing_until_something_is_bound(self) -> None:
        assert MetricsExposition().render() is None

    def test_binding_and_unbinding_are_reversible(self) -> None:
        exposition = MetricsExposition()
        exposition.bind(build_scrape_registry())
        assert exposition.bound
        assert exposition.render() is not None
        exposition.unbind()
        assert not exposition.bound
        assert exposition.render() is None


class TestTheEndpointWithNothingBound:
    def test_it_reports_the_target_down_rather_than_empty(self, unbound: None) -> None:
        """503, not 200 with an empty body.

        An empty exposition is indistinguishable from a healthy service that
        has served no traffic, so Prometheus would mark the target up and every
        alert built on these series would go quiet instead of firing.
        """
        client = TestClient(an_app(a_settings()))
        response = client.get("/metrics")
        assert response.status_code == 503

    def test_the_body_names_the_settings_that_would_turn_it_on(
        self, unbound: None
    ) -> None:
        client = TestClient(an_app(a_settings()))
        body = client.get("/metrics").text
        assert body == UNAVAILABLE_BODY
        assert "PROMETHEUS_ENABLED" in body
        # A `#` line is a comment in the exposition format, so a scraper that
        # ignores the status code still parses this rather than erroring.
        assert all(line.startswith("#") for line in body.splitlines())


class TestTheEndpointOnceConfigured:
    def test_it_serves_the_prometheus_exposition_format(
        self, configured: tuple[TestClient, Observability]
    ) -> None:
        client, _ = configured
        response = client.get("/metrics")
        assert response.status_code == 200
        assert response.headers["content-type"] == CONTENT_TYPE_LATEST

    def test_a_served_request_appears_as_rate_errors_and_duration(
        self, configured: tuple[TestClient, Observability]
    ) -> None:
        """One request, and all three of RED readable from one histogram."""
        client, _ = configured
        client.get("/items/7")
        body = client.get("/metrics").text

        assert "http_server_request_duration_seconds_count{" in body
        assert 'http_route="/items/{item_id}"' in body
        assert 'http_response_status_code="200"' in body
        assert "http_server_request_duration_seconds_bucket" in body
        assert "http_server_request_duration_seconds_sum" in body

    def test_a_failing_request_is_recorded_as_a_5xx(
        self, configured: tuple[TestClient, Observability]
    ) -> None:
        client, _ = configured
        client.get("/boom")
        body = client.get("/metrics").text
        assert 'http_response_status_code="500"' in body
        assert 'http_route="/boom"' in body

    def test_an_unmatched_path_collapses_into_one_series(
        self, configured: tuple[TestClient, Observability]
    ) -> None:
        """Otherwise a URL scanner writes a new series per URL it tries."""
        client, _ = configured
        client.get("/no-such-path")
        client.get("/no-such-path-either")
        body = client.get("/metrics").text
        counts = [
            line
            for line in body.splitlines()
            if line.startswith("http_server_request_duration_seconds_count")
            and 'http_response_status_code="404"' in line
        ]
        assert len(counts) == 1
        assert counts[0].endswith(" 2.0")

    def test_the_scrape_endpoint_does_not_measure_itself(
        self, configured: tuple[TestClient, Observability]
    ) -> None:
        """`OTEL_EXCLUDED_URLS` covers it, and at 4/min it would dominate."""
        client, _ = configured
        client.get("/metrics")
        body = client.get("/metrics").text
        assert 'http_route="/metrics"' not in body

    def test_the_resource_arrives_as_target_info_rather_than_as_labels(
        self, configured: tuple[TestClient, Observability]
    ) -> None:
        """One series carrying the identity, rather than it on every series.

        `target_info` is also only emitted once there is an OpenTelemetry
        metric to emit it alongside, which is why a request comes first: a
        scrape of a process that has served nothing carries the process
        collectors and nothing else.
        """
        client, _ = configured
        client.get("/items/1")
        body = client.get("/metrics").text

        assert "target_info{" in body
        assert 'service_name="boilerplate-python-fastapi"' in body
        duration_series = [
            line
            for line in body.splitlines()
            if line.startswith("http_server_request_duration_seconds_count")
        ]
        assert duration_series
        assert all("service_name" not in line for line in duration_series)

    def test_the_scope_labels_are_left_off(
        self, configured: tuple[TestClient, Observability]
    ) -> None:
        """They are constant here, and a constant label is a longer query."""
        client, _ = configured
        client.get("/items/1")
        body = client.get("/metrics").text
        assert "otel_scope_name" not in body

    def test_shutting_down_takes_the_endpoint_with_it(
        self, configured: tuple[TestClient, Observability]
    ) -> None:
        client, handle = configured
        assert client.get("/metrics").status_code == 200
        shutdown_observability(handle, a_settings())
        assert client.get("/metrics").status_code == 503


class TestWhatTurnsItOff:
    def test_prometheus_disabled_builds_no_registry(self, unbound: None) -> None:
        settings = a_settings(PROMETHEUS_ENABLED=False)
        handle = configure_observability(settings, install_globally=False)
        try:
            assert handle.scrape_registry is None
            assert not metrics_exposition.bound
        finally:
            shutdown_observability(handle, settings)

    def test_metrics_disabled_builds_no_registry_either(self, unbound: None) -> None:
        """There is no `MeterProvider` to read from, so there is nothing to serve."""
        settings = a_settings(OTEL_METRICS_ENABLED=False)
        handle = configure_observability(settings, install_globally=False)
        try:
            assert handle.meter_provider is None
            assert handle.scrape_registry is None
        finally:
            shutdown_observability(handle, settings)

    def test_telemetry_off_entirely_leaves_the_endpoint_at_503(
        self, unbound: None
    ) -> None:
        settings = a_settings(OTEL_ENABLED=False)
        app = an_app(settings)
        handle = configure_observability(settings, app=app, install_globally=False)
        try:
            assert handle.scrape_registry is None
            assert TestClient(app).get("/metrics").status_code == 503
        finally:
            shutdown_observability(handle, settings)


class TestTheScrapeToken:
    @pytest.fixture
    def guarded(self, unbound: None) -> Iterator[TestClient]:
        settings = a_settings(PROMETHEUS_SCRAPE_TOKEN=SCRAPE_TOKEN)
        app = an_app(settings)
        handle = configure_observability(settings, app=app, install_globally=False)
        yield TestClient(app)
        shutdown_observability(handle, settings)

    def test_the_right_token_is_let_through(self, guarded: TestClient) -> None:
        response = guarded.get(
            "/metrics", headers={"Authorization": f"Bearer {SCRAPE_TOKEN}"}
        )
        assert response.status_code == 200

    def test_no_credentials_are_refused(self, guarded: TestClient) -> None:
        response = guarded.get("/metrics")
        assert response.status_code == 401
        assert response.headers["www-authenticate"] == "Bearer"

    @pytest.mark.parametrize(
        "header",
        [
            f"Bearer {SCRAPE_TOKEN}x",
            f"Bearer {SCRAPE_TOKEN[:-1]}",
            # A prefix of the real token: the case a naive `==` would leak the
            # length of through timing, and `compare_digest` does not.
            f"Bearer {SCRAPE_TOKEN[:10]}",
            f"Basic {SCRAPE_TOKEN}",
            SCRAPE_TOKEN,
            "Bearer",
            "",
        ],
    )
    def test_anything_else_is_refused(self, guarded: TestClient, header: str) -> None:
        response = guarded.get("/metrics", headers={"Authorization": header})
        assert response.status_code == 401

    def test_the_scheme_is_matched_case_insensitively(
        self, guarded: TestClient
    ) -> None:
        """RFC 9110 says the scheme is case-insensitive, and clients vary."""
        response = guarded.get(
            "/metrics", headers={"Authorization": f"bearer {SCRAPE_TOKEN}"}
        )
        assert response.status_code == 200

    def test_an_empty_token_setting_means_no_guard(self, unbound: None) -> None:
        settings = a_settings(PROMETHEUS_SCRAPE_TOKEN="")
        app = an_app(settings)
        handle = configure_observability(settings, app=app, install_globally=False)
        try:
            assert TestClient(app).get("/metrics").status_code == 200
        finally:
            shutdown_observability(handle, settings)


class TestTheConfiguredPath:
    def test_the_route_is_served_where_the_setting_says(self, unbound: None) -> None:
        settings = a_settings(PROMETHEUS_METRICS_PATH="/internal/metrics")
        app = an_app(settings)
        handle = configure_observability(settings, app=app, install_globally=False)
        try:
            client = TestClient(app)
            assert client.get("/internal/metrics").status_code == 200
            assert client.get("/metrics").status_code == 404
        finally:
            shutdown_observability(handle, settings)

    def test_it_is_kept_out_of_the_openapi_document(self, unbound: None) -> None:
        """Nothing generating a client from this API wants a text/plain route."""
        settings = a_settings()
        app = an_app(settings)
        assert "/metrics" not in app.openapi()["paths"]

    def test_the_shipped_default_excludes_the_scrape_path_from_telemetry(
        self,
    ) -> None:
        """Read off the field, not an instance: `.env` would override it.

        The exclusion is what keeps a 15-second scrape from being the
        highest-rate route on the dashboard, and it is only correct if the
        default path and the default exclusion agree — so both are asserted
        against each other rather than against a literal.
        """
        fields = Settings.model_fields
        default_path = fields["PROMETHEUS_METRICS_PATH"].default
        excluded = str(fields["OTEL_EXCLUDED_URLS"].default).split(",")
        assert default_path.lstrip("/") in excluded
