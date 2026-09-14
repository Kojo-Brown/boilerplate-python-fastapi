"""The scrape endpoint: a pull-based reader on the same `MeterProvider`.

Prometheus is a *reader*, not an exporter, and that distinction is the whole
shape of this module. `PeriodicExportingMetricReader` pushes to a collector on
a timer; `PrometheusMetricReader` collects on demand, when someone GETs
`/metrics`. Both hang off the one `MeterProvider`, so an instrument is recorded
once and read twice, and `OTEL_EXPORTER=none` with `PROMETHEUS_ENABLED=true` is
a complete, sensible deployment: metrics collected in this process and pulled
out of it, with no collector anywhere.

Three things this module is careful about.

**The registry is ours, not the global one.** `prometheus_client` has a
process-wide `REGISTRY` and every collector registered into it stays there for
the life of the process, so a second `MeterProvider` — a test's, a reload's —
raises `Duplicated timeseries`. Building a `CollectorRegistry` per
`configure_observability` makes the exposition an object with a lifetime rather
than a global, which is what lets the tests here exercise the real endpoint
against a real reader instead of a mock of one.

**The endpoint is bound late.** The registry does not exist until
`configure_observability` runs, and the router is built before that — the same
ordering problem `log_forwarder` has, solved the same way. Until something is
bound, `/metrics` answers 503 rather than 200-with-nothing: an empty exposition
is indistinguishable from a healthy service that has served no traffic, and
Prometheus would mark the target up and the alerts would go quiet. 503 marks it
down, which is the truth.

**Scraping is authenticated if you ask.** `PROMETHEUS_SCRAPE_TOKEN` is empty by
default because the usual deployment does not route `/metrics` off the cluster
at all. When it is set, the comparison is `secrets.compare_digest`: the naive
`==` on a secret leaks its prefix through timing, and this endpoint is by
design reachable by anything that can reach the service.

## One process, one registry — and what that means for workers

The registry lives in this process's memory, so N uvicorn workers behind one
port are N independent sets of counters and a scrape reaches one of them at
random. Run one worker per container and scale with replicas — Prometheus is
built to sum across targets — or, if you must fork, set `PROMETHEUS_MULTIPROC_DIR`
and use `prometheus_client`'s multiprocess collector, which this module does
not wire up because it changes the meaning of every gauge in the exposition.
"""

from __future__ import annotations

import secrets
from typing import Final

import structlog
from fastapi import APIRouter, HTTPException, Request, Response, status
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    generate_latest,
)
from prometheus_client.gc_collector import GCCollector
from prometheus_client.platform_collector import PlatformCollector
from prometheus_client.process_collector import ProcessCollector

from src.config import Settings

logger = structlog.get_logger(__name__)

#: Body returned when nothing is bound. A comment line is valid in the
#: exposition format, so a scraper that ignores the status code still gets
#: something legible rather than a parse error.
UNAVAILABLE_BODY: Final[str] = (
    "# metrics unavailable: no Prometheus reader is attached.\n"
    "# Set OTEL_ENABLED=true, OTEL_METRICS_ENABLED=true and "
    "PROMETHEUS_ENABLED=true.\n"
)


def build_scrape_registry() -> CollectorRegistry:
    """A fresh registry carrying the process and interpreter collectors.

    `process_*` (CPU, RSS, open file descriptors) and `python_gc_*` are the
    numbers that explain a RED dashboard when it goes wrong — latency that
    climbs with resident memory is a leak, latency that climbs with
    `process_cpu_seconds_total` already at its limit is saturation — and
    neither is something OpenTelemetry emits. They cost three collectors that
    read `/proc` at scrape time and nothing between scrapes.

    `ProcessCollector` degrades to exporting nothing where there is no `/proc`,
    so this is safe off Linux; it simply gets quieter.
    """
    registry = CollectorRegistry()
    ProcessCollector(registry=registry)
    PlatformCollector(registry=registry)
    GCCollector(registry=registry)
    return registry


def build_prometheus_reader(registry: CollectorRegistry) -> PrometheusMetricReader:
    """The pull reader, writing into `registry`.

    `scope_info_enabled=False` drops the `otel_scope_name` and
    `otel_scope_version` labels the exporter would otherwise add to every
    series. They identify which instrumentation library recorded a metric,
    which matters when two libraries emit the same instrument name and is
    otherwise a constant label on every series and an extra clause in every
    query. This service has one HTTP instrumentation, so they are noise, and
    the dashboard's PromQL is simpler without them.

    `target_info` is kept: it is one series carrying `service_name`,
    `service_version` and `service_instance_id` from the resource, and it is
    how a Grafana panel labels a replica without those becoming labels on
    every metric.
    """
    return PrometheusMetricReader(registry=registry, scope_info_enabled=False)


class MetricsExposition:
    """Holds the registry the scrape endpoint renders, or nothing.

    A bound object rather than a module global for the same reason as
    `log_forwarder`: the binding has to be undoable, so that
    `shutdown_observability` leaves no endpoint pointed at a reader whose
    provider has been shut down, and so that a test can configure one of its
    own and clean up after itself.
    """

    def __init__(self) -> None:
        self._registry: CollectorRegistry | None = None

    @property
    def bound(self) -> bool:
        return self._registry is not None

    def bind(self, registry: CollectorRegistry) -> None:
        self._registry = registry

    def unbind(self) -> None:
        self._registry = None

    def render(self) -> bytes | None:
        """The current exposition, or `None` when nothing is bound.

        Collection happens here, inside the request: `generate_latest` asks the
        reader for a fresh reading of every instrument. That is the correct
        behaviour for a pull model — the numbers are as of the scrape — and it
        is why the endpoint below runs off the event loop.
        """
        registry = self._registry
        if registry is None:
            return None
        return generate_latest(registry)


#: The process-wide exposition. `configure_observability` binds it.
metrics_exposition: Final[MetricsExposition] = MetricsExposition()


def _authorize(request: Request, token: str) -> None:
    """Refuse the scrape unless it presents `token` as a bearer credential."""
    if not token:
        return
    header = request.headers.get("Authorization", "")
    scheme, _, presented = header.partition(" ")
    if scheme.lower() != "bearer" or not secrets.compare_digest(presented, token):
        logger.warning("metrics.scrape_unauthorized", client=request.client)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing scrape credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


def build_metrics_router(settings: Settings) -> APIRouter:
    """The `/metrics` route, at the configured path.

    A factory rather than a module-level router because the path is a setting
    and a route's path is fixed when it is declared — and because a router
    built from an explicit `Settings` is one a test can build twice with
    different values.

    `include_in_schema=False`: the exposition format is not JSON and nothing
    generating a client from this API wants a `/metrics` operation in it.
    """

    router = APIRouter(tags=["metrics"])

    @router.get(
        settings.PROMETHEUS_METRICS_PATH,
        include_in_schema=False,
        response_class=Response,
    )
    def metrics(request: Request) -> Response:
        """Render the current exposition.

        Declared `def` rather than `async def` on purpose. Rendering walks
        every series in the registry and formats it, which is real CPU work
        proportional to the cardinality of this process; FastAPI runs a
        synchronous handler in a worker thread, so a scrape of a large registry
        does not stall the event loop the way an `async def` doing the same
        work would.
        """
        _authorize(request, settings.PROMETHEUS_SCRAPE_TOKEN)
        body = metrics_exposition.render()
        if body is None:
            return Response(
                content=UNAVAILABLE_BODY,
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                media_type="text/plain; charset=utf-8",
            )
        return Response(content=body, media_type=CONTENT_TYPE_LATEST)

    return router


__all__ = [
    "UNAVAILABLE_BODY",
    "MetricsExposition",
    "build_metrics_router",
    "build_prometheus_reader",
    "build_scrape_registry",
    "metrics_exposition",
]
