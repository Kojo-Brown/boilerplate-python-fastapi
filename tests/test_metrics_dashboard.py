"""`dashboards/grafana/red.json`, checked against what this service exposes.

A dashboard in a repository is documentation that claims to be executable, and
the usual fate of one is to keep claiming it long after a rename made every
panel read "No data". Nothing about importing it into Grafana catches that:
Grafana will happily run a query for a metric that does not exist.

So every PromQL expression in the dashboard is parsed here, the metric names
and label names are pulled out of it, and both are checked against a *live*
exposition — a real app, really instrumented, really scraped through
`/metrics`. Not against the constants in `src/observability/red.py`: those are
a description of the contract and could be wrong in the same commit as the
dashboard. The exposition is the contract.

The parsing is deliberately crude. It is not a PromQL implementation, and it
does not need to be — it needs to find every identifier that is used where a
metric name goes, and over-report rather than under-report, since a false
positive here is a test failure somebody reads and a false negative is the bug
this file exists to prevent.
"""

import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.config import Settings
from src.observability.prometheus import build_metrics_router, metrics_exposition
from src.observability.setup import configure_observability, shutdown_observability

REPO_ROOT = Path(__file__).resolve().parent.parent
DASHBOARD = REPO_ROOT / "dashboards" / "grafana" / "red.json"

#: Suffixes the exposition format appends to a histogram's base name. A query
#: naming `..._bucket` is naming a metric whose `# TYPE` line says `...`.
HISTOGRAM_SUFFIXES = ("_bucket", "_count", "_sum")

#: PromQL identifiers that are not metric names. Functions, aggregation
#: operators, modifiers and the reserved bucket label.
PROMQL_KEYWORDS = frozenset(
    {
        "abs",
        "and",
        "avg",
        "bool",
        "by",
        "ceil",
        "clamp",
        "clamp_max",
        "clamp_min",
        "count",
        "count_values",
        "delta",
        "floor",
        "group_left",
        "group_right",
        "histogram_quantile",
        "id",
        "ignoring",
        "increase",
        "irate",
        "label_replace",
        "le",
        "max",
        "min",
        "offset",
        "on",
        "or",
        "quantile",
        "rate",
        "round",
        "sort",
        "sort_desc",
        "stddev",
        "sum",
        "topk",
        "unless",
        "vector",
        "without",
    }
)

#: Labels Prometheus attaches at scrape time from the scrape configuration, so
#: they are never in an exposition and a query may still use them.
TARGET_LABELS = frozenset({"job", "instance"})


def a_settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "DATABASE_URL": "postgresql+asyncpg://fake:fake@localhost/fake",
        "SECRET_KEY": "test-secret-key-not-a-real-one",
        "ENVIRONMENT": "test",
        "OTEL_ENABLED": True,
        "OTEL_EXPORTER": "none",
        "PROMETHEUS_ENABLED": True,
        "OTEL_EXCLUDED_URLS": "health,health/ready,metrics",
        "OTEL_INSTRUMENT_HTTPX": False,
        "OTEL_INSTRUMENT_SQLALCHEMY": False,
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


@pytest.fixture(scope="module")
def dashboard() -> dict[str, Any]:
    with DASHBOARD.open(encoding="utf-8") as handle:
        loaded: dict[str, Any] = json.load(handle)
    return loaded


@pytest.fixture(scope="module")
def exposition() -> Iterator[str]:
    """A real scrape of a real app that has served a success and a failure.

    Both are needed: `error.type` and the 5xx status only appear on a series
    that has recorded an error, and the dashboard's error panel queries them.
    """
    settings = a_settings()
    metrics_exposition.unbind()
    app = FastAPI()

    @app.get("/items/{item_id}")
    def read_item(item_id: int) -> dict[str, int]:
        return {"id": item_id}

    @app.get("/boom")
    def boom() -> dict[str, str]:
        raise RuntimeError("deliberate")

    app.include_router(build_metrics_router(settings))
    handle = configure_observability(settings, app=app, install_globally=False)
    try:
        client = TestClient(app, raise_server_exceptions=False)
        client.get("/items/1")
        client.get("/boom")
        client.get("/no-such-path")
        yield client.get("/metrics").text
    finally:
        shutdown_observability(handle, settings)
        metrics_exposition.unbind()


#: `label_values(<promql>, <label>)` — a Grafana datasource function, not
#: PromQL. Its second argument is a label name, which is the one place in the
#: file where a bare identifier means a label rather than a metric.
LABEL_VALUES = re.compile(
    r"^\s*label_values\(\s*(?P<query>.*?)\s*,\s*(?P<label>[a-zA-Z_]\w*)\s*\)\s*$",
    re.DOTALL,
)

#: An identifier, not preceded by a word character or a dot — so the `e` of
#: `1e-9` and the `m` of `5m` are not mistaken for metric names.
IDENTIFIER = re.compile(r"(?<![\w.])[a-zA-Z_][a-zA-Z0-9_]*")

#: Grafana's own variables: `$__rate_interval`, `$job`, `${datasource}`.
GRAFANA_VARIABLE = re.compile(r"\$\{?(?:__)?\w+\}?")

#: A grouping clause. Its contents are label names, never metric names.
GROUPING = re.compile(r"\b(?:by|without|on|ignoring)\s*\(([^)]*)\)")


def expressions(dashboard: dict[str, Any]) -> list[str]:
    """Every query string in the file: panel targets and variable queries."""
    found: list[str] = []
    for panel in dashboard["panels"]:
        for target in panel.get("targets", []):
            found.append(target["expr"])
    for variable in dashboard["templating"]["list"]:
        query = variable.get("query")
        if isinstance(query, str) and variable.get("type") == "query":
            found.append(query)
    return found


def as_promql(expression: str) -> tuple[str, set[str]]:
    """Split a dashboard query into PromQL and any label named outside it.

    Returns the expression with Grafana's variables removed, and the set of
    label names that the dashboard syntax — rather than the PromQL — refers to.
    """
    labels: set[str] = set()
    match = LABEL_VALUES.match(expression)
    if match:
        expression = match.group("query")
        labels.add(match.group("label"))
    return GRAFANA_VARIABLE.sub("", expression), labels


def metric_names_in(expression: str) -> set[str]:
    """Identifiers used where a metric name goes.

    Label matchers and grouping clauses are removed first, so a label name is
    never mistaken for a metric. What survives is identifiers, minus the PromQL
    vocabulary and minus anything immediately followed by `(`, which is a
    function call rather than a selector.
    """
    promql, _ = as_promql(expression)
    without_matchers = re.sub(r"\{[^{}]*\}", "", promql)
    without_groupings = GROUPING.sub("", without_matchers)
    names = set()
    for match in IDENTIFIER.finditer(without_groupings):
        name = match.group()
        if name in PROMQL_KEYWORDS:
            continue
        if without_groupings[match.end() : match.end() + 1] == "(":
            continue
        names.add(name)
    return names


def label_names_in(expression: str) -> set[str]:
    """Label names: matched in `{...}`, grouped in `by (...)`, or named by
    `label_values`."""
    promql, labels = as_promql(expression)
    for matcher in re.findall(r"\{([^{}]*)\}", promql):
        labels.update(re.findall(r"([a-zA-Z_]\w*)\s*(?:=~|!~|=|!=)", matcher))
    for grouping in GROUPING.findall(promql):
        labels.update(
            name for name in IDENTIFIER.findall(grouping) if name not in PROMQL_KEYWORDS
        )
    return labels


def exposed_metric_names(exposition: str) -> set[str]:
    return set(re.findall(r"^# TYPE (\S+)", exposition, re.MULTILINE))


def exposed_label_names(exposition: str) -> set[str]:
    labels = set()
    for matcher in re.findall(r"^\S+\{([^}]*)\}", exposition, re.MULTILINE):
        labels.update(re.findall(r"([a-zA-Z_][a-zA-Z0-9_]*)=", matcher))
    return labels


def base_name(metric: str) -> str:
    for suffix in HISTOGRAM_SUFFIXES:
        if metric.endswith(suffix):
            return metric[: -len(suffix)]
    return metric


class TestTheFileItself:
    def test_it_is_checked_in_and_is_valid_json(
        self, dashboard: dict[str, Any]
    ) -> None:
        assert dashboard["uid"]
        assert dashboard["title"]
        assert dashboard["panels"]

    def test_every_panel_has_a_title_and_a_stable_id(
        self, dashboard: dict[str, Any]
    ) -> None:
        ids = [panel["id"] for panel in dashboard["panels"]]
        assert all(panel["title"] for panel in dashboard["panels"])
        assert len(ids) == len(set(ids))

    def test_no_panel_pins_a_datasource_uid(self, dashboard: dict[str, Any]) -> None:
        """A hard-coded uid is the other way a checked-in dashboard breaks:
        it imports cleanly and queries a datasource that exists in one Grafana
        and not the next. Everything here goes through the `$datasource`
        variable, so importing it asks which Prometheus to use."""
        for panel in dashboard["panels"]:
            for target in panel.get("targets", []):
                assert target["datasource"]["uid"] == "${datasource}"

    def test_there_is_a_panel_for_each_letter_of_red(
        self, dashboard: dict[str, Any]
    ) -> None:
        titles = {panel["title"] for panel in dashboard["panels"]}
        assert {"Rate", "Errors", "Duration (p99)"} <= titles


class TestTheQueriesAgainstALiveScrape:
    def test_there_are_queries_to_check(self, dashboard: dict[str, Any]) -> None:
        """A guard on the parsing above: a regex that silently matched nothing
        would make every assertion below vacuously true."""
        assert len(expressions(dashboard)) >= 10

    def test_every_metric_queried_is_a_metric_this_service_exposes(
        self, dashboard: dict[str, Any], exposition: str
    ) -> None:
        exposed = exposed_metric_names(exposition)
        missing = {
            metric
            for expression in expressions(dashboard)
            for metric in metric_names_in(expression)
            if base_name(metric) not in exposed
        }
        assert not missing, (
            f"the dashboard queries {sorted(missing)}, which a scrape of this "
            f"service does not contain. Exposed: {sorted(exposed)}"
        )

    def test_every_label_queried_is_a_label_this_service_emits(
        self, dashboard: dict[str, Any], exposition: str
    ) -> None:
        exposed = exposed_label_names(exposition) | TARGET_LABELS
        missing = {
            label
            for expression in expressions(dashboard)
            for label in label_names_in(expression)
            if label not in exposed
        }
        assert not missing, (
            f"the dashboard groups or filters by {sorted(missing)}, which is "
            f"not a label on any series. Emitted: {sorted(exposed)}"
        )

    def test_the_error_panel_matches_the_status_codes_that_are_emitted(
        self, exposition: str
    ) -> None:
        """The error query filters `http_response_status_code=~"5.."`, which
        only works while the exporter renders the code as a bare number — an
        exporter that decided to render it as `500.0` would leave the panel at
        zero during an outage and nothing else here would notice."""
        assert 'http_response_status_code="500"' in exposition

    def test_the_route_label_is_the_one_the_variable_reads(
        self, dashboard: dict[str, Any], exposition: str
    ) -> None:
        route_variable = next(
            variable
            for variable in dashboard["templating"]["list"]
            if variable["name"] == "route"
        )
        assert "http_route" in route_variable["query"]
        assert 'http_route="/items/{item_id}"' in exposition
