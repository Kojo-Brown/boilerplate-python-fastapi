"""RED — rate, errors, duration — and the contract a checked-in dashboard needs.

The three numbers that describe a request-driven service are how many requests
arrive, how many of them fail, and how long they take. All three are already
being recorded: `opentelemetry-instrumentation-fastapi` emits
`http.server.request.duration` as a histogram per request, and a histogram
carries its own count, so rate is `_count`, errors is `_count` filtered on the
status attribute, and duration is the buckets. There is deliberately no
hand-rolled middleware here recording a second histogram of the same thing —
two instruments measuring one request is twice the series and two numbers that
disagree at the edges.

What this module does instead is turn that instrumentation's output into
something a dashboard can be *checked in* against, which takes two decisions
the instrumentation does not make for you.

**Which attributes survive.** The ASGI instrumentation tags the duration
histogram with `url.scheme` and `network.protocol.version` as well as the three
that matter. Neither ever varies within a deployment, and in Prometheus a label
that never varies is not free — it is carried on every one of the ~17 series a
histogram costs per route/status pair, forever. `red_views` drops them with an
attribute allowlist, which is the SDK's own mechanism for this and applies
before aggregation, so the series is never created rather than created and
ignored.

**Where the bucket boundaries come from.** `histogram_quantile` over a
Prometheus histogram interpolates *within* a bucket, so the boundaries are not
a display detail: they decide how wrong a p99 can be, and a dashboard's numbers
change under it if they change. They currently match what the semantic
conventions recommend, so pinning them in `RED_DURATION_BUCKETS` changes
nothing today — and that is the point. It means a contrib upgrade that revises
the default cannot silently move the p99 on a dashboard this repository ships
and claims is correct.

The names in `PROMETHEUS_*` below are what those instruments are called after
the Prometheus exporter's translation (dots to underscores, unit appended,
`_bucket`/`_count`/`_sum` suffixes for a histogram). They are written down
because `dashboards/grafana/red.json` queries them and
`tests/test_metrics_dashboard.py` asserts the two still agree — a dashboard
nobody checks is a dashboard that is wrong by the second refactor.
"""

from __future__ import annotations

from typing import Final

from opentelemetry.sdk.metrics.view import (
    ExplicitBucketHistogramAggregation,
    View,
)

#: Latency histogram boundaries, in seconds. See the module docstring for why
#: these are stated here rather than left to the instrumentation's default.
RED_DURATION_BUCKETS: Final[tuple[float, ...]] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.25,
    0.5,
    0.75,
    1.0,
    2.5,
    5.0,
    7.5,
    10.0,
)

#: The OpenTelemetry instrument names the views below apply to.
INSTRUMENT_REQUEST_DURATION: Final[str] = "http.server.request.duration"
INSTRUMENT_ACTIVE_REQUESTS: Final[str] = "http.server.active_requests"
INSTRUMENT_RESPONSE_BODY_SIZE: Final[str] = "http.server.response.body.size"

#: The attributes kept on the duration histogram, and so the labels a query may
#: group by. `error.type` is here rather than folded into the status code
#: because a request whose connection died before a response was started has an
#: error and no status at all, and that case is the one worth seeing.
RED_DURATION_ATTRIBUTES: Final[frozenset[str]] = frozenset(
    {
        "http.request.method",
        "http.route",
        "http.response.status_code",
        "error.type",
    }
)

#: Active requests is a gauge, so it is one series per label set at any instant
#: and the route would make it one per endpoint. Method alone answers the
#: question it is for — is the server saturated — at a hundredth of the cost.
RED_ACTIVE_REQUEST_ATTRIBUTES: Final[frozenset[str]] = frozenset(
    {"http.request.method"}
)

#: Prometheus spellings, for the dashboard and the test that checks it.
PROMETHEUS_REQUEST_DURATION: Final[str] = "http_server_request_duration_seconds"
PROMETHEUS_ACTIVE_REQUESTS: Final[str] = "http_server_active_requests"
PROMETHEUS_RESPONSE_BODY_SIZE: Final[str] = "http_server_response_body_size_bytes"
PROMETHEUS_TARGET_INFO: Final[str] = "target_info"

#: Every metric name a scrape of this service can contain, base names only —
#: `_bucket`, `_count` and `_sum` are appended by the exposition format and are
#: matched as suffixes by the dashboard test.
PROMETHEUS_METRIC_NAMES: Final[frozenset[str]] = frozenset(
    {
        PROMETHEUS_REQUEST_DURATION,
        PROMETHEUS_ACTIVE_REQUESTS,
        PROMETHEUS_RESPONSE_BODY_SIZE,
        PROMETHEUS_TARGET_INFO,
    }
)

#: Prometheus label spellings of `RED_DURATION_ATTRIBUTES`. The exporter
#: replaces every character that is not `[a-zA-Z0-9_]` with an underscore.
PROMETHEUS_DURATION_LABELS: Final[frozenset[str]] = frozenset(
    attribute.replace(".", "_") for attribute in RED_DURATION_ATTRIBUTES
)


def red_views() -> tuple[View, ...]:
    """Views that pin the RED contract onto the HTTP server instruments.

    Views belong to the `MeterProvider` and therefore apply to *every* reader
    hanging off it, Prometheus and OTLP alike. That is the correct scope: an
    attribute worth dropping for cardinality is worth dropping on the push path
    too, and a dashboard built on the collector's copy of these series should
    read the same as one built on a scrape.

    Nothing is dropped wholesale. `http.server.response.body.size` is not part
    of RED and the dashboard does not plot it, but it is the only signal this
    service has for egress volume, so it keeps its attributes trimmed rather
    than being aggregated away — a view is provider-wide, and dropping it here
    would take it off the OTLP export as well.
    """
    return (
        View(
            instrument_name=INSTRUMENT_REQUEST_DURATION,
            attribute_keys=set(RED_DURATION_ATTRIBUTES),
            aggregation=ExplicitBucketHistogramAggregation(
                boundaries=RED_DURATION_BUCKETS
            ),
        ),
        View(
            instrument_name=INSTRUMENT_ACTIVE_REQUESTS,
            attribute_keys=set(RED_ACTIVE_REQUEST_ATTRIBUTES),
        ),
        View(
            instrument_name=INSTRUMENT_RESPONSE_BODY_SIZE,
            attribute_keys=set(RED_DURATION_ATTRIBUTES),
        ),
    )


__all__ = [
    "INSTRUMENT_ACTIVE_REQUESTS",
    "INSTRUMENT_REQUEST_DURATION",
    "INSTRUMENT_RESPONSE_BODY_SIZE",
    "PROMETHEUS_ACTIVE_REQUESTS",
    "PROMETHEUS_DURATION_LABELS",
    "PROMETHEUS_METRIC_NAMES",
    "PROMETHEUS_REQUEST_DURATION",
    "PROMETHEUS_RESPONSE_BODY_SIZE",
    "PROMETHEUS_TARGET_INFO",
    "RED_ACTIVE_REQUEST_ATTRIBUTES",
    "RED_DURATION_ATTRIBUTES",
    "RED_DURATION_BUCKETS",
    "red_views",
]
