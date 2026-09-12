"""Turning two settings into the three OTLP endpoints and their headers.

Small enough to look unnecessary, and here because both halves have a rule that
is easy to get subtly wrong in each of the three signal modules separately.

**The endpoint is a base, and the signal path is appended to it.** The OTLP
specification gives `OTEL_EXPORTER_OTLP_ENDPOINT` that meaning for the HTTP
transport — `http://collector:4318` becomes `http://collector:4318/v1/traces`
— while the *signal-specific* variables (`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`)
are used verbatim. Passing a base URL where a full one is expected is the
commonest OTLP misconfiguration there is: the collector answers 404, the
exporter logs a failure per batch, and everything else about the process looks
healthy. This module builds the full URL once so that the exporters are always
handed the verbatim form.

**Headers are `key=value` pairs and one of them is usually a credential.** The
value may contain `=` — a base64 token often ends in one — so each pair splits
on the *first* separator only. Anything without one is dropped rather than
guessed at, because the alternative to ignoring a malformed pair is sending an
unparseable `Authorization` header and reading "401" instead of "you wrote it
wrong".
"""

from __future__ import annotations

from typing import Final

from src.immutable import FrozenDict

#: The OTLP/HTTP paths, per the specification. Not configurable: they are part
#: of the protocol rather than of a deployment — which is why the mapping is a
#: `FrozenDict` and not a `dict` behind a `Final`, per `src/immutable.py`.
SIGNAL_PATHS: Final[FrozenDict[str, str]] = FrozenDict(
    {
        "traces": "/v1/traces",
        "metrics": "/v1/metrics",
        "logs": "/v1/logs",
    }
)


#: Spans or log records per export request, before the queue bound is taken
#: into account. The SDK's own default, named here because the two have to be
#: compared.
DEFAULT_EXPORT_BATCH_SIZE: Final[int] = 512


def export_batch_size(max_queue_size: int) -> int:
    """The batch size to pair with a queue of `max_queue_size`.

    The SDK refuses a batch larger than the queue that feeds it — reasonably,
    since such a batch could never fill — by raising at construction, which in
    a deployment means a process that will not start because somebody asked it
    to buffer *fewer* spans. "Hold at most 64" is a coherent request, so it is
    honoured by shrinking the batch rather than refusing the configuration.
    """
    return min(DEFAULT_EXPORT_BATCH_SIZE, max_queue_size)


def signal_endpoint(base_endpoint: str, signal: str) -> str:
    """The full OTLP/HTTP URL for one signal.

    Args:
        base_endpoint: `OTEL_EXPORTER_OTLP_ENDPOINT`, with or without a
            trailing slash.
        signal: "traces", "metrics" or "logs".

    Raises:
        ValueError: For an unknown signal, or an empty endpoint. The second is
            a configuration error rather than a reason to fall back to the
            SDK's own default of localhost, which would quietly export to a
            collector that happens to be running next door.
    """
    try:
        path = SIGNAL_PATHS[signal]
    except KeyError:
        raise ValueError(
            f"Unknown OTLP signal {signal!r}; expected one of "
            f"{', '.join(sorted(SIGNAL_PATHS))}."
        ) from None
    stripped = base_endpoint.strip().rstrip("/")
    if not stripped:
        raise ValueError(
            "OTEL_EXPORTER_OTLP_ENDPOINT is empty; set it to the base URL of "
            "an OTLP/HTTP collector, e.g. http://localhost:4318."
        )
    return f"{stripped}{path}"


def parse_otlp_headers(raw: str) -> dict[str, str]:
    """`"api-key=abc,x-tenant=7"` to a header mapping.

    Whitespace around a name or a value is stripped, an empty name is dropped,
    and a later pair wins over an earlier one with the same name — the
    behaviour of a mapping, which is what the exporter takes.
    """
    headers: dict[str, str] = {}
    for pair in raw.split(","):
        if "=" not in pair:
            continue
        name, _, value = pair.partition("=")
        key = name.strip()
        if not key:
            continue
        headers[key] = value.strip()
    return headers


__all__ = [
    "DEFAULT_EXPORT_BATCH_SIZE",
    "SIGNAL_PATHS",
    "export_batch_size",
    "parse_otlp_headers",
    "signal_endpoint",
]
