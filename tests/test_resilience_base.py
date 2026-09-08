"""The classification rules, which every other decision in the package uses."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime

import httpx
import pytest

from src.resilience.base import (
    DEFAULT_IDEMPOTENCY_KEY_HEADERS,
    CircuitBreakerConfig,
    CircuitOpenError,
    RetryPolicy,
    is_failure_status,
    is_pool_exhaustion,
    is_replayable,
    may_repeat,
    origin_of,
    request_was_sent,
    retry_after_seconds,
)

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)


def at_now() -> float:
    return NOW.timestamp()


def test_origin_is_scheme_host_and_port() -> None:
    assert (
        origin_of(httpx.Request("GET", "https://api.test/v1/charges"))
        == "https://api.test"
    )
    assert (
        origin_of(httpx.Request("GET", "https://api.test:8443/v1/charges"))
        == "https://api.test:8443"
    )


def test_origin_ignores_the_path_so_one_server_gets_one_breaker() -> None:
    a = origin_of(httpx.Request("GET", "https://api.test/one"))
    b = origin_of(httpx.Request("POST", "https://api.test/two?x=1"))
    assert a == b


def test_origin_separates_schemes_and_hosts() -> None:
    assert origin_of(httpx.Request("GET", "http://api.test/")) != origin_of(
        httpx.Request("GET", "https://api.test/")
    )
    assert origin_of(httpx.Request("GET", "https://a.test/")) != origin_of(
        httpx.Request("GET", "https://b.test/")
    )


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504, 599])
def test_server_side_statuses_are_failures(status: int) -> None:
    assert is_failure_status(status) is True


@pytest.mark.parametrize("status", [200, 201, 301, 400, 401, 404, 409, 422])
def test_client_side_statuses_are_not_failures(status: int) -> None:
    """A wave of 404s is this application's bug, not the dependency's outage."""
    assert is_failure_status(status) is False


@pytest.mark.parametrize("status", [501, 505])
def test_permanent_server_statuses_are_not_failures(status: int) -> None:
    """They answer identically forever: not retryable, so not evidence either."""
    assert is_failure_status(status) is False


def test_pool_exhaustion_is_recognised_and_nothing_else_is() -> None:
    assert is_pool_exhaustion(httpx.PoolTimeout("full")) is True
    assert is_pool_exhaustion(httpx.ConnectTimeout("slow")) is False
    assert is_pool_exhaustion(httpx.ReadTimeout("slow")) is False


@pytest.mark.parametrize(
    "exc", [httpx.ConnectError("refused"), httpx.ConnectTimeout("slow")]
)
def test_connect_failures_prove_the_request_was_never_sent(
    exc: httpx.TransportError,
) -> None:
    assert request_was_sent(exc) is False


@pytest.mark.parametrize(
    "exc",
    [
        httpx.ReadTimeout("slow"),
        httpx.WriteTimeout("slow"),
        httpx.ReadError("reset"),
        httpx.RemoteProtocolError("truncated"),
    ],
)
def test_post_connect_failures_leave_the_outcome_unknown(
    exc: httpx.TransportError,
) -> None:
    assert request_was_sent(exc) is True


@pytest.mark.parametrize("method", ["GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE"])
def test_idempotent_methods_may_repeat(method: str) -> None:
    request = httpx.Request(method, "https://api.test/x")
    assert may_repeat(request, idempotency_key_headers=DEFAULT_IDEMPOTENCY_KEY_HEADERS)


@pytest.mark.parametrize("method", ["POST", "PATCH"])
def test_non_idempotent_methods_may_not(method: str) -> None:
    request = httpx.Request(method, "https://api.test/x")
    assert not may_repeat(
        request, idempotency_key_headers=DEFAULT_IDEMPOTENCY_KEY_HEADERS
    )


@pytest.mark.parametrize("header", ["Idempotency-Key", "PayPal-Request-Id"])
def test_an_idempotency_key_makes_a_post_repeatable(header: str) -> None:
    request = httpx.Request("POST", "https://api.test/x", headers={header: "ref-1"})
    assert may_repeat(request, idempotency_key_headers=DEFAULT_IDEMPOTENCY_KEY_HEADERS)


def test_the_idempotency_header_match_is_case_insensitive() -> None:
    """`httpx.Headers` is, and the far end's casing is not ours to predict."""
    request = httpx.Request(
        "POST", "https://api.test/x", headers={"IDEMPOTENCY-KEY": "ref-1"}
    )
    assert may_repeat(request, idempotency_key_headers=DEFAULT_IDEMPOTENCY_KEY_HEADERS)


def test_an_unlisted_header_does_not_count() -> None:
    request = httpx.Request(
        "POST", "https://api.test/x", headers={"X-Request-Id": "ref-1"}
    )
    assert not may_repeat(
        request, idempotency_key_headers=DEFAULT_IDEMPOTENCY_KEY_HEADERS
    )


@pytest.mark.parametrize(
    "request_",
    [
        httpx.Request("GET", "https://api.test/x"),
        httpx.Request("POST", "https://api.test/x", json={"a": 1}),
        httpx.Request("POST", "https://api.test/x", content=b"raw"),
        httpx.Request("POST", "https://api.test/x", data={"a": "1"}),
        httpx.Request("POST", "https://api.test/x", files={"f": ("n.txt", b"abc")}),
    ],
    ids=["empty", "json", "bytes", "form", "multipart"],
)
def test_buffered_bodies_are_replayable(request_: httpx.Request) -> None:
    assert is_replayable(request_) is True


def test_a_streamed_body_is_not_replayable() -> None:
    """The second attempt would send zero bytes, successfully. See `base.py`."""

    async def body() -> AsyncIterator[bytes]:
        yield b"chunk"  # pragma: no cover - never iterated

    request = httpx.Request("POST", "https://api.test/x", content=body())
    assert is_replayable(request) is False


def test_a_multipart_body_really_does_replay_identically() -> None:
    """`is_replayable` is only worth anything if the claim holds. It is checked
    here rather than assumed, because a stream that re-renders empty is the
    exact failure the predicate exists to prevent and it is silent."""
    request = httpx.Request(
        "POST", "https://api.test/x", files={"f": ("n.txt", b"abcdef")}
    )
    first = b"".join(request.stream)  # type: ignore[arg-type]
    second = b"".join(request.stream)  # type: ignore[arg-type]
    assert first == second
    assert b"abcdef" in first


def test_retry_after_absent_is_none() -> None:
    assert retry_after_seconds(httpx.Response(503), now=at_now) is None


def test_retry_after_in_seconds() -> None:
    response = httpx.Response(429, headers={"Retry-After": "5"})
    assert retry_after_seconds(response, now=at_now) == 5.0


def test_retry_after_with_surrounding_whitespace() -> None:
    response = httpx.Response(429, headers={"Retry-After": " 7 "})
    assert retry_after_seconds(response, now=at_now) == 7.0


def test_a_negative_retry_after_clamps_to_now() -> None:
    response = httpx.Response(429, headers={"Retry-After": "-3"})
    assert retry_after_seconds(response, now=at_now) == 0.0


def test_retry_after_as_an_http_date() -> None:
    when = format_datetime(NOW + timedelta(seconds=12), usegmt=True)
    response = httpx.Response(503, headers={"Retry-After": when})
    assert retry_after_seconds(response, now=at_now) == pytest.approx(12.0, abs=1.0)


def test_an_http_date_in_the_past_is_now_not_nothing() -> None:
    """0.0 and `None` are different answers: one is a header, one is silence."""
    when = format_datetime(NOW - timedelta(minutes=5), usegmt=True)
    response = httpx.Response(503, headers={"Retry-After": when})
    assert retry_after_seconds(response, now=at_now) == 0.0


@pytest.mark.parametrize("raw", ["soon", "", "12.5", "Mon, 99 Xxx 2026"])
def test_a_malformed_retry_after_falls_back_rather_than_raising(raw: str) -> None:
    response = httpx.Response(503, headers={"Retry-After": raw})
    assert retry_after_seconds(response, now=at_now) is None


def test_circuit_open_error_is_a_transport_error() -> None:
    """The whole reason existing `except httpx.TransportError` handlers keep
    covering an outage once a breaker is in front of them."""
    error = CircuitOpenError("https://api.test", 12.5)
    assert isinstance(error, httpx.TransportError)
    assert error.origin == "https://api.test"
    assert error.retry_after == 12.5
    assert "https://api.test" in str(error)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"failure_threshold": 0},
        {"window_size": 2, "failure_threshold": 5},
        {"reset_timeout": 0.0},
        {"success_threshold": 0},
        {"half_open_max_calls": 0},
    ],
)
def test_an_unusable_breaker_config_fails_at_construction(
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        CircuitBreakerConfig(**kwargs)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"attempts": 0},
        {"base_delay": -1.0},
        {"base_delay": 2.0, "max_delay": 1.0},
        {"max_retry_after": -1.0},
    ],
)
def test_an_unusable_retry_policy_fails_at_construction(
    kwargs: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)  # type: ignore[arg-type]


def test_the_default_policies_are_usable() -> None:
    assert RetryPolicy().attempts == 3
    assert CircuitBreakerConfig().failure_threshold == 5
