"""The retry loop, and how it composes with the breaker.

Every test here drives a real `httpx.AsyncClient` or the transport directly
against `httpx.MockTransport`, so what is asserted is the number and shape of
the requests a dependency would actually receive. Sleeping is injected and
recorded rather than spent, which is what keeps a test of a five-second backoff
instant.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator, Callable

import httpx
import pytest

from src.resilience.base import CircuitBreakerConfig, CircuitOpenError, RetryPolicy
from src.resilience.circuit import CircuitBreakerRegistry
from src.resilience.factory import resilient_async_client, resilient_transport
from src.resilience.transport import ResilientTransport
from src.structured.deadline import deadline
from src.structured.errors import DeadlineExceeded

URL = "https://api.test/v1/things"


class RecordingSleeper:
    """Records what it was asked to wait without waiting."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


class Responder:
    """Replays a scripted sequence of responses and exceptions, in order.

    The last entry repeats once the script runs out, so a test that wants "and
    it keeps failing" does not have to count the attempts twice.
    """

    def __init__(self, *script: httpx.Response | BaseException) -> None:
        self.script: list[httpx.Response | BaseException] = list(script)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        index = min(len(self.requests) - 1, len(self.script) - 1)
        outcome = self.script[index]
        if isinstance(outcome, BaseException):
            raise outcome
        # A fresh Response each time: one handed back twice would come back
        # already closed on the second attempt.
        return httpx.Response(
            outcome.status_code, headers=outcome.headers, content=outcome.content
        )

    @property
    def calls(self) -> int:
        return len(self.requests)


def build(
    responder: Callable[[httpx.Request], httpx.Response],
    *,
    retry: RetryPolicy | None = None,
    sleep: RecordingSleeper | None = None,
    breakers: CircuitBreakerRegistry | None = None,
    rng: random.Random | None = None,
) -> ResilientTransport:
    """A transport wired to a mock, with a registry of its own by default."""
    return ResilientTransport(
        httpx.MockTransport(responder),
        retry=retry if retry is not None else RetryPolicy(jitter=False),
        breakers=breakers if breakers is not None else CircuitBreakerRegistry(),
        sleep=sleep if sleep is not None else RecordingSleeper(),
        rng=rng if rng is not None else random.Random(0),
    )


async def send(
    transport: ResilientTransport, method: str = "GET", **kwargs: object
) -> httpx.Response:
    async with httpx.AsyncClient(transport=transport) as client:
        return await client.request(method, URL, **kwargs)  # type: ignore[arg-type]


# ---- retrying a failed response -------------------------------------------


async def test_a_transient_failure_is_retried_and_the_success_returned() -> None:
    responder = Responder(httpx.Response(503), httpx.Response(200, text="ok"))
    response = await send(build(responder))

    assert response.status_code == 200
    assert response.text == "ok"
    assert responder.calls == 2


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
async def test_every_server_side_status_is_retried(status: int) -> None:
    responder = Responder(httpx.Response(status), httpx.Response(200))
    await send(build(responder))
    assert responder.calls == 2


@pytest.mark.parametrize("status", [200, 400, 404, 409, 422, 501])
async def test_nothing_else_is(status: int) -> None:
    responder = Responder(httpx.Response(status))
    response = await send(build(responder))

    assert response.status_code == status
    assert responder.calls == 1


async def test_the_last_failure_is_returned_once_the_attempts_run_out() -> None:
    """Returned, not raised: the caller asked for a response and a 503 is one."""
    responder = Responder(httpx.Response(503, text="down"))
    response = await send(build(responder, retry=RetryPolicy(attempts=3, jitter=False)))

    assert response.status_code == 503
    assert response.text == "down"
    assert responder.calls == 3


async def test_one_attempt_disables_retrying_entirely() -> None:
    responder = Responder(httpx.Response(503))
    await send(build(responder, retry=RetryPolicy(attempts=1)))
    assert responder.calls == 1


class _WatchedStream(httpx.AsyncByteStream):
    """A response body that reports being closed, as a real one would."""

    def __init__(self, closed: list[str], label: str) -> None:
        self._closed = closed
        self._label = label

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"body"

    async def aclose(self) -> None:
        self._closed.append(self._label)


class _StreamingTransport(httpx.AsyncBaseTransport):
    """Returns unread, streamed responses — `MockTransport` returns read ones."""

    def __init__(self, closed: list[str], *statuses: int) -> None:
        self._closed = closed
        self._statuses = list(statuses)
        self.calls = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        status = self._statuses[min(self.calls, len(self._statuses) - 1)]
        self.calls += 1
        return httpx.Response(
            status, stream=_WatchedStream(self._closed, f"{status}#{self.calls}")
        )


async def test_the_discarded_response_is_closed_before_the_backoff() -> None:
    """Otherwise the pool holds an unread response for the whole wait and the
    next attempt contends with its own predecessor."""
    closed: list[str] = []
    inner = _StreamingTransport(closed, 503, 200)
    transport = ResilientTransport(
        inner,
        retry=RetryPolicy(attempts=2, jitter=False),
        breakers=CircuitBreakerRegistry(),
        sleep=RecordingSleeper(),
    )

    async with httpx.AsyncClient(transport=transport) as client:
        response = await client.get(URL)

    assert response.status_code == 200
    # The 503 was closed by the retry loop; the 200 by the client reading it.
    assert closed[0] == "503#1"


async def test_a_response_that_is_returned_is_not_closed_by_the_loop() -> None:
    """Closing the one the caller gets back would hand them an empty body."""
    closed: list[str] = []
    inner = _StreamingTransport(closed, 503)
    transport = ResilientTransport(
        inner,
        retry=RetryPolicy(attempts=2, jitter=False),
        breakers=CircuitBreakerRegistry(),
        sleep=RecordingSleeper(),
    )

    async with httpx.AsyncClient(transport=transport) as client:
        response = await client.get(URL)

    assert response.status_code == 503
    assert response.text == "body"


# ---- retrying a transport error -------------------------------------------


async def test_a_transport_error_is_retried() -> None:
    responder = Responder(httpx.ConnectError("refused"), httpx.Response(200))
    response = await send(build(responder))

    assert response.status_code == 200
    assert responder.calls == 2


async def test_the_original_error_propagates_once_the_attempts_run_out() -> None:
    """No `RetryError` wrapper — `src/decorators/retry.py` explains why."""
    error = httpx.ReadTimeout("slow")
    responder = Responder(error)

    with pytest.raises(httpx.ReadTimeout) as excinfo:
        await send(build(responder))

    assert excinfo.value is error
    assert responder.calls == 3


async def test_pool_exhaustion_is_not_retried() -> None:
    """It is this process out of connections. Retrying queues another waiter
    on the pool that is already full."""
    responder = Responder(httpx.PoolTimeout("full"))

    with pytest.raises(httpx.PoolTimeout):
        await send(build(responder))

    assert responder.calls == 1


async def test_pool_exhaustion_does_not_count_against_the_dependency() -> None:
    breakers = CircuitBreakerRegistry(
        config=CircuitBreakerConfig(failure_threshold=1, window_size=1)
    )
    transport = build(Responder(httpx.PoolTimeout("full")), breakers=breakers)

    with pytest.raises(httpx.PoolTimeout):
        await send(transport)

    assert breakers.get("https://api.test").failures_in_window == 0


# ---- what may be repeated -------------------------------------------------


async def test_a_post_is_not_repeated_after_a_failure_status() -> None:
    """The far end answered, so it saw the request. Repeating it is a second
    charge, a second order, a second email."""
    responder = Responder(httpx.Response(503))
    await send(build(responder), method="POST", json={"a": 1})
    assert responder.calls == 1


async def test_a_post_with_an_idempotency_key_is_repeated() -> None:
    responder = Responder(httpx.Response(503), httpx.Response(200))
    await send(
        build(responder),
        method="POST",
        json={"a": 1},
        headers={"Idempotency-Key": "ref-1"},
    )
    assert responder.calls == 2


async def test_a_post_is_repeated_after_a_connect_failure() -> None:
    """No connection was established, so the request provably never arrived."""
    responder = Responder(httpx.ConnectError("refused"), httpx.Response(200))
    await send(build(responder), method="POST", json={"a": 1})
    assert responder.calls == 2


async def test_a_post_is_not_repeated_after_a_read_timeout() -> None:
    """Bytes were on the wire. Whether it took effect is unknowable here, and
    a read timeout is the failure most likely to mean 'slow', not 'absent'."""
    responder = Responder(httpx.ReadTimeout("slow"))

    with pytest.raises(httpx.ReadTimeout):
        await send(build(responder), method="POST", json={"a": 1})

    assert responder.calls == 1


async def test_a_streamed_body_is_never_repeated() -> None:
    """The second attempt would send an empty body, successfully."""

    async def body() -> AsyncIterator[bytes]:
        yield b"chunk"

    responder = Responder(httpx.Response(503))
    await send(build(responder), method="PUT", content=body())

    assert responder.calls == 1


async def test_a_buffered_body_is_sent_again_in_full() -> None:
    """The retry has to be a real repeat, not a repeat of the headers."""
    responder = Responder(httpx.Response(503), httpx.Response(200))
    await send(build(responder), method="PUT", json={"a": 1})

    assert responder.calls == 2
    assert [bytes(r.content) for r in responder.requests] == [b'{"a":1}'] * 2


# ---- the backoff schedule -------------------------------------------------


async def test_the_backoff_doubles() -> None:
    sleeper = RecordingSleeper()
    await send(
        build(
            Responder(httpx.Response(503)),
            retry=RetryPolicy(attempts=4, base_delay=0.5, jitter=False),
            sleep=sleeper,
        )
    )
    assert sleeper.delays == [0.5, 1.0, 2.0]


async def test_the_backoff_is_capped() -> None:
    sleeper = RecordingSleeper()
    await send(
        build(
            Responder(httpx.Response(503)),
            retry=RetryPolicy(attempts=4, base_delay=1.0, max_delay=1.5, jitter=False),
            sleep=sleeper,
        )
    )
    assert sleeper.delays == [1.0, 1.5, 1.5]


async def test_jitter_draws_from_below_the_ceiling() -> None:
    """Full jitter: the wait is uniform in `[0, ceiling]`, never the ceiling
    itself, so a dependency that disappointed every worker at once does not get
    them all back in step."""
    sleeper = RecordingSleeper()
    await send(
        build(
            Responder(httpx.Response(503)),
            retry=RetryPolicy(attempts=4, base_delay=1.0, jitter=True),
            sleep=sleeper,
            rng=random.Random(1234),
        )
    )

    assert len(sleeper.delays) == 3
    assert sleeper.delays[0] <= 1.0
    assert sleeper.delays[1] <= 2.0
    assert sleeper.delays[2] <= 4.0
    assert sleeper.delays != [1.0, 2.0, 4.0]


async def test_the_jitter_source_is_injectable_and_reproducible() -> None:
    first, second = RecordingSleeper(), RecordingSleeper()
    for sleeper in (first, second):
        await send(
            build(
                Responder(httpx.Response(503)),
                retry=RetryPolicy(attempts=3, base_delay=1.0, jitter=True),
                sleep=sleeper,
                rng=random.Random(7),
            )
        )
    assert first.delays == second.delays


# ---- Retry-After ----------------------------------------------------------


async def test_retry_after_wins_over_the_computed_backoff() -> None:
    sleeper = RecordingSleeper()
    await send(
        build(
            Responder(
                httpx.Response(429, headers={"Retry-After": "4"}), httpx.Response(200)
            ),
            retry=RetryPolicy(attempts=2, base_delay=0.1, jitter=False),
            sleep=sleeper,
        )
    )

    # The header is a floor the server asked for; the extra is the jitter that
    # keeps every rate-limited client from returning in the same millisecond.
    assert len(sleeper.delays) == 1
    assert 4.0 <= sleeper.delays[0] <= 4.1


async def test_a_retry_after_beyond_the_cap_ends_the_retrying() -> None:
    """A far end asking for an hour is not asking to be retried inside this
    request. Holding the socket open for it would be the worse answer."""
    responder = Responder(httpx.Response(503, headers={"Retry-After": "3600"}))
    response = await send(
        build(responder, retry=RetryPolicy(attempts=3, max_retry_after=30.0))
    )

    assert response.status_code == 503
    assert responder.calls == 1


async def test_retry_after_can_be_ignored_by_policy() -> None:
    sleeper = RecordingSleeper()
    await send(
        build(
            Responder(httpx.Response(503, headers={"Retry-After": "600"})),
            retry=RetryPolicy(
                attempts=2, base_delay=0.25, jitter=False, respect_retry_after=False
            ),
            sleep=sleeper,
        )
    )
    assert sleeper.delays == [0.25]


async def test_a_malformed_retry_after_falls_back_to_the_backoff() -> None:
    sleeper = RecordingSleeper()
    await send(
        build(
            Responder(httpx.Response(503, headers={"Retry-After": "soon"})),
            retry=RetryPolicy(attempts=2, base_delay=0.25, jitter=False),
            sleep=sleeper,
        )
    )
    assert sleeper.delays == [0.25]


# ---- the breaker, from the transport's side -------------------------------


async def test_enough_failures_open_the_circuit_and_the_next_call_fails_fast() -> None:
    breakers = CircuitBreakerRegistry(
        config=CircuitBreakerConfig(failure_threshold=2, window_size=4)
    )
    responder = Responder(httpx.Response(503))
    transport = build(
        responder, retry=RetryPolicy(attempts=2, jitter=False), breakers=breakers
    )

    await send(transport)
    assert responder.calls == 2

    with pytest.raises(CircuitOpenError):
        await send(transport)
    assert responder.calls == 2


async def test_a_circuit_that_opens_mid_retry_stops_the_loop() -> None:
    """The retry that is already running is exactly the traffic the breaker
    exists to stop, so it must not sleep and re-send underneath it."""
    breakers = CircuitBreakerRegistry(
        config=CircuitBreakerConfig(failure_threshold=2, window_size=4)
    )
    responder = Responder(httpx.Response(503))
    transport = build(
        responder, retry=RetryPolicy(attempts=5, jitter=False), breakers=breakers
    )

    with pytest.raises(CircuitOpenError):
        await send(transport)

    assert responder.calls == 2


async def test_a_client_error_never_trips_the_circuit() -> None:
    """A wave of 404s is this application asking for the wrong thing."""
    breakers = CircuitBreakerRegistry(
        config=CircuitBreakerConfig(failure_threshold=2, window_size=4)
    )
    transport = build(Responder(httpx.Response(404)), breakers=breakers)

    for _ in range(5):
        assert (await send(transport)).status_code == 404

    assert breakers.get("https://api.test").failures_in_window == 0


async def test_one_dependency_failing_does_not_block_another() -> None:
    breakers = CircuitBreakerRegistry(
        config=CircuitBreakerConfig(failure_threshold=1, window_size=2)
    )

    def responder(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503 if request.url.host == "down.test" else 200)

    transport = build(responder, retry=RetryPolicy(attempts=1), breakers=breakers)
    async with httpx.AsyncClient(transport=transport) as client:
        await client.get("https://down.test/x")
        with pytest.raises(CircuitOpenError):
            await client.get("https://down.test/x")
        assert (await client.get("https://up.test/x")).status_code == 200


async def test_the_registry_is_reachable_for_a_health_check() -> None:
    breakers = CircuitBreakerRegistry()
    transport = build(Responder(httpx.Response(200)), breakers=breakers)
    assert transport.breakers is breakers


# ---- cancellation ---------------------------------------------------------


async def test_cancellation_is_not_retried_and_is_not_a_failure() -> None:
    breakers = CircuitBreakerRegistry(
        config=CircuitBreakerConfig(failure_threshold=1, window_size=2)
    )
    responder = Responder(asyncio.CancelledError())
    transport = build(responder, breakers=breakers)

    with pytest.raises(asyncio.CancelledError):
        await send(transport)

    assert responder.calls == 1
    assert breakers.get("https://api.test").failures_in_window == 0


# ---- the enclosing deadline ------------------------------------------------


async def test_a_retry_that_would_not_fit_the_budget_is_not_started() -> None:
    """Sleeping into a `DeadlineExceeded` spends the caller's last 200ms
    producing an answer nobody is waiting for."""
    sleeper = RecordingSleeper()
    responder = Responder(httpx.Response(503))
    transport = build(
        responder,
        retry=RetryPolicy(attempts=3, base_delay=5.0, jitter=False),
        sleep=sleeper,
    )

    async with deadline(0.5, name="request"):
        response = await send(transport)

    assert response.status_code == 503
    assert responder.calls == 1
    assert sleeper.delays == []


async def test_a_retry_that_fits_the_budget_still_runs() -> None:
    responder = Responder(httpx.Response(503), httpx.Response(200))
    transport = build(
        responder, retry=RetryPolicy(attempts=2, base_delay=0.001, jitter=False)
    )

    async with deadline(30.0, name="request"):
        response = await send(transport)

    assert response.status_code == 200
    assert responder.calls == 2


async def test_no_enclosing_deadline_means_nothing_to_consult() -> None:
    responder = Responder(httpx.Response(503), httpx.Response(200))
    assert (await send(build(responder))).status_code == 200


def test_the_deadline_import_is_the_one_the_application_uses() -> None:
    """A regression guard: `DeadlineExceeded` living somewhere else would make
    the budget check above silently vacuous."""
    assert issubclass(DeadlineExceeded, Exception)


# ---- the factory -----------------------------------------------------------


async def test_the_factory_builds_a_client_that_retries() -> None:
    responder = Responder(httpx.Response(503), httpx.Response(200, json={"ok": True}))
    sleeper = RecordingSleeper()

    client = resilient_async_client(
        base_url="https://api.test",
        transport=httpx.MockTransport(responder),
        retry=RetryPolicy(attempts=2, jitter=False),
        breakers=CircuitBreakerRegistry(),
        sleep=sleeper,
    )
    async with client:
        response = await client.get("/v1/things")

    assert response.json() == {"ok": True}
    assert responder.calls == 2
    assert sleeper.delays == [0.1]


async def test_the_factory_passes_headers_and_base_url_through() -> None:
    responder = Responder(httpx.Response(200))
    client = resilient_async_client(
        base_url="https://api.test",
        headers={"X-Api-Version": "2026-01-01"},
        limits=httpx.Limits(max_connections=5),
        transport=httpx.MockTransport(responder),
        breakers=CircuitBreakerRegistry(),
    )
    async with client:
        await client.get("/v1/things")

    sent = responder.requests[0]
    assert str(sent.url) == URL
    assert sent.headers["X-Api-Version"] == "2026-01-01"


async def test_the_transport_factory_wraps_a_real_transport_by_default() -> None:
    transport = resilient_transport(breakers=CircuitBreakerRegistry())
    async with transport:
        pass


async def test_the_transport_closes_what_it_wraps() -> None:
    closed: list[bool] = []

    class Closing(httpx.AsyncBaseTransport):
        async def handle_async_request(
            self, request: httpx.Request
        ) -> httpx.Response:  # pragma: no cover
            return httpx.Response(200)

        async def aclose(self) -> None:
            closed.append(True)

    await ResilientTransport(Closing(), breakers=CircuitBreakerRegistry()).aclose()
    assert closed == [True]
