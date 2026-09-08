"""Building an `httpx.AsyncClient` that already has the policy.

One function, because the mistake this package is trying to prevent is a call
site that reaches for `httpx.AsyncClient()` out of habit. Anything that talks to
a service this application does not control should call `resilient_async_client`
instead, and everything else about the client — base URL, headers, timeouts,
limits — stays exactly the httpx API it was.
"""

from __future__ import annotations

import asyncio
import random

import httpx

from src.decorators.base import DEFAULT_RNG, AsyncSleeper
from src.resilience.base import DEFAULT_WALL_CLOCK, RetryPolicy, WallClock
from src.resilience.circuit import CircuitBreakerRegistry
from src.resilience.transport import ResilientTransport

#: httpx's own default, restated so that passing `timeout=None` to the factory
#: means "no timeout" — the httpx meaning — instead of "use the default".
DEFAULT_TIMEOUT: float = 10.0


def resilient_transport(
    *,
    transport: httpx.AsyncBaseTransport | None = None,
    retry: RetryPolicy | None = None,
    breakers: CircuitBreakerRegistry | None = None,
    sleep: AsyncSleeper = asyncio.sleep,
    rng: random.Random = DEFAULT_RNG,
    wall_clock: WallClock = DEFAULT_WALL_CLOCK,
) -> ResilientTransport:
    """Wrap `transport` — or a fresh `AsyncHTTPTransport` — with the policy."""
    inner = transport if transport is not None else httpx.AsyncHTTPTransport()
    return ResilientTransport(
        inner,
        retry=retry,
        breakers=breakers,
        sleep=sleep,
        rng=rng,
        wall_clock=wall_clock,
    )


def resilient_async_client(
    *,
    base_url: str = "",
    timeout: httpx.Timeout | float | None = DEFAULT_TIMEOUT,
    headers: dict[str, str] | None = None,
    limits: httpx.Limits | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    retry: RetryPolicy | None = None,
    breakers: CircuitBreakerRegistry | None = None,
    sleep: AsyncSleeper = asyncio.sleep,
    rng: random.Random = DEFAULT_RNG,
    wall_clock: WallClock = DEFAULT_WALL_CLOCK,
) -> httpx.AsyncClient:
    """An `AsyncClient` whose every request retries and respects the breaker.

    `timeout` is per *attempt*, which is the only thing a transport-level
    timeout can be, so a policy of three attempts can spend three times it. An
    enclosing `deadline()` is what bounds the whole thing — see the module
    docstring of `transport.py`.

    Args:
        base_url: Prefix for relative request URLs, as on `httpx.AsyncClient`.
        timeout: Per-attempt timeout. `None` disables it, as in httpx.
        headers: Default headers for every request.
        limits: Connection-pool limits.
        transport: What to wrap. Defaults to a real `AsyncHTTPTransport`; a
            test passes `httpx.MockTransport`.
        retry: Retry policy. Defaults to three attempts with full jitter.
        breakers: Breaker registry. Defaults to the process-wide one, so two
            clients built for the same dependency share its state.
        sleep: Awaitable sleep between attempts.
        rng: Jitter source.
        wall_clock: Now, in epoch seconds, for a `Retry-After` HTTP-date.

    Example:
        >>> client = resilient_async_client(base_url="https://api.stripe.com")
        >>> async with client:
        ...     response = await client.get("/v1/charges")
    """
    return httpx.AsyncClient(
        base_url=base_url,
        timeout=timeout,
        headers=headers,
        limits=limits if limits is not None else httpx.Limits(),
        transport=resilient_transport(
            transport=transport,
            retry=retry,
            breakers=breakers,
            sleep=sleep,
            rng=rng,
            wall_clock=wall_clock,
        ),
    )
