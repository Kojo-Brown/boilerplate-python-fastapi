"""Retry and the breaker, applied to every request an `AsyncClient` makes.

An `httpx.AsyncBaseTransport` rather than a decorator on each call site, for
three reasons that a decorator cannot give:

* **Nothing has to opt in.** A gateway adapter written six months from now gets
  the policy by being handed a client, and cannot forget it.
* **The retry sees the request, not the wrapper's arguments.** Whether a body
  can be replayed, whether the method is idempotent, whether the far end sent a
  `Retry-After` — every input to the decision is on the `Request` and the
  `Response`, and at this layer they are the arguments.
* **A retried attempt is a real, separate request.** Connection reuse, the
  pool, redirects, cookies and event hooks all behave as httpx intends,
  because the retry happens *below* the client rather than around it.

## The composition: breaker inside the retry loop

Each attempt asks the breaker for admission, rather than the loop asking once
and then retrying underneath it. Both orders are defensible and they differ in
two places that matter.

A breaker *outside* the loop counts one logical call as one failure however
many attempts it made, so a dependency that is down is discovered three times
more slowly than it is being hammered. Worse, a retry loop that started before
the circuit opened keeps sleeping and re-sending after some other caller has
already established that the dependency is gone — which is exactly the traffic
the breaker exists to stop.

With the breaker inside, each attempt is counted, and the moment the circuit
opens — because of this request's own failures or anyone else's — the next
`acquire` raises and the loop ends. `CircuitOpenError` is therefore never
retried: it is not a failure of the dependency, it is this process declining to
find out, and repeating it would burn the caller's remaining attempts on a
decision that cannot change inside a few hundred milliseconds.

## Where the bulkhead sits

`BulkheadTransport` is a second, thinner transport that `ResilientTransport`
wraps *around* — so the layering, outermost first, is retry → breaker →
bulkhead → network, and each is inside the one before it.

That the bulkhead is inside the retry loop is the load-bearing half. A slot
taken for the whole retry sequence is a slot held across the backoff sleeps,
which is capacity spent on doing nothing at exactly the moment the dependency
has none to spare. Per attempt, the sleep happens with the compartment given
back — and a retry that arrives to find the compartment full is refused, which
is correct rather than unfortunate: if the dependency is saturated, a retry is
precisely the traffic worth shedding.

That the breaker is outside the bulkhead is the other half, and it is the
cheaper test first. `CircuitBreaker.acquire` never waits: it admits or raises
in the same tick. `Bulkhead.acquire` can wait up to its acquire timeout. With
the order reversed, every call to a dependency that is already known to be down
would queue for a slot before being told the circuit is open — spending the
compartment, and the caller's time, to reach a decision that was available for
free.

## What is deliberately not here

A total retry budget. Three attempts against a fifteen-second timeout is a
forty-five second request, which is not what the caller who wrote `timeout=15`
had in mind. The bound belongs to the caller rather than to this transport, and
this codebase already has one: an enclosing `deadline()` from
`src/structured/deadline.py` is consulted before every sleep, and a retry that
would not fit inside the remaining budget is declined instead of started. With
no enclosing deadline there is nothing to consult and the attempts run to
exhaustion — see `docs/resilience.md`.
"""

from __future__ import annotations

import asyncio
import random
import types
from collections.abc import AsyncIterator

import httpx
import structlog

from src.decorators.base import DEFAULT_RNG, AsyncSleeper, backoff_delay
from src.resilience.base import (
    DEFAULT_WALL_CLOCK,
    BulkheadTimeoutError,
    RetryPolicy,
    WallClock,
    is_failure_status,
    is_local_shortage,
    is_replayable,
    may_repeat,
    origin_of,
    request_was_sent,
    retry_after_seconds,
)
from src.resilience.bulkhead import (
    DEFAULT_BULKHEADS,
    BulkheadRegistry,
    BulkheadSlot,
)
from src.resilience.circuit import DEFAULT_REGISTRY, CircuitBreakerRegistry
from src.structured.deadline import current_deadline

logger = structlog.get_logger(__name__)


class _SlotBoundStream(httpx.AsyncByteStream):
    """A response body that gives its bulkhead slot back when it is closed.

    The reason this exists at all is that `handle_async_request` returns as
    soon as the response *headers* are in, and httpx reads the body afterwards
    — inside `AsyncClient.send`, or later still if the caller asked for
    `stream=True`. Releasing the slot on return would therefore stop counting a
    call at the point it stops being interesting to the transport and long
    before it stops occupying this process, which is the one thing a
    compartment has to count correctly. A dependency that answers headers
    instantly and then drips ten megabytes would consume no capacity at all.

    So the slot outlives the send and is bound to the body: httpx closes a
    response when it is fully read, when `aclose` is called, and when a
    `stream=True` block exits, and all three arrive here.
    """

    __slots__ = ("_slot", "_stream")

    def __init__(self, stream: httpx.AsyncByteStream, slot: BulkheadSlot) -> None:
        self._stream = stream
        self._slot = slot

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self._stream:
            yield chunk

    async def aclose(self) -> None:
        try:
            await self._stream.aclose()
        finally:
            # In a `finally`, because a transport whose close fails would
            # otherwise take a slot with it, one per failure, until the
            # compartment is empty of everything but ghosts.
            self._slot.release()


class BulkheadTransport(httpx.AsyncBaseTransport):
    """Bounds how many requests to one origin are in flight, and how long.

    Wrapped *by* `ResilientTransport` rather than the other way round — see
    "Where the bulkhead sits" in the module docstring for why that order is
    the load-bearing part.

    Args:
        transport: What actually sends the request.
        bulkheads: Registry of per-origin compartments. Defaults to the
            process-wide `DEFAULT_BULKHEADS`, so every client built by the
            factory shares one compartment per dependency; pass a fresh
            registry to isolate a test.
    """

    def __init__(
        self,
        transport: httpx.AsyncBaseTransport,
        *,
        bulkheads: BulkheadRegistry | None = None,
    ) -> None:
        self._transport = transport
        self._bulkheads = bulkheads if bulkheads is not None else DEFAULT_BULKHEADS

    @property
    def bulkheads(self) -> BulkheadRegistry:
        """The registry, for a health check that wants to report compartments."""
        return self._bulkheads

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        origin = origin_of(request)
        bulkhead = self._bulkheads.get(origin)
        hard_timeout = bulkhead.config.execution_timeout

        slot = await bulkhead.acquire()
        try:
            if hard_timeout is None:
                response = await self._transport.handle_async_request(request)
            else:
                timer = asyncio.timeout(hard_timeout)
                try:
                    async with timer:
                        response = await self._transport.handle_async_request(request)
                except TimeoutError as exc:
                    # `asyncio.timeout` converts *its own* cancellation into
                    # `TimeoutError`, and an enclosing `deadline()` that fires
                    # here produces one too. Only the first is this scope
                    # expiring, and renaming the second would report a spent
                    # request budget as a slow dependency — two failures with
                    # different fixes. `deadline()` draws the same line, in the
                    # same way, for the same reason.
                    if not timer.expired():
                        raise
                    raise BulkheadTimeoutError(origin, hard_timeout) from exc
        except BaseException:
            # Includes cancellation and the enclosing deadline. Nothing was
            # returned, so nothing else will ever release this slot.
            slot.release()
            raise

        stream = response.stream
        if response.is_closed or not isinstance(stream, httpx.AsyncByteStream):
            # Already fully in memory — every `httpx.MockTransport` response is,
            # because `Response.__init__` reads a `ByteStream` eagerly — so
            # there is no close left to hang the release on.
            slot.release()
        else:
            response.stream = _SlotBoundStream(stream, slot)
        return response

    async def aclose(self) -> None:
        await self._transport.aclose()

    async def __aenter__(self) -> BulkheadTransport:
        await self._transport.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: types.TracebackType | None = None,
    ) -> None:
        await self._transport.__aexit__(exc_type, exc_value, traceback)


class ResilientTransport(httpx.AsyncBaseTransport):
    """Wraps another transport with retry-with-jitter and a per-origin breaker.

    Args:
        transport: What actually sends the request. `httpx.AsyncHTTPTransport()`
            in production, an `httpx.MockTransport` in a test.
        retry: The retry policy. `RetryPolicy(attempts=1)` leaves only the
            breaker.
        breakers: Registry of per-origin breakers. Defaults to the process-wide
            `DEFAULT_REGISTRY`, so every client built by the factory shares one
            view of each dependency; pass a fresh registry to isolate a test.
        sleep: Awaitable sleep between attempts. Injectable so a test asserts
            the schedule without spending it.
        rng: Jitter source. Seed it to make a test's delays reproducible.
        wall_clock: Now, in epoch seconds, for `Retry-After` in its HTTP-date
            form. Nothing else here reads it.
    """

    def __init__(
        self,
        transport: httpx.AsyncBaseTransport,
        *,
        retry: RetryPolicy | None = None,
        breakers: CircuitBreakerRegistry | None = None,
        sleep: AsyncSleeper = asyncio.sleep,
        rng: random.Random = DEFAULT_RNG,
        wall_clock: WallClock = DEFAULT_WALL_CLOCK,
    ) -> None:
        self._transport = transport
        self._retry = retry if retry is not None else RetryPolicy()
        self._breakers = breakers if breakers is not None else DEFAULT_REGISTRY
        self._sleep = sleep
        self._rng = rng
        self._wall_clock = wall_clock

    @property
    def breakers(self) -> CircuitBreakerRegistry:
        """The registry, for a health check that wants to report circuit state."""
        return self._breakers

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        origin = origin_of(request)
        breaker = self._breakers.get(origin)
        # Asked once, before anything has touched the request. A transport
        # underneath is entitled to buffer the body as it sends it —
        # `httpx.MockTransport` does exactly that — which would make a stream
        # that was consumable once look replayable by the time the first
        # attempt failed, and turn "never retried" into "retried, with an empty
        # body, successfully".
        replayable = is_replayable(request)

        for attempt in range(1, self._retry.attempts + 1):
            call = breaker.acquire()
            try:
                response = await self._transport.handle_async_request(request)
            except asyncio.CancelledError:
                # The caller stopped caring. Not a failure of the dependency,
                # and never retried — see `src/decorators/retry.py`, which
                # refuses it for the same reason.
                call.release()
                raise
            except httpx.TransportError as exc:
                if is_local_shortage(exc):
                    # This process out of capacity, not the dependency out of
                    # health. Released without an outcome and raised without a
                    # retry — see `is_local_shortage`.
                    call.release()
                    raise
                call.failed()
                delay = self._delay_before_next(
                    request, attempt, None, exc, replayable=replayable
                )
                if delay is None:
                    # Bare `raise`, so the caller sees the original error with
                    # its own traceback and no frames from the retry loop.
                    raise
            else:
                if not is_failure_status(response.status_code):
                    call.succeeded()
                    return response
                call.failed()
                delay = self._delay_before_next(
                    request, attempt, response, None, replayable=replayable
                )
                if delay is None:
                    return response
                # Release the connection before sleeping on it. Without this
                # the pool holds an unread response for the whole backoff and
                # the next attempt contends with its own predecessor.
                await response.aclose()
            finally:
                # A no-op once settled; it does real work only when something
                # other than a `TransportError` escapes the send, where the
                # half-open probe slot would otherwise leak.
                call.release()

            await self._sleep(delay)

        raise AssertionError("unreachable")  # pragma: no cover

    async def aclose(self) -> None:
        await self._transport.aclose()

    async def __aenter__(self) -> ResilientTransport:
        await self._transport.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: types.TracebackType | None = None,
    ) -> None:
        await self._transport.__aexit__(exc_type, exc_value, traceback)

    def _delay_before_next(
        self,
        request: httpx.Request,
        attempt: int,
        response: httpx.Response | None,
        error: httpx.TransportError | None,
        *,
        replayable: bool,
    ) -> float | None:
        """Seconds to wait before attempt `attempt + 1`, or None to give up.

        Exactly one of `response` (carrying a failure status) and `error` is
        set. Returning None rather than raising keeps the caller's two ways of
        giving up — return the response, re-raise the error — in one place.
        """
        context = {
            "origin": origin_of(request),
            "method": request.method,
            "attempt": attempt,
            "status": None if response is None else response.status_code,
            "error": None if error is None else type(error).__name__,
        }

        reason = self._refusal_reason(request, attempt, error, replayable=replayable)
        if reason == "attempts_exhausted":
            # The one outcome worth finding in a log unprompted: the request
            # failed and this transport spent every attempt it had on it.
            logger.warning("http.retry_exhausted", of=self._retry.attempts, **context)
            return None
        if reason is not None:
            logger.debug("http.retry_declined", reason=reason, **context)
            return None

        delay = self._delay_for(attempt, response)
        if delay is None:
            logger.info("http.retry_declined", reason="retry_after_too_long", **context)
            return None

        budget = current_deadline()
        if budget is not None and budget.remaining() <= delay:
            logger.info(
                "http.retry_declined",
                reason="deadline",
                remaining_ms=round(budget.remaining() * 1000, 3),
                delay_ms=round(delay * 1000, 3),
                **context,
            )
            return None

        logger.warning(
            "http.retry_scheduled",
            of=self._retry.attempts,
            delay_ms=round(delay * 1000, 3),
            **context,
        )
        return delay

    def _refusal_reason(
        self,
        request: httpx.Request,
        attempt: int,
        error: httpx.TransportError | None,
        *,
        replayable: bool,
    ) -> str | None:
        """Why this attempt must be the last, or None if another may follow."""
        if attempt >= self._retry.attempts:
            return "attempts_exhausted"
        if not replayable:
            return "body_not_replayable"
        # A connect-phase failure is the one case where the far end provably
        # never saw the request, so even a POST may be repeated.
        if error is not None and not request_was_sent(error):
            return None
        if not may_repeat(
            request, idempotency_key_headers=self._retry.idempotency_key_headers
        ):
            return "not_idempotent"
        return None

    def _delay_for(self, attempt: int, response: httpx.Response | None) -> float | None:
        """The wait, honouring `Retry-After`; None if it asks for too long.

        When the far end names a delay it wins over the computed backoff, and
        a jitter draw is added *on top* rather than replacing it: the header is
        a floor the server asked for, so waking before it is disobedience,
        while waking at exactly it puts every rate-limited client back on the
        wire in the same millisecond. `[0, base_delay)` is enough spread to
        break that up and small enough not to matter against a delay measured
        in seconds.
        """
        if response is not None and self._retry.respect_retry_after:
            named = retry_after_seconds(response, now=self._wall_clock)
            if named is not None:
                if named > self._retry.max_retry_after:
                    return None
                return named + self._rng.uniform(0.0, self._retry.base_delay)

        return backoff_delay(
            attempt,
            base_delay=self._retry.base_delay,
            max_delay=self._retry.max_delay,
            jitter=self._retry.jitter,
            rng=self._rng,
        )
