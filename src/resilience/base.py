"""What counts as a failure, and what may be tried again.

Nothing here holds state or performs I/O. The state machine is in `circuit.py`
and the retry loop is in `transport.py`; both ask this module the same two
questions, and asking them in one place is what keeps the breaker and the retry
loop from disagreeing about what a failure is.

## Why retryability and breaker-failure share a predicate

A failure that is not worth retrying is not evidence of an outage either. A
`501 Not Implemented` will answer identically in ten minutes, and counting it
towards a trip would open the circuit on a dependency that is up, answering,
and telling us — correctly — that we asked for something it does not do. So
`is_failure_status` is the single predicate: it decides both whether an attempt
counts against the breaker and whether the response is worth repeating.

## Why "may be retried" is two questions, not one

Retrying is safe when the request definitely was not processed, or when
processing it twice is harmless. Those are different facts and the transport
knows the first one better than the caller does:

* A `ConnectError` or `ConnectTimeout` means no connection was established, so
  the request cannot have been processed. Any method may be repeated —
  including a `POST` that would otherwise be off limits.
* A read timeout, a write error, a dropped connection mid-response, and *every
  response status* mean the request reached the far end. Whether it took effect
  is unknowable from here, so only an idempotent method may be repeated.

Collapsing the two into "retry idempotent methods only" gives up free retries
on the most common failure in a rolling deploy — a connection refused by an
instance that has already gone — and collapsing them the other way double-posts
under a read timeout, which is the failure most likely to mean the far end is
slow rather than absent, and so most likely to have worked.

## The idempotency-key escape hatch

A `POST` carrying `Idempotency-Key` or `PayPal-Request-Id` is retryable, because
the caller has told the far end how to deduplicate it. That is a claim this
module cannot verify — a header the receiver ignores is a header — so it is
opt-in per header name and the default set is exactly the two this codebase
already sends from `src/payments`.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from email.utils import parsedate_to_datetime
from enum import StrEnum
from typing import Final

import httpx

#: Returns seconds since the epoch. Wall clock rather than monotonic, because
#: the only thing it is used for is `Retry-After` in its HTTP-date form, which
#: names an absolute instant and can only be subtracted from an absolute now.
WallClock = Callable[[], float]

DEFAULT_WALL_CLOCK: WallClock = time.time

#: Methods RFC 9110 defines as idempotent. `POST` and `PATCH` are absent
#: because repeating them is defined to be a second effect, not a retry.
IDEMPOTENT_METHODS: Final[frozenset[str]] = frozenset(
    {"GET", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE"}
)

#: Request headers whose presence makes any method retryable. Matched
#: case-insensitively — `httpx.Headers` is already case-insensitive, so
#: membership is tested by lookup rather than by lowering the name.
DEFAULT_IDEMPOTENCY_KEY_HEADERS: Final[frozenset[str]] = frozenset(
    {"idempotency-key", "paypal-request-id"}
)

#: 5xx answers that are a durable statement about the request rather than a
#: symptom of load or an outage. Excluded from `is_failure_status` so they
#: neither trip the breaker nor cost two more round trips to be told again.
PERMANENT_SERVER_STATUSES: Final[frozenset[int]] = frozenset({501, 505})

TOO_MANY_REQUESTS: Final[int] = 429


class CircuitState(StrEnum):
    """Where a breaker is in its cycle. See `circuit.py` for the transitions."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class BulkheadRejection(StrEnum):
    """Why a compartment refused a call. All three are local shortages.

    Distinguished because they call for different fixes and are otherwise
    indistinguishable in a log: `QUEUE_FULL` says the dependency is saturated
    and the backlog is already as deep as it is allowed to get, `ACQUIRE_TIMEOUT`
    says this call queued and its turn did not come, and `NO_BUDGET` says the
    enclosing request deadline had less time left than the wait would take, so
    nothing was queued at all.
    """

    QUEUE_FULL = "queue_full"
    ACQUIRE_TIMEOUT = "acquire_timeout"
    NO_BUDGET = "no_budget"


class CircuitOpenError(httpx.TransportError):
    """The breaker refused to attempt the request.

    An `httpx.TransportError` rather than a new exception family, and that is
    the load-bearing decision in this package. Every existing caller of an
    outbound client here — both payment adapters, the webhook notifier —
    already answers "the dependency could not be reached" by catching
    `httpx.TransportError` and translating it into this application's own 502
    or 503. An open circuit *is* that answer, arrived at without spending a
    socket to find out. Given a type of its own it would instead escape every
    one of those handlers and surface as an unhandled 500 — turning a
    deliberately handled outage into an internal error at exactly the moment
    the breaker exists to make things better.

    `retry_after` is the breaker's own estimate of when it will next let a
    probe through, so a caller can put a number in a `Retry-After` header of
    its own instead of inventing one.
    """

    def __init__(self, origin: str, retry_after: float) -> None:
        super().__init__(
            f"Circuit for '{origin}' is open; "
            f"not attempting the request for another {retry_after:.1f}s."
        )
        self.origin = origin
        self.retry_after = retry_after


class BulkheadFullError(httpx.TransportError):
    """The compartment for this dependency had no room and no time to wait.

    An `httpx.TransportError` for exactly the reason `CircuitOpenError` is one:
    every existing caller of an outbound client here already answers "the
    dependency could not be reached" by catching that base class, and a new
    exception family would escape all of them and surface as an unhandled 500
    at the moment the bulkhead is supposed to be helping.

    It is *not* an `httpx.TimeoutException` even when the reason is
    `ACQUIRE_TIMEOUT`, and that distinction is load-bearing rather than
    pedantic. A `TimeoutException` from this transport means the dependency did
    not answer in time; this means the dependency was never asked, because this
    process is already spending as much of itself on that dependency as it is
    allowed to. Reporting the second as the first sends whoever reads the log
    to the wrong system entirely — and, more concretely, `request_was_sent`
    would classify it as possibly-delivered and refuse to repeat a `POST` that
    provably never left.

    `retry_after` is a hint a caller can put in a `Retry-After` header of its
    own, and is the compartment's acquire timeout: a slot is a matter of one
    call finishing, not of a dependency recovering, so it is short.
    """

    def __init__(
        self,
        origin: str,
        reason: BulkheadRejection,
        *,
        limit: int,
        queued: int,
        waited: float,
        retry_after: float,
    ) -> None:
        super().__init__(
            f"Bulkhead for '{origin}' refused the request ({reason}): "
            f"{limit} in flight, {queued} queued, waited {waited:.3f}s."
        )
        self.origin = origin
        self.reason = reason
        self.limit = limit
        self.queued = queued
        self.waited = waited
        self.retry_after = retry_after


class BulkheadTimeoutError(httpx.TimeoutException):
    """One attempt held a compartment slot past its hard timeout.

    A `TimeoutException` — unlike `BulkheadFullError` — because it means what
    every other timeout in httpx means: the request went out and no answer came
    back in the time allowed. So it counts against the breaker, and it is
    retryable on the same terms as a read timeout.

    It exists at all because **httpx's own timeouts are per phase and each one
    resets whenever progress is made**. `Timeout(read=10.0)` bounds the wait
    for the next read, not the exchange: a far end that dribbles a header line
    every nine seconds never trips it and holds a connection, a slot and a task
    for as long as it cares to. And a client built with `timeout=None` — which
    the factory permits, because httpx does — has no phase timeout at all, so
    this is the only thing standing between one hung dependency and a
    compartment that never empties.

    It bounds the request through to the response *headers*, which is the span
    `handle_async_request` owns. The body is read afterwards and is not covered
    — see `docs/bulkheads.md`, which is also why the slot stays held until the
    body is closed rather than until the send returns.
    """

    def __init__(self, origin: str, seconds: float) -> None:
        super().__init__(
            f"Request to '{origin}' held a bulkhead slot for longer than "
            f"the {seconds:g}s hard timeout."
        )
        self.origin = origin
        self.seconds = seconds


@dataclass(frozen=True, slots=True)
class BulkheadConfig:
    """How much of this process one dependency may occupy.

    Args:
        limit: Calls to one origin allowed in flight at once. This is the
            compartment. Deliberately below `httpx.Limits().max_connections`
            (100) by default, so that the *bulkhead* is what sheds rather than
            the connection pool: shedding here is per dependency, immediate,
            and carries a typed error naming the origin, where pool exhaustion
            is process-wide, costs the full pool timeout first, and arrives as
            a `PoolTimeout` that says nothing about which dependency filled it.
        max_queue: Calls allowed to wait for a slot. Bounded because a wait
            queue with no bound is a memory limit wearing a timeout's clothes —
            every waiter is a task, a request object and whatever the handler
            above it is still holding. Past this, callers are refused
            immediately rather than admitted to a backlog that cannot drain in
            `acquire_timeout` anyway.
        acquire_timeout: The longest a call waits for a slot. Short on purpose:
            queueing *is* the pile-up a bulkhead exists to prevent, so this is
            the small amount of it that smooths a burst, not a way to make the
            limit soft. An enclosing `deadline()` shortens it further.
        execution_timeout: Hard ceiling on one attempt, through to the response
            headers, or `None` to rely on httpx's phase timeouts alone. Not a
            duplicate of the client's `timeout`: that one bounds each phase and
            restarts whenever a byte arrives, and it is absent entirely from a
            client built with `timeout=None`. Without a ceiling that cannot be
            reset, a handful of hung calls hold slots for the life of the
            process and the limit stops being a limit.

    Raises:
        ValueError: The policy is unusable — checked at construction so a bad
            compartment fails at startup rather than during the incident it was
            configured for.
    """

    limit: int = 20
    max_queue: int = 40
    acquire_timeout: float = 1.0
    execution_timeout: float | None = 30.0

    def __post_init__(self) -> None:
        if self.limit < 1:
            raise ValueError("limit must be at least 1.")
        if self.max_queue < 0:
            raise ValueError("max_queue must not be negative.")
        if self.acquire_timeout < 0:
            raise ValueError("acquire_timeout must not be negative.")
        if self.execution_timeout is not None and self.execution_timeout <= 0:
            raise ValueError("execution_timeout must be positive, or None.")


@dataclass(frozen=True, slots=True)
class CircuitBreakerConfig:
    """The trip, wait and recover thresholds for one breaker.

    Args:
        failure_threshold: Failures inside the window that open the circuit.
        window_size: How many recent outcomes are remembered. Counting inside a
            bounded window rather than counting *consecutive* failures is what
            makes a dependency failing half its calls trip the breaker at all:
            under a consecutive rule any interleaved success resets the count,
            so a 50% error rate — an unambiguous outage to anyone watching a
            dashboard — never opens the circuit.
        reset_timeout: Seconds the circuit stays open before a probe is let
            through. This is the whole cost of a false trip, so it is short by
            default; it is also the longest a genuinely dead dependency is left
            alone, which is the trade being made.
        success_threshold: Consecutive probe successes that close the circuit
            again. Above one because a dependency restarting can answer a
            single request correctly and fall over on the next.
        half_open_max_calls: Probes allowed through at once while half-open.
            One by default: the point of the state is to spend a single request
            finding out, and a half-open state that admits everything is a
            thundering herd aimed at a service that has just come back up.

    Raises:
        ValueError: Any threshold is unusable — checked here, at construction,
            so a bad policy fails at startup rather than during the incident it
            was configured for.
    """

    failure_threshold: int = 5
    window_size: int = 20
    reset_timeout: float = 30.0
    success_threshold: int = 2
    half_open_max_calls: int = 1

    def __post_init__(self) -> None:
        if self.failure_threshold < 1:
            raise ValueError("failure_threshold must be at least 1.")
        if self.window_size < self.failure_threshold:
            raise ValueError(
                "window_size must be at least failure_threshold, otherwise the "
                "circuit can never trip."
            )
        if self.reset_timeout <= 0:
            raise ValueError("reset_timeout must be positive.")
        if self.success_threshold < 1:
            raise ValueError("success_threshold must be at least 1.")
        if self.half_open_max_calls < 1:
            raise ValueError("half_open_max_calls must be at least 1.")


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How many times to try again, how long to wait, and for what.

    Args:
        attempts: Total attempts, not extra ones — `attempts=3` is one try and
            two retries. 1 disables retrying while leaving the breaker on.
        base_delay: Seconds before the second attempt, pre-jitter.
        max_delay: Ceiling on the pre-jitter backoff.
        jitter: Draw each wait from `[0, ceiling]` rather than using the
            ceiling. See `backoff_delay` in `src/decorators/base.py` for why
            this is on by default.
        idempotency_key_headers: Request headers that make a non-idempotent
            method retryable.
        respect_retry_after: Honour a `Retry-After` on a 429 or 503 in place of
            the computed backoff.
        max_retry_after: The longest `Retry-After` worth waiting for. A far end
            asking for an hour is not asking to be retried inside this request;
            past this the response is returned to the caller unchanged, which
            is the honest answer rather than a socket held open for an hour.

    Raises:
        ValueError: The policy is unusable.
    """

    attempts: int = 3
    base_delay: float = 0.1
    max_delay: float = 5.0
    jitter: bool = True
    idempotency_key_headers: frozenset[str] = field(
        default=DEFAULT_IDEMPOTENCY_KEY_HEADERS
    )
    respect_retry_after: bool = True
    max_retry_after: float = 30.0

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("attempts must be at least 1.")
        if self.base_delay < 0:
            raise ValueError("base_delay must not be negative.")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must be at least base_delay.")
        if self.max_retry_after < 0:
            raise ValueError("max_retry_after must not be negative.")


def origin_of(request: httpx.Request) -> str:
    """The breaker key: `scheme://host:port`.

    Per origin rather than per process, because one dependency being down is
    not a reason to stop calling a different one — a global breaker turns a
    Stripe outage into a PayPal outage. Per *origin* rather than per URL for
    the mirror-image reason: a breaker keyed by path never sees enough traffic
    on any one endpoint to trip, and the thing that fails is a server.

    The port is taken from `httpx.URL.port`, which is `None` when the URL uses
    the scheme's default, so `https://api.test` and `https://api.test:443` are
    deliberately *not* merged: they are one server, but they are also two
    strings we would have to canonicalise per scheme to prove it, and the cost
    of getting that wrong (two breakers for one dependency) is smaller than the
    cost of a scheme table that drifts.
    """
    return f"{request.url.scheme}://{request.url.netloc.decode('ascii')}"


def is_failure_status(status_code: int) -> bool:
    """Whether this response counts as the dependency failing.

    429 because a rate limit is the far end shedding load, which is the state
    a breaker exists to stop making worse. 5xx because it is a server fault by
    definition. Not 4xx otherwise: a wave of 404s or 401s is this application
    asking for the wrong thing, and opening a circuit on it takes a caller's
    bug and turns it into an outage for every other caller of that dependency.
    """
    if status_code == TOO_MANY_REQUESTS:
        return True
    return status_code >= 500 and status_code not in PERMANENT_SERVER_STATUSES


def is_pool_exhaustion(exc: BaseException) -> bool:
    """Whether the failure is ours rather than the dependency's.

    `httpx.PoolTimeout` means this process ran out of connections in its own
    pool: the request never left, and the far end may be in perfect health.
    Counting it towards a trip opens a circuit on a remote service because of a
    local shortage, and retrying it queues another waiter on the pool that is
    already full. So it is neither a breaker failure nor retryable, and the
    caller gets it immediately — see `docs/resilience.md`.

    Kept as its own predicate now that `is_local_shortage` exists, because the
    two answer different questions: this one is "was it the pool", which is
    what a log line and a metric want to distinguish, and that one is "was it
    ours", which is what the transport acts on.
    """
    return isinstance(exc, httpx.PoolTimeout)


def is_local_shortage(exc: BaseException) -> bool:
    """Whether `exc` is this process running out of its own capacity.

    The two members are one fact discovered at two different depths.
    `PoolTimeout` is the connection pool full; `BulkheadFullError` is the
    compartment for a single dependency full, which is the same shortage caught
    earlier, attributed to the dependency that caused it, and answered in
    bounded time rather than after a full pool timeout.

    Both get identical treatment from the transport, and the treatment is the
    interesting part: **neither counts towards the breaker and neither is
    retried.** Counting them opens a circuit on a remote service because of a
    local shortage — the dependency may be in perfect health and merely
    popular — and retrying them adds another waiter to the queue that is
    already the problem. The caller gets the error immediately, which is the
    honest answer and the only one that stops the pile-up rather than joining
    it. See `docs/bulkheads.md`.
    """
    return isinstance(exc, httpx.PoolTimeout | BulkheadFullError)


def request_was_sent(exc: httpx.TransportError) -> bool:
    """Whether the far end may have received the request despite `exc`.

    `False` only for the connect-phase failures, where no connection existed to
    carry a request. Everything else — a read timeout, a write error, a
    connection dropped mid-response — happened with bytes already on the wire.
    """
    return not isinstance(exc, httpx.ConnectError | httpx.ConnectTimeout)


def may_repeat(
    request: httpx.Request, *, idempotency_key_headers: frozenset[str]
) -> bool:
    """Whether repeating `request` is safe when it may already have been sent."""
    if request.method.upper() in IDEMPOTENT_METHODS:
        return True
    return any(name in request.headers for name in idempotency_key_headers)


def is_replayable(request: httpx.Request) -> bool:
    """Whether `request`'s body can be sent a second time.

    A request built from bytes, a string, JSON, form fields or files carries a
    stream httpx can iterate synchronously, which is the same property as being
    able to iterate it twice: `ByteStream` holds the buffer and `MultipartStream`
    re-renders its fields, seeking each file back to the start. A request built
    from an async generator carries an `AsyncIteratorByteStream`, which is
    consumed by the first attempt — retrying it sends a body of zero bytes,
    successfully, and the far end stores the empty version. That failure is
    silent at every layer, so the stream is inspected rather than assumed.
    """
    return isinstance(request.stream, httpx.SyncByteStream)


def retry_after_seconds(
    response: httpx.Response, *, now: WallClock = DEFAULT_WALL_CLOCK
) -> float | None:
    """Parse `Retry-After`, in either of its two forms, or `None`.

    Returns `None` for an absent, malformed or already-past header rather than
    raising: a far end that sends nonsense here should fall back to this
    application's own backoff, not fail the request with a parse error. A date
    in the past yields 0.0 — "now" — which is a real answer and not the same as
    no header at all.
    """
    raw = response.headers.get("retry-after")
    if raw is None:
        return None

    value = raw.strip()
    try:
        return max(0.0, float(int(value)))
    except ValueError:
        pass

    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    return max(0.0, when.timestamp() - now())
