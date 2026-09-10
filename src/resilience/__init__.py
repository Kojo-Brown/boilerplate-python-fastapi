"""Circuit breaking and retry-with-jitter for outbound HTTP.

Three layers, and the split is the same one `src/kafka` and `src/redis_streams`
use: `base.py` decides what a failure is and what may be repeated and holds no
state, `circuit.py` and `bulkhead.py` are the state, and `transport.py` is the
httpx integration that uses all three. Nothing in the first three imports the
last, which is what lets the breaker and the compartments be tested against a
clock rather than against a server.

Two mechanisms, divided by what they answer:

* The **circuit breaker** stops calling a dependency that has *failed*.
* The **bulkhead** bounds how much of this process a dependency may occupy
  while it is merely *slow* — which never raises, never logs, and is the more
  common way a third party takes an application down.

Start at `docs/resilience.md`. The short version:

    from src.resilience import resilient_async_client

    client = resilient_async_client(base_url="https://api.example.test")

Everything sent through that client retries transient failures with full-jitter
backoff, is capped at `BulkheadConfig.limit` calls in flight to that origin, and
stops being attempted entirely once the dependency has failed enough to be
called down — surfacing a `CircuitOpenError` or a `BulkheadFullError`, both of
which are `httpx.TransportError`s precisely so that code already handling
"could not reach it" keeps working unchanged.
"""

from src.resilience.base import (
    DEFAULT_IDEMPOTENCY_KEY_HEADERS,
    IDEMPOTENT_METHODS,
    PERMANENT_SERVER_STATUSES,
    BulkheadConfig,
    BulkheadFullError,
    BulkheadRejection,
    BulkheadTimeoutError,
    CircuitBreakerConfig,
    CircuitOpenError,
    CircuitState,
    RetryPolicy,
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
    Bulkhead,
    BulkheadRegistry,
    BulkheadSlot,
    BulkheadStats,
)
from src.resilience.circuit import (
    DEFAULT_REGISTRY,
    CircuitBreaker,
    CircuitBreakerRegistry,
    CircuitCall,
)
from src.resilience.factory import (
    resilient_async_client,
    resilient_transport,
)
from src.resilience.transport import BulkheadTransport, ResilientTransport

__all__ = [
    "DEFAULT_BULKHEADS",
    "DEFAULT_IDEMPOTENCY_KEY_HEADERS",
    "DEFAULT_REGISTRY",
    "IDEMPOTENT_METHODS",
    "PERMANENT_SERVER_STATUSES",
    "Bulkhead",
    "BulkheadConfig",
    "BulkheadFullError",
    "BulkheadRegistry",
    "BulkheadRejection",
    "BulkheadSlot",
    "BulkheadStats",
    "BulkheadTimeoutError",
    "BulkheadTransport",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerRegistry",
    "CircuitCall",
    "CircuitOpenError",
    "CircuitState",
    "ResilientTransport",
    "RetryPolicy",
    "is_failure_status",
    "is_local_shortage",
    "is_replayable",
    "may_repeat",
    "origin_of",
    "request_was_sent",
    "resilient_async_client",
    "resilient_transport",
    "retry_after_seconds",
]
