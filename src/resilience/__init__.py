"""Circuit breaking and retry-with-jitter for outbound HTTP.

Three layers, and the split is the same one `src/kafka` and `src/redis_streams`
use: `base.py` decides what a failure is and what may be repeated and holds no
state, `circuit.py` is the state machine, and `transport.py` is the httpx
integration that uses both. Nothing in the first two imports the third, which
is what lets the breaker be tested against a clock rather than against a
server.

Start at `docs/resilience.md`. The short version:

    from src.resilience import resilient_async_client

    client = resilient_async_client(base_url="https://api.example.test")

Everything sent through that client retries transient failures with full-jitter
backoff and stops trying entirely, per origin, once the dependency has failed
enough to be called down — surfacing a `CircuitOpenError`, which is an
`httpx.TransportError` precisely so that code already handling "could not reach
it" keeps working unchanged.
"""

from src.resilience.base import (
    DEFAULT_IDEMPOTENCY_KEY_HEADERS,
    IDEMPOTENT_METHODS,
    PERMANENT_SERVER_STATUSES,
    CircuitBreakerConfig,
    CircuitOpenError,
    CircuitState,
    RetryPolicy,
    is_failure_status,
    is_replayable,
    may_repeat,
    origin_of,
    request_was_sent,
    retry_after_seconds,
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
from src.resilience.transport import ResilientTransport

__all__ = [
    "DEFAULT_IDEMPOTENCY_KEY_HEADERS",
    "DEFAULT_REGISTRY",
    "IDEMPOTENT_METHODS",
    "PERMANENT_SERVER_STATUSES",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerRegistry",
    "CircuitCall",
    "CircuitOpenError",
    "CircuitState",
    "ResilientTransport",
    "RetryPolicy",
    "is_failure_status",
    "is_replayable",
    "may_repeat",
    "origin_of",
    "request_was_sent",
    "resilient_async_client",
    "resilient_transport",
    "retry_after_seconds",
]
