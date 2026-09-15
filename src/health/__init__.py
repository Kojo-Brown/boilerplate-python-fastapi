"""Liveness and readiness probes with real dependency checks.

`src/health/router.py` has the endpoints and what each one is for,
`src/health/base.py` the contract a check implements, `src/health/registry.py`
how they are run, and `src/health/wiring.py` which ones this configuration
asks for. `docs/health.md` is the operator's view: probe manifests, what each
status means, and how to add a check.
"""

from src.health.base import (
    CheckOutcome,
    CheckStatus,
    Criticality,
    HealthCheck,
    ReadinessReport,
    ReadinessStatus,
    aggregate,
)
from src.health.checks import DatabaseCheck, RedisCheck, redact_url
from src.health.registry import (
    DEFAULT_CACHE_TTL_SECONDS,
    DEFAULT_TIMEOUT_SECONDS,
    DuplicateCheckNameError,
    HealthRegistry,
)
from src.health.router import (
    CheckReport,
    LivenessResponse,
    ReadinessResponse,
    router,
)
from src.health.wiring import (
    RedisUse,
    build_redis_checks,
    build_registry,
    get_health_registry,
    redis_uses,
)

__all__ = [
    "DEFAULT_CACHE_TTL_SECONDS",
    "DEFAULT_TIMEOUT_SECONDS",
    "CheckOutcome",
    "CheckReport",
    "CheckStatus",
    "Criticality",
    "DatabaseCheck",
    "DuplicateCheckNameError",
    "HealthCheck",
    "HealthRegistry",
    "LivenessResponse",
    "ReadinessReport",
    "ReadinessResponse",
    "ReadinessStatus",
    "RedisCheck",
    "RedisUse",
    "aggregate",
    "build_redis_checks",
    "build_registry",
    "get_health_registry",
    "redact_url",
    "redis_uses",
    "router",
]
