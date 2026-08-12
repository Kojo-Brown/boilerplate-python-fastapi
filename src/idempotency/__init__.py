"""Idempotent request handling: the store contract and its backends.

The middleware that uses all of this lives in `src/middleware/idempotency.py`,
next to the other ASGI middleware rather than here, so that "what is an
idempotency store" and "how does a request use one" stay separable.
"""

from src.idempotency.base import (
    IDEMPOTENCY_KEY_HEADER,
    IDEMPOTENCY_REPLAYED_HEADER,
    IdempotencyKeyInProgressError,
    IdempotencyKeyInvalidError,
    IdempotencyKeyReusedError,
    IdempotencyRecord,
    IdempotencyStore,
    IdempotencyStoreUnavailableError,
    StoredResponse,
    request_fingerprint,
    scope_fingerprint,
    storage_key,
    validate_idempotency_key,
)
from src.idempotency.factory import create_idempotency_store, get_idempotency_store
from src.idempotency.memory import InMemoryIdempotencyStore
from src.idempotency.redis_store import RedisIdempotencyStore

__all__ = [
    "IDEMPOTENCY_KEY_HEADER",
    "IDEMPOTENCY_REPLAYED_HEADER",
    "IdempotencyKeyInProgressError",
    "IdempotencyKeyInvalidError",
    "IdempotencyKeyReusedError",
    "IdempotencyRecord",
    "IdempotencyStore",
    "IdempotencyStoreUnavailableError",
    "InMemoryIdempotencyStore",
    "RedisIdempotencyStore",
    "StoredResponse",
    "create_idempotency_store",
    "get_idempotency_store",
    "request_fingerprint",
    "scope_fingerprint",
    "storage_key",
    "validate_idempotency_key",
]
