"""Backend-agnostic idempotency contract.

Everything here is free of redis, starlette and the settings object, so a store
can be written against it without inheriting any of the three. The concrete
stores live in `redis_store.py` and `memory.py`; `factory.py` chooses between
them, and `src/middleware/idempotency.py` is the only caller.

The model is deliberately two-phase. A request first *reserves* its key, then
either *completes* it with the response that was produced or *releases* it so a
retry may run. A single-phase "write the response when you have it" store
cannot answer the question that matters most — *is an identical request already
in flight?* — and answering it wrongly is how a double-submit becomes two
charges.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Final, Protocol, runtime_checkable

from src.exceptions import (
    AppException,
    BadRequestError,
    ConflictError,
    UnprocessableEntityError,
)

IDEMPOTENCY_KEY_HEADER: Final[str] = "Idempotency-Key"

# Set on replays only. A client that cannot tell a replay from a fresh execution
# cannot tell "my retry worked" from "my retry ran the work twice", which is the
# one thing this middleware exists to make knowable.
IDEMPOTENCY_REPLAYED_HEADER: Final[str] = "Idempotency-Replayed"

MAX_KEY_LENGTH: Final[int] = 255

# Printable ASCII with no space. The key is an opaque token as far as this API
# is concerned, but it ends up in a Redis key, a log line and an error body, so
# control characters and whitespace are refused rather than escaped in three
# places. RFC-draft guidance is to send a UUID; anything that survives this
# pattern is accepted.
_KEY_RE: Final[re.Pattern[str]] = re.compile(r"^[\x21-\x7e]+$")


class IdempotencyKeyInvalidError(BadRequestError):
    """The header was present but is not a usable key."""

    error_code = "IDEMPOTENCY_KEY_INVALID"


class IdempotencyKeyInProgressError(ConflictError):
    """An identical key is currently executing.

    409 rather than a queued wait: holding the second request open until the
    first finishes turns a client's double-submit into two occupied server
    workers, and the client already has to handle a retry-later answer.
    """

    error_code = "IDEMPOTENCY_KEY_IN_PROGRESS"
    headers = {"Retry-After": "1"}

    def __init__(self, key: str) -> None:
        super().__init__(
            "A request with this Idempotency-Key is still in progress.",
            details={"idempotency_key": key},
        )


class IdempotencyKeyReusedError(UnprocessableEntityError):
    """The key was seen before, attached to a different request.

    422 follows the `Idempotency-Key` header draft: the key is syntactically
    fine, so this is not a 400, and the request is refused rather than being
    answered with the *other* request's response — replaying a response for a
    payload the caller never sent is worse than an error.
    """

    error_code = "IDEMPOTENCY_KEY_REUSED"

    def __init__(self, key: str) -> None:
        super().__init__(
            "This Idempotency-Key was already used for a different request.",
            details={"idempotency_key": key},
        )


class IdempotencyStoreUnavailableError(AppException):
    """The store could not be reached, or answered with something unreadable.

    503 by default, because the alternative — serving the request anyway — is
    exactly the double-execution the caller asked to be protected from. See
    `IDEMPOTENCY_FAIL_OPEN` for the deployments that would rather take that
    risk than take the outage.
    """

    status_code = 503
    error_code = "IDEMPOTENCY_STORE_UNAVAILABLE"
    headers = {"Retry-After": "1"}

    def __init__(
        self, message: str = "Idempotency store unavailable", details: object = None
    ) -> None:
        super().__init__(message, details)


@dataclass(frozen=True, slots=True)
class StoredResponse:
    """A response captured verbatim, ready to be replayed byte for byte.

    Headers are a tuple of pairs rather than a mapping because `Set-Cookie` and
    friends legally repeat, and collapsing them into a dict would silently drop
    all but the last. They are stored lowercase, as ASGI delivers them.
    """

    status_code: int
    headers: tuple[tuple[str, str], ...]
    body: bytes


@dataclass(frozen=True, slots=True)
class IdempotencyRecord:
    """What the store holds against a key.

    `response is None` means *reserved but not finished*: some request claimed
    the key and has not reported an outcome yet. That state is the whole reason
    the record exists rather than the response being stored on its own.
    """

    fingerprint: str
    response: StoredResponse | None = None

    @property
    def in_progress(self) -> bool:
        return self.response is None


@runtime_checkable
class IdempotencyStore(Protocol):
    """The operations the middleware needs from a store.

    Narrow on purpose. There is no `update`, no scan and no way to list keys:
    everything an idempotent request needs is reachable from a single key, and
    a broader surface would be one more thing every backend has to get right.

    Implementations own their own TTLs. The middleware never passes one,
    because the two lifetimes involved — how long a reservation may sit
    unfinished, and how long a finished response stays replayable — are
    properties of the deployment's storage, not of the request in hand.
    """

    @property
    def name(self) -> str:
        """Short identifier, e.g. `"redis"`. Used in logs."""
        ...

    async def reserve(self, key: str, fingerprint: str) -> IdempotencyRecord | None:
        """Atomically claim `key` for a first execution.

        Returns `None` when the claim succeeded and the caller now owns the
        key. Returns the record that is already stored otherwise — which the
        caller inspects to tell a concurrent execution (`in_progress`) from a
        finished one it may replay.

        Atomicity is the requirement, not a nicety: a `get`-then-`set` pair
        lets two simultaneous retries both see nothing and both execute, which
        is the failure this whole module exists to prevent.
        """
        ...

    async def complete(self, key: str, record: IdempotencyRecord) -> None:
        """Store the finished record, replacing the reservation."""
        ...

    async def release(self, key: str) -> None:
        """Drop the reservation so a retry may execute. Idempotent itself."""
        ...

    async def get(self, key: str) -> IdempotencyRecord | None:
        """Return the stored record, or `None` if the key is unknown."""
        ...

    async def close(self) -> None:
        """Release any connections held. Called once from the app lifespan."""
        ...


def validate_idempotency_key(key: str) -> str:
    """Return `key` unchanged if it is usable, else raise.

    Called before the key reaches the store, so a malformed key is a 400 the
    client can fix rather than a storage error that looks like an outage.
    """
    if not key:
        raise IdempotencyKeyInvalidError("Idempotency-Key must not be empty.")

    if len(key) > MAX_KEY_LENGTH:
        raise IdempotencyKeyInvalidError(
            f"Idempotency-Key must be at most {MAX_KEY_LENGTH} characters.",
            details={"length": len(key)},
        )

    if not _KEY_RE.match(key):
        raise IdempotencyKeyInvalidError(
            "Idempotency-Key must contain only printable, non-space ASCII."
        )

    return key


def request_fingerprint(
    *, method: str, path: str, query: bytes, body: bytes, content_type: str
) -> str:
    """Hash the parts of a request that make it the *same* request.

    Two requests carrying one key must agree on all of these before one may be
    answered with the other's stored response. Headers beyond `Content-Type`
    are excluded deliberately: a retry through a different proxy, with a fresh
    `X-Request-ID` or a re-issued bearer token, is still the same request, and
    fingerprinting the whole header set would turn every such retry into a 422.

    `Content-Type` is in because it changes how the same bytes are parsed —
    the identical body read as JSON and as form data is two different requests.

    Length-prefixing each field keeps the concatenation unambiguous, so
    `POST /a/b` with an empty body cannot collide with `POST /a` whose body
    happens to start with `/b`.
    """
    digest = hashlib.sha256()
    for part in (
        method.upper().encode(),
        path.encode(),
        query,
        content_type.encode(),
        body,
    ):
        digest.update(str(len(part)).encode())
        digest.update(b"\x00")
        digest.update(part)
    return digest.hexdigest()


def scope_fingerprint(authorization: str | None) -> str:
    """Derive the namespace a key lives in from the caller's credential.

    Keys are chosen by clients, so two callers will eventually pick the same
    one. Without a per-caller namespace, the second would be handed the first's
    stored response — a cross-account data leak dressed up as a retry. Hashing
    the credential (rather than storing it) keeps bearer tokens out of Redis
    keys and out of any log line that echoes one.

    Requests with no credential share the `anon` namespace. They are still
    protected by the fingerprint — a replay requires an identical method, path,
    query, content type and body — but two *genuinely different* anonymous
    callers who pick the same key and send byte-identical requests would share
    a response. Anything that must not be shared that way needs a credential,
    which every non-public route here already requires.
    """
    if not authorization:
        return "anon"
    return hashlib.sha256(authorization.encode()).hexdigest()[:32]


def storage_key(scope: str, key: str) -> str:
    """Combine the caller namespace and the client's key into a store key."""
    return f"{scope}:{key}"
