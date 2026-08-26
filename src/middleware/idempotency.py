"""`Idempotency-Key` middleware: dedupe unsafe requests, replay the response.

A client that never hears back from a POST has no safe move. Retrying may
charge a card twice; not retrying may lose the order. The `Idempotency-Key`
header is the client's half of the fix — a token it keeps stable across
retries — and this middleware is the server's half: the first request carrying
a key executes and has its response stored, and every later request carrying
the same key is answered from that store without the route running again.

## Why middleware rather than a dependency

A FastAPI dependency runs *inside* the route, so it can see the request but
not the response, and the response is the thing that has to be replayed. It
also cannot cover a route it was not added to, and the endpoint someone forgets
to decorate is exactly the one that will double-charge. The trade is that a
middleware cannot use `Depends`, so its store is injected at construction —
see `src/main.py`.

## Position in the stack

Starlette runs the *last* added middleware outermost, and this one is added
before `RequestIDMiddleware` so that it ends up **inside** it. Two consequences
are deliberate:

* Every log line emitted here carries the current request's `request_id`, so a
  replay is traceable to the retry that asked for it and not only to the
  original.
* `X-Request-ID` is stamped by the outer middleware *after* this one returns,
  so a replayed response carries the *replaying* request's id rather than a
  stale one. That header is therefore never part of a stored record.

## What is stored, and what is not

Responses are stored whole — status, headers, body — with two exceptions.
`Set-Cookie` is dropped (see `_STRIPPED_RESPONSE_HEADERS`), and 5xx/retryable
statuses are not stored at all: they release the reservation instead, because
a client retrying a 503 must be allowed to actually run.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from functools import partial
from typing import Final

import structlog
from starlette.requests import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from src.exception_handlers import render_app_exception
from src.exceptions import AppException
from src.idempotency.base import (
    IDEMPOTENCY_KEY_HEADER,
    IDEMPOTENCY_REPLAYED_HEADER,
    IdempotencyKeyInProgressError,
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
from src.structured.cancel import finalize

logger = structlog.get_logger(__name__)

# GET/HEAD/OPTIONS are already idempotent by definition, and their retries are
# the browser's business. Storing a response for them would be a cache, which
# is a different feature with different invalidation rules.
DEFAULT_METHODS: Final[frozenset[str]] = frozenset({"POST", "PATCH", "PUT", "DELETE"})

# Only the versioned API. Health probes and the OAuth redirect handlers below
# `/auth` are not things a client retries with a stored key.
DEFAULT_PATH_PREFIXES: Final[tuple[str, ...]] = ("/api/",)

# Statuses that tell the client to try again. Storing one would pin a transient
# failure to the key for the whole record TTL, so the retry it invites would be
# answered with the very error it is retrying — forever.
RETRYABLE_STATUSES: Final[frozenset[int]] = frozenset({408, 425, 429})

# A session cookie is a credential minted for one caller. Keys are namespaced by
# the caller's Authorization header, but an unauthenticated pair of requests
# shares the `anon` namespace, so replaying a stored `Set-Cookie` could hand one
# caller another's session. Nothing here sets cookies on a keyed method anyway —
# the OAuth routes that do are GETs — so dropping it costs nothing real.
_STRIPPED_RESPONSE_HEADERS: Final[frozenset[bytes]] = frozenset({b"set-cookie"})

# How long the release of a reservation is protected from cancellation while
# the request is already unwinding. A constant rather than a setting because
# it is not a policy anybody would tune: it exists only so a store that has
# stopped answering cannot hold shutdown open, and every value between "long
# enough for one Redis round-trip" and "short enough not to matter" behaves
# identically. Well under the reservation TTL, so overrunning it costs a held
# reservation that expires on its own rather than a hung process.
RELEASE_TIMEOUT_SECONDS: Final[float] = 5.0


@dataclass(frozen=True, slots=True)
class IdempotencyConfig:
    """Request-side policy. Storage lifetimes belong to the store, not here."""

    methods: frozenset[str] = DEFAULT_METHODS
    path_prefixes: tuple[str, ...] = DEFAULT_PATH_PREFIXES

    # Bodies are buffered to be fingerprinted, so the cap is a memory bound.
    # A request over it is passed straight through unprotected rather than
    # rejected: refusing a large upload because it carried an optional header
    # would be a worse failure than not deduplicating it. The event is logged.
    max_request_body_bytes: int = 1024 * 1024

    # Same bound on the way out. An oversized response is returned normally and
    # its reservation released, so a retry re-executes rather than being told
    # to wait for a record that was never written.
    max_response_body_bytes: int = 1024 * 1024

    # Serve the request anyway when the store is unreachable. Off by default:
    # a payments endpoint would rather return 503 than take a double charge.
    # Turn it on where losing the request is worse than executing it twice.
    fail_open: bool = False

    enabled: bool = True


class _ReplayReceive:
    """Feeds buffered ASGI messages back to the app, then defers to the socket.

    The middleware has to read the request body to fingerprint it, and a body
    can only be read once. Everything consumed is replayed here so the route
    downstream sees an untouched stream.
    """

    def __init__(self, buffered: list[Message], receive: Receive) -> None:
        self._pending = deque(buffered)
        self._receive = receive

    async def __call__(self) -> Message:
        if self._pending:
            return self._pending.popleft()
        return await self._receive()


class IdempotencyMiddleware:
    """Pure-ASGI implementation.

    ASGI rather than `BaseHTTPMiddleware` because this needs to buffer the
    request stream and rewrite the response stream, and `BaseHTTPMiddleware`
    interposes a task and a queue between the two to offer a `Request`/
    `Response` API this code would not use anyway.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        store: IdempotencyStore,
        config: IdempotencyConfig | None = None,
    ) -> None:
        self.app = app
        self.store = store
        self.config = config if config is not None else IdempotencyConfig()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not self.config.enabled:
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        if not self._applies_to(request):
            await self.app(scope, receive, send)
            return

        raw_key = request.headers.get(IDEMPOTENCY_KEY_HEADER)
        if raw_key is None:
            # The header is optional. Requiring it belongs to the routes that
            # cannot be safely retried without one, as a dependency, not to a
            # middleware that would break every other client at once.
            await self.app(scope, receive, send)
            return

        try:
            key = validate_idempotency_key(raw_key)
        except AppException as exc:
            await self._send_error(exc, scope, send)
            return

        buffered, body, truncated = await self._buffer_request(receive)
        downstream_receive = _ReplayReceive(buffered, receive)

        if truncated:
            logger.warning(
                "idempotency.request_too_large",
                limit=self.config.max_request_body_bytes,
            )
            await self.app(scope, downstream_receive, send)
            return

        fingerprint = request_fingerprint(
            method=request.method,
            path=request.url.path,
            query=request.url.query.encode(),
            body=body,
            content_type=request.headers.get("content-type", ""),
        )
        full_key = storage_key(
            scope_fingerprint(request.headers.get("authorization")), key
        )

        try:
            existing = await self.store.reserve(full_key, fingerprint)
        except IdempotencyStoreUnavailableError as exc:
            if not self.config.fail_open:
                logger.error("idempotency.store_unavailable", store=self.store.name)
                await self._send_error(exc, scope, send)
                return
            logger.warning("idempotency.fail_open", store=self.store.name)
            await self.app(scope, downstream_receive, send)
            return

        if existing is not None:
            await self._handle_existing(existing, key, fingerprint, scope, send)
            return

        await self._execute(scope, downstream_receive, send, full_key, key, fingerprint)

    def _applies_to(self, request: Request) -> bool:
        return request.method in self.config.methods and request.url.path.startswith(
            self.config.path_prefixes
        )

    async def _buffer_request(
        self, receive: Receive
    ) -> tuple[list[Message], bytes, bool]:
        """Read the request body, keeping every message for replay.

        Returns the messages consumed, the body assembled from them, and
        whether the body ran past the configured cap. On truncation the
        messages read so far are still replayed and the rest streams straight
        through — the client's upload must not be damaged by a decision to
        stop fingerprinting it.
        """
        messages: list[Message] = []
        body = bytearray()
        limit = self.config.max_request_body_bytes

        while True:
            message = await receive()
            messages.append(message)

            if message["type"] != "http.request":
                # http.disconnect: nothing more is coming.
                return messages, bytes(body), False

            body.extend(message.get("body", b""))
            if len(body) > limit:
                return messages, b"", True
            if not message.get("more_body", False):
                return messages, bytes(body), False

    async def _handle_existing(
        self,
        record: IdempotencyRecord,
        key: str,
        fingerprint: str,
        scope: Scope,
        send: Send,
    ) -> None:
        if record.fingerprint != fingerprint:
            # Checked before `in_progress` on purpose: a key attached to a
            # different payload is a client bug whether or not the first
            # request has finished, and 409-then-422 would make the answer
            # depend on timing.
            logger.warning("idempotency.key_reused", idempotency_key=key)
            await self._send_error(IdempotencyKeyReusedError(key), scope, send)
            return

        stored = record.response
        if stored is None:
            logger.info("idempotency.in_progress", idempotency_key=key)
            await self._send_error(IdempotencyKeyInProgressError(key), scope, send)
            return

        logger.info(
            "idempotency.replayed",
            idempotency_key=key,
            status_code=stored.status_code,
        )
        await self._send_stored(stored, key, send)

    async def _execute(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
        full_key: str,
        key: str,
        fingerprint: str,
    ) -> None:
        """Run the app once, forwarding its response while capturing a copy."""
        status_code = 500
        headers: list[tuple[bytes, bytes]] = []
        body = bytearray()
        oversized = False
        finished = False

        async def capturing_send(message: Message) -> None:
            nonlocal status_code, headers, body, oversized, finished

            if message["type"] == "http.response.start":
                status_code = int(message["status"])
                headers = [(bytes(k), bytes(v)) for k, v in message.get("headers", [])]
                message = {
                    **message,
                    "headers": [
                        *message.get("headers", []),
                        (IDEMPOTENCY_KEY_HEADER.lower().encode(), key.encode()),
                    ],
                }
            elif message["type"] == "http.response.body":
                if not oversized:
                    body.extend(message.get("body", b""))
                    if len(body) > self.config.max_response_body_bytes:
                        oversized = True
                        body.clear()
                if not message.get("more_body", False):
                    finished = True

            await send(message)

        try:
            await self.app(scope, receive, capturing_send)
        except BaseException:
            # Includes CancelledError: a client that disconnected mid-flight
            # has no response to store, and holding the reservation would
            # answer its retry with 409 for the whole reservation TTL.
            #
            # `finalize` rather than a bare `await`, because this is the one
            # `await` in the middleware that runs on a task somebody has
            # already cancelled. A second cancellation — a shutdown draining
            # its tasks, an enclosing `TaskGroup` aborting — lands in the
            # middle of the release and leaves exactly the held reservation
            # this block exists to avoid, and only under load. `finalize` also
            # cannot replace the exception being unwound, which a `protect`
            # here would: the cancellation it absorbs is re-armed on this task
            # instead, so the `raise` below is still the original.
            await finalize(
                partial(self._release, full_key),
                name="idempotency-release",
                timeout=RELEASE_TIMEOUT_SECONDS,
            )
            raise

        if not finished or oversized or status_code >= 500:
            await self._release(full_key)
            return
        if status_code in RETRYABLE_STATUSES:
            await self._release(full_key)
            return

        stored = StoredResponse(
            status_code=status_code,
            headers=tuple(
                (name.decode("latin-1"), value.decode("latin-1"))
                for name, value in headers
                if name.lower() not in _STRIPPED_RESPONSE_HEADERS
            ),
            body=bytes(body),
        )
        try:
            await self.store.complete(
                full_key, IdempotencyRecord(fingerprint=fingerprint, response=stored)
            )
        except IdempotencyStoreUnavailableError:
            # The response is already on the wire; there is nothing to fail.
            # The reservation will expire on its own and the retry will
            # re-execute, which is the pre-idempotency behaviour rather than a
            # new failure mode.
            logger.error("idempotency.store_failed_after_response", idempotency_key=key)

    async def _release(self, full_key: str) -> None:
        try:
            await self.store.release(full_key)
        except IdempotencyStoreUnavailableError:
            logger.error("idempotency.release_failed", store=self.store.name)

    async def _send_stored(self, stored: StoredResponse, key: str, send: Send) -> None:
        headers: list[tuple[bytes, bytes]] = [
            (name.encode("latin-1"), value.encode("latin-1"))
            for name, value in stored.headers
        ]
        headers.append((IDEMPOTENCY_KEY_HEADER.lower().encode(), key.encode()))
        headers.append((IDEMPOTENCY_REPLAYED_HEADER.lower().encode(), b"true"))

        await send(
            {
                "type": "http.response.start",
                "status": stored.status_code,
                "headers": headers,
            }
        )
        await send({"type": "http.response.body", "body": stored.body})

    async def _send_error(self, exc: AppException, scope: Scope, send: Send) -> None:
        """Render an `AppException` without the app's exception handlers.

        Those handlers live inside the router, which a middleware short-circuit
        never reaches, so the envelope is rendered here from the same helper
        they use. Anything else would give idempotency failures a shape no
        other error in this API has.
        """
        await render_app_exception(exc)(scope, _no_receive, send)


async def _no_receive() -> Message:  # pragma: no cover - never awaited
    """A `receive` for the error path.

    `Response.__call__` takes one but never reads it: only the streaming
    responses that watch for `http.disconnect` do. Returning a disconnect
    rather than raising keeps that an assumption this code survives being
    wrong about.
    """
    return {"type": "http.disconnect"}
