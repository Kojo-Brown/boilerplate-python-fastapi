"""Establishing the tenant before anything else in the request can run.

## Why a middleware and not a dependency

FastAPI resolves `Depends(get_db)` *before* `Depends(get_current_user)`,
because the latter needs the former. By the time a dependency could set the
tenant, the session that will carry the first query already exists — and if
the handler's first statement opens the transaction, the `begin` listener has
already read an empty context. A middleware runs before any of that, which is
the only place the ordering is guaranteed.

It also covers the requests that have no dependencies at all: a 404 from the
router, a 429 from the rate limiter, an idempotent replay served by
`IdempotencyMiddleware`. None of those reach a handler, and all of them can
touch the database.

## Where it sits in the stack

Added in `src/main.py` after `RequestIDMiddleware`, so it runs *inside* it:
a tenant refusal is a response that should be logged with a request id and
leave with an `X-Request-ID` header like every other. It runs *outside*
`IdempotencyMiddleware`, because a replayed response was cached under a key
scoped to the tenant that produced it, and resolving the tenant after the
cache lookup would be the wrong order.

## Why it renders its own error

Exception handlers are installed inside the middleware stack, so an
`AppException` raised here never reaches `app_exception_handler`. It is
rendered with `render_app_exception` — the same function the handler uses —
which is what keeps a tenant refusal the same JSON envelope as every other
error rather than Starlette's bare plaintext 500.
"""

from __future__ import annotations

import structlog
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send

from src.exception_handlers import render_app_exception
from src.exceptions import AppException
from src.tenancy.context import tenant_scope
from src.tenancy.resolver import resolve_tenant

logger = structlog.get_logger(__name__)

#: ASGI close code for a policy violation (RFC 6455 §7.4.1). A WebSocket has
#: no status line to put a 400 or a 403 in, so the handshake is closed before
#: it is accepted and uvicorn renders that to the client as HTTP 403.
WS_POLICY_VIOLATION = 1008


class TenantContextMiddleware:
    """Resolve the request's tenant and run the rest of it in that scope."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        header_name: str,
        trust_header: bool,
    ) -> None:
        self.app = app
        self.header_name = header_name
        self.trust_header = trust_header

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        headers = Headers(scope=scope)
        try:
            resolution = resolve_tenant(
                header_value=headers.get(self.header_name),
                authorization=headers.get("authorization"),
                trust_header=self.trust_header,
            )
        except AppException as exc:
            logger.warning(
                "tenant.refused",
                error_code=exc.error_code,
                message=exc.message,
            )
            await self._refuse(scope, receive, send, exc)
            return

        # Bound for the whole request rather than logged once, so that every
        # line a handler writes carries it. An incident in a multi-tenant
        # system starts with "which customer", and a log that answers it only
        # on the first line of each request is one `grep` away from useless.
        structlog.contextvars.bind_contextvars(
            tenant_id=str(resolution.tenant_id) if resolution.tenant_id else None,
            tenant_source=resolution.source,
        )

        with tenant_scope(resolution.tenant_id):
            await self.app(scope, receive, send)

    async def _refuse(
        self, scope: Scope, receive: Receive, send: Send, exc: AppException
    ) -> None:
        if scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": WS_POLICY_VIOLATION})
            return
        await render_app_exception(exc)(scope, receive, send)


__all__ = ["WS_POLICY_VIOLATION", "TenantContextMiddleware"]
