import time
import uuid
from collections.abc import Awaitable, Callable

import structlog
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp, Receive, Scope, Send

logger = structlog.get_logger(__name__)

REQUEST_ID_HEADER = "X-Request-ID"


class RequestIDMiddleware:
    """
    ASGI middleware that assigns a unique request ID to every inbound request,
    binds it (plus method / path / client IP) to the structlog context so every
    log emitted during the request carries those fields automatically, logs a
    structured entry on completion with status code and wall-clock duration, and
    echoes the request ID back to the caller via X-Request-ID response header.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        request = Request(scope)

        request_id = request.headers.get(REQUEST_ID_HEADER) or str(uuid.uuid4())
        client_host = request.client.host if request.client else "unknown"

        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            client=client_host,
        )

        logger.info(
            "request.started",
            query=str(request.url.query) or None,
        )

        status_code = 500
        start = time.perf_counter()

        async def send_with_header(message: object) -> None:
            nonlocal status_code
            if isinstance(message, dict) and message.get("type") == "http.response.start":
                status_code = message.get("status", 500)  # type: ignore[assignment]
                headers = list(message.get("headers", []))
                headers.append(
                    (REQUEST_ID_HEADER.lower().encode(), request_id.encode())
                )
                message = {**message, "headers": headers}
            await send(message)  # type: ignore[arg-type]

        try:
            await self.app(scope, receive, send_with_header)
        finally:
            duration_ms = round((time.perf_counter() - start) * 1000, 2)
            logger.info(
                "request.completed",
                status_code=status_code,
                duration_ms=duration_ms,
            )
            structlog.contextvars.clear_contextvars()


def make_request_id_handler(
    request: Request,
) -> Callable[[Request], Awaitable[Response]]:
    """Utility that returns the bound request_id for the current request.

    Useful in route handlers that need to expose the request ID to callers.
    """

    async def _handler(_: Request) -> Response:  # pragma: no cover
        return Response(
            content=structlog.contextvars.get_contextvars().get("request_id", ""),
            media_type="text/plain",
        )

    return _handler
