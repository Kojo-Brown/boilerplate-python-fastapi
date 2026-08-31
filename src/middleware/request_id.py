import time
import uuid
from collections.abc import Awaitable, Callable

import structlog
from starlette.requests import HTTPConnection, Request
from starlette.responses import Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

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

        # `HTTPConnection` rather than `Request`, because the latter asserts
        # `scope["type"] == "http"` in its constructor. This branch has always
        # said it handled `websocket` scopes and, until there was a WebSocket
        # route to send through it, never had to: the first connection to
        # `/api/v1/ws` failed the assertion and became a 500 the client saw as
        # a rejected upgrade. Everything used below — headers, url, client — is
        # on the base class and identical for both scope types.
        connection = HTTPConnection(scope)
        is_http = scope["type"] == "http"

        request_id = connection.headers.get(REQUEST_ID_HEADER) or str(uuid.uuid4())
        client_host = connection.client.host if connection.client else "unknown"

        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            request_id=request_id,
            # A WebSocket handshake is a GET whose point is the upgrade, so the
            # method says nothing a reader of these logs wants; the scope type
            # is what distinguishes a connection from a request.
            method=scope["method"] if is_http else "WEBSOCKET",
            path=connection.url.path,
            client=client_host,
        )

        logger.info(
            "request.started",
            query=str(connection.url.query) or None,
        )

        status_code = 500
        start = time.perf_counter()

        async def send_with_header(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = int(message.get("status", 500))
                headers = list(message.get("headers", []))
                headers.append(
                    (REQUEST_ID_HEADER.lower().encode(), request_id.encode())
                )
                message = {**message, "headers": headers}
            elif message["type"] == "websocket.accept":
                # There is no status line on an accepted upgrade. 101 is what
                # the server puts on the wire, and recording it keeps the
                # completion log one shape rather than two.
                status_code = 101
            elif message["type"] == "websocket.close" and status_code == 500:
                # Closed before it was ever accepted, which uvicorn answers as
                # HTTP 403 — the close code passed by the application is
                # discarded at that stage, so 403 is what the client saw and
                # 403 is what belongs in the log. A close *after* accept leaves
                # the 101 alone: the connection did open.
                status_code = 403
            await send(message)

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
