import structlog
from fastapi import Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from slowapi.errors import RateLimitExceeded
from starlette.exceptions import HTTPException as StarletteHTTPException

from src.exceptions import AppException

logger = structlog.get_logger(__name__)


def _error_body(
    error_code: str,
    message: str,
    status_code: int,
    details: object = None,
) -> dict[str, object]:
    body: dict[str, object] = {
        "error": error_code,
        "message": message,
        "status": status_code,
    }
    if details is not None:
        body["details"] = details
    return body


async def app_exception_handler(request: Request, exc: AppException) -> JSONResponse:
    logger.warning(
        "application_exception",
        error_code=exc.error_code,
        message=exc.message,
        status_code=exc.status_code,
        path=str(request.url.path),
    )
    return JSONResponse(
        status_code=exc.status_code,
        content=_error_body(exc.error_code, exc.message, exc.status_code, exc.details),
        headers=exc.headers,
    )


async def http_exception_handler(
    request: Request, exc: StarletteHTTPException
) -> JSONResponse:
    message = str(exc.detail) if exc.detail else "HTTP error"
    logger.warning(
        "http_exception",
        message=message,
        status_code=exc.status_code,
        path=str(request.url.path),
    )
    return JSONResponse(
        status_code=exc.status_code,
        content=_error_body("HTTP_ERROR", message, exc.status_code),
        headers=getattr(exc, "headers", None),
    )


async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    field_errors = [
        {
            "field": ".".join(str(loc) for loc in err["loc"] if loc != "body"),
            "message": err["msg"],
            "type": err["type"],
        }
        for err in exc.errors()
    ]
    logger.warning(
        "validation_error",
        errors=field_errors,
        path=str(request.url.path),
    )
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        content=_error_body(
            "VALIDATION_ERROR",
            "Request validation failed",
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            field_errors,
        ),
    )


async def rate_limit_exceeded_handler(
    request: Request, exc: RateLimitExceeded
) -> JSONResponse:
    """Return the standard error envelope plus the Retry-After header.

    slowapi's built-in handler only emits Retry-After when ``headers_enabled`` is
    set, which additionally requires every limited endpoint to take a ``Response``
    parameter. Handling it here keeps 429s in the same shape as every other error
    and gives clients a deterministic backoff without touching the routes.
    """
    # RateLimitExceeded always carries the Limit that tripped, but the attribute
    # is declared Optional, so fall back to the shortest sane backoff.
    item = exc.limit.limit if exc.limit is not None else None
    retry_after = item.get_expiry() if item is not None else 60
    description = str(item) if item is not None else "too many requests"

    logger.warning(
        "rate_limit_exceeded",
        limit=description,
        retry_after=retry_after,
        path=str(request.url.path),
    )
    return JSONResponse(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        content=_error_body(
            "RATE_LIMIT_EXCEEDED",
            f"Rate limit exceeded: {description}",
            status.HTTP_429_TOO_MANY_REQUESTS,
        ),
        headers={"Retry-After": str(retry_after)},
    )


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception(
        "unhandled_exception",
        path=str(request.url.path),
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=_error_body(
            "INTERNAL_SERVER_ERROR",
            "An unexpected error occurred",
            status.HTTP_500_INTERNAL_SERVER_ERROR,
        ),
    )
