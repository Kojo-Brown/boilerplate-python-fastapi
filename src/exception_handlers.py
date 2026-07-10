import structlog
from fastapi import Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
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


async def app_exception_handler(
    request: Request, exc: AppException
) -> JSONResponse:
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
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=_error_body(
            "VALIDATION_ERROR",
            "Request validation failed",
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            field_errors,
        ),
    )


async def unhandled_exception_handler(
    request: Request, exc: Exception
) -> JSONResponse:
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
