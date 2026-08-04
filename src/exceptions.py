from fastapi import status


class AppException(Exception):
    """Base application exception. Subclass and set status_code + error_code.

    ``headers`` lets a failure mode declare the response headers its status code
    is required to carry, so the edge never has to special-case a particular
    exception to get them right. The class already owns ``status_code``, so
    carrying the headers that status mandates belongs at the same level.
    """

    status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR
    error_code: str = "INTERNAL_SERVER_ERROR"
    headers: dict[str, str] | None = None

    def __init__(self, message: str, details: object = None) -> None:
        self.message = message
        self.details = details
        super().__init__(message)


class NotFoundError(AppException):
    status_code = status.HTTP_404_NOT_FOUND
    error_code = "NOT_FOUND"

    def __init__(
        self, message: str = "Resource not found", details: object = None
    ) -> None:
        super().__init__(message, details)


class BadRequestError(AppException):
    status_code = status.HTTP_400_BAD_REQUEST
    error_code = "BAD_REQUEST"

    def __init__(self, message: str = "Bad request", details: object = None) -> None:
        super().__init__(message, details)


class UnauthorizedError(AppException):
    status_code = status.HTTP_401_UNAUTHORIZED
    error_code = "UNAUTHORIZED"
    # RFC 9110 §11.6.1 requires every 401 to name the challenge scheme, and this
    # API only authenticates bearer tokens. Declaring it once here means no
    # route has to remember it.
    headers = {"WWW-Authenticate": "Bearer"}

    def __init__(self, message: str = "Unauthorized", details: object = None) -> None:
        super().__init__(message, details)


class ForbiddenError(AppException):
    status_code = status.HTTP_403_FORBIDDEN
    error_code = "FORBIDDEN"

    def __init__(self, message: str = "Forbidden", details: object = None) -> None:
        super().__init__(message, details)


class ConflictError(AppException):
    status_code = status.HTTP_409_CONFLICT
    error_code = "CONFLICT"

    def __init__(self, message: str = "Conflict", details: object = None) -> None:
        super().__init__(message, details)


class UnprocessableEntityError(AppException):
    status_code = status.HTTP_422_UNPROCESSABLE_CONTENT
    error_code = "UNPROCESSABLE_ENTITY"

    def __init__(
        self, message: str = "Unprocessable entity", details: object = None
    ) -> None:
        super().__init__(message, details)
