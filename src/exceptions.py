from fastapi import status


class AppException(Exception):
    """Base application exception. Subclass and set status_code + error_code."""

    status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR
    error_code: str = "INTERNAL_SERVER_ERROR"

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
    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    error_code = "UNPROCESSABLE_ENTITY"

    def __init__(
        self, message: str = "Unprocessable entity", details: object = None
    ) -> None:
        super().__init__(message, details)
