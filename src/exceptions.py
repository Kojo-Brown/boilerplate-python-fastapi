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


class PreconditionFailedError(AppException):
    """The request's `If-Match` did not describe the resource's current state.

    Distinct from `ConflictError` on purpose. A 409 says the request conflicts
    with the resource's rules — a duplicate email, an order already refunded —
    and repeating it verbatim will fail the same way. A 412 says the request
    was fine but out of date: re-read, reapply, retry, and it succeeds. Folding
    the two together would tell a client to give up on the recoverable case.
    """

    status_code = status.HTTP_412_PRECONDITION_FAILED
    error_code = "PRECONDITION_FAILED"

    def __init__(
        self,
        message: str = "Precondition failed",
        details: object = None,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message, details)
        if headers is not None:
            # Shadows the class attribute for this instance. The header a 412
            # wants to carry is the resource's *current* ETag, which is a
            # per-occurrence value and cannot live on the class the way the
            # 401 challenge does.
            self.headers = headers


class PreconditionRequiredError(AppException):
    """An unsafe request arrived without the `If-Match` its route requires.

    RFC 6585 §3 exists for precisely this: without it the honest alternatives
    are to accept the blind write, or to answer 403/400 and leave the client
    guessing which of its headers the server wanted.
    """

    status_code = status.HTTP_428_PRECONDITION_REQUIRED
    error_code = "PRECONDITION_REQUIRED"

    def __init__(
        self, message: str = "Precondition required", details: object = None
    ) -> None:
        super().__init__(message, details)


class UnprocessableEntityError(AppException):
    status_code = status.HTTP_422_UNPROCESSABLE_CONTENT
    error_code = "UNPROCESSABLE_ENTITY"

    def __init__(
        self, message: str = "Unprocessable entity", details: object = None
    ) -> None:
        super().__init__(message, details)
