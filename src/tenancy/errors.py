"""The three ways a request can fail to name exactly one tenant.

All three are refusals about *which* tenant, never about what the row
contained — a request that names a tenant correctly and asks for a row
belonging to another one is not an error at all. Row-level security removes
that row from the result, so the caller is told the row does not exist, which
is the only honest answer: confirming it exists elsewhere would make the
isolation boundary enumerable one 403 at a time.
"""

from fastapi import status

from src.exceptions import AppException


class TenantRequiredError(AppException):
    """The route needs a tenant and the request did not resolve to one.

    A 400 rather than a 403: nothing was refused, because nothing was asked
    on behalf of anybody. The client sent a request this API cannot route —
    no `X-Tenant-ID`, and no access token carrying a `tid` claim — and the fix
    is on its side rather than in a permission grant.
    """

    status_code = status.HTTP_400_BAD_REQUEST
    error_code = "TENANT_REQUIRED"

    def __init__(
        self, message: str = "No tenant in scope", details: object = None
    ) -> None:
        super().__init__(message, details)


class InvalidTenantError(AppException):
    """A tenant was named, but not in a form this API can use."""

    status_code = status.HTTP_400_BAD_REQUEST
    error_code = "INVALID_TENANT"

    def __init__(
        self, message: str = "Malformed tenant identifier", details: object = None
    ) -> None:
        super().__init__(message, details)


class TenantMismatchError(AppException):
    """The request named one tenant and its credential named another.

    The one case in this module that is genuinely a 403. A token is issued for
    a tenant and says so; a header asking for a different one is a request to
    act outside the grant, and answering it with the token's tenant instead —
    the "be liberal in what you accept" reading — would turn an attempted
    escalation into a silently successful ordinary request.
    """

    status_code = status.HTTP_403_FORBIDDEN
    error_code = "TENANT_MISMATCH"

    def __init__(
        self,
        message: str = "Tenant does not match the presented credential",
        details: object = None,
    ) -> None:
        super().__init__(message, details)


__all__ = ["InvalidTenantError", "TenantMismatchError", "TenantRequiredError"]
