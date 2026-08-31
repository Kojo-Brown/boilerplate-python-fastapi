from collections.abc import Callable, Coroutine
from typing import Any

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.utils import InvalidAccessTokenError, verify_access_token
from src.database import get_db
from src.exceptions import ForbiddenError, UnauthorizedError
from src.models.user import User
from src.repositories.user import UserRepository

_bearer = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
    db: AsyncSession = Depends(get_db),
) -> User:
    """Resolve the bearer token to the row it names, or refuse the request.

    The claim checks live in `src/auth/utils.py` because the WebSocket
    endpoint makes the same ones before its handshake completes; what stays
    here is the part that is specific to *this* transport — turning a refusal
    into the 401 envelope with the `WWW-Authenticate` challenge on it.
    """
    try:
        claims = verify_access_token(credentials.credentials)
    except InvalidAccessTokenError as exc:
        raise UnauthorizedError(str(exc)) from exc

    user = await UserRepository(db).get(claims.subject)

    if user is None:
        raise UnauthorizedError("User not found")

    if not user.is_active:
        raise ForbiddenError("Inactive user")

    return user


def require_role(*roles: str) -> Callable[..., Coroutine[Any, Any, User]]:
    """Build a dependency that admits only users holding one of ``roles``.

    The returned coroutine takes the authenticated user as its single argument,
    resolved by FastAPI via ``Depends(get_current_user)``.
    """

    async def _check_role(current_user: User = Depends(get_current_user)) -> User:
        if current_user.role not in roles:
            raise ForbiddenError("Insufficient permissions")
        return current_user

    return _check_role
