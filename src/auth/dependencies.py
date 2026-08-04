import uuid
from collections.abc import Callable, Coroutine
from typing import Any

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.utils import decode_token
from src.database import get_db
from src.exceptions import ForbiddenError, UnauthorizedError
from src.models.user import User
from src.repositories.user import UserRepository

_bearer = HTTPBearer()


def _subject_to_uuid(raw: object) -> uuid.UUID:
    """Read the ``sub`` claim as the primary key it is supposed to be.

    The claim is attacker-supplied up to the point the signature is verified,
    and a signed token issued by an older or different service can still carry a
    ``sub`` that is not a UUID. Handing that straight to the database asks
    Postgres to adjudicate it, which it does by raising — a 500 for what is
    plainly an unusable credential. Parsing it here keeps the answer a 401.
    """
    if not isinstance(raw, str):
        raise UnauthorizedError("Token is missing a subject")
    try:
        return uuid.UUID(raw)
    except ValueError as exc:
        raise UnauthorizedError("Token subject is not a valid user id") from exc


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
    db: AsyncSession = Depends(get_db),
) -> User:
    token = credentials.credentials
    try:
        payload = decode_token(token)
    except ValueError as exc:
        raise UnauthorizedError("Invalid or expired token") from exc

    if payload.get("type") != "access":
        raise UnauthorizedError("Invalid token type")

    user = await UserRepository(db).get(_subject_to_uuid(payload.get("sub")))

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
