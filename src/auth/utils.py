"""Issuing and verifying the tokens this API authenticates with.

`verify_access_token` is here rather than in `src/auth/dependencies.py` because
it now has two callers with nothing else in common. The HTTP path resolves a
bearer header per request; `src/ws/auth.py` resolves one credential per
*connection*, before the WebSocket handshake completes. What counts as a valid
access token has to be the same answer in both, and a second copy of the claim
checks is how one of them ends up accepting a refresh token or a `sub` the
database cannot look up.

The function raises `InvalidAccessTokenError` rather than `UnauthorizedError`,
which is the other half of the same point: a 401 with a `WWW-Authenticate`
header is the *HTTP* rendering of this failure, and a WebSocket has no status
line to put it in. The transport decides how to say no; this module only
decides whether the answer is no.
"""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from jose import JWTError, jwt

from src.auth.password import hash_password, needs_rehash, verify_password
from src.config import settings

__all__ = [
    "AccessTokenClaims",
    "InvalidAccessTokenError",
    "hash_password",
    "needs_rehash",
    "verify_access_token",
    "verify_password",
]


def create_access_token(user_id: str, email: str, role: str) -> str:
    expire = datetime.now(UTC) + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    payload = {
        "sub": user_id,
        "email": email,
        "role": role,
        "type": "access",
        "exp": expire,
    }
    return jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def create_refresh_token(user_id: str, jti: str) -> tuple[str, datetime]:
    expire = datetime.now(UTC) + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
    payload = {
        "sub": user_id,
        "jti": jti,
        "type": "refresh",
        "exp": expire,
    }
    token = jwt.encode(payload, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return token, expire


def decode_token(token: str) -> dict[str, object]:
    try:
        return jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
    except JWTError as exc:
        raise ValueError(f"Invalid token: {exc}") from exc


class InvalidAccessTokenError(ValueError):
    """The presented string is not a usable access token for this API.

    A `ValueError` so that callers already catching one from `decode_token`
    keep working; a distinct type so that a caller which wants to tell "the
    signature did not verify" from "this is a refresh token" can.
    """


@dataclass(frozen=True, slots=True)
class AccessTokenClaims:
    """The claims of a verified access token, in the types they stand for.

    Frozen: this is evidence about a credential that was already presented, and
    a caller that could edit it would be editing what the caller after it
    believes was proven.

    Args:
        subject: The `sub` claim, parsed as the user id it names. Parsed here
            rather than passed on as a string so a signed token carrying a
            `sub` that is not a UUID — one issued by an older service, say —
            is an authentication failure rather than a database error.
        email: The `email` claim, or `""` when the token predates it. Not
            authoritative: the row is.
        role: The `role` claim, on the same terms.
        expires_at: The `exp` claim as an aware UTC datetime.

            This is the field the WebSocket endpoint exists to read. A request
            is over long before its token is, so nothing on the HTTP path ever
            has to care that `exp` is a moment rather than a check; a
            connection routinely outlives the credential that opened it, and
            an endpoint that verifies `exp` only at the handshake grants an
            access that does not end. See `src/ws/connection.py`.
    """

    subject: uuid.UUID
    email: str
    role: str
    expires_at: datetime


def verify_access_token(token: str) -> AccessTokenClaims:
    """Verify `token` and return its claims.

    Signature and expiry are checked by `decode_token`; what is left here is
    everything a *signed* token can still be wrong about.

    Raises:
        InvalidAccessTokenError: the signature or expiry did not verify, the
            token is not of type `access`, or `sub` is absent or unparseable.
    """
    try:
        payload = decode_token(token)
    except ValueError as exc:
        raise InvalidAccessTokenError("Invalid or expired token") from exc

    # A refresh token is signed by the same key and would otherwise verify.
    # Accepting one here would make a credential deliberately given a long life
    # — and stored by clients accordingly — usable as if it were the short-lived
    # one, which is the whole distinction between the two.
    if payload.get("type") != "access":
        raise InvalidAccessTokenError("Invalid token type")

    raw_subject = payload.get("sub")
    if not isinstance(raw_subject, str):
        raise InvalidAccessTokenError("Token is missing a subject")
    try:
        subject = uuid.UUID(raw_subject)
    except ValueError as exc:
        raise InvalidAccessTokenError("Token subject is not a valid user id") from exc

    raw_expiry = payload.get("exp")
    if not isinstance(raw_expiry, int | float):
        # `decode_token` rejects an *expired* token but tolerates one with no
        # `exp` at all, which is a credential that never stops working. Refused
        # here rather than defaulted to "now", because a token this shape is a
        # bug in whatever issued it and silently treating it as expired would
        # report that as an ordinary session timeout.
        raise InvalidAccessTokenError("Token has no expiry")

    email = payload.get("email")
    role = payload.get("role")
    return AccessTokenClaims(
        subject=subject,
        email=email if isinstance(email, str) else "",
        role=role if isinstance(role, str) else "",
        expires_at=datetime.fromtimestamp(raw_expiry, tz=UTC),
    )
