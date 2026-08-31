"""Authenticating a WebSocket, which the browser API goes out of its way to prevent.

    const ws = new WebSocket(url, protocols)   // ← that is the entire API

There is no third argument. A browser's `WebSocket` constructor **cannot set a
request header**, so `Authorization: Bearer …` — the scheme the rest of this
API uses, and the one `src/auth/dependencies.py` implements — is unavailable to
exactly the clients this endpoint exists for. Every WebSocket authentication
design is a way around that sentence, and there are only three.

## The query string, which is the popular answer and is rejected here

`wss://host/ws?token=eyJ…` works everywhere and puts a live credential in the
one part of the request that is written down by everything it passes: the
server's access log, every proxy and load balancer in between, the browser's
own history, and — for any page that ever links onward — the `Referer` header.
Rotating a leaked token is cheap; discovering it leaked into six log
aggregators is not. A token in the query string is **not accepted** by this
endpoint, and `tests/test_ws_auth.py` pins that, because "it authenticates
anyway if you also send it properly" is how a rejected mechanism comes back.

## The first message after `accept()`, which is worse than it looks

Accept the connection, wait for a `{"type":"auth"}` frame, close if it does not
come. It reads cleanly and it inverts the property that makes authentication
useful: **the connection now exists before anyone has proven anything.** An
unauthenticated peer holds a socket, a task and a slot in whatever limit the
process has, for as long as the grace period — so the cheapest denial of
service against this endpoint becomes opening connections and saying nothing.
It also throws away the handshake's own failure channel: before `accept()` the
server can still answer with an HTTP status, and after it the only vocabulary
left is close codes.

## `Sec-WebSocket-Protocol`, which is what this endpoint uses

The constructor's second argument is a list of subprotocols, sent as a request
*header*, and a browser will send whatever strings it is given:

    new WebSocket(url, ["bearer.auth.v1", accessToken])

So the credential travels in a header after all, out of the URL and out of the
logs. Two obligations come with it. The server **must** select one of the
offered protocols or the handshake fails — and it must select the *tag*, never
the token, or the credential is echoed back in the response headers. And a JWT
has to be a legal subprotocol token: base64url plus `.` is, which is the only
reason this works without encoding it further.

Non-browser clients — service-to-service, the test suite, anything using
`httpx` or `websockets` directly — can set headers, so `Authorization` is
accepted too and is preferred when both are present.

## Refusing before the handshake completes, and what the client actually sees

Authentication runs *before* `accept()`, so a refusal is an HTTP response to
the upgrade request rather than a close frame on an established connection.

Two things about that were measured against uvicorn 0.51 and starlette 1.3
rather than assumed, because both are surprising:

**The close code is discarded.** `await websocket.close(code=4401)` before
accept does not deliver 4401 to anybody; the handshake fails with **HTTP 403**,
whatever code was passed. A client cannot distinguish "bad token" from "not
allowed" at this stage, and an endpoint whose error handling depends on that
distinction is relying on something the transport does not carry.

**The denial-response extension does not survive the round trip.** ASGI 2.4's
`websocket.http.response` is advertised in `scope["extensions"]` and starlette
implements `send_denial_response`, which would allow the same 401 JSON envelope
the rest of this API returns. Under uvicorn's `websockets-sansio` handler
(the default) the bytes do reach a plain HTTP client — a raw `httpx` request
sees `401` and the envelope — but the server also logs `ASGI callable returned
without completing handshake`, and the reference Python client fails with
`InvalidMessage: did not receive a valid HTTP response` rather than reporting
the status. An error channel that real clients cannot read is not an error
channel, so this endpoint closes instead and takes the 403.

The compensation is that a refusal is *logged* here with the reason, which is
where an operator looks anyway — the client end of a failed handshake was never
going to carry much.
"""

from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Final

import structlog
from starlette.websockets import WebSocket

from src.auth.utils import (
    AccessTokenClaims,
    InvalidAccessTokenError,
    verify_access_token,
)
from src.models.user import User

logger = structlog.get_logger(__name__)

#: The subprotocol a browser offers alongside its token, and the one this
#: server selects in reply. Versioned because it names a wire convention: a
#: later scheme that put something else in the second position would be
#: `bearer.auth.v2`, and old clients would keep working.
AUTH_SUBPROTOCOL: Final[str] = "bearer.auth.v1"

#: Query parameter deliberately *not* read. Named so the refusal can be
#: specific in the log — "you sent a token where this endpoint will not look"
#: is a far better operator experience than a bare 403.
REJECTED_QUERY_PARAM: Final[str] = "token"

#: Resolves a verified subject to the row it names, or `None`. A callable
#: rather than a `UserStore` because the endpoint must *not* hold a database
#: session for the life of a connection — see `src/api/v1/ws.py`.
UserLookup = Callable[[uuid.UUID], Awaitable[User | None]]


class WebSocketAuthError(Exception):
    """The connection may not be established.

    Args:
        reason: Operator-facing explanation. Logged, not sent: the handshake
            has no body to put it in and the close code is discarded (see the
            module docstring), so this exists for the server's logs.
    """

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True, slots=True)
class AuthenticatedClient:
    """A caller that proved who it is, and the terms it proved it on.

    Args:
        user: The row. Authoritative for identity and role — the token's own
            `email` and `role` claims are a snapshot from whenever it was
            issued, and a role revoked since then is revoked.
        claims: The verified token claims, carried for `expires_at`.
        subprotocol: The value to pass to `accept()`, or `None`. Not a
            constant: it must be `AUTH_SUBPROTOCOL` when the client offered it
            and `None` when it did not, because selecting a subprotocol the
            client never proposed fails the handshake at the client end.
    """

    user: User
    claims: AccessTokenClaims
    subprotocol: str | None

    @property
    def expires_at(self) -> datetime:
        """When the credential behind this connection stops being valid."""
        return self.claims.expires_at


def extract_credential(websocket: WebSocket) -> tuple[str, str | None]:
    """Find the bearer token on the handshake request.

    Returns the token and the subprotocol to echo — `AUTH_SUBPROTOCOL` when the
    token arrived that way, `None` when it came from a header.

    Raises:
        WebSocketAuthError: no credential, or one presented in a form this
            endpoint does not accept.
    """
    header = websocket.headers.get("authorization")
    if header:
        scheme, _, token = header.partition(" ")
        if scheme.lower() != "bearer" or not token.strip():
            raise WebSocketAuthError(
                "Authorization header is not a non-empty Bearer credential"
            )
        return token.strip(), None

    offered = list(websocket.scope.get("subprotocols") or ())
    if AUTH_SUBPROTOCOL in offered:
        index = offered.index(AUTH_SUBPROTOCOL)
        # Strictly the *next* one. Searching the list for "the entry that looks
        # like a JWT" would make the position meaningless and would happily
        # authenticate a client that sent its token in a slot a future protocol
        # version means to use for something else.
        if index + 1 < len(offered) and offered[index + 1]:
            return offered[index + 1], AUTH_SUBPROTOCOL
        raise WebSocketAuthError(
            f"Subprotocol {AUTH_SUBPROTOCOL!r} was offered without a token after it"
        )

    if REJECTED_QUERY_PARAM in websocket.query_params:
        raise WebSocketAuthError(
            f"Credential presented as the {REJECTED_QUERY_PARAM!r} query "
            "parameter, which this endpoint does not read: a token in a URL is "
            f"logged by every hop. Offer {AUTH_SUBPROTOCOL!r} and the token as "
            "subprotocols, or send an Authorization header."
        )

    raise WebSocketAuthError("No credential on the handshake request")


async def authenticate(websocket: WebSocket, lookup: UserLookup) -> AuthenticatedClient:
    """Resolve the handshake's credential to an active user.

    The same three questions `get_current_user` asks, in the same order and
    against the same `verify_access_token`, because a WebSocket that admitted a
    caller the HTTP API would refuse is a hole in the API rather than a feature
    of the transport.

    Raises:
        WebSocketAuthError: any of them answers no.
    """
    token, subprotocol = extract_credential(websocket)

    try:
        claims = verify_access_token(token)
    except InvalidAccessTokenError as exc:
        raise WebSocketAuthError(str(exc)) from exc

    user = await lookup(claims.subject)
    if user is None:
        raise WebSocketAuthError("Token subject does not name a user")
    if not user.is_active:
        raise WebSocketAuthError("User is inactive")

    return AuthenticatedClient(user=user, claims=claims, subprotocol=subprotocol)


__all__ = [
    "AUTH_SUBPROTOCOL",
    "REJECTED_QUERY_PARAM",
    "AuthenticatedClient",
    "UserLookup",
    "WebSocketAuthError",
    "authenticate",
    "extract_credential",
]
