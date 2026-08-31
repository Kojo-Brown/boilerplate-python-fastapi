"""Credential extraction and verification, over real `WebSocket` scopes.

`WebSocket` is constructed from a scope directly rather than faked. Everything
under test here — `headers`, `query_params`, `scope["subprotocols"]` — is
starlette's own parsing of an ASGI handshake, and a fake would be asserting
against this file's idea of that parsing rather than against starlette's.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from jose import jwt
from starlette.websockets import WebSocket

from src.auth.utils import create_access_token, create_refresh_token
from src.config import settings
from src.models.user import User
from src.ws.auth import (
    AUTH_SUBPROTOCOL,
    UserLookup,
    WebSocketAuthError,
    authenticate,
    extract_credential,
)

#: Obviously not a credential. Used where the token's *content* is irrelevant
#: because extraction fails before anything verifies it.
PLACEHOLDER_TOKEN = "not-a-real-token"


def handshake(
    *,
    headers: list[tuple[bytes, bytes]] | None = None,
    subprotocols: list[str] | None = None,
    query: str = "",
) -> WebSocket:
    """A `WebSocket` over a handshake scope, with no I/O behind it."""
    scope = {
        "type": "websocket",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "scheme": "ws",
        "server": ("testserver", 80),
        "path": "/api/v1/ws",
        "raw_path": b"/api/v1/ws",
        "root_path": "",
        "query_string": query.encode(),
        "headers": [(b"host", b"testserver"), *(headers or [])],
        "subprotocols": subprotocols or [],
        "client": ("127.0.0.1", 5000),
    }

    async def receive() -> dict[str, str]:  # pragma: no cover - never awaited
        return {"type": "websocket.connect"}

    async def send(message: object) -> None:  # pragma: no cover - never awaited
        return None

    return WebSocket(scope, receive=receive, send=send)


def lookup_returning(user: User | None) -> UserLookup:
    async def _lookup(_: uuid.UUID) -> User | None:
        return user

    return _lookup


def token_for(user: User) -> str:
    return create_access_token(str(user.id), user.email, user.role)


class TestTheHeaderCarrier:
    def test_a_bearer_header_is_accepted_and_echoes_no_subprotocol(self) -> None:
        """Non-browser clients can set headers, so nothing has to be negotiated."""
        socket = handshake(headers=[(b"authorization", b"Bearer abc.def.ghi")])

        assert extract_credential(socket) == ("abc.def.ghi", None)

    def test_the_scheme_is_matched_case_insensitively(self) -> None:
        """RFC 9110 §11.1 makes the scheme token case-insensitive."""
        socket = handshake(headers=[(b"authorization", b"bEaReR abc")])

        assert extract_credential(socket)[0] == "abc"

    @pytest.mark.parametrize(
        "value", [b"Bearer", b"Bearer ", b"Basic abc", b"abc.def.ghi"]
    )
    def test_a_header_that_is_not_a_bearer_credential_is_refused(
        self, value: bytes
    ) -> None:
        socket = handshake(headers=[(b"authorization", value)])

        with pytest.raises(WebSocketAuthError, match="Bearer"):
            extract_credential(socket)


class TestTheSubprotocolCarrier:
    def test_the_token_is_taken_from_after_the_tag(self) -> None:
        socket = handshake(subprotocols=[AUTH_SUBPROTOCOL, "abc.def.ghi"])

        assert extract_credential(socket) == ("abc.def.ghi", AUTH_SUBPROTOCOL)

    def test_the_tag_is_echoed_and_the_token_never_is(self) -> None:
        """Selecting the token would put the credential in a response header."""
        _, echoed = extract_credential(
            handshake(subprotocols=[AUTH_SUBPROTOCOL, "secret.jwt.value"])
        )

        assert echoed == AUTH_SUBPROTOCOL

    def test_the_tag_may_arrive_after_other_offers(self) -> None:
        """A client is free to offer real subprotocols alongside it."""
        socket = handshake(subprotocols=["graphql-ws", AUTH_SUBPROTOCOL, "tok"])

        assert extract_credential(socket) == ("tok", AUTH_SUBPROTOCOL)

    def test_the_position_is_the_contract_rather_than_the_shape(self) -> None:
        """Not "whichever entry looks like a JWT".

        Searching for a token-shaped string would make the position meaningless
        and would authenticate a client that put its token in a slot a later
        protocol version means to use for something else.
        """
        socket = handshake(subprotocols=["a.b.c", AUTH_SUBPROTOCOL])

        with pytest.raises(WebSocketAuthError, match="without a token after it"):
            extract_credential(socket)

    def test_an_empty_token_slot_is_refused(self) -> None:
        socket = handshake(subprotocols=[AUTH_SUBPROTOCOL, ""])

        with pytest.raises(WebSocketAuthError):
            extract_credential(socket)

    def test_a_header_wins_when_both_are_present(self) -> None:
        socket = handshake(
            headers=[(b"authorization", b"Bearer from-header")],
            subprotocols=[AUTH_SUBPROTOCOL, "from-subprotocol"],
        )

        assert extract_credential(socket) == ("from-header", None)


class TestTheQueryStringIsNotACarrier:
    def test_a_token_in_the_query_string_does_not_authenticate(self) -> None:
        """The popular design, refused: a URL is logged by every hop.

        Pinned as a test rather than left to the docstring, because "it works
        anyway if you also send it properly" is exactly how a rejected
        mechanism comes back one refactor later.
        """
        socket = handshake(query=f"token={PLACEHOLDER_TOKEN}")

        with pytest.raises(WebSocketAuthError, match="query"):
            extract_credential(socket)

    def test_the_refusal_says_what_to_do_instead(self) -> None:
        with pytest.raises(WebSocketAuthError) as caught:
            extract_credential(handshake(query=f"token={PLACEHOLDER_TOKEN}"))

        assert AUTH_SUBPROTOCOL in caught.value.reason
        assert "Authorization" in caught.value.reason

    def test_a_query_token_is_ignored_even_beside_a_valid_header(self) -> None:
        """It must not be read, not merely not preferred."""
        socket = handshake(
            headers=[(b"authorization", b"Bearer real")],
            query=f"token={PLACEHOLDER_TOKEN}",
        )

        assert extract_credential(socket)[0] == "real"


class TestNoCredentialAtAll:
    def test_a_bare_handshake_is_refused(self) -> None:
        with pytest.raises(WebSocketAuthError, match="No credential"):
            extract_credential(handshake())

    def test_unrelated_subprotocols_are_not_a_credential(self) -> None:
        with pytest.raises(WebSocketAuthError, match="No credential"):
            extract_credential(handshake(subprotocols=["graphql-ws"]))


class TestAuthenticate:
    async def test_a_valid_token_resolves_to_the_row(self, mock_user: User) -> None:
        socket = handshake(
            headers=[(b"authorization", f"Bearer {token_for(mock_user)}".encode())]
        )

        client = await authenticate(socket, lookup_returning(mock_user))

        assert client.user is mock_user
        assert client.subprotocol is None

    async def test_the_expiry_comes_back_with_it(self, mock_user: User) -> None:
        """The field the connection's deadline is built from."""
        socket = handshake(subprotocols=[AUTH_SUBPROTOCOL, token_for(mock_user)])

        client = await authenticate(socket, lookup_returning(mock_user))

        expected = datetime.now(UTC) + timedelta(
            minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES
        )
        assert abs((client.expires_at - expected).total_seconds()) < 5

    async def test_a_garbage_token_is_refused(self, mock_user: User) -> None:
        socket = handshake(headers=[(b"authorization", b"Bearer not.a.jwt")])

        with pytest.raises(WebSocketAuthError, match="Invalid or expired"):
            await authenticate(socket, lookup_returning(mock_user))

    async def test_a_refresh_token_is_refused(self, mock_user: User) -> None:
        """Signed by the same key, and deliberately long-lived.

        Accepting one here would make the credential clients are told to store
        usable as if it were the short-lived one, which is the entire
        distinction between the two.
        """
        refresh, _ = create_refresh_token(str(mock_user.id), jti="fake-jti")
        socket = handshake(headers=[(b"authorization", f"Bearer {refresh}".encode())])

        with pytest.raises(WebSocketAuthError, match="Invalid token type"):
            await authenticate(socket, lookup_returning(mock_user))

    async def test_an_expired_token_is_refused(self, mock_user: User) -> None:
        expired = jwt.encode(
            {
                "sub": str(mock_user.id),
                "type": "access",
                "exp": datetime.now(UTC) - timedelta(minutes=1),
            },
            settings.SECRET_KEY,
            algorithm=settings.ALGORITHM,
        )
        socket = handshake(headers=[(b"authorization", f"Bearer {expired}".encode())])

        with pytest.raises(WebSocketAuthError, match="Invalid or expired"):
            await authenticate(socket, lookup_returning(mock_user))

    async def test_a_subject_that_is_not_a_user_id_is_refused_before_the_database(
        self, mock_user: User
    ) -> None:
        """Handing it to Postgres would make an unusable credential a 500."""
        token = create_access_token("not-a-uuid", "a@example.com", "user")
        socket = handshake(headers=[(b"authorization", f"Bearer {token}".encode())])

        async def _never_called(_: uuid.UUID) -> User | None:
            raise AssertionError("the database must not be reached")

        with pytest.raises(WebSocketAuthError, match="not a valid user id"):
            await authenticate(socket, _never_called)

    async def test_a_signed_token_naming_nobody_is_refused(
        self, mock_user: User
    ) -> None:
        socket = handshake(
            headers=[(b"authorization", f"Bearer {token_for(mock_user)}".encode())]
        )

        with pytest.raises(WebSocketAuthError, match="does not name a user"):
            await authenticate(socket, lookup_returning(None))

    async def test_a_deactivated_user_is_refused(self, mock_user: User) -> None:
        """The row is authoritative, not the token's claims.

        A user deactivated after their token was issued must not be able to
        open a connection that then outlives the deactivation by an hour.
        """
        token = token_for(mock_user)
        mock_user.is_active = False
        socket = handshake(headers=[(b"authorization", f"Bearer {token}".encode())])

        with pytest.raises(WebSocketAuthError, match="inactive"):
            await authenticate(socket, lookup_returning(mock_user))

    async def test_the_subprotocol_to_echo_survives_authentication(
        self, mock_user: User
    ) -> None:
        socket = handshake(subprotocols=[AUTH_SUBPROTOCOL, token_for(mock_user)])

        client = await authenticate(socket, lookup_returning(mock_user))

        assert client.subprotocol == AUTH_SUBPROTOCOL
