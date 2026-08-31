"""The wire contract: what a frame carries, and what a close frame may say.

A WebSocket gives you an ordered, bidirectional stream of *frames* and nothing
else. There is no method, no status, no content type — so every application
that uses one invents a small protocol on top, and this module is that
protocol written down in one place instead of implied by a `match` statement
in a handler.

## Client to server, and why it is only ever a JSON object in a text frame

Four message types (`join`, `leave`, `publish`, `ping`), each a JSON object
with a `type`. Three refusals shape the decoder:

**A binary frame is not accepted.** The obvious permissiveness — decode
`bytes` as UTF-8 and carry on — is wrong at the browser end rather than here.
`WebSocket.send(str)` and `WebSocket.send(blob)` are different calls producing
different opcodes, and a client that reaches this endpoint over a binary frame
has a bug that a lenient server hides until the day something in the path
(a proxy, a load balancer's WebSocket inspection) treats the two differently.

**A JSON scalar or array is not a message.** `json.loads("4")` succeeds, and
the natural next line is `payload["type"]`, which raises `TypeError` on an
`int` and `TypeError` on a `list`. Anything that is not an object is refused
by the decoder rather than by an unhandled exception four frames later.

**A message is bounded before it is parsed.** `max_bytes` is checked against
the encoded frame, not the decoded object, and it is checked first: a
50-megabyte string of nested brackets costs the parser more than it costs the
sender, and the point of a ceiling is not to be reached after the work.

That last one has a limit this module cannot enforce. By the time a frame
reaches application code the ASGI server has already read and buffered it, so
the *effective* ceiling is uvicorn's `--ws-max-size` (16 MiB by default) and
`WS_MAX_MESSAGE_BYTES` can only be lower, never higher. A deployment that
means it sets both.

## Server to client

One object per frame, always with a `type`, and `error` is an ordinary message
rather than a close: a client that sends one bad frame has a bug in one code
path, not a connection that should be torn down under the rest of its work.
The codes are a closed vocabulary (`ErrorCode`) so a client can branch on them
without matching on prose.

## Close codes, and the 123 bytes nobody budgets for

A close frame's payload is **at most 125 bytes**, two of which are the code.
That leaves 123 bytes for the reason *after* UTF-8 encoding, and exceeding it
is not a truncated reason — RFC 6455 §5.5 makes the frame invalid, and the
`websockets` library raises rather than sending it, so an over-long reason
turns an orderly close into a dropped connection with a traceback. Every
reason here is a short constant, and `close_reason` enforces the budget for
anything assembled at runtime.

Codes in 4000–4999 are the private range, which is the only part of the space
an application may define. 1008 (policy violation) would be defensible for
most of these and is deliberately not used: it is one code for every reason,
so a client cannot tell "you flooded us" from "your token expired" — and those
two want opposite reconnect behaviour.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import IntEnum, StrEnum
from typing import Any, Final

#: Bytes available for a close reason once the two-byte code is deducted from
#: the 125-byte control-frame payload limit (RFC 6455 §5.5).
MAX_CLOSE_REASON_BYTES: Final[int] = 123


class CloseCode(IntEnum):
    """Close codes this endpoint sends, and what a client should do about each.

    Values below 4000 are the RFC's; the rest are this application's, in the
    private range reserved for exactly that.
    """

    #: The server is going away — shutdown, or the connection reached
    #: `WS_MAX_CONNECTION_SECONDS`. Reconnect immediately.
    GOING_AWAY = 1001

    #: An unexpected server-side failure. Reconnect with backoff.
    INTERNAL_ERROR = 1011

    #: The frame stream cannot be interpreted — a binary frame, or text that is
    #: not UTF-8. A client hitting this has a bug; reconnecting will not fix it.
    PROTOCOL_ERROR = 4400

    #: The access token behind this connection has expired. **Reconnect with a
    #: fresh token**, which is the whole reason this is distinct from 1001: the
    #: client has to refresh before retrying, and retrying without refreshing
    #: gets the same close forever.
    TOKEN_EXPIRED = 4401

    #: Nothing was received for `WS_IDLE_TIMEOUT_SECONDS`. Reconnect when there
    #: is something to say.
    IDLE_TIMEOUT = 4408

    #: The client ignored `rate_limited` errors and kept sending. Reconnect
    #: after a pause, and fix the send rate — an immediate reconnect is the
    #: same flood with a new socket.
    RATE_LIMITED = 4429

    #: The connection fell too far behind on *outbound* messages and could not
    #: be kept in sync. Reconnect and refetch state; see `src/ws/rooms.py`.
    OVERFLOW = 4430


class ClientMessageType(StrEnum):
    """Everything a client may send."""

    JOIN = "join"
    LEAVE = "leave"
    PUBLISH = "publish"

    #: Application-level, and not redundant with the protocol's own ping.
    #: A WebSocket ping/pong is handled by the ASGI *server* and there is no
    #: ASGI message type for either, so an application literally cannot see one
    #: — nor send one. A client that wants to prove this endpoint (rather than
    #: the load balancer in front of it) is still answering has to ask in band.
    PING = "ping"


class ServerMessageType(StrEnum):
    """Everything this endpoint sends."""

    #: First frame after the handshake. Carries the connection's own deadline.
    READY = "ready"
    JOINED = "joined"
    LEFT = "left"
    MESSAGE = "message"
    PONG = "pong"
    ERROR = "error"


class ErrorCode(StrEnum):
    """The closed vocabulary an `error` frame's `code` is drawn from."""

    #: Not JSON, not a JSON object, or missing a usable `type`.
    MALFORMED_MESSAGE = "malformed_message"

    #: A well-formed object naming a type this endpoint does not have.
    UNKNOWN_TYPE = "unknown_type"

    #: A required field is absent or the wrong shape.
    INVALID_FIELD = "invalid_field"

    #: The room name is not one this endpoint will accept — see `src/ws/rooms.py`.
    INVALID_ROOM = "invalid_room"

    #: Publishing to, or leaving, a room this connection has not joined.
    NOT_IN_ROOM = "not_in_room"

    #: `WS_MAX_ROOMS_PER_CONNECTION` reached.
    ROOM_LIMIT_REACHED = "room_limit_reached"

    #: The connection's own budget is spent. Carries `retry_after` in seconds.
    RATE_LIMITED = "rate_limited"

    #: The frame exceeded `WS_MAX_MESSAGE_BYTES`.
    MESSAGE_TOO_LARGE = "message_too_large"


class MalformedMessage(Exception):
    """A received frame could not be turned into a `ClientMessage`.

    Carries the `ErrorCode` to answer with, so the receive loop reports the
    failure without re-deciding what kind of failure it was.
    """

    def __init__(self, code: ErrorCode, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class ClientMessage:
    """One decoded inbound message.

    Frozen because it is passed to a handler that may hold it (a `publish`
    payload is fanned out to every member of a room) and a mutable message
    would let one recipient's handler alter what the next one receives.

    Args:
        type: The message type, already validated against `ClientMessageType`.
        room: The room named by a `join`, `leave` or `publish`. `None` for
            `ping`.
        data: The payload of a `publish`. Any JSON value including `null`,
            which is why its absence is `_MISSING` rather than `None` in the
            decoder.
    """

    type: ClientMessageType
    room: str | None = None
    data: Any = None


def _reject_non_finite(literal: str) -> float:
    """Refuse `NaN`/`Infinity` at the door rather than at the far end.

    `json.loads` accepts all three bare literals by default; `json.dumps`
    configured as `encode_server_message` configures it raises on all three.
    Left alone, that asymmetry is a live fault rather than a curiosity: a
    client publishes `{"data": NaN}`, it parses happily, and the *broadcast* is
    what fails — which, without care about where the encoding happens, is one
    sender costing every member of a room their connection. Refusing here makes
    it that client's error, reported to that client, before any other part of
    the process has seen the value.
    """
    raise MalformedMessage(
        ErrorCode.MALFORMED_MESSAGE,
        f"{literal} is not accepted: it is not JSON any other language's "
        "parser will read, so it is refused here rather than re-emitted.",
    )


def decode_client_message(raw: str, *, max_bytes: int) -> ClientMessage:
    """Parse one inbound text frame.

    Args:
        raw: The frame's text.
        max_bytes: Ceiling on the frame's UTF-8 length. Checked before parsing.

    Raises:
        MalformedMessage: any reason the frame is not a usable message.
        ValueError: `max_bytes` is not positive.
    """
    if max_bytes <= 0:
        raise ValueError(f"max_bytes must be positive, got {max_bytes}.")

    size = len(raw.encode("utf-8"))
    if size > max_bytes:
        raise MalformedMessage(
            ErrorCode.MESSAGE_TOO_LARGE,
            f"Message is {size} bytes; the limit is {max_bytes}.",
        )

    try:
        payload = json.loads(raw, parse_constant=_reject_non_finite)
    except ValueError as exc:
        # The parser's own message quotes the input's position but not its
        # content, so it is safe to pass on and is the only thing that tells a
        # client *where* its serialiser went wrong.
        raise MalformedMessage(
            ErrorCode.MALFORMED_MESSAGE, f"Message is not valid JSON: {exc}"
        ) from exc

    if not isinstance(payload, dict):
        raise MalformedMessage(
            ErrorCode.MALFORMED_MESSAGE,
            f"Message must be a JSON object, got {type(payload).__name__}.",
        )

    raw_type = payload.get("type")
    if not isinstance(raw_type, str):
        raise MalformedMessage(
            ErrorCode.MALFORMED_MESSAGE, "Message is missing a string 'type'."
        )
    try:
        message_type = ClientMessageType(raw_type)
    except ValueError as exc:
        known = ", ".join(sorted(t.value for t in ClientMessageType))
        raise MalformedMessage(
            ErrorCode.UNKNOWN_TYPE,
            f"Unknown type {raw_type!r}; expected one of {known}.",
        ) from exc

    if message_type is ClientMessageType.PING:
        return ClientMessage(type=message_type)

    room = payload.get("room")
    if not isinstance(room, str):
        raise MalformedMessage(
            ErrorCode.INVALID_FIELD, f"{message_type.value} requires a string 'room'."
        )

    if message_type is not ClientMessageType.PUBLISH:
        return ClientMessage(type=message_type, room=room)

    # `null` is a legitimate payload and `payload.get("data")` cannot tell it
    # from an absent key, so the membership test is the check.
    if "data" not in payload:
        raise MalformedMessage(
            ErrorCode.INVALID_FIELD, "publish requires a 'data' field."
        )
    return ClientMessage(type=message_type, room=room, data=payload["data"])


def encode_server_message(message: dict[str, Any]) -> str:
    """Serialise one outbound message.

    Compact separators because every byte here is multiplied by the size of a
    room, and `allow_nan=False` for the reason `src/sse/bridge.py` and
    `src/outbox/codec.py` both give: Python emits bare `NaN` and `Infinity`,
    which no other language's JSON parser accepts — including the browser's,
    where it is a `SyntaxError` in the client's `onmessage` rather than
    anything this server ever hears about.

    Raises:
        ValueError: the message contains a value JSON cannot represent, a
            non-finite float among them.
    """
    return json.dumps(message, allow_nan=False, separators=(",", ":"))


def error_message(
    code: ErrorCode, message: str, *, retry_after: float | None = None
) -> dict[str, Any]:
    """Build an `error` frame's body.

    `retry_after` is included only when there is one, so its presence is the
    signal that waiting is what fixes this — every other error is answered by
    sending something different, not by sending the same thing later.
    """
    body: dict[str, Any] = {
        "type": ServerMessageType.ERROR.value,
        "code": code.value,
        "message": message,
    }
    if retry_after is not None:
        body["retry_after"] = round(retry_after, 3)
    return body


def close_reason(text: str) -> str:
    """Truncate `text` to what a close frame can actually carry.

    Truncation on a UTF-8 *byte* boundary that is also a character boundary:
    slicing the string by `MAX_CLOSE_REASON_BYTES` characters is not the same
    thing on any non-ASCII reason, and slicing the encoded bytes can split a
    multi-byte sequence, which is a decode error at the other end rather than a
    shortened message.
    """
    encoded = text.encode("utf-8")
    if len(encoded) <= MAX_CLOSE_REASON_BYTES:
        return text
    return encoded[:MAX_CLOSE_REASON_BYTES].decode("utf-8", errors="ignore")


__all__ = [
    "MAX_CLOSE_REASON_BYTES",
    "ClientMessage",
    "ClientMessageType",
    "CloseCode",
    "ErrorCode",
    "MalformedMessage",
    "ServerMessageType",
    "close_reason",
    "decode_client_message",
    "encode_server_message",
    "error_message",
]
