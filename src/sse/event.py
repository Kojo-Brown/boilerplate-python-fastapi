"""The `text/event-stream` wire format, and the four ways it is got wrong.

An SSE frame looks trivial — `data: hello`, a blank line, done — and that
appearance is the problem. The format has no length prefix and no escaping
mechanism: fields are terminated by line breaks, so **any line break inside a
value is not an error, it is a frame boundary**. Every bug below is silent on
the server and arrives at the client as data that parsed cleanly into something
wrong.

## 1. A newline in `data` splits the record

`data: {"body": "line one\\nline two"}` is two lines on the wire. The parser
concatenates multi-line `data` fields with `\\n` between them, so a *JSON*
payload split this way happens to survive re-joining — and a plain-text one is
delivered as two `data` lines of one event, which is also fine. What is not
fine is a payload split across a *blank* line, which ends the event early and
dispatches the remainder as a second one. Encoding here always emits one
`data:` line per source line, which is the only construction that round-trips.

## 2. `\\r` is a line break too, and only to the client

WHATWG's parser splits on CRLF, LF **and bare CR**. Python's `str.split("\\n")`
does not. A value carrying a lone `\\r` — a Windows-authored string that lost
its `\\n`, a copy-pasted terminal line — therefore serialises as one line here
and parses as two there, and the second becomes a field name the client does
not recognise and drops. The whole value is normalised through `splitlines`
first, which knows all three (and is then restricted to those three, since
`splitlines` also breaks on U+2028, U+0085 and friends that SSE treats as
ordinary characters — splitting on those would corrupt a value the client would
have accepted intact).

## 3. An event with no data is never delivered

The dispatch algorithm returns early when the data buffer is empty: a frame of
`event: ping` and nothing else fires no listener at all, not even a
zero-length one. It is the natural way to write a keepalive and it does not
work, so `ServerSentEvent` refuses it — a keepalive is a *comment*
(`comment_frame`), which is a different construct that deliberately dispatches
nothing.

## 4. A NUL in `id` silently disables resumption

`id` is ignored — not rejected, ignored — if it contains U+0000. The stream
keeps working, the client keeps its previous last-event-id, and the loss shows
up only after a reconnect, as a replay window that is wrong by however many
events carried a NUL. Refusing it here turns a silent protocol behaviour into
an exception with a traceback.

Field names are `event`, `data`, `id` and `retry`; anything else is ignored by
the client, which is why unknown fields are not offered as an escape hatch.

References: WHATWG HTML §9.2 (server-sent events), RFC 8895 §2.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

#: The media type an `EventSource` will accept and nothing else.
SSE_MEDIA_TYPE: Final[str] = "text/event-stream"

#: The three terminators the SSE parser recognises. Deliberately narrower than
#: `str.splitlines()`, which also breaks on U+000B, U+000C, U+001C-U+001E,
#: U+0085, U+2028 and U+2029 — characters SSE passes through untouched, so
#: splitting a value on one of them would produce two `data:` lines where the
#: client expected one string.
_LINE_BREAKS: Final[tuple[str, ...]] = ("\r\n", "\r", "\n")


def _split_lines(value: str) -> list[str]:
    """Split `value` exactly where an SSE parser would, and nowhere else."""
    lines = [value]
    for terminator in _LINE_BREAKS:
        lines = [part for line in lines for part in line.split(terminator)]
    return lines


def _reject_line_breaks(field: str, value: str) -> None:
    """Raise if `value` would end its field early on the wire."""
    if any(terminator in value for terminator in _LINE_BREAKS):
        raise ValueError(
            f"{field} may not contain a line break: {value!r} would be parsed "
            "as the end of the field and the start of another."
        )


@dataclass(frozen=True, slots=True)
class ServerSentEvent:
    """One dispatchable SSE frame.

    Frozen because a frame is fanned out to every subscriber on a topic: a
    mutable one would let a slow consumer observe an event a fast one had
    already changed.

    Args:
        data: The payload. May contain line breaks; each line becomes its own
            `data:` field and the client rejoins them with `\\n`. Must not be
            empty — see the module docstring.
        event: The listener name, or `None` for the default `message`
            listener. `addEventListener("x")` fires only for `event: x`, so
            naming an event is what makes it filterable client-side.
        id: Sets the client's last-event-id, which it sends back as
            `Last-Event-ID` on reconnect. Leave `None` when nothing can
            replay from it: an id the server cannot honour later is a promise
            of resumption that quietly loses events instead.
        retry: Reconnection delay in milliseconds the client should use after
            the connection drops. Applies until changed, so it is normally
            sent once at the top of a stream rather than per event.

    Raises:
        ValueError: `data` is empty, `event` or `id` carries a line break,
            `id` carries a NUL, or `retry` is negative.
    """

    data: str
    event: str | None = None
    id: str | None = None
    retry: int | None = None

    def __post_init__(self) -> None:
        if not self.data:
            raise ValueError(
                "data must not be empty: a frame whose data buffer is empty is "
                "not dispatched by the client, so the event would be sent and "
                "silently never fire. Use comment_frame() for a keepalive."
            )
        if self.event is not None:
            _reject_line_breaks("event", self.event)
        if self.id is not None:
            _reject_line_breaks("id", self.id)
            # Not merely invalid: the client *ignores* an id field containing
            # NUL and keeps its previous one, so the damage is a wrong replay
            # position after the next reconnect rather than a visible error.
            if "\x00" in self.id:
                raise ValueError(
                    f"id may not contain NUL: {self.id!r} would be ignored by "
                    "the client, leaving its last-event-id stale."
                )
        if self.retry is not None and self.retry < 0:
            raise ValueError(f"retry must not be negative, got {self.retry}.")

    def encode(self) -> bytes:
        """Serialise to UTF-8, terminated by the blank line that dispatches it.

        Field order is `event`, `id`, `retry`, `data`. The parser is
        order-insensitive, but keeping the payload last means a frame read in a
        log or a `curl` session shows its metadata before its body.
        """
        fields: list[str] = []
        if self.event is not None:
            fields.append(f"event: {self.event}")
        if self.id is not None:
            fields.append(f"id: {self.id}")
        if self.retry is not None:
            fields.append(f"retry: {self.retry}")
        fields.extend(f"data: {line}" for line in _split_lines(self.data))
        # The trailing empty string is what puts a blank line after the last
        # field. Without it the client holds the frame in its buffer until the
        # *next* one arrives, which looks exactly like a stalled stream.
        return ("\n".join(fields) + "\n\n").encode("utf-8")


def comment_frame(text: str = "") -> bytes:
    """Encode an SSE comment: bytes on the wire that dispatch no event.

    A line beginning with `:` is ignored by the parser, which makes it the only
    way to write to the stream without the client seeing anything. That is
    exactly what a keepalive needs to be — see `src/sse/heartbeat.py` for why
    writing nothing at all is not an option.

    Args:
        text: Comment body. Must not contain a line break; a second line would
            not start with `:` and would be parsed as a field.

    Raises:
        ValueError: `text` contains a line break.
    """
    _reject_line_breaks("comment", text)
    return f": {text}\n\n".encode()


def retry_frame(milliseconds: int) -> bytes:
    """Encode a bare `retry:` directive, with no event attached.

    Sent once when a stream opens. It is the server's only say in how quickly a
    client comes back: an `EventSource` reconnects on its own, and its default
    delay is whatever the browser chose (about 3 seconds in practice, and not
    specified). On an endpoint that a thousand clients hold open, the default
    is also the size of the thundering herd after a deploy, which is why this
    is configurable rather than left to the client.

    Raises:
        ValueError: `milliseconds` is negative.
    """
    if milliseconds < 0:
        raise ValueError(f"retry must not be negative, got {milliseconds}.")
    return f"retry: {milliseconds}\n\n".encode()


__all__ = [
    "SSE_MEDIA_TYPE",
    "ServerSentEvent",
    "comment_frame",
    "retry_frame",
]
