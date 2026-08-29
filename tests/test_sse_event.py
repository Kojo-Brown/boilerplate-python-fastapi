"""The SSE wire format, and the four silent corruptions it invites.

Every assertion here is about what a *client* would parse, which is the only
thing that matters for a format whose fields are terminated by line breaks and
which has no escaping mechanism at all.
"""

from __future__ import annotations

import pytest

from src.sse.event import (
    SSE_MEDIA_TYPE,
    ServerSentEvent,
    comment_frame,
    retry_frame,
)


def parse_fields(frame: bytes) -> list[tuple[str, str]]:
    """Split a frame the way an SSE client does: on CR, LF and CRLF.

    Deliberately not `bytes.splitlines()` — the point of several tests below is
    that Python's idea of a line break and the SSE parser's differ, so a helper
    using Python's would agree with the code under test for the wrong reason.
    """
    text = frame.decode("utf-8")
    for terminator in ("\r\n", "\r"):
        text = text.replace(terminator, "\n")
    fields: list[tuple[str, str]] = []
    for line in text.split("\n"):
        if not line:
            continue
        name, _, value = line.partition(":")
        fields.append((name, value.removeprefix(" ")))
    return fields


def data_of(frame: bytes) -> str:
    """Rejoin `data` fields the way a client builds its `event.data`."""
    return "\n".join(value for name, value in parse_fields(frame) if name == "data")


class TestEncoding:
    def test_a_minimal_event_is_a_data_line_and_a_blank_line(self) -> None:
        assert ServerSentEvent(data="hello").encode() == b"data: hello\n\n"

    def test_the_frame_ends_with_a_blank_line(self) -> None:
        """Without it the client holds the event until the next one arrives."""
        frame = ServerSentEvent(data="hello", event="greeting").encode()

        assert frame.endswith(b"\n\n")

    def test_metadata_precedes_the_payload(self) -> None:
        frame = ServerSentEvent(data="x", event="e", id="1", retry=1000).encode()

        assert parse_fields(frame) == [
            ("event", "e"),
            ("id", "1"),
            ("retry", "1000"),
            ("data", "x"),
        ]

    def test_the_body_is_utf8_rather_than_escaped(self) -> None:
        frame = ServerSentEvent(data="ünïcødé 🎉").encode()

        assert data_of(frame) == "ünïcødé 🎉"

    def test_optional_fields_are_absent_rather_than_empty(self) -> None:
        """`id:` with no value clears the client's last-event-id."""
        names = [name for name, _ in parse_fields(ServerSentEvent(data="x").encode())]

        assert names == ["data"]


class TestMultilineData:
    """A line break in `data` is a frame boundary, not a character."""

    def test_each_line_becomes_its_own_data_field(self) -> None:
        frame = ServerSentEvent(data="one\ntwo").encode()

        assert parse_fields(frame) == [("data", "one"), ("data", "two")]

    def test_a_multiline_payload_round_trips(self) -> None:
        payload = "one\ntwo\nthree"

        assert data_of(ServerSentEvent(data=payload).encode()) == payload

    def test_a_bare_carriage_return_round_trips(self) -> None:
        """`str.split("\\n")` misses this one; the client's parser does not."""
        assert data_of(ServerSentEvent(data="one\rtwo").encode()) == "one\ntwo"

    def test_crlf_is_one_break_and_not_two(self) -> None:
        frame = ServerSentEvent(data="one\r\ntwo").encode()

        assert parse_fields(frame) == [("data", "one"), ("data", "two")]

    def test_no_blank_line_appears_inside_a_frame(self) -> None:
        """A blank line mid-frame would dispatch the rest as a second event."""
        frame = ServerSentEvent(data="one\n\ntwo").encode()

        assert frame.count(b"\n\n") == 1
        assert frame.endswith(b"\n\n")
        assert data_of(frame) == "one\n\ntwo"

    @pytest.mark.parametrize(
        "char", ["\u2028", "\u2029", "\x0b", "\x0c", "\x85", "\x1c", "\x1e"]
    )
    def test_characters_python_calls_line_breaks_and_sse_does_not_survive(
        self, char: str
    ) -> None:
        """`str.splitlines()` would split these; the SSE parser would not.

        Splitting on one would hand the client a value with a `\\n` the sender
        never wrote — a corruption in the opposite direction to the one this
        module mostly guards against.
        """
        payload = f"before{char}after"

        assert data_of(ServerSentEvent(data=payload).encode()) == payload


class TestRejectedFrames:
    def test_empty_data_is_refused(self) -> None:
        """A frame with an empty data buffer is never dispatched by a client."""
        with pytest.raises(ValueError, match="not dispatched"):
            ServerSentEvent(data="")

    @pytest.mark.parametrize("terminator", ["\n", "\r", "\r\n"])
    def test_a_line_break_in_the_event_name_is_refused(self, terminator: str) -> None:
        with pytest.raises(ValueError, match="line break"):
            ServerSentEvent(data="x", event=f"na{terminator}me")

    @pytest.mark.parametrize("terminator", ["\n", "\r", "\r\n"])
    def test_a_line_break_in_the_id_is_refused(self, terminator: str) -> None:
        with pytest.raises(ValueError, match="line break"):
            ServerSentEvent(data="x", id=f"1{terminator}2")

    def test_a_nul_in_the_id_is_refused(self) -> None:
        """The client ignores such an id, leaving its last-event-id stale."""
        with pytest.raises(ValueError, match="NUL"):
            ServerSentEvent(data="x", id="1\x002")

    def test_a_negative_retry_is_refused(self) -> None:
        with pytest.raises(ValueError, match="negative"):
            ServerSentEvent(data="x", retry=-1)

    def test_an_event_is_immutable(self) -> None:
        """It is fanned out to every subscriber on a topic."""
        event = ServerSentEvent(data="x")

        with pytest.raises(AttributeError):
            event.data = "y"  # type: ignore[misc]


class TestComments:
    def test_a_comment_starts_with_a_colon(self) -> None:
        assert comment_frame("keep-alive") == b": keep-alive\n\n"

    def test_a_comment_carries_no_fields(self) -> None:
        """`partition(":")` gives an empty name, which no client dispatches."""
        assert parse_fields(comment_frame("keep-alive")) == [("", "keep-alive")]

    def test_an_empty_comment_is_still_a_comment(self) -> None:
        assert comment_frame().startswith(b":")

    @pytest.mark.parametrize("terminator", ["\n", "\r", "\r\n"])
    def test_a_line_break_in_a_comment_is_refused(self, terminator: str) -> None:
        """The second line would not start with `:` and would parse as a field."""
        with pytest.raises(ValueError, match="line break"):
            comment_frame(f"one{terminator}two")


class TestRetryDirective:
    def test_it_carries_the_delay_and_nothing_else(self) -> None:
        assert parse_fields(retry_frame(3000)) == [("retry", "3000")]

    def test_zero_is_allowed(self) -> None:
        """A valid instruction: reconnect immediately."""
        assert retry_frame(0) == b"retry: 0\n\n"

    def test_a_negative_delay_is_refused(self) -> None:
        with pytest.raises(ValueError, match="negative"):
            retry_frame(-1)


def test_the_media_type_is_the_one_eventsource_accepts() -> None:
    assert SSE_MEDIA_TYPE == "text/event-stream"
