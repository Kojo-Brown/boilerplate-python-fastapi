"""The wire contract: what the decoder refuses, and what the encoder cannot emit."""

from __future__ import annotations

import json
import math

import pytest

from src.ws.protocol import (
    MAX_CLOSE_REASON_BYTES,
    ClientMessage,
    ClientMessageType,
    CloseCode,
    ErrorCode,
    MalformedMessage,
    ServerMessageType,
    close_reason,
    decode_client_message,
    encode_server_message,
    error_message,
)

LIMIT = 1024


def decode(raw: str, *, max_bytes: int = LIMIT) -> ClientMessage:
    return decode_client_message(raw, max_bytes=max_bytes)


class TestDecodingWhatIsValid:
    def test_a_ping_needs_no_other_field(self) -> None:
        message = decode('{"type":"ping"}')

        assert message.type is ClientMessageType.PING
        assert message.room is None

    @pytest.mark.parametrize("kind", ["join", "leave"])
    def test_join_and_leave_carry_a_room(self, kind: str) -> None:
        message = decode(json.dumps({"type": kind, "room": "lobby"}))

        assert message.type is ClientMessageType(kind)
        assert message.room == "lobby"

    def test_publish_carries_a_room_and_a_payload(self) -> None:
        message = decode('{"type":"publish","room":"lobby","data":{"body":"hi"}}')

        assert message.room == "lobby"
        assert message.data == {"body": "hi"}

    def test_a_null_payload_is_a_payload(self) -> None:
        """`data: null` is a value a client may mean, so absence is the check.

        `payload.get("data")` cannot tell the two apart, which is why the
        decoder tests membership instead — a bug that would reject a legal
        message and be invisible in every test that sends an object.
        """
        message = decode('{"type":"publish","room":"lobby","data":null}')

        assert message.data is None

    def test_unknown_fields_are_ignored_rather_than_rejected(self) -> None:
        """Forward compatibility: an older server must survive a newer client."""
        message = decode('{"type":"ping","future_field":1}')

        assert message.type is ClientMessageType.PING


class TestDecodingWhatIsNot:
    def test_a_frame_over_the_ceiling_is_refused_before_it_is_parsed(self) -> None:
        oversized = json.dumps({"type": "publish", "room": "a", "data": "x" * 200})

        with pytest.raises(MalformedMessage) as caught:
            decode(oversized, max_bytes=64)

        assert caught.value.code is ErrorCode.MESSAGE_TOO_LARGE

    def test_the_ceiling_is_bytes_and_not_characters(self) -> None:
        """A three-byte character is three bytes of a client's budget.

        Counting `len(str)` instead would let a message of multi-byte
        characters be several times the ceiling it was measured against — which
        is the whole quantity the ceiling exists to bound.
        """
        # Written out rather than via `json.dumps`, which escapes non-ASCII to
        # `\uXXXX` by default and would make every character one byte again —
        # hiding the exact case this is about. A browser sends the raw UTF-8.
        payload = '{"type":"publish","room":"a","data":"' + "☃" * 20 + '"}'
        assert len(payload) < 80 < len(payload.encode("utf-8"))

        with pytest.raises(MalformedMessage) as caught:
            decode(payload, max_bytes=80)

        assert caught.value.code is ErrorCode.MESSAGE_TOO_LARGE

    def test_a_non_positive_ceiling_is_the_callers_bug_not_the_clients(self) -> None:
        with pytest.raises(ValueError, match="max_bytes must be positive"):
            decode('{"type":"ping"}', max_bytes=0)

    def test_text_that_is_not_json_is_reported_as_such(self) -> None:
        with pytest.raises(MalformedMessage) as caught:
            decode("{not json")

        assert caught.value.code is ErrorCode.MALFORMED_MESSAGE

    @pytest.mark.parametrize("raw", ["4", '"hello"', "[1,2]", "null", "true"])
    def test_json_that_is_not_an_object_is_not_a_message(self, raw: str) -> None:
        """Each of these parses. `payload["type"]` on any of them raises."""
        with pytest.raises(MalformedMessage) as caught:
            decode(raw)

        assert caught.value.code is ErrorCode.MALFORMED_MESSAGE

    @pytest.mark.parametrize("raw", ["{}", '{"type":1}', '{"type":null}'])
    def test_a_missing_or_non_string_type_is_malformed(self, raw: str) -> None:
        with pytest.raises(MalformedMessage) as caught:
            decode(raw)

        assert caught.value.code is ErrorCode.MALFORMED_MESSAGE

    def test_an_unknown_type_is_distinguished_from_a_malformed_one(self) -> None:
        """Different bugs at the client end: a typo, versus a broken serialiser."""
        with pytest.raises(MalformedMessage) as caught:
            decode('{"type":"subscribe","room":"lobby"}')

        assert caught.value.code is ErrorCode.UNKNOWN_TYPE
        assert "publish" in caught.value.message

    @pytest.mark.parametrize("kind", ["join", "leave", "publish"])
    def test_a_room_bearing_type_without_a_room_is_an_invalid_field(
        self, kind: str
    ) -> None:
        with pytest.raises(MalformedMessage) as caught:
            decode(json.dumps({"type": kind}))

        assert caught.value.code is ErrorCode.INVALID_FIELD

    def test_a_non_string_room_is_an_invalid_field(self) -> None:
        with pytest.raises(MalformedMessage) as caught:
            decode('{"type":"join","room":42}')

        assert caught.value.code is ErrorCode.INVALID_FIELD

    def test_publish_without_data_is_an_invalid_field(self) -> None:
        with pytest.raises(MalformedMessage) as caught:
            decode('{"type":"publish","room":"lobby"}')

        assert caught.value.code is ErrorCode.INVALID_FIELD


class TestTheAsymmetryBetweenLoadsAndDumps:
    """`json.loads` accepts three literals `json.dumps` will not re-emit.

    Left alone this is not a curiosity — the value parses at the sender and
    fails at the *broadcast*, which is one client's payload costing a whole
    room its connections. The decoder is where it has to be caught, because
    that is the only place the failure still belongs to the client that sent
    it.
    """

    @pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity"])
    def test_a_non_finite_payload_is_refused_at_the_door(self, literal: str) -> None:
        raw = f'{{"type":"publish","room":"lobby","data":{literal}}}'
        # The premise this rests on, asserted rather than assumed: a plain
        # parse accepts the literal and yields a float no encoder will take.
        assert not math.isfinite(json.loads(raw)["data"])

        with pytest.raises(MalformedMessage) as caught:
            decode(raw)

        assert caught.value.code is ErrorCode.MALFORMED_MESSAGE

    @pytest.mark.parametrize("literal", ["NaN", "Infinity"])
    def test_the_encoder_would_indeed_have_refused_it(self, literal: str) -> None:
        """The other half of the same fact, asserted rather than assumed."""
        value = json.loads(literal)

        with pytest.raises(ValueError, match="Out of range float"):
            encode_server_message({"data": value})

    def test_a_nested_non_finite_is_caught_too(self) -> None:
        with pytest.raises(MalformedMessage):
            decode('{"type":"publish","room":"lobby","data":{"deep":[NaN]}}')


class TestEncoding:
    def test_the_output_is_compact(self) -> None:
        assert encode_server_message({"a": 1, "b": 2}) == '{"a":1,"b":2}'

    def test_it_round_trips(self) -> None:
        body = {"type": "message", "room": "lobby", "data": {"body": "hé\n"}}

        assert json.loads(encode_server_message(body)) == body

    def test_an_error_frame_names_a_code_from_the_vocabulary(self) -> None:
        body = error_message(ErrorCode.NOT_IN_ROOM, "nope")

        assert body["type"] == ServerMessageType.ERROR.value
        assert body["code"] == ErrorCode.NOT_IN_ROOM.value
        assert "retry_after" not in body

    def test_retry_after_appears_only_when_waiting_is_the_remedy(self) -> None:
        """Its presence is the signal, so it must not be sent as a default."""
        body = error_message(ErrorCode.RATE_LIMITED, "slow down", retry_after=1.23456)

        assert body["retry_after"] == 1.235


class TestCloseReasons:
    def test_a_short_reason_is_left_alone(self) -> None:
        assert close_reason("client disconnected") == "client disconnected"

    def test_an_over_long_reason_is_cut_to_the_frame_budget(self) -> None:
        """Over 123 bytes is an invalid control frame, not a truncated message."""
        trimmed = close_reason("x" * 300)

        assert len(trimmed.encode("utf-8")) <= MAX_CLOSE_REASON_BYTES

    def test_truncation_never_splits_a_character(self) -> None:
        """Slicing the encoded bytes would; slicing the string would overshoot.

        A reason of three-byte characters is 123 bytes at 41 of them, so a
        naive `text[:123]` sends an invalid frame and a naive
        `encoded[:123]` splits the 42nd character in half.
        """
        trimmed = close_reason("☃" * 100)

        assert len(trimmed.encode("utf-8")) <= MAX_CLOSE_REASON_BYTES
        assert trimmed == "☃" * 41
        # Decodable, which a byte-sliced value would not be.
        assert trimmed.encode("utf-8").decode("utf-8") == trimmed


class TestCloseCodes:
    def test_application_codes_are_in_the_private_range(self) -> None:
        """4000-4999 is the only part of the space an application may define."""
        application = {
            CloseCode.PROTOCOL_ERROR,
            CloseCode.TOKEN_EXPIRED,
            CloseCode.IDLE_TIMEOUT,
            CloseCode.RATE_LIMITED,
            CloseCode.OVERFLOW,
        }

        assert all(4000 <= int(code) <= 4999 for code in application)

    def test_every_code_is_distinct(self) -> None:
        """They exist to be told apart; two sharing a value defeats the point."""
        assert len({int(code) for code in CloseCode}) == len(list(CloseCode))
