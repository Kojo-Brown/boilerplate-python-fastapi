"""The JSON codec: what it refuses, and why the refusals are worth the code."""

from __future__ import annotations

import pytest

from src.kafka.base import (
    ConsumedMessage,
    MessageNotDecodableError,
    MessageNotSerializableError,
    Partition,
    utc_now,
)
from src.kafka.codec import decode_json, encode_json

P0 = Partition(topic="orders", number=0)


def a_message(value: bytes | None) -> ConsumedMessage:
    return ConsumedMessage(
        partition=P0,
        offset=3,
        key="k",
        value=value,
        headers=(),
        timestamp=utc_now(),
    )


class TestEncoding:
    def test_it_round_trips(self) -> None:
        assert decode_json(a_message(encode_json({"id": 1}))) == {"id": 1}

    def test_keys_are_sorted_so_one_value_is_one_encoding(self) -> None:
        """What makes a record comparable without parsing it."""
        assert encode_json({"b": 1, "a": 2}) == encode_json({"a": 2, "b": 1})

    def test_it_is_compact(self) -> None:
        assert encode_json({"a": 1, "b": 2}) == b'{"a":1,"b":2}'

    def test_non_ascii_is_sent_as_utf8_rather_than_escaped(self) -> None:
        assert encode_json({"name": "café"}) == '{"name":"café"}'.encode()

    @pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
    def test_a_non_finite_float_is_refused(self, value: float) -> None:
        """`json.dumps` emits `NaN` happily and no other language parses it, so
        the disagreement surfaces in a consumer nobody here maintains."""
        with pytest.raises(MessageNotSerializableError):
            encode_json({"amount": value})

    def test_an_unencodable_object_is_refused_by_name(self) -> None:
        with pytest.raises(MessageNotSerializableError, match="not JSON-encodable"):
            encode_json({"when": object()})


class TestDecoding:
    def test_a_tombstone_is_refused_rather_than_returned_as_none(self) -> None:
        """A delete is not an empty update, and a decoder that blurred them
        would push the distinction into every handler's `if`."""
        with pytest.raises(MessageNotDecodableError, match="tombstone"):
            decode_json(a_message(None))

    def test_invalid_utf8_names_the_record(self) -> None:
        with pytest.raises(MessageNotDecodableError, match="orders-0@3"):
            decode_json(a_message(b"\xff\xfe"))

    def test_invalid_json_names_the_record(self) -> None:
        """An error saying only "invalid JSON" cannot be acted on: nobody can
        go and look at the record it means."""
        with pytest.raises(MessageNotDecodableError, match="orders-0@3"):
            decode_json(a_message(b"{not json"))

    def test_a_json_scalar_decodes(self) -> None:
        assert decode_json(a_message(b"42")) == 42
