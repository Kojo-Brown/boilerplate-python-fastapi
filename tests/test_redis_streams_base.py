"""The values: entry ids, the field rules, and the error contract.

None of this needs a server or an event loop. It is here because an entry id
compared as a string is a bug that survives every integration test until a
stream carries ten messages in one millisecond, and because the two publish
refusals are the difference between a clear `ValueError` at the call site and
an opaque `ResponseError` from the server.
"""

from __future__ import annotations

import pytest

from src.exceptions import AppException
from src.immutable import FrozenDict
from src.redis_streams.base import (
    ClaimedMessage,
    PendingEntry,
    StreamDecodeError,
    StreamEntryId,
    StreamError,
    StreamGroupError,
    StreamLifecycleError,
    StreamMessage,
    StreamPublishError,
    StreamUnavailableError,
    default_consumer_name,
    validate_fields,
)


class TestStreamEntryId:
    def test_it_parses_the_form_the_server_sends(self) -> None:
        assert StreamEntryId.parse(b"1698412345678-4") == StreamEntryId(
            1698412345678, 4
        )

    def test_it_parses_the_form_a_person_types(self) -> None:
        assert StreamEntryId.parse("12-0") == StreamEntryId(12, 0)

    def test_it_renders_back_to_what_it_was_parsed_from(self) -> None:
        assert str(StreamEntryId.parse("1698412345678-4")) == "1698412345678-4"

    def test_ordering_is_numeric_not_lexicographic(self) -> None:
        """`"9-0" > "10-0"` as strings, which is why these are not strings."""
        assert StreamEntryId(9, 0) < StreamEntryId(10, 0)
        assert StreamEntryId(5, 9) < StreamEntryId(5, 10)

    def test_it_sorts_a_mixed_batch(self) -> None:
        ids = [StreamEntryId(2, 0), StreamEntryId(1, 10), StreamEntryId(1, 2)]

        assert sorted(ids) == [
            StreamEntryId(1, 2),
            StreamEntryId(1, 10),
            StreamEntryId(2, 0),
        ]

    def test_the_exclusive_form_is_what_pages_a_scan(self) -> None:
        """An inclusive start re-reads the entry the last page ended on, and a
        PEL bigger than one page then never advances past it."""
        assert StreamEntryId(12, 3).exclusive == "(12-3"

    @pytest.mark.parametrize("value", ["", "12", "nonsense", "$", ">", "*", "a-b"])
    def test_it_refuses_anything_that_is_not_an_id(self, value: str) -> None:
        """`$`, `>` and `*` are legal arguments to stream commands and none of
        them is an id — letting one through would produce an id of whatever
        `int("$")` did not return."""
        with pytest.raises(ValueError, match="Malformed stream entry id"):
            StreamEntryId.parse(value)

    def test_a_negative_part_is_refused(self) -> None:
        with pytest.raises(ValueError, match="cannot be negative"):
            StreamEntryId(-1, 0)

    def test_it_is_hashable_so_it_can_key_a_pending_map(self) -> None:
        assert len({StreamEntryId(1, 0), StreamEntryId(1, 0), StreamEntryId(1, 1)}) == 2


class TestValidateFields:
    def test_an_ordinary_message_passes(self) -> None:
        validate_fields({"type": b"order.created", "id": b"42"})

    def test_an_empty_hash_is_refused(self) -> None:
        with pytest.raises(ValueError, match="at least one field"):
            validate_fields({})

    @pytest.mark.parametrize("value", [1, "text", None, 1.5])
    def test_a_value_that_is_not_bytes_is_refused(self, value: object) -> None:
        """redis-py would encode it, and `1` and `"1"` would both reach the
        consumer as `b"1"` — a type nobody chose."""
        with pytest.raises(TypeError, match="must be bytes"):
            validate_fields({"n": value})  # type: ignore[dict-item]


class TestMessages:
    def test_a_field_that_is_not_there_is_none(self) -> None:
        message = StreamMessage(
            stream="s",
            id=StreamEntryId(1, 0),
            fields=FrozenDict[str, bytes]({"a": b"1"}),
            delivery_count=1,
        )

        assert message.field("a") == b"1"
        assert message.field("missing") is None

    def test_a_message_is_not_claimed_unless_it_says_so(self) -> None:
        message = StreamMessage(
            stream="s",
            id=StreamEntryId(1, 0),
            fields=FrozenDict[str, bytes]({"a": b"1"}),
            delivery_count=1,
        )

        assert message.claimed is False

    def test_a_claim_result_carries_both_halves(self) -> None:
        result = ClaimedMessage(messages=(), missing=(StreamEntryId(1, 0),))

        assert result.messages == ()
        assert result.missing == (StreamEntryId(1, 0),)

    def test_a_pending_entry_reports_idleness_in_seconds(self) -> None:
        """Redis reports milliseconds; converting at the edge means nothing
        outside the package has to remember which unit it is holding."""
        entry = PendingEntry(
            id=StreamEntryId(1, 0), consumer="c", idle=1.5, delivery_count=2
        )

        assert entry.idle == 1.5


class TestDefaultConsumerName:
    def test_two_calls_do_not_collide(self) -> None:
        """Two replicas sharing a consumer name share a PEL, and each sees the
        other's in-flight work as its own."""
        assert default_consumer_name() != default_consumer_name()

    def test_it_carries_the_host_and_pid(self) -> None:
        import os
        import socket

        name = default_consumer_name()

        assert name.startswith(f"{socket.gethostname()}-{os.getpid()}-")


class TestTheErrorContract:
    @pytest.mark.parametrize(
        ("error", "status", "code"),
        [
            (StreamError, 503, "STREAM_ERROR"),
            (StreamUnavailableError, 503, "STREAM_UNAVAILABLE"),
            (StreamPublishError, 503, "STREAM_PUBLISH_FAILED"),
            (StreamGroupError, 503, "STREAM_GROUP_FAILED"),
            (StreamDecodeError, 500, "STREAM_NOT_DECODABLE"),
            (StreamLifecycleError, 500, "STREAM_LIFECYCLE"),
        ],
    )
    def test_each_failure_renders_as_itself(
        self, error: type[StreamError], status: int, code: str
    ) -> None:
        """503 for anything about the server, 500 for anything about us: a
        handler publishing as part of a request should let the first reach the
        client as "try again" and never the second."""
        raised = error("something went wrong", details={"stream": "s"})

        assert isinstance(raised, AppException)
        assert raised.status_code == status
        assert raised.error_code == code
        assert raised.details == {"stream": "s"}
