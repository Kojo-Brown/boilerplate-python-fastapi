"""The value types and the refusals they make.

Small, and worth having: every one of these is a decision the rest of the
package depends on being made in one place — that a committed offset is one
past the record, that headers keep their duplicates, that a null value without
a key is refused rather than sent.
"""

from __future__ import annotations

import pytest

from src.kafka.base import (
    ConsumedMessage,
    ConsumerError,
    LifecycleError,
    MessageNotDecodableError,
    MessageNotSerializableError,
    MessagingError,
    Partition,
    PublishedMessage,
    PublishError,
    normalize_headers,
    utc_now,
    validate_record,
)

P0 = Partition(topic="orders", number=0)


def a_message(**overrides: object) -> ConsumedMessage:
    fields: dict[str, object] = {
        "partition": P0,
        "offset": 7,
        "key": "order-1",
        "value": b"{}",
        "headers": (),
        "timestamp": utc_now(),
    }
    fields.update(overrides)
    return ConsumedMessage(**fields)  # type: ignore[arg-type]


class TestPartition:
    def test_it_reads_as_kafka_writes_it(self) -> None:
        assert str(P0) == "orders-0"

    def test_it_is_a_usable_dict_key(self) -> None:
        """Every commit map in this package is keyed by one."""
        assert {P0: 1}[Partition(topic="orders", number=0)] == 1

    def test_partitions_sort(self) -> None:
        unsorted = [Partition("b", 0), Partition("a", 1), Partition("a", 0)]
        assert sorted(unsorted) == [
            Partition("a", 0),
            Partition("a", 1),
            Partition("b", 0),
        ]


class TestConsumedMessage:
    def test_the_offset_to_commit_is_one_past_this_record(self) -> None:
        """The whole difference between resuming and replaying forever."""
        assert a_message(offset=7).next_offset == 8

    def test_a_null_value_is_a_tombstone(self) -> None:
        assert a_message(value=None).is_tombstone
        assert not a_message(value=b"").is_tombstone

    def test_the_topic_comes_from_the_partition(self) -> None:
        assert a_message().topic == "orders"

    def test_a_header_lookup_returns_the_first_of_a_repeated_name(self) -> None:
        """The first is the producer's; later ones were appended in transit."""
        message = a_message(headers=(("trace", b"a"), ("trace", b"b")))
        assert message.header("trace") == b"a"
        assert message.all_headers("trace") == (b"a", b"b")

    def test_a_missing_header_is_none_rather_than_an_error(self) -> None:
        assert a_message().header("absent") is None
        assert a_message().all_headers("absent") == ()


class TestPublishedMessage:
    def test_it_names_its_topic(self) -> None:
        published = PublishedMessage(partition=P0, offset=3, timestamp=utc_now())
        assert published.topic == "orders"


class TestNormalizeHeaders:
    def test_none_is_no_headers(self) -> None:
        assert normalize_headers(None) == ()

    def test_a_mapping_becomes_pairs(self) -> None:
        assert normalize_headers({"a": b"1"}) == (("a", b"1"),)

    def test_pairs_keep_their_order_and_their_duplicates(self) -> None:
        """A mapping cannot express this, which is why the wire form is a tuple."""
        wire = (("trace", b"a"), ("trace", b"b"))
        assert normalize_headers(wire) == wire

    def test_a_non_bytes_value_is_refused(self) -> None:
        with pytest.raises(TypeError):
            normalize_headers({"a": "not bytes"})  # type: ignore[dict-item]


class TestValidateRecord:
    def test_an_empty_topic_is_refused(self) -> None:
        with pytest.raises(ValueError, match="topic"):
            validate_record("", "k", b"v")

    def test_a_tombstone_without_a_key_is_refused(self) -> None:
        """It reads as "send nothing" and means "delete a key" — with no key."""
        with pytest.raises(ValueError, match="tombstone"):
            validate_record("orders", None, None)

    def test_a_keyed_tombstone_is_allowed(self) -> None:
        validate_record("orders", "order-1", None)

    def test_an_unkeyed_record_with_a_value_is_allowed(self) -> None:
        validate_record("orders", None, b"v")


class TestErrors:
    def test_a_broker_problem_is_a_503(self) -> None:
        """Not a 500: the request was fine, the dependency is not."""
        assert PublishError("x").status_code == 503
        assert ConsumerError("x").status_code == 503
        assert MessagingError("x").status_code == 503

    def test_a_payload_problem_is_a_500(self) -> None:
        """Retrying sends the same unencodable object again."""
        assert MessageNotSerializableError("x").status_code == 500
        assert MessageNotDecodableError("x").status_code == 500
        assert LifecycleError("x").status_code == 500

    def test_every_failure_carries_a_distinct_code(self) -> None:
        codes = {
            error("x").error_code
            for error in (
                MessagingError,
                PublishError,
                ConsumerError,
                MessageNotSerializableError,
                MessageNotDecodableError,
                LifecycleError,
            )
        }
        assert len(codes) == 6
