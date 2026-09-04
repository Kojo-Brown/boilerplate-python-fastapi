"""The headers a record carries between tiers, and the three states of reading them.

The sharp cases are `TestStamping` — where appending instead of replacing gives
a record that reads as attempt 1 forever — and `TestUnreadableIsNotAbsent`,
where treating a corrupt envelope as a missing one puts the record on an
endless lap of the ladder with every log line calling it a first failure.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from src.dlq.base import MalformedEnvelopeError
from src.dlq.envelope import (
    HEADER_ATTEMPTS,
    HEADER_ERROR,
    HEADER_FIRST_FAILED_AT,
    HEADER_NOT_BEFORE,
    HEADER_ORIGIN_OFFSET,
    HEADER_ORIGIN_PARTITION,
    HEADER_ORIGIN_TOPIC,
    HEADER_REPLAYS,
    MAX_ERROR_LENGTH,
    TRUNCATION_MARKER,
    DeadLetterEnvelope,
    advance,
    read,
    read_replays,
    stamp,
    strip,
    truncate_error,
)
from src.kafka.base import ConsumedMessage, Headers, Partition, utc_now

NOW = datetime(2026, 9, 4, 12, 0, 0, tzinfo=UTC)
ORIGIN = Partition(topic="orders.events", number=1)


def an_envelope(**overrides: object) -> DeadLetterEnvelope:
    fields: dict[str, object] = {
        "origin_topic": "orders.events",
        "origin_partition": 1,
        "origin_offset": 42,
        "attempts": 1,
        "first_failed_at": NOW,
        "not_before": NOW + timedelta(seconds=5),
        "error": "RuntimeError: nope",
        "replays": 0,
    }
    fields.update(overrides)
    return DeadLetterEnvelope(**fields)  # type: ignore[arg-type]


def a_message(
    *,
    partition: Partition = ORIGIN,
    offset: int = 42,
    key: str | None = "k",
    value: bytes | None = b"payload",
    headers: Headers = (),
) -> ConsumedMessage:
    return ConsumedMessage(
        partition=partition,
        offset=offset,
        key=key,
        value=value,
        headers=headers,
        timestamp=utc_now(),
    )


class TestTruncation:
    def test_a_short_error_is_left_alone(self) -> None:
        assert truncate_error("boom") == "boom"

    def test_a_long_error_is_cut_and_says_so(self) -> None:
        truncated = truncate_error("x" * (MAX_ERROR_LENGTH + 100))

        assert len(truncated) == MAX_ERROR_LENGTH
        assert truncated.endswith(TRUNCATION_MARKER)

    def test_a_limit_shorter_than_the_marker_just_cuts(self) -> None:
        """Otherwise the marker would be longer than the room for it."""
        assert truncate_error("abcdef", limit=3) == "abc"

    def test_it_refuses_a_limit_of_zero(self) -> None:
        with pytest.raises(ValueError, match="at least 1"):
            truncate_error("abc", limit=0)

    def test_to_headers_truncates_an_error_set_directly(self) -> None:
        headers = dict(an_envelope(error="y" * 5_000).to_headers())

        assert len(headers[HEADER_ERROR].decode()) == MAX_ERROR_LENGTH


class TestEnvelopeValidation:
    @pytest.mark.parametrize(
        ("overrides", "message"),
        [
            ({"attempts": 0}, "1-based"),
            ({"origin_partition": -1}, "origin_partition cannot be negative"),
            ({"origin_offset": -1}, "origin_offset cannot be negative"),
            ({"replays": -1}, "replays cannot be negative"),
            ({"first_failed_at": datetime(2026, 1, 1)}, "timezone-aware"),
            ({"not_before": datetime(2026, 1, 1)}, "timezone-aware"),
        ],
    )
    def test_it_refuses_an_envelope_that_cannot_be_read_back(
        self, overrides: dict[str, object], message: str
    ) -> None:
        with pytest.raises(ValueError, match=message):
            an_envelope(**overrides)


class TestDueTime:
    def test_a_record_is_not_due_before_its_not_before(self) -> None:
        envelope = an_envelope(not_before=NOW + timedelta(seconds=30))

        assert not envelope.is_due(NOW)
        assert envelope.wait_for(NOW) == 30.0

    def test_a_record_is_due_at_exactly_its_not_before(self) -> None:
        envelope = an_envelope(not_before=NOW)

        assert envelope.is_due(NOW)
        assert envelope.wait_for(NOW) == 0.0

    def test_the_wait_never_goes_negative(self) -> None:
        """A late poll must read as "due", not as a negative sleep."""
        envelope = an_envelope(not_before=NOW)

        assert envelope.wait_for(NOW + timedelta(hours=1)) == 0.0

    def test_age_is_measured_from_the_first_failure(self) -> None:
        envelope = an_envelope(first_failed_at=NOW)

        assert envelope.age(NOW + timedelta(seconds=90)) == 90.0
        assert envelope.age(NOW - timedelta(seconds=90)) == 0.0


class TestStamping:
    def test_it_keeps_the_applications_own_headers(self) -> None:
        stamped = stamp((("traceparent", b"00-abc"), ("schema", b"7")), an_envelope())

        assert stamped[:2] == (("traceparent", b"00-abc"), ("schema", b"7"))

    def test_it_replaces_rather_than_appends_its_own(self) -> None:
        """Appending is the bug this is written to prevent.

        `ConsumedMessage.header` returns the *first* match, so a second
        `x-dlq-attempts` appended behind the first would never be read: the
        record would climb one rung of the ladder and then circle it forever,
        with nothing about it looking wrong.
        """
        first = stamp((("traceparent", b"00-abc"),), an_envelope(attempts=1))
        second = stamp(first, an_envelope(attempts=2))

        assert [name for name, _ in second].count(HEADER_ATTEMPTS) == 1
        assert read(a_message(headers=second)) is not None
        envelope = read(a_message(headers=second))
        assert envelope is not None
        assert envelope.attempts == 2

    def test_it_preserves_a_repeated_application_header(self) -> None:
        """Duplicate names are legal on the wire and are not this code's to fix."""
        headers = (("baggage", b"a"), ("baggage", b"b"))

        assert strip(stamp(headers, an_envelope()))[:2] == headers

    def test_strip_removes_every_dlq_header_including_unknown_ones(self) -> None:
        """A prefix test, not a list, so a field added later cannot be missed."""
        headers = (("x-dlq-something-new", b"1"), ("keep", b"2"))

        assert strip(headers) == (("keep", b"2"),)


class TestReadingAnEnvelope:
    def test_a_record_that_has_never_failed_has_no_envelope(self) -> None:
        assert read(a_message()) is None

    def test_it_round_trips_through_the_wire(self) -> None:
        original = an_envelope(attempts=3, replays=2)

        recovered = read(a_message(headers=original.to_headers()))

        assert recovered == original

    def test_a_missing_error_reads_as_empty_rather_than_failing(self) -> None:
        """The error is the one field a record can honestly be missing.

        Everything else is needed to decide where the record goes; the error is
        only there for a person to read.
        """
        headers = tuple(
            (name, value)
            for name, value in an_envelope().to_headers()
            if name != HEADER_ERROR
        )

        envelope = read(a_message(headers=headers))

        assert envelope is not None
        assert envelope.error == ""

    def test_a_missing_replay_count_reads_as_zero(self) -> None:
        headers = tuple(
            (name, value)
            for name, value in an_envelope().to_headers()
            if name != HEADER_REPLAYS
        )

        envelope = read(a_message(headers=headers))

        assert envelope is not None
        assert envelope.replays == 0

    def test_a_timestamp_is_normalised_to_utc(self) -> None:
        """A producer in another zone must not shift the due time by its offset."""
        headers = (
            (HEADER_ATTEMPTS, b"1"),
            (HEADER_ORIGIN_TOPIC, b"orders.events"),
            (HEADER_ORIGIN_PARTITION, b"1"),
            (HEADER_ORIGIN_OFFSET, b"42"),
            (HEADER_FIRST_FAILED_AT, b"2026-09-04T14:00:00+02:00"),
            (HEADER_NOT_BEFORE, b"2026-09-04T14:00:05+02:00"),
        )

        envelope = read(a_message(headers=headers))

        assert envelope is not None
        assert envelope.first_failed_at == NOW


class TestUnreadableIsNotAbsent:
    """A corrupt envelope must not read as a record that has never failed.

    Reading it as absent resets the attempt count on every pass, so the record
    rides the ladder forever and every log line about it says "first failure".
    Raising instead lets the router send it where an uninterpretable record
    belongs, which is the dead-letter topic.
    """

    @pytest.mark.parametrize(
        "headers",
        [
            pytest.param(((HEADER_ATTEMPTS, b"not-a-number"),), id="attempts-not-int"),
            pytest.param(((HEADER_ATTEMPTS, b"\xff\xfe"),), id="attempts-not-utf8"),
            pytest.param(((HEADER_ATTEMPTS, b"1"),), id="attempts-alone"),
        ],
    )
    def test_a_broken_envelope_raises(self, headers: Headers) -> None:
        with pytest.raises(MalformedEnvelopeError):
            read(a_message(headers=headers))

    def test_a_truncated_timestamp_raises(self) -> None:
        headers = tuple(
            (name, b"2026-09-04T12:" if name == HEADER_NOT_BEFORE else value)
            for name, value in an_envelope().to_headers()
        )

        with pytest.raises(MalformedEnvelopeError, match="ISO-8601"):
            read(a_message(headers=headers))

    def test_a_naive_timestamp_raises_rather_than_being_assumed_utc(self) -> None:
        """Assuming UTC would misread a local-time producer by its whole offset.

        And comparing a naive timestamp to `utc_now()` raises anyway, so the
        alternative to failing here is failing later with no context.
        """
        headers = tuple(
            (name, b"2026-09-04T12:00:00" if name == HEADER_NOT_BEFORE else value)
            for name, value in an_envelope().to_headers()
        )

        with pytest.raises(MalformedEnvelopeError, match="no UTC offset"):
            read(a_message(headers=headers))

    def test_a_zero_attempt_count_raises_rather_than_producing_a_value_error(
        self,
    ) -> None:
        """One exception type for "this record's metadata is not usable"."""
        headers = tuple(
            (name, b"0" if name == HEADER_ATTEMPTS else value)
            for name, value in an_envelope().to_headers()
        )

        with pytest.raises(MalformedEnvelopeError, match="not valid"):
            read(a_message(headers=headers))


class TestReadingTheReplayCount:
    """A replayed record carries the lap count and no envelope at all.

    Its attempt count was reset deliberately, so `read` reports it as a record
    that has never failed — which is true. Without a separate reader the lap
    count would be lost at that record's first failure, which is exactly the
    pass it exists to describe.
    """

    def test_a_record_with_no_headers_has_been_replayed_zero_times(self) -> None:
        assert read_replays(a_message()) == 0

    def test_it_reads_the_count_from_a_record_with_no_envelope(self) -> None:
        assert read_replays(a_message(headers=((HEADER_REPLAYS, b"2"),))) == 2

    def test_a_corrupt_count_raises_rather_than_reading_as_zero(self) -> None:
        with pytest.raises(MalformedEnvelopeError):
            read_replays(a_message(headers=((HEADER_REPLAYS, b"lots"),)))


class TestAdvancing:
    def test_a_first_failure_takes_provenance_from_the_record(self) -> None:
        envelope = advance(
            None, a_message(), now=NOW, delay=5.0, error="RuntimeError: nope"
        )

        assert envelope.origin_topic == "orders.events"
        assert envelope.origin_partition == 1
        assert envelope.origin_offset == 42
        assert envelope.attempts == 1
        assert envelope.first_failed_at == NOW
        assert envelope.not_before == NOW + timedelta(seconds=5)

    def test_provenance_stays_pointing_at_the_origin_topic(self) -> None:
        """The coordinates are the origin record's, not the tier record's.

        Their job is to point at the record the application actually published,
        so that someone holding a dead letter can read the records either side
        of it. Following the record down the ladder would make them point at a
        topic nobody produces to.
        """
        previous = an_envelope(attempts=1)
        tier_record = a_message(
            partition=Partition(topic="orders.events.retry.1", number=0), offset=7
        )

        envelope = advance(
            previous, tier_record, now=NOW, delay=25.0, error="RuntimeError: again"
        )

        assert envelope.origin_topic == "orders.events"
        assert envelope.origin_partition == 1
        assert envelope.origin_offset == 42
        assert envelope.attempts == 2

    def test_the_first_failure_time_survives_every_hop(self) -> None:
        previous = an_envelope(first_failed_at=NOW)
        later = NOW + timedelta(minutes=10)

        envelope = advance(previous, a_message(), now=later, delay=5.0, error="e")

        assert envelope.first_failed_at == NOW
        assert envelope.age(later) == 600.0

    def test_the_replay_count_survives_every_hop(self) -> None:
        """Or a record on its third lap reads as a first failure each time."""
        envelope = advance(
            an_envelope(replays=2), a_message(), now=NOW, delay=5.0, error="e"
        )

        assert envelope.replays == 2

    def test_a_zero_delay_is_due_at_exactly_now(self) -> None:
        """A dead-lettered record never waits, and says so on its face."""
        envelope = advance(None, a_message(), now=NOW, delay=0.0, error="e")

        assert envelope.not_before == NOW == envelope.first_failed_at
        assert envelope.is_due(NOW)
