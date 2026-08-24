"""The producer half: what publishing writes, and what it refuses to write.

`OutboxPublisher` has one job and one hard rule. The job is to put a row where
the caller's transaction will carry it; the rule is that a refusal happens
before anything is staged, in the request that published the event, rather than
at the commit or in the relay an hour later.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ClassVar

import pytest

from src.events.base import DomainEvent, EventPublisher
from src.events.catalog import UserRegistered
from src.models.outbox import EVENT_ID_MAX_LENGTH, OutboxEvent
from src.outbox.base import EventNotSerializableError, OutboxRecord, RowSink
from src.outbox.codec import OutboxCodec
from src.outbox.publisher import OutboxPublisher
from tests.fakes import RecordingSink

PINNED_AT = datetime(2026, 8, 24, 12, 30, tzinfo=UTC)


@dataclass(frozen=True, kw_only=True)
class Unstorable(DomainEvent):
    event_name: ClassVar[str] = "test.unstorable"

    payload: tuple[int, ...]


@pytest.fixture
def sink() -> RecordingSink:
    return RecordingSink()


@pytest.fixture
def publisher(sink: RecordingSink) -> OutboxPublisher:
    return OutboxPublisher(sink, codec=OutboxCodec([UserRegistered, Unstorable]))


def a_registration(**overrides: object) -> UserRegistered:
    fields: dict[str, object] = {
        "event_id": "evt-1",
        "occurred_at": PINNED_AT,
        "user_id": "user-1",
        "email": "new@example.com",
        "via": "password",
    }
    fields.update(overrides)
    return UserRegistered(**fields)  # type: ignore[arg-type]


# --- what gets written ---------------------------------------------------


async def test_publishing_stages_one_row(
    publisher: OutboxPublisher, sink: RecordingSink
) -> None:
    await publisher.publish(a_registration())

    (row,) = sink.added
    assert isinstance(row, OutboxEvent)
    assert row.event_name == "user.registered"
    assert row.event_id == "evt-1"
    assert row.occurred_at == PINNED_AT
    assert row.payload == {
        "user_id": "user-1",
        "email": "new@example.com",
        "via": "password",
    }


async def test_the_returned_record_identifies_the_staged_row(
    publisher: OutboxPublisher, sink: RecordingSink
) -> None:
    """The id is generated here rather than left to the column default, which
    the ORM would only resolve at flush time — so a caller can log the row it
    staged without forcing a round trip to find out what it is called."""
    record = await publisher.publish(a_registration())

    (row,) = sink.added
    assert isinstance(record, OutboxRecord)
    assert isinstance(row, OutboxEvent)
    assert record.id == row.id
    assert isinstance(record.id, uuid.UUID)
    assert record.event_id == "evt-1"
    assert record.event_name == "user.registered"


async def test_two_events_are_two_rows(
    publisher: OutboxPublisher, sink: RecordingSink
) -> None:
    first = await publisher.publish(a_registration(event_id="evt-1"))
    second = await publisher.publish(a_registration(event_id="evt-2"))

    assert len(sink.added) == 2
    assert first.id != second.id


async def test_publishing_does_not_flush_or_commit() -> None:
    """The INSERT rides along with the commit the producer was making anyway.

    Asserted against a sink that would raise if anything else were called,
    because "no extra round trip" is a performance claim that decays silently.
    """

    class StrictSink:
        def __init__(self) -> None:
            self.added: list[object] = []

        def add(self, instance: object) -> None:
            self.added.append(instance)

        def __getattr__(self, name: str) -> object:
            raise AssertionError(f"publish() reached for {name!r} on the session")

    sink = StrictSink()
    await OutboxPublisher(sink).publish(a_registration())

    assert len(sink.added) == 1


# --- what is refused -----------------------------------------------------


async def test_an_unserialisable_event_stages_nothing(
    publisher: OutboxPublisher, sink: RecordingSink
) -> None:
    """A half-staged row would commit with the transaction and then fail to
    decode forever."""
    with pytest.raises(EventNotSerializableError):
        await publisher.publish(
            Unstorable(event_id="evt-3", occurred_at=PINNED_AT, payload=(1, 2))
        )

    assert sink.added == []


async def test_an_over_long_event_id_is_refused_before_the_insert(
    publisher: OutboxPublisher, sink: RecordingSink
) -> None:
    """Postgres raises on an over-long varchar rather than truncating, so the
    alternative is a DataError at COMMIT — a 500 on a request whose body
    mentions neither events nor this column."""
    with pytest.raises(EventNotSerializableError) as exc_info:
        await publisher.publish(a_registration(event_id="e" * 65))

    assert str(EVENT_ID_MAX_LENGTH) in str(exc_info.value)
    assert sink.added == []


async def test_an_event_id_at_the_limit_is_accepted(
    publisher: OutboxPublisher, sink: RecordingSink
) -> None:
    await publisher.publish(a_registration(event_id="e" * EVENT_ID_MAX_LENGTH))

    assert len(sink.added) == 1


async def test_an_over_long_event_name_is_refused(sink: RecordingSink) -> None:
    @dataclass(frozen=True, kw_only=True)
    class Verbose(DomainEvent):
        event_name: ClassVar[str] = "n" * 256

    with pytest.raises(EventNotSerializableError):
        await OutboxPublisher(sink, codec=OutboxCodec([Verbose])).publish(Verbose())

    assert sink.added == []


# --- conformance ---------------------------------------------------------


def test_the_publisher_is_an_event_publisher(sink: RecordingSink) -> None:
    """Which is what lets `AuthService` keep the call it already had."""
    assert isinstance(OutboxPublisher(sink), EventPublisher)


def test_a_recording_sink_is_a_row_sink(sink: RecordingSink) -> None:
    assert isinstance(sink, RowSink)


def test_a_session_is_already_a_row_sink() -> None:
    """Which is why there is no adapter: `add` is the whole port."""
    from unittest.mock import MagicMock

    from sqlalchemy.ext.asyncio import AsyncSession

    assert isinstance(MagicMock(spec=AsyncSession), RowSink)
