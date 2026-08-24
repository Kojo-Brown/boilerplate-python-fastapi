"""What survives the round trip through a row, and what is refused on the way in.

The codec is the only place where an event stops being a Python object, and the
producer and the consumer of a row can be different processes running different
builds. So the interesting assertions here are not "it encodes" but "what comes
back is what went in", and "what could not come back is refused before it goes".
"""

from __future__ import annotations

import dataclasses
import inspect
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import IntEnum, StrEnum
from types import UnionType
from typing import ClassVar, Union, get_args, get_origin, get_type_hints

import pytest

from src.events import catalog
from src.events.base import DomainEvent
from src.events.catalog import EVENT_TYPES, UserLoggedIn, UserRegistered
from src.outbox.base import (
    EventNotDecodableError,
    EventNotSerializableError,
    UnknownEventTypeError,
)
from src.outbox.codec import IDENTITY_FIELDS, OutboxCodec, default_codec

PINNED_AT = datetime(2026, 8, 24, 12, 30, tzinfo=UTC)


@dataclass(frozen=True, kw_only=True)
class ScalarEvent(DomainEvent):
    """One field of each type a payload may hold."""

    event_name: ClassVar[str] = "test.scalars"

    text: str
    count: int
    ratio: float
    flag: bool
    absent: str | None


@dataclass(frozen=True, kw_only=True)
class ListEvent(DomainEvent):
    event_name: ClassVar[str] = "test.list"

    tags: tuple[str, ...]


class Colour(StrEnum):
    RED = "red"


class Size(IntEnum):
    LARGE = 2


@dataclass(frozen=True, kw_only=True)
class EnumEvent(DomainEvent):
    event_name: ClassVar[str] = "test.enum"

    colour: Colour
    size: Size


@pytest.fixture
def codec() -> OutboxCodec:
    return OutboxCodec([ScalarEvent, ListEvent, EnumEvent, UserRegistered])


# --- the round trip ------------------------------------------------------


def test_a_scalar_event_survives_the_round_trip(codec: OutboxCodec) -> None:
    event = ScalarEvent(
        event_id="evt-1",
        occurred_at=PINNED_AT,
        text="hello",
        count=3,
        ratio=1.5,
        flag=True,
        absent=None,
    )

    payload = codec.encode(event)
    decoded = codec.decode(
        "test.scalars", payload, event_id="evt-1", occurred_at=PINNED_AT
    )

    # Equality, not field-by-field: a frozen dataclass compares by value, and
    # anything the codec changed on the way through shows up right here.
    assert decoded == event


def test_the_identity_fields_are_columns_rather_than_payload(
    codec: OutboxCodec,
) -> None:
    """Storing them twice would let the two copies disagree, and nothing would
    say which one the subscriber should believe."""
    payload = codec.encode(
        UserRegistered(event_id="evt-2", occurred_at=PINNED_AT, user_id="u", email="e")
    )

    assert set(payload) == {"user_id", "email", "via"}
    assert IDENTITY_FIELDS.isdisjoint(payload)


def test_decoding_restores_the_identity_from_the_columns(codec: OutboxCodec) -> None:
    decoded = codec.decode(
        "user.registered",
        {"user_id": "u", "email": "e", "via": "oauth"},
        event_id="evt-3",
        occurred_at=PINNED_AT,
    )

    assert decoded.event_id == "evt-3"
    assert decoded.occurred_at == PINNED_AT
    assert isinstance(decoded, UserRegistered)
    assert decoded.via == "oauth"


# --- what is refused, and when -------------------------------------------


def test_a_non_scalar_field_is_refused_and_names_itself(codec: OutboxCodec) -> None:
    """A tuple would encode as a JSON array and decode as a list, so the event
    a subscriber receives would not equal the one that was published."""
    event = ListEvent(event_id="evt-4", occurred_at=PINNED_AT, tags=("a", "b"))

    with pytest.raises(EventNotSerializableError) as exc_info:
        codec.encode(event)

    assert "tags" in str(exc_info.value)
    assert exc_info.value.details == {"event": "test.list", "field": "tags"}


def test_enum_members_are_refused_despite_passing_isinstance(
    codec: OutboxCodec,
) -> None:
    """`StrEnum` is a `str` and `IntEnum` is an `int`, so an `isinstance` check
    would let both through — and both would come back as their base type."""
    event = EnumEvent(
        event_id="evt-5", occurred_at=PINNED_AT, colour=Colour.RED, size=Size.LARGE
    )

    with pytest.raises(EventNotSerializableError):
        codec.encode(event)

    # And the reason it matters, demonstrated rather than described: the naive
    # encoding round-trips to a value that is equal to the member but is not it.
    assert Colour.RED.value == "red"
    assert not isinstance("red", Colour)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_a_non_finite_float_is_refused(codec: OutboxCodec, value: float) -> None:
    """Postgres jsonb rejects these, so without the check the failure is a
    DataError at COMMIT — on a request whose body mentions no floats at all."""
    event = ScalarEvent(
        event_id="evt-6",
        occurred_at=PINNED_AT,
        text="t",
        count=1,
        ratio=value,
        flag=False,
        absent=None,
    )

    with pytest.raises(EventNotSerializableError) as exc_info:
        codec.encode(event)

    assert "ratio" in str(exc_info.value)


def test_an_unknown_event_type_is_reported_as_such(codec: OutboxCodec) -> None:
    """The ordinary cause is a relay running behind the producer mid-deploy, so
    the relay retries rather than discarding — see `test_outbox_relay.py`."""
    with pytest.raises(UnknownEventTypeError) as exc_info:
        codec.decode("test.never-registered", {}, event_id="e", occurred_at=PINNED_AT)

    assert exc_info.value.details == {"event": "test.never-registered"}


def test_a_payload_that_no_longer_fits_its_class_is_reported_as_such(
    codec: OutboxCodec,
) -> None:
    """What a renamed or removed field looks like from the reading side."""
    with pytest.raises(EventNotDecodableError) as exc_info:
        codec.decode(
            "user.registered",
            {"user_id": "u", "email": "e", "renamed_since": "x"},
            event_id="e",
            occurred_at=PINNED_AT,
        )

    assert "user.registered" in str(exc_info.value)


def test_a_missing_required_field_is_reported_as_such(codec: OutboxCodec) -> None:
    with pytest.raises(EventNotDecodableError):
        codec.decode(
            "user.registered", {"user_id": "u"}, event_id="e", occurred_at=PINNED_AT
        )


# --- the registry --------------------------------------------------------


def test_two_classes_cannot_claim_one_event_name() -> None:
    """Resolving this by import order would decode the loser's rows into the
    winner's class: the same fields with a different meaning."""

    @dataclass(frozen=True, kw_only=True)
    class Impostor(DomainEvent):
        event_name: ClassVar[str] = "test.scalars"

    with pytest.raises(ValueError, match="test.scalars"):
        OutboxCodec([ScalarEvent, Impostor])


def test_registering_the_same_class_twice_is_harmless() -> None:
    codec = OutboxCodec([ScalarEvent, ScalarEvent])

    assert set(codec.registered) == {"test.scalars"}


def test_only_domain_events_can_be_registered() -> None:
    with pytest.raises(TypeError):
        OutboxCodec([str])  # type: ignore[list-item]


def test_the_default_codec_is_the_application_catalogue() -> None:
    assert set(default_codec().registered) == {
        event_type.event_name for event_type in EVENT_TYPES
    }


def test_every_publishable_catalogue_event_is_registered() -> None:
    """The fitness function.

    An event that is added to the catalogue and forgotten here does not fail
    anywhere near the omission: it publishes fine, the row commits fine, and
    the relay then cannot decode it. The symptom is rows accumulating in
    production behind an "unknown event type" error, which is a slow and
    confusing way to learn about a one-line oversight.

    Bases are excluded — a class nobody publishes has no rows to decode — and
    `DomainEvent` itself lives in another module.
    """
    declared = {
        obj
        for _, obj in inspect.getmembers(catalog, inspect.isclass)
        if issubclass(obj, DomainEvent) and obj.__module__ == catalog.__name__
    }
    bases = {base for event in declared for base in event.__mro__[1:]}
    publishable = declared - bases

    expected: set[type[DomainEvent]] = {UserRegistered, UserLoggedIn}
    assert publishable == set(EVENT_TYPES)
    assert publishable == expected


def _scalar_annotation(annotation: object) -> bool:
    """Whether a declared field type is one the codec can carry.

    Optionals are unwrapped, since `str | None` is two scalars and encodes as
    either of them. Anything else — a container, an enum, a `datetime`, another
    dataclass — is not.
    """
    if annotation in (str, int, float, bool, type(None)):
        return True
    if get_origin(annotation) in (Union, UnionType):
        return all(_scalar_annotation(arg) for arg in get_args(annotation))
    return False


def test_every_registered_event_declares_only_scalar_fields() -> None:
    """The static half of the check `encode` makes at runtime.

    `encode` refuses a bad *value*, which means the first request to publish
    the event is the one that fails. Reading the declared types instead moves
    that to the moment the field is added, which is the moment somebody is in a
    position to choose a different one.
    """
    offenders: list[str] = []
    for event_type in EVENT_TYPES:
        hints = get_type_hints(event_type)
        for field in dataclasses.fields(event_type):
            if field.name in IDENTITY_FIELDS:
                continue
            if not _scalar_annotation(hints[field.name]):
                offenders.append(f"{event_type.__qualname__}.{field.name}")

    assert offenders == []


def test_the_check_above_would_notice(codec: OutboxCodec) -> None:
    """`ListEvent` is what a rejected field looks like, so the assertion above
    passing means something."""
    hints = get_type_hints(ListEvent)

    assert not _scalar_annotation(hints["tags"])
    assert _scalar_annotation(get_type_hints(UserRegistered)["via"])
