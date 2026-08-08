"""The event contract: identity, immutability, naming and the bus errors."""

from __future__ import annotations

import re
from dataclasses import FrozenInstanceError, dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

import pytest

from src.events.base import (
    DomainEvent,
    EventBusError,
    EventCycleError,
    EventDispatchError,
)
from src.exceptions import AppException


@dataclass(frozen=True, kw_only=True)
class OrderPlaced(DomainEvent):
    order_id: str
    total_cents: int


@dataclass(frozen=True, kw_only=True)
class OrderShipped(DomainEvent):
    event_name: ClassVar[str] = "order.shipped.v2"

    order_id: str


# --- identity ---


def test_event_gets_an_id_and_timestamp_without_being_asked() -> None:
    event = OrderPlaced(order_id="ord_1", total_cents=500)

    assert event.event_id
    assert event.occurred_at.tzinfo is not None


def test_occurred_at_is_utc_and_now() -> None:
    before = datetime.now(UTC)
    event = OrderPlaced(order_id="ord_1", total_cents=500)

    assert event.occurred_at.utcoffset() == timedelta(0)
    assert before <= event.occurred_at <= datetime.now(UTC)


def test_each_event_gets_a_distinct_id() -> None:
    first = OrderPlaced(order_id="ord_1", total_cents=500)
    second = OrderPlaced(order_id="ord_1", total_cents=500)

    assert first.event_id != second.event_id


def test_identity_fields_can_be_pinned_for_a_test() -> None:
    stamped = datetime(2026, 1, 1, tzinfo=UTC)
    event = OrderPlaced(
        event_id="evt_fixed", occurred_at=stamped, order_id="ord_1", total_cents=500
    )

    assert event.event_id == "evt_fixed"
    assert event.occurred_at == stamped


# --- immutability ---


def test_event_is_frozen() -> None:
    event = OrderPlaced(order_id="ord_1", total_cents=500)

    with pytest.raises(FrozenInstanceError):
        event.order_id = "ord_2"  # type: ignore[misc]


def test_subclass_fields_are_keyword_only() -> None:
    """Positional construction is refused, which is what keeps the base's
    defaulted fields from colliding with a subclass's required ones."""
    # Through `Any`, because the point is the runtime behaviour: mypy rejects
    # the same call statically, and four `type: ignore` codes would bury it.
    constructor: Any = OrderPlaced

    with pytest.raises(TypeError, match="positional"):
        constructor("ord_1", 500)


# --- naming ---


def test_event_name_defaults_to_the_class_name() -> None:
    assert OrderPlaced.event_name == "OrderPlaced"


def test_event_name_can_be_pinned_against_a_rename() -> None:
    assert OrderShipped.event_name == "order.shipped.v2"


def test_subclass_does_not_inherit_its_parents_name() -> None:
    @dataclass(frozen=True, kw_only=True)
    class OrderPlacedLate(OrderPlaced):
        pass

    assert OrderPlacedLate.event_name == "OrderPlacedLate"


def test_base_event_keeps_its_own_name() -> None:
    assert DomainEvent.event_name == "DomainEvent"


# --- errors ---


def test_dispatch_error_reports_every_failure() -> None:
    errors: tuple[BaseException, ...] = (ValueError("boom"), KeyError("missing"))
    error = EventDispatchError("user.registered", errors)

    assert error.errors == errors
    assert "2 subscriber(s)" in error.message
    assert error.details == {
        "event": "user.registered",
        "errors": ["ValueError: boom", "KeyError: 'missing'"],
    }


def test_cycle_error_names_the_event_that_closed_the_ring() -> None:
    error = EventCycleError("user.registered", 8)

    assert "user.registered" in error.message
    assert error.details == {"event": "user.registered", "max_depth": 8}


@pytest.mark.parametrize(
    ("error", "expected_code"),
    [
        (EventBusError("nope"), "EVENT_BUS_ERROR"),
        (EventDispatchError("e", (ValueError(),)), "EVENT_DISPATCH_FAILED"),
        (EventCycleError("e", 8), "EVENT_CYCLE_DETECTED"),
    ],
)
def test_bus_errors_carry_a_status_and_code_for_the_edge(
    error: EventBusError, expected_code: str
) -> None:
    """They reach the exception handler like any other AppException, so a
    subscriber failure that a caller chose to raise still renders as JSON."""
    assert isinstance(error, AppException)
    assert error.status_code == 500
    assert error.error_code == expected_code


def test_dispatch_error_message_is_safe_to_render() -> None:
    error = EventDispatchError("user.registered", (ValueError("boom"),))

    assert re.fullmatch(r"1 subscriber\(s\) of 'user\.registered' failed\.", str(error))
