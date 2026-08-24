"""Turning an event into a row's payload, and back again on the other side.

The two halves run in different processes, possibly on different deployments,
minutes or hours apart. Everything below follows from that: the payload has to
be self-describing enough to reconstruct the event, and any disagreement about
what a value *means* has to be impossible rather than unlikely.

**Only JSON scalars, and the check runs at publish time.** A field whose value
is not a `str`, `int`, `float`, `bool` or `None` is refused, by exact type, in
the request that published the event. Three reasons, in order of how much they
cost when ignored:

1. *Asymmetry is silent.* A tuple encodes as a JSON array and comes back a
   list; a `StrEnum` encodes as a string and comes back a string. The event the
   subscriber receives then differs from the one the producer published, in a
   frozen dataclass whose equality nobody thought to doubt.
2. *A guessing codec has to keep guessing.* Coercing back by declared type
   means resolving annotations — which are strings here, thanks to
   `from __future__ import annotations` — and then owning a type registry that
   has to stay right for every field anyone ever adds.
3. *A domain event is a flat record of facts.* A field that will not fit in a
   scalar is usually a document that has been put in an event, and the fix is
   to name the facts a subscriber actually needs.

The exact-type check is deliberately stricter than `isinstance`: `IntEnum` and
`StrEnum` pass an `isinstance` check against `int` and `str` and would then hit
failure mode 1.

**`event_id` and `occurred_at` are not in the payload.** They have columns —
one because consumers deduplicate on it, the other because the lag between
"happened" and "delivered" is worth being able to measure — and storing them
twice would allow the two copies to disagree.

**Non-finite floats are refused too.** `float("nan")` is a Python float that
JSON cannot represent and Postgres `jsonb` rejects. Left alone it fails at
`COMMIT`, which is to say it fails the request in a place that names neither
the event nor the field.
"""

from __future__ import annotations

import dataclasses
import math
from collections.abc import Iterable, Mapping
from datetime import datetime
from functools import lru_cache
from typing import Any, Final

from src.events.base import DomainEvent
from src.events.catalog import EVENT_TYPES
from src.immutable import FrozenDict
from src.outbox.base import (
    EventNotDecodableError,
    EventNotSerializableError,
    UnknownEventTypeError,
)

#: Event fields that have columns of their own and are therefore not payload.
IDENTITY_FIELDS: Final[frozenset[str]] = frozenset({"event_id", "occurred_at"})

#: The exact types a payload value may have. Matched with `type(value) is`,
#: never `isinstance`, so a subclass with its own semantics does not slip
#: through and come back as its base.
SCALAR_TYPES: Final[tuple[type, ...]] = (str, int, float, bool, type(None))


class OutboxCodec:
    """Encodes events for the outbox table and decodes the rows back.

    Constructed with the event types it may decode, so a test can register two
    of its own without touching the application catalogue and without its
    events becoming visible to anything else.
    """

    def __init__(self, event_types: Iterable[type[DomainEvent]]) -> None:
        """
        Args:
            event_types: Every event class whose rows this codec may read.

        Raises:
            TypeError: Something in `event_types` is not a `DomainEvent`
                subclass.
            ValueError: Two different classes claim the same `event_name`.
                Refused rather than resolved by import order, because the
                loser's rows would decode into the winner's class — the same
                fields with a different meaning, which is worse than an error.
        """
        registry: dict[str, type[DomainEvent]] = {}
        for event_type in event_types:
            if not (
                isinstance(event_type, type) and issubclass(event_type, DomainEvent)
            ):
                raise TypeError(
                    f"{event_type!r} is not a DomainEvent subclass and cannot "
                    "be registered with the outbox codec."
                )
            name = event_type.event_name
            existing = registry.get(name)
            if existing is not None and existing is not event_type:
                raise ValueError(
                    f"Two event classes claim event_name '{name}': "
                    f"{existing.__qualname__} and {event_type.__qualname__}."
                )
            registry[name] = event_type
        self._registry: FrozenDict[str, type[DomainEvent]] = FrozenDict(registry)

    @property
    def registered(self) -> FrozenDict[str, type[DomainEvent]]:
        """The name-to-class map this codec decodes with."""
        return self._registry

    def encode(self, event: DomainEvent) -> dict[str, Any]:
        """The event's own fields, as a JSON object the row can hold.

        Returns a plain `dict` because it goes straight into the JSONB column
        and SQLAlchemy's serialiser wants a real mapping; it is built here and
        handed over, so nothing else holds a reference to mutate.

        Raises:
            EventNotSerializableError: A field is not a JSON scalar, or is a
                float that JSON cannot represent. Names the field, since
                "somewhere in this event" is not a diagnosis.
        """
        payload: dict[str, Any] = {}
        for field in dataclasses.fields(event):
            if field.name in IDENTITY_FIELDS:
                continue
            value = getattr(event, field.name)
            self._check_scalar(event, field.name, value)
            payload[field.name] = value
        return payload

    @staticmethod
    def _check_scalar(event: DomainEvent, field_name: str, value: object) -> None:
        if not any(type(value) is scalar for scalar in SCALAR_TYPES):
            raise EventNotSerializableError(
                f"Field '{field_name}' of {type(event).event_name} is a "
                f"{type(value).__name__}; outbox payloads hold JSON scalars "
                "only, so that what a subscriber receives is what was "
                "published.",
                details={"event": type(event).event_name, "field": field_name},
            )
        if type(value) is float and not math.isfinite(value):
            raise EventNotSerializableError(
                f"Field '{field_name}' of {type(event).event_name} is "
                f"{value!r}, which JSON cannot represent and Postgres jsonb "
                "rejects at COMMIT.",
                details={"event": type(event).event_name, "field": field_name},
            )

    def decode(
        self,
        event_name: str,
        payload: Mapping[str, Any],
        *,
        event_id: str,
        occurred_at: datetime,
    ) -> DomainEvent:
        """Rebuild the event a row describes.

        Raises:
            UnknownEventTypeError: No registered class claims `event_name`.
                Usually a relay running behind the producer mid-deploy, so the
                caller should retry rather than discard.
            EventNotDecodableError: The payload does not fit the class — a
                field that has been renamed, added without a default, or
                removed.
        """
        event_type = self._registry.get(event_name)
        if event_type is None:
            raise UnknownEventTypeError(
                f"No event type is registered for '{event_name}'. A relay "
                "running an older build than the producer will see this until "
                "the deployment finishes.",
                details={"event": event_name},
            )
        try:
            return event_type(
                event_id=event_id, occurred_at=occurred_at, **dict(payload)
            )
        except TypeError as exc:
            raise EventNotDecodableError(
                f"Payload for '{event_name}' does not fit "
                f"{event_type.__qualname__}: {exc}",
                details={"event": event_name, "fields": sorted(payload)},
            ) from exc


@lru_cache(maxsize=1)
def default_codec() -> OutboxCodec:
    """The codec over `src.events.catalog.EVENT_TYPES`.

    Cached because it is immutable and building it walks the catalogue. A test
    that registers its own types constructs `OutboxCodec` directly rather than
    clearing this, so nothing it registers can leak into another test.
    """
    return OutboxCodec(EVENT_TYPES)


__all__ = [
    "IDENTITY_FIELDS",
    "SCALAR_TYPES",
    "OutboxCodec",
    "default_codec",
]
