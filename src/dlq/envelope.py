"""The retry metadata a record carries between tiers, and how it is read back.

Headers rather than the payload, for one reason that decides everything else:
**the payload is the application's.** A record that reaches a dead-letter topic
has to be replayable byte-for-byte into the origin topic, and a router that
wrapped the value in an envelope would hand the handler something it did not
publish — or would force every handler in the system to learn to unwrap. So the
value and the key are passed through untouched, and everything this package
knows lives beside them.

## Replacing, not appending

Kafka headers are an ordered sequence of pairs and duplicate names are legal —
which is why `ConsumedMessage.header` returns the *first* match, that being the
one the producer set rather than one a proxy appended in transit. The
consequence here is sharp: a router that appended `x-dlq-attempts: 2` to a
record that already carried `x-dlq-attempts: 1` would produce a record whose
attempt count reads as 1 forever. It would climb one rung of the ladder and
then circle it, and nothing about the record would look wrong.

`stamp` therefore strips every `x-dlq-` header before writing the new ones, and
leaves every other header exactly where it was — a trace context, a schema id
and a tenant tag all survive a trip through the ladder, because losing them is
how a dead letter becomes undebuggable at the moment it matters.

## Unreadable is not the same as absent

`read` distinguishes three states, and the third is why it can raise:

- **absent** — no `x-dlq-attempts` header at all. A record on its way through
  the origin topic for the first time. `None`.
- **present and readable** — an envelope.
- **present and unreadable** — a truncated timestamp, a non-numeric count,
  bytes that are not UTF-8. `MalformedEnvelopeError`, because the alternative
  is to read it as absent, and a record whose attempt count resets to zero on
  every pass rides the ladder forever while every log line about it looks like
  a first failure.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Final

from src.dlq.base import MalformedEnvelopeError
from src.kafka.base import ConsumedMessage, Headers

#: Every header this package writes starts with this, and `stamp` removes
#: anything that does. Chosen so that stripping is a prefix test rather than a
#: list that a new field could be left out of.
HEADER_PREFIX: Final[str] = "x-dlq-"

HEADER_ORIGIN_TOPIC: Final[str] = "x-dlq-origin-topic"
HEADER_ORIGIN_PARTITION: Final[str] = "x-dlq-origin-partition"
HEADER_ORIGIN_OFFSET: Final[str] = "x-dlq-origin-offset"
HEADER_ATTEMPTS: Final[str] = "x-dlq-attempts"
HEADER_FIRST_FAILED_AT: Final[str] = "x-dlq-first-failed-at"
HEADER_NOT_BEFORE: Final[str] = "x-dlq-not-before"
HEADER_ERROR: Final[str] = "x-dlq-error"
HEADER_REPLAYS: Final[str] = "x-dlq-replays"

#: Ceiling on the stored `x-dlq-error`. A traceback-sized string on every
#: record is a real cost — the header travels with the record through every
#: tier, is held in the broker's page cache and is copied on every replay — and
#: the log already has the full exception. This is the part someone reads on
#: the record itself, so it wants to be the first line, not the whole stack.
MAX_ERROR_LENGTH: Final[int] = 512

#: What a truncated error is suffixed with, so nobody mistakes the cut for the
#: end of the message.
TRUNCATION_MARKER: Final[str] = "…[truncated]"


def _decode(name: str, raw: bytes) -> str:
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise MalformedEnvelopeError(
            f"Header {name!r} is not valid UTF-8.", {"header": name}
        ) from exc


def _decode_int(name: str, raw: bytes) -> int:
    text = _decode(name, raw)
    try:
        return int(text)
    except ValueError as exc:
        raise MalformedEnvelopeError(
            f"Header {name!r} is not an integer: {text!r}.", {"header": name}
        ) from exc


def _decode_time(name: str, raw: bytes) -> datetime:
    text = _decode(name, raw)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise MalformedEnvelopeError(
            f"Header {name!r} is not an ISO-8601 timestamp: {text!r}.",
            {"header": name},
        ) from exc
    if parsed.tzinfo is None:
        # A naive timestamp is not a timestamp: comparing it to `utc_now()`
        # raises, and assuming UTC would silently misread a producer that meant
        # local time by however many hours it is offset. Every writer here
        # emits an offset, so a value without one did not come from this code.
        raise MalformedEnvelopeError(
            f"Header {name!r} has no UTC offset: {text!r}.", {"header": name}
        )
    return parsed.astimezone(UTC)


def truncate_error(text: str, *, limit: int = MAX_ERROR_LENGTH) -> str:
    """Shorten a description to fit in a header, saying that it was shortened."""
    if limit < 1:
        raise ValueError("limit must be at least 1.")
    if len(text) <= limit:
        return text
    if limit <= len(TRUNCATION_MARKER):
        return text[:limit]
    return text[: limit - len(TRUNCATION_MARKER)] + TRUNCATION_MARKER


@dataclass(frozen=True, slots=True)
class DeadLetterEnvelope:
    """Where a record came from, how often it has failed, and when it is due.

    `origin_partition` and `origin_offset` are the coordinates in the *origin*
    topic and are stamped once, at the first failure. They are not updated as
    the record moves through the tiers, which is deliberate: their job is to
    point at the record in the log that the application actually produced, so
    that whoever is holding a dead letter can go and read the records either
    side of it. The tier coordinates are on the `ConsumedMessage` in hand.
    """

    origin_topic: str
    origin_partition: int
    origin_offset: int
    #: Handler attempts that have failed, counting the one that produced this
    #: envelope. 1-based, so it lines up with `RetryLadder.destination`.
    attempts: int
    first_failed_at: datetime
    #: The earliest a consumer may hand this record to a handler. Equal to
    #: `first_failed_at` for a record going straight to a dead-letter topic,
    #: which never waits.
    not_before: datetime
    error: str
    #: How many times an operator has put this record back on the origin topic.
    #: Survives the ladder so that a record which keeps returning is visible as
    #: one recurring problem rather than as a stream of unrelated first
    #: failures.
    replays: int = 0

    def __post_init__(self) -> None:
        if self.attempts < 1:
            raise ValueError("attempts is 1-based and counts the failure just seen.")
        if self.origin_partition < 0:
            raise ValueError("origin_partition cannot be negative.")
        if self.origin_offset < 0:
            raise ValueError("origin_offset cannot be negative.")
        if self.replays < 0:
            raise ValueError("replays cannot be negative.")
        if self.first_failed_at.tzinfo is None or self.not_before.tzinfo is None:
            raise ValueError("Envelope timestamps must be timezone-aware.")

    def is_due(self, now: datetime) -> bool:
        return now >= self.not_before

    def wait_for(self, now: datetime) -> float:
        """Seconds until this record is due; zero once it is."""
        return max(0.0, (self.not_before - now).total_seconds())

    def age(self, now: datetime) -> float:
        """Seconds since the first failure — how long this has been going on."""
        return max(0.0, (now - self.first_failed_at).total_seconds())

    def to_headers(self) -> Headers:
        """This envelope as wire headers, in a stable order."""
        return (
            (HEADER_ORIGIN_TOPIC, self.origin_topic.encode("utf-8")),
            (HEADER_ORIGIN_PARTITION, str(self.origin_partition).encode("ascii")),
            (HEADER_ORIGIN_OFFSET, str(self.origin_offset).encode("ascii")),
            (HEADER_ATTEMPTS, str(self.attempts).encode("ascii")),
            (HEADER_FIRST_FAILED_AT, self.first_failed_at.isoformat().encode("ascii")),
            (HEADER_NOT_BEFORE, self.not_before.isoformat().encode("ascii")),
            (HEADER_ERROR, truncate_error(self.error).encode("utf-8")),
            (HEADER_REPLAYS, str(self.replays).encode("ascii")),
        )

    def log_fields(self) -> dict[str, object]:
        """The parts worth putting in a log line, named for a search."""
        return {
            "origin_topic": self.origin_topic,
            "origin_partition": self.origin_partition,
            "origin_offset": self.origin_offset,
            "attempts": self.attempts,
            "replays": self.replays,
            "error": self.error,
        }


def strip(headers: Headers) -> Headers:
    """Everything except this package's own headers, in wire order."""
    return tuple(
        (name, value) for name, value in headers if not name.startswith(HEADER_PREFIX)
    )


def stamp(headers: Headers, envelope: DeadLetterEnvelope) -> Headers:
    """`headers` with the caller's own preserved and the envelope replaced.

    Replaced rather than appended — see the module docstring. The application's
    headers keep their relative order and come first, because a consumer that
    reads them by position (which is a bad idea, and is done anyway) sees the
    same layout it would have seen without a dead-letter queue in the path.
    """
    return strip(headers) + envelope.to_headers()


def read(message: ConsumedMessage) -> DeadLetterEnvelope | None:
    """The envelope on `message`, or `None` if it has never failed.

    Raises `MalformedEnvelopeError` when the headers are present but cannot be
    read. Presence is decided by `x-dlq-attempts` alone: it is written on every
    hop, so a record carrying it and nothing else is a record something went
    wrong with, not a record that has not failed.
    """
    raw_attempts = message.header(HEADER_ATTEMPTS)
    if raw_attempts is None:
        return None

    attempts = _decode_int(HEADER_ATTEMPTS, raw_attempts)
    raw_first_failed = message.header(HEADER_FIRST_FAILED_AT)
    raw_not_before = message.header(HEADER_NOT_BEFORE)
    raw_origin_topic = message.header(HEADER_ORIGIN_TOPIC)
    raw_origin_partition = message.header(HEADER_ORIGIN_PARTITION)
    raw_origin_offset = message.header(HEADER_ORIGIN_OFFSET)
    if (
        raw_first_failed is None
        or raw_not_before is None
        or raw_origin_topic is None
        or raw_origin_partition is None
        or raw_origin_offset is None
    ):
        raise MalformedEnvelopeError(
            "Record carries x-dlq-attempts without the rest of the envelope.",
            {"topic": message.topic, "offset": message.offset},
        )

    raw_replays = message.header(HEADER_REPLAYS)
    raw_error = message.header(HEADER_ERROR)
    try:
        return DeadLetterEnvelope(
            origin_topic=_decode(HEADER_ORIGIN_TOPIC, raw_origin_topic),
            origin_partition=_decode_int(HEADER_ORIGIN_PARTITION, raw_origin_partition),
            origin_offset=_decode_int(HEADER_ORIGIN_OFFSET, raw_origin_offset),
            attempts=attempts,
            first_failed_at=_decode_time(HEADER_FIRST_FAILED_AT, raw_first_failed),
            not_before=_decode_time(HEADER_NOT_BEFORE, raw_not_before),
            error="" if raw_error is None else _decode(HEADER_ERROR, raw_error),
            replays=0
            if raw_replays is None
            else _decode_int(HEADER_REPLAYS, raw_replays),
        )
    except ValueError as exc:
        # `DeadLetterEnvelope.__post_init__` rejects a negative offset or a
        # zero attempt count. Those are as unreadable as a corrupt timestamp,
        # and the caller should not have to catch two exception types to mean
        # "this record's metadata is not usable".
        raise MalformedEnvelopeError(
            f"Record's dead-letter envelope is not valid: {exc}",
            {"topic": message.topic, "offset": message.offset},
        ) from exc


def read_replays(message: ConsumedMessage) -> int:
    """How many times this record has been replayed, envelope or not.

    Separate from `read` because a replayed record deliberately carries *only*
    this header: its attempt count was reset so it gets the whole ladder again,
    and `read` therefore reports it — correctly — as a record that has never
    failed. Without this, the count would be lost at the record's first failure
    after a replay, and the one number that says "this is the third time" would
    reset on exactly the pass it exists to describe.
    """
    raw = message.header(HEADER_REPLAYS)
    return 0 if raw is None else _decode_int(HEADER_REPLAYS, raw)


def advance(
    previous: DeadLetterEnvelope | None,
    message: ConsumedMessage,
    *,
    now: datetime,
    delay: float,
    error: str,
    replays: int = 0,
    origin_topic: str | None = None,
) -> DeadLetterEnvelope:
    """The envelope for the next hop, given the one that arrived (if any).

    The provenance fields come from `previous` when there is one, so they stay
    pointing at the origin topic rather than following the record down the
    ladder. On a first failure they are taken from the record in hand, which is
    usually the origin record.

    `origin_topic` overrides that last part, and the caller that has a ladder
    in hand should always pass `ladder.origin_topic`. A record can reach a
    first failure on a *tier* topic — hand-published there by an operator, or
    produced by an older deployment — and stamping the tier's own name as the
    origin makes the record unroutable on its second failure: the next hop
    would try to build a ladder on `orders.events.retry.1`, which is refused,
    and the record would stall its partition on a `ValueError` forever.

    `replays` is used only on a first failure, and only because a replayed
    record has no envelope to carry it — see `read_replays`. It is a parameter
    rather than read from `message` here so that the caller can read it inside
    whatever already handles a malformed header, instead of this function
    raising from underneath one.
    """
    if previous is None:
        return DeadLetterEnvelope(
            origin_topic=origin_topic if origin_topic is not None else message.topic,
            origin_partition=message.partition.number,
            origin_offset=message.offset,
            attempts=1,
            first_failed_at=now,
            not_before=_due_at(now, delay),
            error=error,
            replays=replays,
        )
    return replace(
        previous,
        attempts=previous.attempts + 1,
        not_before=_due_at(now, delay),
        error=error,
    )


def _due_at(now: datetime, delay: float) -> datetime:
    """`now` plus `delay`, and exactly `now` when there is no wait.

    The zero case is spelled out rather than left to `timedelta(seconds=0)` so
    that a dead-lettered record's `not_before` is byte-identical to the
    timestamp on the same record's first failure, which is what makes "this
    never waited" readable off the record instead of inferred from the topic.
    """
    return now if delay <= 0 else now + timedelta(seconds=delay)


__all__ = [
    "HEADER_ATTEMPTS",
    "HEADER_ERROR",
    "HEADER_FIRST_FAILED_AT",
    "HEADER_NOT_BEFORE",
    "HEADER_ORIGIN_OFFSET",
    "HEADER_ORIGIN_PARTITION",
    "HEADER_ORIGIN_TOPIC",
    "HEADER_PREFIX",
    "HEADER_REPLAYS",
    "MAX_ERROR_LENGTH",
    "TRUNCATION_MARKER",
    "DeadLetterEnvelope",
    "advance",
    "read",
    "read_replays",
    "stamp",
    "strip",
    "truncate_error",
]
