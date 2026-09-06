"""What a stream message is, and the port a consumer group is used through.

Nothing here imports `redis`. That is what lets the consume policy in
`consumer.py` — claim-then-read, the delivery cap, what happens to a message a
handler raised on — run against an in-process model where idle time is a number
a test sets, while the parts that are genuinely Redis are exercised against a
real server in CI.

## A pending entries list is not a committed offset

This is the fact the whole package is shaped by, and it is the one thing that
does *not* transfer from `src/kafka`. A Kafka group stores one number per
partition, so "record 5 failed, record 6 succeeded" cannot be expressed and the
runner there stops the whole partition at its first failure. A Redis Streams
group stores a *set*: every message handed to a consumer sits in that group's
pending entries list (the PEL) until it is acknowledged by id, so acknowledging
6 while 5 is still pending is not a workaround, it is the data model.

Three consequences, and they are why this package exists next to `src/kafka`
rather than inside it:

- **Failure isolation is per message.** A message whose handler raised is
  simply not acknowledged. Everything behind it in the stream continues.
  There is no head-of-line blocking to trade against, so there is no dead
  letter *ladder* here either — the escape hatch is the delivery cap below.
- **Nothing redelivers a message on its own.** A Kafka record left uncommitted
  comes back at the next rebalance or restart. A pending entry is owned by the
  consumer that read it, and if that process is gone it stays owned by a name
  nobody is running, forever. Redelivery is something another consumer must
  ask for, and asking is `XPENDING` plus `XCLAIM` — the "stalled-message
  claiming" this package is named for.
- **Redelivery is counted.** The PEL carries a delivery counter per message,
  so a poison message is visible as a number rather than as a partition whose
  lag grows. `StreamConsumerConfig.max_deliveries` is what stops it going
  round the group forever.

## Entry ids are a clock, not a sequence

`1698412345678-0` is milliseconds since the epoch and a counter within the
millisecond. They are assigned by the server, monotonic within a stream, and
comparable — but the first half is wall-clock time, so an id is not a count of
anything and the gap between two ids says nothing about how many messages are
between them. `StreamEntryId` exists so ordering comparisons are done on the
pair rather than on a string, where `"9-0" > "10-0"` lexicographically and is
wrong.
"""

from __future__ import annotations

import os
import socket
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final, Protocol, runtime_checkable
from uuid import uuid4

from src.exceptions import AppException
from src.immutable import FrozenDict

#: One message's payload: the field-value hash `XADD` takes. Values stay
#: `bytes` for the same reason `ConsumedMessage.value` does — what a producer
#: put on the stream is what a consumer should be handed, and a codec is the
#: caller's decision, not the transport's.
Fields = Mapping[str, bytes]


class StreamError(AppException):
    """Base for the failures this package produces.

    503 rather than 500, matching `src/kafka`: every subclass means a
    dependency of this service is unhappy rather than that the caller was
    wrong, and a handler publishing to a stream as part of serving a request
    should let that reach the client as "try again".
    """

    status_code = 503
    error_code = "STREAM_ERROR"

    def __init__(self, message: str, details: object = None) -> None:
        super().__init__(message, details)


class StreamUnavailableError(StreamError):
    """The server could not be reached, or answered an error.

    Deliberately does not distinguish "the write did not happen" from "the
    write may have happened": a timed-out `XADD` is an unknown, and treating it
    as a negative is how a retry becomes a duplicate that nobody expected.
    Consumers of a stream must be idempotent for that reason alone.
    """

    error_code = "STREAM_UNAVAILABLE"


class StreamPublishError(StreamError):
    """A message could not be appended to a stream."""

    error_code = "STREAM_PUBLISH_FAILED"


class StreamGroupError(StreamError):
    """A group operation — create, read, claim, acknowledge — failed."""

    error_code = "STREAM_GROUP_FAILED"


class StreamDecodeError(StreamError):
    """A message came back with field names that are not text.

    A 500 rather than a 503: nothing is wrong with the server, and reading the
    message again produces the same bytes. Field *names* are this codebase's
    own and are always UTF-8; values are opaque and are never decoded here.
    """

    status_code = 500
    error_code = "STREAM_NOT_DECODABLE"


class StreamLifecycleError(StreamError):
    """A group was used before `start()` or after `stop()`.

    A hard error rather than an implicit start, for the reason
    `src/kafka/base.py` gives: an implicit start builds a connection pool from
    inside whichever coroutine happened to publish first, so the pool's
    lifetime is a request's rather than the process's and shutdown has nothing
    to close.
    """

    status_code = 500
    error_code = "STREAM_LIFECYCLE"


@dataclass(frozen=True, slots=True, order=True)
class StreamEntryId:
    """A stream entry id: `<milliseconds>-<sequence>`.

    `order=True` because every range this package asks for is expressed as one
    of these, and the ordering has to be numeric. Comparing the string forms
    would put `1698412345678-10` before `1698412345678-9`, which is the kind of
    bug that only appears once a millisecond carries ten messages.
    """

    ms: int
    seq: int

    def __post_init__(self) -> None:
        if self.ms < 0 or self.seq < 0:
            raise ValueError(f"Entry id parts cannot be negative: {self.ms}-{self.seq}")

    @classmethod
    def parse(cls, value: str | bytes) -> StreamEntryId:
        """Read the form the server sends and the form a person types.

        Refuses anything else rather than guessing. `$`, `>` and `*` are all
        legal *arguments* to stream commands and none of them is an id — they
        are positions, and letting one through here would produce an id of
        whatever `int("$")` did not return.
        """
        text = value.decode() if isinstance(value, bytes) else value
        head, _, tail = text.partition("-")
        if not tail:
            raise ValueError(f"Malformed stream entry id: {text!r}")
        try:
            return cls(int(head), int(tail))
        except ValueError as exc:
            raise ValueError(f"Malformed stream entry id: {text!r}") from exc

    def __str__(self) -> str:
        return f"{self.ms}-{self.seq}"

    @property
    def exclusive(self) -> str:
        """This id as an *exclusive* range start: `(1698412345678-0`.

        Redis 6.2 added the `(` prefix, and paging without it is a loop: an
        inclusive start re-reads the entry the previous page ended on, so a
        scan of a PEL larger than one page never advances past it.
        """
        return f"({self}"


#: The id `XADD` is given when the server should assign one. Not a
#: `StreamEntryId` because it is not an id — it is the instruction to make one.
AUTO_ID: Final[str] = "*"

#: What `XREADGROUP` is given to mean "messages never delivered to anyone in
#: this group". The alternative, `0`, means "this consumer's own pending
#: entries", which is a different question and is answered here by claiming.
NEW_MESSAGES: Final[str] = ">"


@dataclass(frozen=True, slots=True)
class StreamMessage:
    """One message, detached from the read that fetched it.

    `delivery_count` is on the message rather than looked up when needed
    because it decides what happens to the message: a claim is where a poison
    message becomes visible, and the count is the only evidence that it is one.
    A message read with `XREADGROUP` for the first time has a count of 1 —
    being handed to a consumer *is* a delivery, which is why the cap is
    `> max_deliveries` rather than `>=`.
    """

    stream: str
    id: StreamEntryId
    fields: FrozenDict[str, bytes]
    delivery_count: int
    #: Whether this message arrived by claiming a stalled entry rather than by
    #: reading new ones. Carried so a log line can say which, and so a handler
    #: that wants to behave differently on a redelivery can — the first
    #: delivery of a claimed message is somebody else's, so `delivery_count`
    #: alone does not answer it.
    claimed: bool = False

    def field(self, name: str) -> bytes | None:
        return self.fields.get(name)


@dataclass(frozen=True, slots=True)
class PendingEntry:
    """One row of `XPENDING`: a message owed by a consumer, and for how long.

    This is what claiming is decided from, and it deliberately carries no
    payload. `XPENDING` reads the group's bookkeeping only, so a scan for
    stalled work costs nothing in bandwidth however large the messages are, and
    the delivery count is known *before* a claim — which is what lets a message
    over its cap be routed to the dead-letter stream rather than handed to a
    handler that has already failed on it four times.
    """

    id: StreamEntryId
    consumer: str
    #: Seconds since this message was last delivered. Redis reports
    #: milliseconds; it is converted here so nothing outside this package has
    #: to remember which unit it is holding.
    idle: float
    delivery_count: int


@dataclass(frozen=True, slots=True)
class ClaimedMessage:
    """What `claim` gives back, and what it could not find.

    Claiming can return fewer messages than were asked for, and the gap is not
    an error: an entry can be in the PEL while the message itself is gone from
    the stream, because `XDEL` and `MAXLEN` trimming delete entries without
    consulting any group. Redis resolves that by dropping the dangling entry
    from the PEL as it claims — measured, not assumed; see
    `docs/redis-streams.md` — so the count belongs in the result rather than in
    a log line nobody reads: those messages are gone, and the acknowledgement
    they will never get is not a leak.

    The other reason for a gap is benign and must not be confused with it: an
    entry whose owner re-read it between the `XPENDING` and the `XCLAIM` is no
    longer idle enough to claim, so it is skipped. Both leave the entry in
    somebody's hands rather than in this consumer's, which is why the runner
    counts them together and calls them `vanished` only in the log.
    """

    messages: tuple[StreamMessage, ...]
    #: Entries that were asked for and not returned.
    missing: tuple[StreamEntryId, ...]


def default_consumer_name() -> str:
    """A consumer name that is stable for a process and unique across them.

    Both halves matter and pull in opposite directions. Stability is what lets
    a process that restarts under the same name find its own pending entries
    where it left them; uniqueness is what keeps two replicas from sharing one
    PEL, where each would see the other's in-flight work as its own and claim
    it back mid-flight.

    Host and pid give both on a real deployment, and the uuid suffix covers the
    case they do not: two containers on one host can share a pid namespace, and
    a pid is reused within minutes on a busy machine. The cost is a consumer
    record per process in `XINFO CONSUMERS` — see `RedisStreamGroup.stop`,
    which removes this consumer when, and only when, it owes nothing.
    """
    return f"{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"


def validate_fields(fields: Fields) -> None:
    """The two refusals every publish makes.

    An empty hash is refused because `XADD key *` with no field is a syntax
    error from the server, arriving as an opaque `ResponseError` at whatever
    call site happened to build an empty dict. A non-bytes value is refused
    because redis-py would encode it — `1` and `"1"` both become `b"1"` — and
    the consumer would be handed a type nobody chose.
    """
    if not fields:
        raise ValueError("A stream message needs at least one field.")
    for name, value in fields.items():
        if not isinstance(value, bytes):
            raise TypeError(
                f"Field {name!r} must be bytes, got {type(value).__name__}. "
                "Encode it at the call site so the consumer is handed what you sent."
            )


@runtime_checkable
class StreamConsumerGroup(Protocol):
    """One stream, one group, one consumer name within it.

    Narrower than `MessageSource` in `src/kafka` in one way and wider in
    another. Narrower because there is no assignment to expose: a group hands
    out individual messages rather than partitions, so nothing here answers
    "what do I own". Wider because redelivery is explicit — `stalled` and
    `claim` are two of the five methods, and in Kafka they have no counterpart
    at all.

    `start`/`stop` rather than a context manager, and both idempotent: the
    lifetime is the process's, and `consumer.py` calls `start` on every pass of
    its loop so a server that is unreachable at start-up is a retry rather than
    a consumer that quietly never consumes.
    """

    @property
    def stream(self) -> str: ...

    @property
    def group(self) -> str: ...

    @property
    def consumer(self) -> str: ...

    async def start(self) -> None:
        """Create the stream and the group if they are not there yet."""
        ...

    async def stop(self) -> None: ...

    async def publish(
        self, fields: Fields, *, stream: str | None = None
    ) -> StreamEntryId:
        """Append one message and return the id the server assigned.

        `stream` defaults to this group's own. It is a parameter because the
        dead-letter stream is written through the same connection and is not
        worth a second object — and because a stream is created by its first
        `XADD`, so writing to one nobody has read from yet is ordinary.
        """
        ...

    async def read(self, *, count: int, block: float) -> Sequence[StreamMessage]:
        """Take up to `count` messages never delivered to this group.

        Blocks for at most `block` seconds when the stream is empty, and
        returns an empty sequence when nothing arrived. A message returned here
        is in this consumer's PEL from that moment: reading is what creates the
        obligation to acknowledge.
        """
        ...

    async def stalled(
        self, *, min_idle: float, count: int, start: StreamEntryId | None = None
    ) -> Sequence[PendingEntry]:
        """Pending entries idle for at least `min_idle` seconds, oldest first.

        Reads the group's bookkeeping only — no payloads — and changes nothing:
        an entry listed here is still owned by whoever owns it, and its
        delivery count is not incremented by having been looked at.

        `start` pages: pass the last id of the previous page and the next page
        begins strictly after it.
        """
        ...

    async def claim(
        self, entries: Sequence[PendingEntry], *, min_idle: float
    ) -> ClaimedMessage:
        """Take ownership of `entries` and get their payloads back.

        `min_idle` is passed to the server as a *condition*, not as a filter
        that has already been applied: the entries were chosen from a `stalled`
        call that has since returned, and the owner may have re-read them in
        between. Re-checking it server-side is what keeps two consumers from
        both claiming the same message when both were scanning at once.

        Every message returned has its delivery count incremented by one, which
        is the count the caller must decide the dead-letter question on.
        """
        ...

    async def ack(self, ids: Sequence[StreamEntryId]) -> int:
        """Remove `ids` from the group's PEL. Returns how many were there.

        A count below `len(ids)` is not an error: acknowledging an id twice is
        how a redelivered message ends, and the second one is a no-op.
        """
        ...

    async def pending_count(self, *, consumer: str | None = None) -> int:
        """How many messages this group — or one consumer in it — still owes."""
        ...


__all__ = [
    "AUTO_ID",
    "NEW_MESSAGES",
    "ClaimedMessage",
    "Fields",
    "PendingEntry",
    "StreamConsumerGroup",
    "StreamDecodeError",
    "StreamEntryId",
    "StreamError",
    "StreamGroupError",
    "StreamLifecycleError",
    "StreamMessage",
    "StreamPublishError",
    "StreamUnavailableError",
    "default_consumer_name",
    "validate_fields",
]
