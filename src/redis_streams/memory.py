"""An in-process model: enough Redis Streams to test the parts that are not.

Here for the same reason `src/kafka/memory.py` and `InMemoryIdempotencyStore`
are: the contract suite needs a second implementation to run against, and a
developer without a server needs to be able to run the application. It is a
model, not an emulator, and it is honest about which behaviours it reproduces —
the ones the consume policy's correctness rests on:

- **ids that are a clock plus a counter**, monotonic within a stream, so
  ordering and range paging behave as they do on a server.
- **a pending entries list per group**, holding an owner, a last-delivery time
  and a delivery counter per message — the three facts claiming decides on.
- **idle time from an injectable clock**, which is the whole reason this class
  exists: a test that had to wait sixty seconds for a message to go stale is a
  test nobody runs.
- **entries that vanish from under a pending entry.** Trimming and deletion
  remove messages without consulting any group, and a claim then finds a
  pending entry with nothing behind it. That path is a real production event
  (any capped stream reaches it eventually) and is unreachable in a test that
  cannot delete an entry out from under a consumer.

What it does not model: persistence, replication, `XAUTOCLAIM`, consumer
removal, `MAXLEN` as an *approximate* trim (it is exact here, and a real server
trims at node boundaries), or any of the timing of a real network. A test that
depends on one of those belongs against the real server, which is what the
`redis` leg of `tests/test_redis_streams_contract.py` is for.

Not a `dataclass` anywhere below except the frozen ones: everything here is
mutable state by design, and the immutability gate's exemption table is for
values that must be mutable rather than for objects that are only state.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence

import structlog

from src.decorators.base import DEFAULT_CLOCK, Clock
from src.immutable import FrozenDict
from src.redis_streams.base import (
    ClaimedMessage,
    Fields,
    PendingEntry,
    StreamEntryId,
    StreamGroupError,
    StreamLifecycleError,
    StreamMessage,
    validate_fields,
)

logger = structlog.get_logger(__name__)


class _Pending:
    """One PEL row: who owns a message, since when, and how many times."""

    __slots__ = ("consumer", "delivered_at", "delivery_count")

    def __init__(self, consumer: str, delivered_at: float) -> None:
        self.consumer = consumer
        self.delivered_at = delivered_at
        self.delivery_count = 1


class _Group:
    """A consumer group: where it has read to, and what it still owes."""

    __slots__ = ("last_delivered", "pending")

    def __init__(self, last_delivered: StreamEntryId) -> None:
        self.last_delivered = last_delivered
        #: Insertion order is id order, because entries only ever enter it by
        #: being read, and reads go forward. Claiming rewrites a row in place
        #: rather than moving it, so the order survives.
        self.pending: dict[StreamEntryId, _Pending] = {}


class _Stream:
    """One stream key: its entries, its groups, and its id counter."""

    __slots__ = ("entries", "groups", "last_id", "waiters")

    def __init__(self) -> None:
        self.entries: dict[StreamEntryId, FrozenDict[str, bytes]] = {}
        self.groups: dict[str, _Group] = {}
        self.last_id = StreamEntryId(0, 0)
        #: Woken by a publish so a blocking read returns as soon as there is
        #: something to return, rather than after its full timeout.
        self.waiters: list[asyncio.Event] = []


class InMemoryStreamServer:
    """The streams themselves, shared by every group built from one server.

    A publisher and a consumer that do not share one of these share nothing at
    all: the messages go into one object and are read from another, and the
    failure looks like a stream that is always empty rather than like a
    misconfiguration. That is why `src/redis_streams/factory.py` caches one
    process-wide, exactly as the Kafka factory caches its broker.
    """

    def __init__(self, *, clock: Clock = DEFAULT_CLOCK) -> None:
        """
        Args:
            clock: Monotonic seconds. Injectable because idle time is what
                claiming is decided on, and a test that has to spend sixty
                real seconds to reach a stalled message will not be written.
        """
        self._clock = clock
        self._streams: dict[str, _Stream] = {}

    @property
    def clock(self) -> Clock:
        return self._clock

    def stream_names(self) -> tuple[str, ...]:
        return tuple(sorted(self._streams))

    def group(
        self,
        *,
        stream: str,
        group: str,
        consumer: str,
        maxlen: int = 0,
    ) -> InMemoryStreamGroup:
        """A client for one group in this server. Creates nothing until `start`."""
        return InMemoryStreamGroup(
            self, stream=stream, group=group, consumer=consumer, maxlen=maxlen
        )

    # -- the state a real server owns ---------------------------------------

    def _stream_for(self, name: str) -> _Stream:
        stream = self._streams.get(name)
        if stream is None:
            stream = _Stream()
            self._streams[name] = stream
        return stream

    def _next_id(self, stream: _Stream) -> StreamEntryId:
        """The server's id rule: milliseconds now, or the last id plus a step.

        The `max` is what keeps ids monotonic when the clock does not move —
        which is the normal case under a pinned test clock, and a real one when
        two messages land in the same millisecond.
        """
        ms = int(self._clock() * 1000)
        if ms > stream.last_id.ms:
            return StreamEntryId(ms, 0)
        return StreamEntryId(stream.last_id.ms, stream.last_id.seq + 1)

    def append(self, name: str, fields: Fields, *, maxlen: int = 0) -> StreamEntryId:
        """`XADD`. Public so a test can produce without building a group."""
        validate_fields(fields)
        stream = self._stream_for(name)
        entry_id = self._next_id(stream)
        stream.entries[entry_id] = FrozenDict[str, bytes](dict(fields))
        stream.last_id = entry_id
        if maxlen:
            self._trim(stream, maxlen)
        for waiter in stream.waiters:
            waiter.set()
        return entry_id

    def delete(self, name: str, entry_id: StreamEntryId) -> int:
        """`XDEL`: remove an entry, leaving any pending entry for it behind.

        That asymmetry is the point of having this. The PEL is not consulted,
        so a group can be left owing a message that no longer exists, and the
        only thing that resolves it is a claim.
        """
        stream = self._stream_for(name)
        return 1 if stream.entries.pop(entry_id, None) is not None else 0

    def _trim(self, stream: _Stream, maxlen: int) -> None:
        """Keep the newest `maxlen` entries. Exact, where a real `~` is not."""
        excess = len(stream.entries) - maxlen
        if excess <= 0:
            return
        for entry_id in sorted(stream.entries)[:excess]:
            del stream.entries[entry_id]

    def entry_count(self, name: str) -> int:
        return len(self._stream_for(name).entries)

    def entries(self, name: str) -> tuple[tuple[StreamEntryId, Fields], ...]:
        """Every entry in a stream, oldest first, without going through a group.

        The only way to look at a stream a group has never read — a
        dead-letter stream, say, whose group would be created at `$` and see
        nothing that is already in it. `XRANGE`, in other words, which the
        consumer protocol has no reason to expose.
        """
        stream = self._stream_for(name)
        return tuple(
            (entry_id, stream.entries[entry_id]) for entry_id in sorted(stream.entries)
        )


class InMemoryStreamGroup:
    """`StreamConsumerGroup` against an `InMemoryStreamServer`."""

    def __init__(
        self,
        server: InMemoryStreamServer,
        *,
        stream: str,
        group: str,
        consumer: str,
        maxlen: int = 0,
    ) -> None:
        if not stream:
            raise ValueError("stream must not be empty.")
        if not group:
            raise ValueError("group must not be empty.")
        if not consumer:
            raise ValueError("consumer must not be empty.")
        if maxlen < 0:
            raise ValueError("maxlen cannot be negative.")
        self._server = server
        self._stream = stream
        self._group = group
        self._consumer = consumer
        self._maxlen = maxlen
        self._started = False

    @property
    def stream(self) -> str:
        return self._stream

    @property
    def group(self) -> str:
        return self._group

    @property
    def consumer(self) -> str:
        return self._consumer

    @property
    def started(self) -> bool:
        return self._started

    async def start(self) -> None:
        """Create the stream and the group. Idempotent.

        A group created on a stream that already has entries starts at its
        last id, which is the `$` a real group is created with here: history
        that arrived before anyone was listening is not this group's work.
        """
        stream = self._server._stream_for(self._stream)
        if self._group not in stream.groups:
            stream.groups[self._group] = _Group(last_delivered=stream.last_id)
        self._started = True

    async def stop(self) -> None:
        """Stop using the group, and leave every pending entry where it is.

        Deliberately not the mirror of `RedisStreamGroup.stop`, which removes
        an empty consumer: there is no consumer *record* here to remove, and
        modelling `XGROUP DELCONSUMER` would mean modelling the data loss it
        causes, which nothing in this package is allowed to depend on.
        """
        self._started = False

    def _require_started(self) -> _Group:
        if not self._started:
            raise StreamLifecycleError(
                "This consumer group has not been started.",
                details={"stream": self._stream, "group": self._group},
            )
        stream = self._server._stream_for(self._stream)
        group = stream.groups.get(self._group)
        if group is None:  # pragma: no cover - only via direct state surgery
            raise StreamGroupError(
                "The consumer group has gone.",
                details={"stream": self._stream, "group": self._group},
            )
        return group

    async def publish(
        self, fields: Fields, *, stream: str | None = None
    ) -> StreamEntryId:
        return self._server.append(
            stream if stream is not None else self._stream,
            fields,
            maxlen=self._maxlen,
        )

    async def read(self, *, count: int, block: float) -> Sequence[StreamMessage]:
        if count < 1:
            raise ValueError("count must be at least 1.")
        self._require_started()
        messages = self._read_now(count)
        if messages or block <= 0:
            return messages
        # One wait, then one more look: a publish sets the event, and a
        # timeout means the stream was still empty when the caller's patience
        # ran out — which is an empty result, not an error.
        stream = self._server._stream_for(self._stream)
        waiter = asyncio.Event()
        stream.waiters.append(waiter)
        try:
            await asyncio.wait_for(waiter.wait(), timeout=block)
        except TimeoutError:
            return ()
        finally:
            stream.waiters.remove(waiter)
        return self._read_now(count)

    def _read_now(self, count: int) -> tuple[StreamMessage, ...]:
        stream = self._server._stream_for(self._stream)
        group = self._require_started()
        now = self._server.clock()
        messages: list[StreamMessage] = []
        for entry_id in sorted(stream.entries):
            if entry_id <= group.last_delivered:
                continue
            group.last_delivered = entry_id
            group.pending[entry_id] = _Pending(self._consumer, now)
            messages.append(
                StreamMessage(
                    stream=self._stream,
                    id=entry_id,
                    fields=stream.entries[entry_id],
                    delivery_count=1,
                )
            )
            if len(messages) >= count:
                break
        return tuple(messages)

    async def stalled(
        self, *, min_idle: float, count: int, start: StreamEntryId | None = None
    ) -> Sequence[PendingEntry]:
        if count < 1:
            raise ValueError("count must be at least 1.")
        group = self._require_started()
        now = self._server.clock()
        found: list[PendingEntry] = []
        for entry_id in sorted(group.pending):
            if start is not None and entry_id <= start:
                continue
            row = group.pending[entry_id]
            idle = now - row.delivered_at
            if idle < min_idle:
                continue
            found.append(
                PendingEntry(
                    id=entry_id,
                    consumer=row.consumer,
                    idle=idle,
                    delivery_count=row.delivery_count,
                )
            )
            if len(found) >= count:
                break
        return tuple(found)

    async def claim(
        self, entries: Sequence[PendingEntry], *, min_idle: float
    ) -> ClaimedMessage:
        group = self._require_started()
        if not entries:
            return ClaimedMessage(messages=(), missing=())
        stream = self._server._stream_for(self._stream)
        now = self._server.clock()
        claimed: list[StreamMessage] = []
        missing: list[StreamEntryId] = []
        for entry in entries:
            row = group.pending.get(entry.id)
            if row is None or now - row.delivered_at < min_idle:
                # Acknowledged, or re-read by its owner since the scan. Either
                # way it is not this consumer's to take.
                missing.append(entry.id)
                continue
            fields = stream.entries.get(entry.id)
            if fields is None:
                # The entry was deleted or trimmed away while pending. A real
                # server drops the dangling PEL row as it claims, and so does
                # this: leaving it would be an obligation nothing can ever
                # discharge, since there is no message left to acknowledge.
                del group.pending[entry.id]
                missing.append(entry.id)
                continue
            row.consumer = self._consumer
            row.delivered_at = now
            row.delivery_count += 1
            claimed.append(
                StreamMessage(
                    stream=self._stream,
                    id=entry.id,
                    fields=fields,
                    delivery_count=row.delivery_count,
                    claimed=True,
                )
            )
        return ClaimedMessage(messages=tuple(claimed), missing=tuple(missing))

    async def ack(self, ids: Sequence[StreamEntryId]) -> int:
        if not ids:
            return 0
        group = self._require_started()
        return sum(1 for entry_id in ids if group.pending.pop(entry_id, None))

    async def pending_count(self, *, consumer: str | None = None) -> int:
        group = self._require_started()
        if consumer is None:
            return len(group.pending)
        return sum(1 for row in group.pending.values() if row.consumer == consumer)


__all__ = ["InMemoryStreamGroup", "InMemoryStreamServer"]
