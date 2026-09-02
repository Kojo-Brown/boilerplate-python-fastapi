"""An in-process broker: enough Kafka to test the parts that are not Kafka.

This exists for the same reason `InMemoryIdempotencyStore` and the in-memory
lock backend do — so that the contract suite has a second implementation to run
against, and so that a developer without a cluster can run the application —
and it is honest about being a model rather than an emulator. What it does
model is the handful of behaviours the runner's correctness rests on:

- **partitions, and keys that stick to them.** A key hashes to a partition, so
  records with one key are ordered with respect to each other and records with
  different keys are not. A null key round-robins.
- **groups with committed offsets.** Offsets live on the group, not the
  consumer, so a new member resumes where the last one committed — which is the
  behaviour "manual commit" exists to produce.
- **assignment across members.** Two sources in one group split the partitions
  and see disjoint records; two groups each see everything.
- **positions that are not offsets.** A member's read position moves as it
  polls, and `seek` moves it back without touching the commit.

What it deliberately does not model: replication, leader election, retention,
compaction, transactions, the group protocol's timings (a member leaves
instantly here, where a real broker waits for a session timeout), and any
persistence at all. A test that depends on one of those is a test that belongs
against the real broker — which is what the `kafka` leg of
`tests/test_kafka_contract.py` is for.

Not a `dataclass` anywhere below: every class here is mutable state by design,
and the immutability gate's exemption table is for values that have to be
mutable, not for objects that are nothing but state.
"""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import Iterable, Mapping, Sequence
from typing import Final

import structlog

from src.kafka.base import (
    ConsumedMessage,
    ConsumerError,
    Headers,
    LifecycleError,
    Partition,
    PublishedMessage,
    normalize_headers,
    utc_now,
    validate_record,
)

logger = structlog.get_logger(__name__)

#: Partitions given to a topic nobody configured. Two rather than one so that
#: the default exercises assignment across members instead of hiding it.
DEFAULT_PARTITIONS: Final[int] = 2


class _Record:
    """One appended record, before anyone has read it."""

    __slots__ = ("headers", "key", "offset", "timestamp", "value")

    def __init__(
        self,
        *,
        offset: int,
        key: str | None,
        value: bytes | None,
        headers: Headers,
    ) -> None:
        self.offset = offset
        self.key = key
        self.value = value
        self.headers = headers
        self.timestamp = utc_now()

    def consumed(self, partition: Partition) -> ConsumedMessage:
        return ConsumedMessage(
            partition=partition,
            offset=self.offset,
            key=self.key,
            value=self.value,
            headers=self.headers,
            timestamp=self.timestamp,
        )


class _Topic:
    """A fixed number of append-only logs."""

    __slots__ = ("_next_partition", "logs", "name")

    def __init__(self, name: str, partitions: int) -> None:
        if partitions < 1:
            raise ValueError("A topic needs at least one partition.")
        self.name = name
        self.logs: list[list[_Record]] = [[] for _ in range(partitions)]
        # Round-robin cursor for null keys, which is what a real producer's
        # default partitioner does with them.
        self._next_partition = itertools.cycle(range(partitions))

    @property
    def partition_count(self) -> int:
        return len(self.logs)

    def partitions(self) -> tuple[Partition, ...]:
        return tuple(
            Partition(topic=self.name, number=index)
            for index in range(self.partition_count)
        )

    def partition_for(self, key: str | None) -> int:
        """Where a record with this key goes.

        `hash()` is deliberately not used: it is salted per process for `str`,
        so the same key would land on different partitions in two runs and a
        test asserting "one key, one partition" would pass or fail by luck.
        """
        if key is None:
            return next(self._next_partition)
        return _fnv1a(key) % self.partition_count

    def append(
        self, key: str | None, value: bytes | None, headers: Headers
    ) -> tuple[Partition, _Record]:
        index = self.partition_for(key)
        log = self.logs[index]
        record = _Record(offset=len(log), key=key, value=value, headers=headers)
        log.append(record)
        return Partition(topic=self.name, number=index), record

    def read(self, partition: Partition, position: int, limit: int) -> list[_Record]:
        log = self.logs[partition.number]
        if position >= len(log):
            return []
        return log[position : position + limit]

    def end_offset(self, partition: Partition) -> int:
        return len(self.logs[partition.number])


def _fnv1a(text: str) -> int:
    """A small, stable string hash.

    FNV-1a rather than `hash()` because partition assignment has to be the same
    in every process and every run; `PYTHONHASHSEED` makes the builtin useless
    for that. Not a cryptographic hash and not meant to match Kafka's murmur2 —
    the promise is stability, not compatibility.
    """
    result = 0x811C9DC5
    for byte in text.encode("utf-8"):
        result ^= byte
        result = (result * 0x01000193) & 0xFFFFFFFF
    return result


class _Group:
    """A consumer group: committed offsets, and who currently holds what."""

    __slots__ = ("committed", "id", "members")

    def __init__(self, group_id: str) -> None:
        self.id = group_id
        self.committed: dict[Partition, int] = {}
        self.members: list[InMemoryMessageSource] = []

    def join(self, member: InMemoryMessageSource) -> None:
        if member not in self.members:
            self.members.append(member)

    def leave(self, member: InMemoryMessageSource) -> None:
        if member in self.members:
            self.members.remove(member)


class InMemoryBroker:
    """The broker. One per test, or one per process for local development."""

    def __init__(self, *, default_partitions: int = DEFAULT_PARTITIONS) -> None:
        self._default_partitions = default_partitions
        self._topics: dict[str, _Topic] = {}
        self._groups: dict[str, _Group] = {}
        #: Replaced rather than cleared on every append, so that several
        #: waiting polls are all woken and none of them can miss a set that
        #: happened between their check and their wait.
        self._appended = asyncio.Event()

    def create_topic(self, name: str, *, partitions: int | None = None) -> None:
        """Declare a topic's partition count before anything uses it.

        Auto-creation exists — a publish to an unknown topic makes one — but a
        test that cares how many partitions there are has to say so, exactly as
        it would have to against a real cluster whose `num.partitions` default
        is 1.
        """
        if name in self._topics:
            raise ValueError(f"Topic {name!r} already exists.")
        self._topics[name] = _Topic(
            name, partitions if partitions is not None else self._default_partitions
        )

    def topic(self, name: str) -> _Topic:
        existing = self._topics.get(name)
        if existing is not None:
            return existing
        created = _Topic(name, self._default_partitions)
        self._topics[name] = created
        return created

    def partitions_for(self, topics: Iterable[str]) -> tuple[Partition, ...]:
        return tuple(
            partition
            for name in sorted(topics)
            for partition in self.topic(name).partitions()
        )

    def group(self, group_id: str) -> _Group:
        existing = self._groups.get(group_id)
        if existing is not None:
            return existing
        created = _Group(group_id)
        self._groups[group_id] = created
        return created

    def committed(self, group_id: str, partition: Partition) -> int | None:
        """What `group_id` has committed for `partition`, if anything."""
        group = self._groups.get(group_id)
        return None if group is None else group.committed.get(partition)

    def end_offset(self, partition: Partition) -> int:
        return self.topic(partition.topic).end_offset(partition)

    def notify(self) -> None:
        """Wake every poll waiting for records."""
        waiting = self._appended
        self._appended = asyncio.Event()
        waiting.set()

    async def wait_for_append(self, timeout: float) -> None:
        """Block until something is appended, or `timeout` passes.

        The event is read *before* awaiting and the caller re-checks the log
        afterwards, so an append landing between the two is not missed.
        """
        waiting = self._appended
        try:
            await asyncio.wait_for(waiting.wait(), timeout)
        except TimeoutError:
            return

    def publisher(self) -> InMemoryMessagePublisher:
        return InMemoryMessagePublisher(self)

    def source(
        self,
        *,
        topics: Sequence[str],
        group_id: str,
        auto_offset_reset: str = "earliest",
    ) -> InMemoryMessageSource:
        return InMemoryMessageSource(
            self,
            topics=topics,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
        )


class InMemoryMessagePublisher:
    """A `MessagePublisher` that appends to an `InMemoryBroker`."""

    def __init__(self, broker: InMemoryBroker) -> None:
        self._broker = broker
        self._started = False

    @property
    def started(self) -> bool:
        return self._started

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._started = False

    async def publish(
        self,
        topic: str,
        *,
        value: bytes | None,
        key: str | None = None,
        headers: Mapping[str, bytes] | Headers | None = None,
    ) -> PublishedMessage:
        if not self._started:
            # The same refusal the Kafka publisher makes, so that a test
            # against this backend catches a missing `start()` rather than
            # discovering it in the deployment that has a real broker.
            raise LifecycleError("Publisher is not started. Call start() first.")
        validate_record(topic, key, value)
        partition, record = self._broker.topic(topic).append(
            key, value, normalize_headers(headers)
        )
        self._broker.notify()
        return PublishedMessage(
            partition=partition, offset=record.offset, timestamp=record.timestamp
        )


class InMemoryMessageSource:
    """A `MessageSource` over an `InMemoryBroker`, with a real group in it."""

    def __init__(
        self,
        broker: InMemoryBroker,
        *,
        topics: Sequence[str],
        group_id: str,
        auto_offset_reset: str = "earliest",
    ) -> None:
        if not topics:
            raise ValueError("At least one topic is required.")
        if not group_id:
            raise ValueError("group_id must not be empty.")
        if auto_offset_reset not in ("earliest", "latest"):
            raise ValueError(f"Unsupported auto_offset_reset {auto_offset_reset!r}.")
        self._broker = broker
        self._topics = tuple(topics)
        self._group_id = group_id
        self._auto_offset_reset = auto_offset_reset
        self._started = False
        self._assignment: tuple[Partition, ...] = ()
        self._positions: dict[Partition, int] = {}

    @property
    def group_id(self) -> str:
        return self._group_id

    @property
    def topics(self) -> tuple[str, ...]:
        return self._topics

    @property
    def started(self) -> bool:
        return self._started

    async def start(self) -> None:
        """Join the group and take an assignment. Idempotent."""
        if self._started:
            return
        self._started = True
        group = self._broker.group(self._group_id)
        group.join(self)
        self._rebalance(group)

    async def stop(self) -> None:
        """Leave the group, giving the partitions back. Idempotent."""
        if not self._started:
            return
        self._started = False
        group = self._broker.group(self._group_id)
        group.leave(self)
        self._assignment = ()
        self._positions.clear()
        self._rebalance(group)

    def _rebalance(self, group: _Group) -> None:
        """Redistribute the group's partitions over its current members.

        Round-robin over sorted partitions and members, which is one of the
        strategies a real broker offers and the one whose outcome a test can
        state. Every member's positions are rebuilt from the group's committed
        offsets, because that is what a real rebalance does: a partition that
        moves takes its *committed* position with it and nothing else, which is
        why uncommitted work is repeated by whoever receives it.
        """
        members = group.members
        for index, member in enumerate(members):
            assigned = tuple(
                partition
                for position, partition in enumerate(
                    self._broker.partitions_for(member.topics)
                )
                if position % len(members) == index
            )
            member._apply_assignment(assigned, group)

    def _apply_assignment(
        self, assignment: tuple[Partition, ...], group: _Group
    ) -> None:
        self._assignment = assignment
        self._positions = {
            partition: self._starting_position(partition, group)
            for partition in assignment
        }

    def _starting_position(self, partition: Partition, group: _Group) -> int:
        committed = group.committed.get(partition)
        if committed is not None:
            return committed
        if self._auto_offset_reset == "latest":
            return self._broker.end_offset(partition)
        return 0

    async def poll(
        self, *, max_records: int, timeout: float
    ) -> Sequence[ConsumedMessage]:
        if not self._started:
            raise LifecycleError("Source is not started. Call start() first.")
        if max_records < 1:
            raise ValueError("max_records must be at least 1.")

        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            batch = self._drain(max_records)
            if batch:
                return batch
            remaining = deadline - loop.time()
            if remaining <= 0:
                return []
            await self._broker.wait_for_append(remaining)

    def _drain(self, max_records: int) -> list[ConsumedMessage]:
        """Take what is available, in partition order, advancing the position."""
        drained: list[ConsumedMessage] = []
        for partition in self._assignment:
            if len(drained) >= max_records:
                break
            position = self._positions[partition]
            records = self._broker.topic(partition.topic).read(
                partition, position, max_records - len(drained)
            )
            for record in records:
                drained.append(record.consumed(partition))
            if records:
                self._positions[partition] = position + len(records)
        return drained

    async def commit(self, offsets: Mapping[Partition, int]) -> None:
        if not self._started:
            raise LifecycleError("Source is not started. Call start() first.")
        if not offsets:
            return
        group = self._broker.group(self._group_id)
        for partition, offset in offsets.items():
            if partition not in self._assignment:
                # What a real broker answers with CommitFailedError: this
                # member no longer owns the partition, so its progress on it is
                # not the group's to record.
                raise ConsumerError(
                    f"Cannot commit {partition}: not assigned to this member."
                )
            if offset < 0:
                raise ValueError(f"Offset for {partition} cannot be negative.")
            group.committed[partition] = offset

    def seek(self, partition: Partition, offset: int) -> None:
        if partition not in self._assignment:
            raise ConsumerError(f"Cannot seek {partition}: not assigned.")
        if offset < 0:
            raise ValueError(f"Offset for {partition} cannot be negative.")
        self._positions[partition] = offset

    def assignment(self) -> frozenset[Partition]:
        return frozenset(self._assignment)

    def position(self, partition: Partition) -> int | None:
        """This member's read position — not the group's committed offset.

        Exposed because the difference between the two is the thing most worth
        asserting in a test: a poll moves the position and leaves the commit
        alone, which is exactly why an uncommitted batch is redelivered.
        """
        return self._positions.get(partition)


__all__ = [
    "DEFAULT_PARTITIONS",
    "InMemoryBroker",
    "InMemoryMessagePublisher",
    "InMemoryMessageSource",
]
