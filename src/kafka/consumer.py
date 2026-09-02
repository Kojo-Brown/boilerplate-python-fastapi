"""The aiokafka consumer, with auto-commit off and the reasons written down.

This class is transport only: fetch, commit, seek, and the translation between
the driver's objects and this package's. All of the *policy* — what to do when
a handler fails, when to commit, how to stop — is in `runner.py`, so that it
can be tested against an in-process broker instead of against a Kafka.

**`enable_auto_commit=False` is not configurable here, and that is deliberate.**
Auto-commit is usually read as "commit less often", and it is not: it commits
the offsets of records the *fetcher* has handed to the application on a timer,
whether or not the application did anything with them. A process that dies
between the commit and the handler has silently skipped every record in that
window — at-most-once delivery, arrived at by leaving a default alone. Exposing
it as a setting would mean one environment variable can turn this service's
delivery guarantee into a different one, with no code change to review.

**`auto_offset_reset="earliest"`.** The driver's default is `latest`, which for
a group with no committed offset means "ignore everything produced before this
consumer started". That is the right answer for a live tail and the wrong one
for a service that was down for ten minutes, and the wrongness is invisible:
the consumer is healthy, its lag is zero, and the records are gone.

**`max_poll_interval_ms` is the deadline the runner's handler timeout sits
under.** aiokafka leaves the group if the application does not come back for
more records within it, and leaving the group means every partition this member
held is reassigned and its in-flight batch is redelivered elsewhere. A handler
with no timeout can therefore turn one slow call into a rebalance loop, which
looks like a broker problem and is not.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final

import structlog
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError as DriverKafkaError

from src.kafka.base import (
    ConsumedMessage,
    ConsumerError,
    Headers,
    LifecycleError,
    Partition,
)

logger = structlog.get_logger(__name__)

#: What `poll` asks the driver for when the caller does not say. Small enough
#: that a batch's worth of handler work stays well inside `max_poll_interval_ms`.
DEFAULT_MAX_RECORDS: Final[int] = 100


@dataclass(frozen=True, slots=True)
class ConsumerConnectionConfig:
    """Group membership and fetch sizing — the driver's half of the settings.

    Split from `runner.ConsumerConfig`, which holds the delivery policy, because
    they are answerable by different people: these are cluster-shaped questions
    (how long may a member be silent, how much may one fetch return), and those
    are application-shaped ones (how long may one record take, how many times
    is it retried).
    """

    group_id: str
    client_id: str = "boilerplate-python-fastapi"
    auto_offset_reset: str = "earliest"
    max_poll_records: int = DEFAULT_MAX_RECORDS
    session_timeout_ms: int = 10000
    heartbeat_interval_ms: int = 3000
    max_poll_interval_ms: int = 300000
    max_partition_fetch_bytes: int = 1048576  # 1 MiB
    #: "read_committed" hides records written by an aborted transaction, at the
    #: cost of reading no further than the last stable offset. Left at the
    #: driver's default because this codebase's producer is idempotent rather
    #: than transactional, so there is nothing for it to hide.
    isolation_level: str = "read_uncommitted"

    def __post_init__(self) -> None:
        if not self.group_id:
            # A consumer with no group is a consumer with nowhere to commit:
            # aiokafka answers `commit()` with IllegalOperation, so the failure
            # would arrive after the first batch had already been handled.
            raise ValueError("group_id must not be empty.")
        if self.auto_offset_reset not in ("earliest", "latest", "none"):
            raise ValueError(
                f"Unsupported auto_offset_reset {self.auto_offset_reset!r}."
            )
        if self.max_poll_records < 1:
            raise ValueError("max_poll_records must be at least 1.")
        if self.heartbeat_interval_ms >= self.session_timeout_ms:
            # The broker evicts a member it has not heard from for the session
            # timeout, so heartbeats have to fit several times over. Kafka's own
            # guidance is a third of it; anything above it never heartbeats in
            # time and the group rebalances continuously.
            raise ValueError("heartbeat_interval_ms must be below session_timeout_ms.")


def _to_message(record: Any) -> ConsumedMessage:
    """Translate one driver record.

    The key is decoded as UTF-8 with `errors="replace"` rather than raising: a
    key this service did not produce is not a reason to stall a partition, and
    the record's bytes are still intact in `value` for a handler that cares.
    """
    raw_key: bytes | None = record.key
    headers: Headers = tuple(
        (str(name), bytes(value)) for name, value in (record.headers or ())
    )
    return ConsumedMessage(
        partition=Partition(topic=str(record.topic), number=int(record.partition)),
        offset=int(record.offset),
        key=raw_key.decode("utf-8", errors="replace") if raw_key is not None else None,
        value=record.value,
        headers=headers,
        timestamp=datetime.fromtimestamp(record.timestamp / 1000, tz=UTC),
    )


class KafkaMessageSource:
    """A `MessageSource` backed by `aiokafka`, with offsets left to the caller."""

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topics: Sequence[str],
        config: ConsumerConnectionConfig,
    ) -> None:
        if not topics:
            raise ValueError("At least one topic is required.")
        self._bootstrap_servers = bootstrap_servers
        self._topics = tuple(topics)
        self._config = config
        self._consumer: Any | None = None

    @property
    def topics(self) -> tuple[str, ...]:
        return self._topics

    @property
    def group_id(self) -> str:
        return self._config.group_id

    @property
    def started(self) -> bool:
        return self._consumer is not None

    async def start(self) -> None:
        """Join the group and take an assignment. Idempotent."""
        if self._consumer is not None:
            return
        consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._config.group_id,
            client_id=self._config.client_id,
            enable_auto_commit=False,
            auto_offset_reset=self._config.auto_offset_reset,
            max_poll_records=self._config.max_poll_records,
            session_timeout_ms=self._config.session_timeout_ms,
            heartbeat_interval_ms=self._config.heartbeat_interval_ms,
            max_poll_interval_ms=self._config.max_poll_interval_ms,
            max_partition_fetch_bytes=self._config.max_partition_fetch_bytes,
            isolation_level=self._config.isolation_level,
        )
        try:
            await consumer.start()
        except DriverKafkaError as exc:
            raise ConsumerError(f"Kafka consumer failed to start: {exc}") from exc
        self._consumer = consumer
        logger.info(
            "kafka.consumer_started",
            topics=list(self._topics),
            group_id=self._config.group_id,
        )

    async def stop(self) -> None:
        """Leave the group and close the sockets. Idempotent.

        Leaving is worth waiting for: a member that vanishes without saying so
        is only noticed when its session times out, and until then its
        partitions are owned by nobody and their lag grows.
        """
        consumer = self._consumer
        if consumer is None:
            return
        self._consumer = None
        await consumer.stop()
        logger.info("kafka.consumer_stopped", group_id=self._config.group_id)

    async def poll(
        self, *, max_records: int, timeout: float
    ) -> Sequence[ConsumedMessage]:
        """Fetch a batch, flattened partition by partition and in offset order.

        `getmany` returns a mapping of partition to records, and flattening it
        loses nothing: Kafka orders records within a partition and makes no
        promise across partitions, so any interleaving of the groups is as
        faithful as the mapping was. The runner regroups by partition anyway —
        it has to, because offsets are per partition — and sorting here keeps
        the sequence deterministic for the tests that assert on it.
        """
        consumer = self._require_started()
        try:
            batches: Mapping[Any, Sequence[Any]] = await consumer.getmany(
                timeout_ms=int(timeout * 1000), max_records=max_records
            )
        except DriverKafkaError as exc:
            raise ConsumerError(f"Fetching records failed: {exc}") from exc

        messages: list[ConsumedMessage] = []
        for topic_partition in sorted(
            batches, key=lambda tp: (str(tp.topic), int(tp.partition))
        ):
            for record in batches[topic_partition]:
                messages.append(_to_message(record))
        return messages

    async def commit(self, offsets: Mapping[Partition, int]) -> None:
        """Store the group's next-read positions.

        An empty mapping is a no-op rather than a commit of "current position":
        `AIOKafkaConsumer.commit()` with no argument commits everything fetched,
        which is exactly the at-most-once behaviour auto-commit was turned off
        to avoid, and it would be reached by the ordinary path of a batch in
        which every handler failed.
        """
        if not offsets:
            return
        consumer = self._require_started()
        payload = {
            TopicPartition(partition.topic, partition.number): offset
            for partition, offset in offsets.items()
        }
        try:
            await consumer.commit(payload)
        except DriverKafkaError as exc:
            # Includes CommitFailedError, which means this member was removed
            # from the group while the batch was in flight. The records are
            # already someone else's, so this is reported and never retried.
            raise ConsumerError(f"Committing offsets failed: {exc}") from exc
        logger.debug(
            "kafka.offsets_committed",
            group_id=self._config.group_id,
            offsets={str(p): o for p, o in sorted(offsets.items())},
        )

    def seek(self, partition: Partition, offset: int) -> None:
        """Re-read from `offset` on the next fetch."""
        consumer = self._require_started()
        try:
            consumer.seek(TopicPartition(partition.topic, partition.number), offset)
        except (DriverKafkaError, ValueError, TypeError) as exc:
            # Raised when the partition is no longer assigned, which happens
            # when a rebalance landed between the fetch and the failure being
            # handled. Nothing is lost: the new owner reads from the last
            # committed offset, which is at or before this one.
            raise ConsumerError(f"Cannot seek {partition} to {offset}: {exc}") from exc

    def assignment(self) -> frozenset[Partition]:
        consumer = self._consumer
        if consumer is None:
            return frozenset()
        return frozenset(
            Partition(topic=str(tp.topic), number=int(tp.partition))
            for tp in consumer.assignment()
        )

    def _require_started(self) -> Any:
        consumer = self._consumer
        if consumer is None:
            raise LifecycleError(
                "Source is not started. Call start() before polling or committing."
            )
        return consumer


__all__ = [
    "DEFAULT_MAX_RECORDS",
    "ConsumerConnectionConfig",
    "KafkaMessageSource",
]
