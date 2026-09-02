"""The aiokafka producer, and the four settings that decide what it guarantees.

The wrapper itself is thin. What is not thin is the configuration, because
`AIOKafkaProducer`'s defaults are tuned for throughput on a topic nobody minds
losing, and three of them have to be changed before a record can be called
durable.

**`acks="all"`.** The default acknowledges as soon as the *leader* has the
record. A leader that dies before its followers have replicated it takes the
record with it, and the producer has already been told the write succeeded, so
nothing anywhere retries. `all` waits for the in-sync replicas, which is the
only setting under which "the broker acknowledged it" means the record survives
the loss of a broker. It costs a round trip inside the cluster, and that is the
price of the guarantee rather than an inefficiency to tune away.

**`enable_idempotence=True`.** The producer retries internally — that is what
makes a transient network error invisible — and a retried record whose original
acknowledgement was merely *lost* is written twice. Idempotence gives the
producer a session id and each record a sequence number, so the broker drops
the duplicate. Two limits worth stating plainly, because "idempotent producer"
is often read as more than it is: it deduplicates within one producer session
(a restarted process is a new session and its retries are new records), and it
says nothing about the application calling `publish` twice, which is the
application's own idempotency problem — see `src/idempotency` for the HTTP half
of it. Turning it on also pins `acks="all"` and bounds in-flight requests, so
the two settings above are one decision.

**`request_timeout_ms` is a ceiling on a caller, not a background knob.**
`publish` awaits the acknowledgement, so a broker that has stopped answering
holds the request handler that called it for this long. It is well under the
30-second default that most reverse proxies give a request for exactly that
reason.

**`linger_ms=0`.** The producer batches records that are waiting anyway; linger
*waits* for more, trading latency for batching. Zero is right for a service
that publishes one record inside a request, and wrong for a bulk export, which
is why it is configurable and documented rather than assumed.

`aiokafka` is not typed — it ships no `py.typed` — so the driver objects here
are `Any` to mypy, and everything crossing back into this codebase is built
from primitives with declared types. See the mypy override in `pyproject.toml`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final

import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError as DriverKafkaError

from src.kafka.base import (
    Headers,
    LifecycleError,
    Partition,
    PublishedMessage,
    PublishError,
    normalize_headers,
    utc_now,
    validate_record,
)

logger = structlog.get_logger(__name__)

#: Kafka's own limit on a record key or value it will index by timestamp. Not
#: enforced here — `max_request_size` is what the broker rejects on — but the
#: number the default `max_request_size` below comes from.
DEFAULT_MAX_REQUEST_BYTES: Final[int] = 1048576  # 1 MiB


@dataclass(frozen=True, slots=True)
class ProducerConfig:
    """Everything the producer needs that is not the broker list.

    A frozen dataclass rather than reading `settings` inside the class, for the
    reason every other module here does it: a test that needs a different
    timeout constructs one, and the production values are assembled in exactly
    one place (`src/kafka/factory.py`).
    """

    client_id: str = "boilerplate-python-fastapi"
    acks: str = "all"
    enable_idempotence: bool = True
    request_timeout_ms: int = 15000
    linger_ms: int = 0
    #: `None`, "gzip", "snappy", "lz4" or "zstd". The non-gzip codecs need the
    #: matching `aiokafka` extra installed, so the default is off rather than a
    #: choice that fails at the first send in a slim image.
    compression_type: str | None = None
    max_request_size: int = DEFAULT_MAX_REQUEST_BYTES

    def __post_init__(self) -> None:
        if self.acks not in ("all", "0", "1", 0, 1):
            raise ValueError(f"Unsupported acks value {self.acks!r}.")
        if self.enable_idempotence and self.acks != "all":
            # aiokafka raises for this too, but at `start()`, which in a
            # deployment is process start-up rather than configuration review.
            raise ValueError("enable_idempotence requires acks='all'.")
        if self.request_timeout_ms <= 0:
            raise ValueError("request_timeout_ms must be positive.")
        if self.linger_ms < 0:
            raise ValueError("linger_ms cannot be negative.")
        if self.max_request_size <= 0:
            raise ValueError("max_request_size must be positive.")


def _timestamp(millis: int | None) -> datetime:
    """A broker timestamp in milliseconds, or now if the broker gave none.

    Old brokers return `None`, and a `PublishedMessage` with an optional
    timestamp would push that irrelevance into every caller. The fallback is
    this process's clock, which is what the record's `CreateTime` was anyway.
    """
    if millis is None:
        return utc_now()
    return datetime.fromtimestamp(millis / 1000, tz=UTC)


class KafkaMessagePublisher:
    """A `MessagePublisher` backed by `aiokafka`.

    The driver is built in `start()` rather than in `__init__`: constructing it
    binds the running event loop, and a publisher assembled at import time — as
    a module-level default would be — belongs to whichever loop happened to
    exist then, which in a test suite is a different loop per test.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        config: ProducerConfig | None = None,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._config = config if config is not None else ProducerConfig()
        self._producer: Any | None = None

    @property
    def config(self) -> ProducerConfig:
        return self._config

    @property
    def started(self) -> bool:
        return self._producer is not None

    async def start(self) -> None:
        """Connect and fetch metadata. Idempotent.

        Failing here fails the application's start-up, which is the intent: a
        service configured to publish and unable to reach a broker is not ready
        to serve, and finding that out at the first request means finding it
        out from a customer.
        """
        if self._producer is not None:
            return
        producer = AIOKafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            client_id=self._config.client_id,
            acks=self._config.acks,
            enable_idempotence=self._config.enable_idempotence,
            request_timeout_ms=self._config.request_timeout_ms,
            linger_ms=self._config.linger_ms,
            compression_type=self._config.compression_type,
            max_request_size=self._config.max_request_size,
        )
        try:
            await producer.start()
        except DriverKafkaError as exc:
            raise PublishError(f"Kafka producer failed to start: {exc}") from exc
        self._producer = producer
        logger.info(
            "kafka.producer_started",
            bootstrap_servers=self._bootstrap_servers,
            client_id=self._config.client_id,
            acks=self._config.acks,
            idempotent=self._config.enable_idempotence,
        )

    async def stop(self) -> None:
        """Flush what is buffered, close the sockets. Idempotent.

        `stop()` flushes, which is why shutdown waits for it: records handed to
        a producer with `linger_ms` set, or simply not yet acknowledged, are in
        this process's memory and nowhere else.
        """
        producer = self._producer
        if producer is None:
            return
        self._producer = None
        await producer.stop()
        logger.info("kafka.producer_stopped")

    async def publish(
        self,
        topic: str,
        *,
        value: bytes | None,
        key: str | None = None,
        headers: Mapping[str, bytes] | Headers | None = None,
    ) -> PublishedMessage:
        """Send one record and wait for the acknowledgement.

        The key is encoded UTF-8 and is what decides the partition, so two
        records with the same key are ordered with respect to each other and
        records with different keys are not. That is the only ordering Kafka
        offers, and choosing the key is therefore a design decision rather than
        a labelling one — a per-user topic key gives per-user ordering, and a
        random key gives none at all.
        """
        producer = self._require_started()
        validate_record(topic, key, value)
        normalized = normalize_headers(headers)
        try:
            metadata = await producer.send_and_wait(
                topic,
                value=value,
                key=key.encode("utf-8") if key is not None else None,
                headers=list(normalized) or None,
            )
        except DriverKafkaError as exc:
            # Deliberately not logged as an error and re-raised as itself: the
            # driver's exceptions name internal classes, and a handler that
            # catches `KafkaError` to answer 503 would be importing the driver
            # into a router. See `PublishError`.
            logger.warning(
                "kafka.publish_failed",
                topic=topic,
                key=key,
                error=f"{type(exc).__name__}: {exc}",
            )
            raise PublishError(f"Publishing to {topic!r} failed: {exc}") from exc

        published = PublishedMessage(
            partition=Partition(
                topic=str(metadata.topic), number=int(metadata.partition)
            ),
            offset=int(metadata.offset),
            timestamp=_timestamp(metadata.timestamp),
        )
        logger.debug(
            "kafka.published",
            topic=published.topic,
            partition=published.partition.number,
            offset=published.offset,
            key=key,
        )
        return published

    def _require_started(self) -> Any:
        producer = self._producer
        if producer is None:
            raise LifecycleError(
                "Publisher is not started. Call start() in the application "
                "lifespan before publishing."
            )
        return producer


__all__ = ["DEFAULT_MAX_REQUEST_BYTES", "KafkaMessagePublisher", "ProducerConfig"]
