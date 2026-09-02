"""The aiokafka producer wrapper: configuration, translation, error mapping.

A double for `AIOKafkaProducer` rather than a broker, because what is asserted
here is what this wrapper *asks the driver for* — `acks="all"`, idempotence,
the key encoded, the headers in wire form — and a broker would only show the
result, which for these settings looks identical either way until the day a
leader fails over.

The round trip against a real broker is `tests/test_kafka_contract.py`.
"""

from __future__ import annotations

from typing import Any

import pytest
from aiokafka.errors import KafkaError as DriverKafkaError

from src.kafka import producer as producer_module
from src.kafka.base import LifecycleError, Partition, PublishError
from src.kafka.producer import KafkaMessagePublisher, ProducerConfig


class FakeRecordMetadata:
    def __init__(self, topic: str, partition: int, offset: int, timestamp: int | None):
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp


class FakeDriverProducer:
    """Stands in for `AIOKafkaProducer`, recording what it was asked."""

    instances: list[FakeDriverProducer] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.sent: list[dict[str, Any]] = []
        self.started = False
        self.stopped = False
        self.start_error: BaseException | None = None
        self.send_error: BaseException | None = None
        self.timestamp: int | None = 1700000000000
        FakeDriverProducer.instances.append(self)

    async def start(self) -> None:
        if self.start_error is not None:
            raise self.start_error
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_and_wait(
        self,
        topic: str,
        *,
        value: bytes | None,
        key: bytes | None,
        headers: list[tuple[str, bytes]] | None,
    ) -> FakeRecordMetadata:
        if self.send_error is not None:
            raise self.send_error
        self.sent.append(
            {"topic": topic, "value": value, "key": key, "headers": headers}
        )
        return FakeRecordMetadata(topic, 3, len(self.sent) - 1, self.timestamp)


@pytest.fixture(autouse=True)
def driver(monkeypatch: pytest.MonkeyPatch) -> type[FakeDriverProducer]:
    FakeDriverProducer.instances.clear()
    monkeypatch.setattr(producer_module, "AIOKafkaProducer", FakeDriverProducer)
    return FakeDriverProducer


async def a_publisher(config: ProducerConfig | None = None) -> KafkaMessagePublisher:
    built = KafkaMessagePublisher(bootstrap_servers="broker:9092", config=config)
    await built.start()
    return built


class TestDurabilitySettings:
    async def test_it_asks_for_acknowledgement_from_the_replicas(self) -> None:
        """`acks=1` loses a record whose leader dies before replicating it, and
        the producer has already been told the write succeeded."""
        await a_publisher()

        assert FakeDriverProducer.instances[0].kwargs["acks"] == "all"

    async def test_the_producer_is_idempotent(self) -> None:
        """Without it, a retry of a lost acknowledgement writes twice."""
        await a_publisher()

        assert FakeDriverProducer.instances[0].kwargs["enable_idempotence"] is True

    async def test_idempotence_without_all_acks_is_refused_at_configuration(
        self,
    ) -> None:
        """aiokafka refuses it too, but at `start()` — which in a deployment is
        process start-up rather than configuration review."""
        with pytest.raises(ValueError, match="acks='all'"):
            ProducerConfig(acks="1", enable_idempotence=True)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"acks": "maybe"},
            {"request_timeout_ms": 0},
            {"linger_ms": -1},
            {"max_request_size": 0},
        ],
    )
    def test_a_nonsensical_configuration_is_refused(
        self, kwargs: dict[str, object]
    ) -> None:
        with pytest.raises(ValueError):
            ProducerConfig(**kwargs)  # type: ignore[arg-type]

    async def test_the_request_timeout_is_a_ceiling_on_the_caller(self) -> None:
        await a_publisher(ProducerConfig(request_timeout_ms=5000))

        assert FakeDriverProducer.instances[0].kwargs["request_timeout_ms"] == 5000


class TestLifecycle:
    async def test_the_driver_is_built_at_start_rather_than_at_construction(
        self,
    ) -> None:
        """Constructing it binds the running loop, so one built at import time
        belongs to whichever loop happened to exist then."""
        publisher = KafkaMessagePublisher(bootstrap_servers="broker:9092")

        assert FakeDriverProducer.instances == []
        assert not publisher.started

        await publisher.start()
        assert publisher.started

    async def test_start_is_idempotent(self) -> None:
        publisher = await a_publisher()
        await publisher.start()

        assert len(FakeDriverProducer.instances) == 1

    async def test_stop_flushes_once_and_is_idempotent(self) -> None:
        publisher = await a_publisher()

        await publisher.stop()
        await publisher.stop()

        assert FakeDriverProducer.instances[0].stopped
        assert not publisher.started

    async def test_publishing_before_start_is_refused_rather_than_connecting(
        self,
    ) -> None:
        """An implicit start would tie the connection's lifetime to whichever
        request happened to publish first, and leave shutdown nothing to close."""
        publisher = KafkaMessagePublisher(bootstrap_servers="broker:9092")

        with pytest.raises(LifecycleError):
            await publisher.publish("t", value=b"x", key="k")

    async def test_a_broker_unreachable_at_start_fails_start_up(self) -> None:
        publisher = KafkaMessagePublisher(bootstrap_servers="broker:9092")

        def build(**kwargs: Any) -> FakeDriverProducer:
            fake = FakeDriverProducer(**kwargs)
            fake.start_error = DriverKafkaError("no brokers available")
            return fake

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(producer_module, "AIOKafkaProducer", build)
            with pytest.raises(PublishError, match="failed to start"):
                await publisher.start()


class TestPublishing:
    async def test_the_key_is_encoded_and_the_headers_are_pairs(self) -> None:
        publisher = await a_publisher()

        await publisher.publish(
            "orders", value=b"{}", key="order-1", headers={"trace": b"abc"}
        )

        [sent] = FakeDriverProducer.instances[0].sent
        assert sent["key"] == b"order-1"
        assert sent["headers"] == [("trace", b"abc")]

    async def test_no_headers_are_sent_as_none(self) -> None:
        """An empty list is a header block the driver would still encode."""
        publisher = await a_publisher()

        await publisher.publish("orders", value=b"{}", key=None)

        assert FakeDriverProducer.instances[0].sent[0]["headers"] is None

    async def test_it_reports_where_the_record_landed(self) -> None:
        publisher = await a_publisher()

        published = await publisher.publish("orders", value=b"{}", key="k")

        assert published.partition == Partition("orders", 3)
        assert published.offset == 0
        assert published.topic == "orders"

    async def test_a_broker_without_timestamps_still_yields_one(self) -> None:
        publisher = await a_publisher()
        FakeDriverProducer.instances[0].timestamp = None

        published = await publisher.publish("orders", value=b"{}", key="k")

        assert published.timestamp.tzinfo is not None

    async def test_a_tombstone_without_a_key_never_reaches_the_driver(self) -> None:
        publisher = await a_publisher()

        with pytest.raises(ValueError, match="tombstone"):
            await publisher.publish("orders", value=None, key=None)

        assert FakeDriverProducer.instances[0].sent == []

    async def test_a_driver_error_becomes_a_domain_error(self) -> None:
        """So that a router answering 503 does not have to import aiokafka."""
        publisher = await a_publisher()
        FakeDriverProducer.instances[0].send_error = DriverKafkaError("broker down")

        with pytest.raises(PublishError, match="orders") as raised:
            await publisher.publish("orders", value=b"{}", key="k")

        assert isinstance(raised.value.__cause__, DriverKafkaError)
        assert raised.value.status_code == 503
