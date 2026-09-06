"""The Redis backend's own behaviour: unit conversion, error translation, and
the two commands whose real semantics decide the design.

The shared contract proves this is a consumer group. What is left here is what
is specific to talking to a server: the pure helpers, which need nothing; the
failure translation, which is checked against a stub client so it runs
everywhere; and the parts that can only be measured — `XGROUP DELCONSUMER`
destroying pending entries, `NOGROUP` after the stream is deleted, and
approximate trimming.

`TestDelConsumerReally...` is the load-bearing one. `RedisStreamGroup.stop`
refuses to remove a consumer that still owes messages, and the reason is not
tidiness: the first test in that class drives the raw command and shows the
messages disappearing. If a future change makes `stop` unconditional, that test
is what says what it cost.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from src.redis_streams.base import (
    StreamDecodeError,
    StreamEntryId,
    StreamGroupError,
    StreamPublishError,
    StreamUnavailableError,
)
from src.redis_streams.redis_group import RedisStreamGroup, decode_fields, to_ms
from tests.test_redis_streams_contract import (
    REDIS_SKIP_REASON,
    REDIS_URL,
    redis_reachable,
)

GROUP = "redis-suite"


class TestToMs:
    def test_seconds_become_milliseconds(self) -> None:
        assert to_ms(1.5) == 1500

    def test_a_sub_millisecond_threshold_truncates_to_zero(self) -> None:
        """Erring low costs one wasted claim attempt; erring high costs a
        message that is never reclaimed."""
        assert to_ms(0.0009) == 0

    def test_a_negative_threshold_is_clamped(self) -> None:
        """`XCLAIM` with a negative min-idle-time is a server error, and the
        call site that produced it is nowhere near the traceback."""
        assert to_ms(-5.0) == 0


class TestDecodeFields:
    def test_names_are_decoded_and_values_are_not(self) -> None:
        assert decode_fields({b"type": b"\x80\x81"}) == {"type": b"\x80\x81"}

    def test_a_field_name_that_is_not_utf8_is_refused(self) -> None:
        with pytest.raises(StreamDecodeError, match="not UTF-8"):
            decode_fields({b"\xff\xfe": b"1"})

    def test_a_name_that_arrived_decoded_is_accepted(self) -> None:
        """A client built with `decode_responses=True` is a misconfiguration
        rather than a corruption, and this is not the place to fail it."""
        assert decode_fields({"type": b"1"}) == {"type": b"1"}


class FailingRedis:
    """A client where every command is a connection error.

    Enough of one to check that each method translates its failure into this
    package's own error, without needing a server that is down.
    """

    def __init__(self) -> None:
        self.closed = False

    async def xgroup_create(self, *args: Any, **kwargs: Any) -> Any:
        raise RedisConnectionError("connection refused")

    async def xadd(self, *args: Any, **kwargs: Any) -> Any:
        raise RedisConnectionError("connection refused")

    async def xreadgroup(self, *args: Any, **kwargs: Any) -> Any:
        raise RedisConnectionError("connection refused")

    async def xpending_range(self, *args: Any, **kwargs: Any) -> Any:
        raise RedisConnectionError("connection refused")

    async def xpending(self, *args: Any, **kwargs: Any) -> Any:
        raise RedisConnectionError("connection refused")

    async def xclaim(self, *args: Any, **kwargs: Any) -> Any:
        raise RedisConnectionError("connection refused")

    async def xack(self, *args: Any, **kwargs: Any) -> Any:
        raise RedisConnectionError("connection refused")

    async def xgroup_delconsumer(self, *args: Any, **kwargs: Any) -> Any:
        raise RedisConnectionError("connection refused")

    async def aclose(self) -> None:
        self.closed = True


class RefusingRedis(FailingRedis):
    """A client that answers commands with `NOGROUP`."""

    async def xreadgroup(self, *args: Any, **kwargs: Any) -> Any:
        raise ResponseError("NOGROUP No such key 's' or consumer group 'g'")

    async def xgroup_create(self, *args: Any, **kwargs: Any) -> Any:
        return True


class ProtestingRedis(FailingRedis):
    """A client that answers every command with an error that is *not* a
    missing group — the key exists and holds something else, say.

    Worth distinguishing from `NOGROUP`: that one is recovered from by
    recreating the group, and treating this as recoverable would mean
    recreating a group on every unrelated server error, forever.
    """

    _WRONGTYPE = "WRONGTYPE Operation against a key holding the wrong kind of value"

    async def xgroup_create(self, *args: Any, **kwargs: Any) -> Any:
        raise ResponseError(self._WRONGTYPE)

    async def xreadgroup(self, *args: Any, **kwargs: Any) -> Any:
        raise ResponseError(self._WRONGTYPE)

    async def xpending_range(self, *args: Any, **kwargs: Any) -> Any:
        raise ResponseError(self._WRONGTYPE)

    async def xpending(self, *args: Any, **kwargs: Any) -> Any:
        raise ResponseError(self._WRONGTYPE)

    async def xclaim(self, *args: Any, **kwargs: Any) -> Any:
        raise ResponseError(self._WRONGTYPE)

    async def xack(self, *args: Any, **kwargs: Any) -> Any:
        raise ResponseError(self._WRONGTYPE)


def failing_group(client: FailingRedis) -> RedisStreamGroup:
    return RedisStreamGroup(
        client,  # type: ignore[arg-type]
        stream="s",
        group=GROUP,
        consumer="c",
    )


class TestFailureTranslation:
    async def test_a_start_against_a_dead_server_says_so(self) -> None:
        group = failing_group(FailingRedis())

        with pytest.raises(StreamUnavailableError, match="Could not reach Redis"):
            await group.start()

        assert group.started is False

    async def test_a_publish_against_a_dead_server_says_so(self) -> None:
        group = failing_group(FailingRedis())

        with pytest.raises(StreamPublishError):
            await group.publish({"n": b"1"})

    @pytest.mark.parametrize(
        "call",
        [
            lambda group: group.read(count=1, block=0),
            lambda group: group.stalled(min_idle=1.0, count=1),
            lambda group: group.ack([StreamEntryId(1, 0)]),
            lambda group: group.pending_count(),
        ],
    )
    async def test_every_group_command_translates_its_failure(self, call: Any) -> None:
        group = failing_group(FailingRedis())
        await _force_started(group)

        with pytest.raises(StreamUnavailableError):
            await call(group)

    async def test_a_claim_against_a_dead_server_says_so(self) -> None:
        group = failing_group(FailingRedis())
        await _force_started(group)

        with pytest.raises(StreamUnavailableError):
            await group.claim(
                [_pending_entry()],
                min_idle=1.0,
            )

    async def test_a_vanished_group_unstarts_itself_so_the_loop_recreates_it(
        self,
    ) -> None:
        """`NOGROUP` means the key was deleted or the group destroyed. The
        runner calls `start()` every pass, so unstarting here is what makes
        that survivable rather than retried forever against nothing."""
        group = failing_group(RefusingRedis())
        await group.start()
        assert group.started is True

        with pytest.raises(StreamGroupError, match="vanished"):
            await group.read(count=1, block=0)

        assert group.started is False

    async def test_a_command_error_that_is_not_a_missing_group_is_not_recoverable(
        self,
    ) -> None:
        """Unstarting on every server error would recreate the group on each
        one, forever; only `NOGROUP` says the group is actually gone."""
        group = failing_group(ProtestingRedis())
        await _force_started(group)

        with pytest.raises(StreamGroupError, match="Could not read from the group"):
            await group.read(count=1, block=0)

        assert group.started is True

    @pytest.mark.parametrize(
        ("call", "message"),
        [
            (
                lambda group: group.stalled(min_idle=1.0, count=1),
                "list pending entries",
            ),
            (lambda group: group.ack([StreamEntryId(1, 0)]), "acknowledge messages"),
            (lambda group: group.pending_count(), "count pending entries"),
        ],
    )
    async def test_every_group_command_names_what_it_was_doing(
        self, call: Any, message: str
    ) -> None:
        group = failing_group(ProtestingRedis())
        await _force_started(group)

        with pytest.raises(StreamGroupError, match=message):
            await call(group)

    async def test_a_refused_claim_names_what_it_was_doing(self) -> None:
        group = failing_group(ProtestingRedis())
        await _force_started(group)

        with pytest.raises(StreamGroupError, match="claim pending entries"):
            await group.claim([_pending_entry()], min_idle=1.0)

    async def test_a_group_that_cannot_be_created_says_which_one(self) -> None:
        """`BUSYGROUP` is success; every other `ResponseError` is not."""
        group = failing_group(ProtestingRedis())

        with pytest.raises(StreamGroupError, match="create the consumer group"):
            await group.start()

        assert group.started is False

    async def test_a_shutdown_that_cannot_reach_the_server_still_stops(self) -> None:
        """A consumer record left behind is untidy; a shutdown that raises on
        the way out is a process that does not exit cleanly."""
        group = failing_group(FailingRedis())
        await _force_started(group)

        await group.stop()

        assert group.started is False

    async def test_a_group_that_owns_its_client_closes_it(self) -> None:
        client = FailingRedis()
        group = RedisStreamGroup(
            client,  # type: ignore[arg-type]
            stream="s",
            group=GROUP,
            consumer="c",
            owns_client=True,
        )

        await group.stop()

        assert client.closed is True

    async def test_a_group_that_borrowed_its_client_leaves_it_open(self) -> None:
        """Closing somebody else's pool during this object's shutdown would
        take out whatever else is using it."""
        client = FailingRedis()
        group = failing_group(client)

        await group.stop()

        assert client.closed is False


class TestConstruction:
    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"stream": ""}, "stream must not be empty"),
            ({"group": ""}, "group must not be empty"),
            ({"maxlen": -1}, "maxlen cannot be negative"),
        ],
    )
    def test_a_nonsensical_group_is_refused(
        self, kwargs: dict[str, Any], message: str
    ) -> None:
        fields: dict[str, Any] = {"stream": "s", "group": GROUP, "consumer": "c"}
        fields.update(kwargs)

        with pytest.raises(ValueError, match=message):
            RedisStreamGroup(FailingRedis(), **fields)  # type: ignore[arg-type]

    def test_an_unnamed_consumer_gets_a_unique_one(self) -> None:
        first = failing_group(FailingRedis())
        second = RedisStreamGroup(
            FailingRedis(),  # type: ignore[arg-type]
            stream="s",
            group=GROUP,
            consumer=None,
        )

        assert first.consumer != second.consumer
        assert first.stream == "s"
        assert first.group == GROUP


# --- Against a real server -------------------------------------------------


@pytest.fixture
async def client() -> AsyncGenerator[Redis]:
    if not redis_reachable():
        pytest.skip(REDIS_SKIP_REASON)
    connection = Redis.from_url(REDIS_URL, decode_responses=False)
    yield connection
    await connection.aclose()


@pytest.fixture
def stream() -> str:
    return f"test-stream:{uuid.uuid4()}"


@pytest.fixture
async def group(client: Redis, stream: str) -> AsyncGenerator[RedisStreamGroup]:
    built = RedisStreamGroup(client, stream=stream, group=GROUP, consumer="worker-1")
    await built.start()
    yield built
    await client.delete(stream)


class TestDelConsumerReallyDiscardsPendingEntries:
    async def test_the_command_this_package_refuses_to_run_blindly(
        self, client: Redis, group: RedisStreamGroup
    ) -> None:
        """The measurement `RedisStreamGroup.stop` is built on.

        Four messages are read and left unacknowledged, the consumer is removed
        with the raw command, and all four are gone: not pending, not
        claimable, and not returned by `XREADGROUP >` either, because the
        group's last-delivered-id is already past them. The return value —
        four — reads like a receipt and is a body count.
        """
        for index in range(4):
            await group.publish({"n": str(index).encode()})
        await group.read(count=10, block=0)
        assert await group.pending_count() == 4

        discarded = await client.xgroup_delconsumer(group.stream, GROUP, group.consumer)

        assert discarded == 4
        assert await group.pending_count() == 0
        assert await group.stalled(min_idle=0, count=10) == ()
        assert await group.read(count=10, block=0) == ()

    async def test_stop_keeps_a_consumer_that_still_owes_messages(
        self, client: Redis, group: RedisStreamGroup
    ) -> None:
        """Which is what leaves them claimable by whoever takes over."""
        await group.publish({"n": b"in-flight"})
        await group.read(count=10, block=0)

        await group.stop()

        assert await client.xpending(group.stream, GROUP) != {
            "pending": 0,
            "min": None,
            "max": None,
            "consumers": [],
        }
        successor = RedisStreamGroup(
            client, stream=group.stream, group=GROUP, consumer="worker-2"
        )
        await successor.start()
        entries = await successor.stalled(min_idle=0, count=10)
        claimed = await successor.claim(entries, min_idle=0)

        assert [message.field("n") for message in claimed.messages] == [b"in-flight"]

    async def test_stop_removes_a_consumer_that_owes_nothing(
        self, client: Redis, group: RedisStreamGroup
    ) -> None:
        """The other half: without this, a rolling deployment leaves one
        consumer record per pod in the group forever."""
        entry_id = await group.publish({"n": b"done"})
        await group.read(count=10, block=0)
        await group.ack([entry_id])

        await group.stop()

        consumers = await client.xinfo_consumers(group.stream, GROUP)
        assert [row["name"] for row in consumers] == []


class TestAgainstARealServer:
    async def test_starting_a_group_that_already_exists_is_not_an_error(
        self, client: Redis, group: RedisStreamGroup
    ) -> None:
        """`BUSYGROUP` is the expected answer on every start but the first,
        including the first start of every other replica."""
        second = RedisStreamGroup(
            client, stream=group.stream, group=GROUP, consumer="worker-2"
        )

        await second.start()

        assert second.started is True

    async def test_a_deleted_stream_is_recovered_from_by_the_next_start(
        self, client: Redis, group: RedisStreamGroup
    ) -> None:
        await client.delete(group.stream)

        with pytest.raises(StreamGroupError, match="vanished"):
            await group.read(count=10, block=0)
        assert group.started is False

        await group.start()
        await group.publish({"n": b"after"})

        messages = await group.read(count=10, block=0)
        assert [message.field("n") for message in messages] == [b"after"]

    async def test_a_capped_stream_is_trimmed_as_it_is_written(
        self, client: Redis, stream: str
    ) -> None:
        """Loosely asserted on purpose: `~` trims whole macro nodes rather than
        counting to an exact length, so the guarantee is "bounded", not
        "ten". An exact cap would pay a latency spike on an unlucky `XADD` for
        a precision nothing here needs."""
        capped = RedisStreamGroup(
            client, stream=stream, group=GROUP, consumer="worker-1", maxlen=10
        )
        await capped.start()
        for index in range(600):
            await capped.publish({"n": str(index).encode()})

        length = await client.xlen(stream)

        assert 10 <= length < 600
        await client.delete(stream)

    async def test_values_survive_the_round_trip_as_bytes(
        self, group: RedisStreamGroup
    ) -> None:
        """The reason `decode_responses` stays off: a value is whatever a
        producer encoded, and decoding it would corrupt anything that is not
        text."""
        payload = bytes(range(256))
        await group.publish({"blob": payload})

        (message,) = await group.read(count=10, block=0)

        assert message.field("blob") == payload


async def _force_started(group: RedisStreamGroup) -> None:
    """Put a group into its started state without a server.

    The alternative is a stub that answers `xgroup_create` and fails everything
    else, which would test the stub's branching rather than the group's.
    """
    group._started = True  # noqa: SLF001


def _pending_entry() -> Any:
    from src.redis_streams.base import PendingEntry

    return PendingEntry(
        id=StreamEntryId(1, 0), consumer="other", idle=99.0, delivery_count=1
    )
