"""One suite, run against every `StreamConsumerGroup` implementation.

The consume policy in `src/redis_streams/consumer.py` rests on promises the
*protocol* makes — that a read puts a message in the PEL, that an idle message
can be claimed by somebody else, that a claim increments the delivery count,
that an entry deleted from under a pending row does not stay owed forever — and
a promise tested against one implementation is a promise about that
implementation.

The Redis leg needs a real server. It is skipped when nothing is listening on
`REDIS_URL`, and CI always has one (see the `redis` service in ci.yml), so
every claim about idle time, ownership and counters below is measured against
the real thing on every pull request rather than asserted against a model.

Both legs use a `min_idle` of tens of milliseconds and really wait it out. That
is the one place this suite spends wall-clock time on purpose: idle time is the
whole subject, and a fake clock on the memory leg would make the two legs test
different things. `tests/test_redis_streams_memory.py` pins a clock instead,
which is where the arithmetic is checked.
"""

from __future__ import annotations

import asyncio
import os
import socket
import uuid
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass
from urllib.parse import urlparse

import pytest
from redis.asyncio import Redis

from src.redis_streams.base import (
    StreamConsumerGroup,
    StreamEntryId,
    StreamLifecycleError,
)
from src.redis_streams.memory import InMemoryStreamServer
from src.redis_streams.redis_group import RedisStreamGroup

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

#: Long enough that a message is not accidentally claimable in the same
#: millisecond it was read, short enough that a suite of these is a rounding
#: error. Every wait below is this value with headroom.
IDLE = 0.05

GROUP = "contract-group"


def redis_reachable(url: str = REDIS_URL) -> bool:
    """Cheap liveness probe used to skip, not to assert.

    A TCP connect proves a listener rather than a Redis, which is deliberate:
    if something else is on the port these tests fail loudly instead of
    skipping quietly, and a silent skip is the failure mode that lets a
    backend rot.
    """
    parsed = urlparse(url)
    try:
        with socket.create_connection(
            (parsed.hostname or "localhost", parsed.port or 6379), timeout=1
        ):
            return True
    except OSError:
        return False


REDIS_SKIP_REASON = f"no Redis listening on {REDIS_URL}"


@dataclass(frozen=True, slots=True)
class Harness:
    """One backend, plus the two things the protocol deliberately cannot do.

    `member` builds another started consumer — in the same group by default,
    which is the other half of every claiming test, or in a named one for the
    tests about where a *group* starts reading. `delete` removes an entry from
    the stream behind the group's back, which is what `XDEL` and trimming do in
    production and what no method on `StreamConsumerGroup` offers.
    """

    stream: str
    group: StreamConsumerGroup
    member: Callable[..., Awaitable[StreamConsumerGroup]]
    delete: Callable[[StreamEntryId], Awaitable[None]]


@pytest.fixture(params=["memory", "redis"])
async def harness(request: pytest.FixtureRequest) -> AsyncGenerator[Harness]:
    """Every implementation of the protocol, one at a time."""
    # A stream per test keeps concurrent runs — and a developer's own Redis —
    # from colliding, without flushing a database that may not be ours.
    stream = f"test-stream:{uuid.uuid4()}"
    built: list[StreamConsumerGroup] = []
    cleanup: Callable[[], Awaitable[None]] | None = None

    if request.param == "memory":
        server = InMemoryStreamServer()

        def make(name: str, group: str) -> StreamConsumerGroup:
            return server.group(stream=stream, group=group, consumer=name)

        async def delete(entry_id: StreamEntryId) -> None:
            server.delete(stream, entry_id)

    else:
        if not redis_reachable():
            pytest.skip(REDIS_SKIP_REASON)
        client = Redis.from_url(REDIS_URL, decode_responses=False)

        def make(name: str, group: str) -> StreamConsumerGroup:
            return RedisStreamGroup(
                Redis.from_url(REDIS_URL, decode_responses=False),
                stream=stream,
                group=group,
                consumer=name,
                owns_client=True,
            )

        async def delete(entry_id: StreamEntryId) -> None:
            await client.xdel(stream, str(entry_id))

        async def close_client() -> None:
            await client.delete(stream)
            await client.aclose()

        cleanup = close_client

    async def member(name: str, group: str = GROUP) -> StreamConsumerGroup:
        built_group = make(name, group)
        await built_group.start()
        built.append(built_group)
        return built_group

    primary = await member("consumer-a")
    yield Harness(stream=stream, group=primary, member=member, delete=delete)

    for group_client in built:
        await group_client.stop()
    if cleanup is not None:
        await cleanup()


async def publish_many(group: StreamConsumerGroup, count: int) -> list[StreamEntryId]:
    return [await group.publish({"n": str(index).encode()}) for index in range(count)]


class TestReadingAndAcknowledging:
    async def test_a_published_message_comes_back_in_order(
        self, harness: Harness
    ) -> None:
        ids = await publish_many(harness.group, 3)

        messages = await harness.group.read(count=10, block=0)

        assert [message.id for message in messages] == ids
        assert [message.field("n") for message in messages] == [b"0", b"1", b"2"]

    async def test_the_first_read_is_delivery_one(self, harness: Harness) -> None:
        """Being handed to a consumer *is* a delivery, which is why the cap in
        `StreamConsumerConfig` counts attempts rather than retries."""
        await harness.group.publish({"n": b"0"})

        (message,) = await harness.group.read(count=10, block=0)

        assert message.delivery_count == 1
        assert message.claimed is False

    async def test_reading_creates_the_obligation_to_acknowledge(
        self, harness: Harness
    ) -> None:
        await publish_many(harness.group, 2)

        await harness.group.read(count=10, block=0)

        assert await harness.group.pending_count() == 2

    async def test_acknowledging_discharges_it(self, harness: Harness) -> None:
        ids = await publish_many(harness.group, 2)
        await harness.group.read(count=10, block=0)

        assert await harness.group.ack(ids) == 2
        assert await harness.group.pending_count() == 0

    async def test_acknowledging_twice_is_a_no_op(self, harness: Harness) -> None:
        """How a redelivered message ends: the second acknowledgement of an id
        is not an error, it is the ordinary outcome of at-least-once."""
        ids = await publish_many(harness.group, 1)
        await harness.group.read(count=10, block=0)
        await harness.group.ack(ids)

        assert await harness.group.ack(ids) == 0

    async def test_a_message_is_delivered_to_one_consumer_in_the_group(
        self, harness: Harness
    ) -> None:
        await publish_many(harness.group, 4)
        other = await harness.member("consumer-b")

        first = await harness.group.read(count=2, block=0)
        second = await other.read(count=10, block=0)

        assert len(first) == 2
        assert len(second) == 2
        assert {m.id for m in first}.isdisjoint({m.id for m in second})

    async def test_an_empty_stream_reads_empty(self, harness: Harness) -> None:
        assert await harness.group.read(count=10, block=0) == ()

    async def test_a_new_group_starts_at_the_end_of_an_existing_stream(
        self, harness: Harness
    ) -> None:
        """`$`, not `0`. A new group is not a request to replay history — and
        this is the opposite of the `earliest` default in `src/kafka`, which is
        why it is asserted rather than assumed."""
        await harness.group.publish({"n": b"before"})
        late = await harness.member("late-member", "late-group")

        assert await late.read(count=10, block=0) == ()

        await harness.group.publish({"n": b"after"})

        messages = await late.read(count=10, block=0)
        assert [message.field("n") for message in messages] == [b"after"]

    async def test_a_blocking_read_returns_as_soon_as_a_message_arrives(
        self, harness: Harness
    ) -> None:
        async def publish_shortly() -> None:
            await asyncio.sleep(0.01)
            await harness.group.publish({"n": b"late"})

        publisher = asyncio.create_task(publish_shortly())
        try:
            messages = await harness.group.read(count=10, block=2.0)
        finally:
            await publisher

        assert [message.field("n") for message in messages] == [b"late"]

    async def test_a_blocking_read_gives_up_on_an_empty_stream(
        self, harness: Harness
    ) -> None:
        assert await harness.group.read(count=10, block=0.05) == ()

    async def test_another_stream_is_not_this_group_s_business(
        self, harness: Harness
    ) -> None:
        await harness.group.publish(
            {"n": b"elsewhere"}, stream=f"{harness.stream}.other"
        )

        assert await harness.group.read(count=10, block=0) == ()


class TestClaiming:
    async def test_a_fresh_message_is_not_stalled(self, harness: Harness) -> None:
        await publish_many(harness.group, 1)
        await harness.group.read(count=10, block=0)

        assert await harness.group.stalled(min_idle=IDLE, count=10) == ()

    async def test_an_untouched_message_becomes_stalled(self, harness: Harness) -> None:
        (entry_id,) = await publish_many(harness.group, 1)
        await harness.group.read(count=10, block=0)
        await asyncio.sleep(IDLE * 2)

        (entry,) = await harness.group.stalled(min_idle=IDLE, count=10)

        assert entry.id == entry_id
        assert entry.consumer == "consumer-a"
        assert entry.delivery_count == 1
        assert entry.idle >= IDLE

    async def test_claiming_moves_ownership_and_counts_the_delivery(
        self, harness: Harness
    ) -> None:
        (entry_id,) = await publish_many(harness.group, 1)
        await harness.group.read(count=10, block=0)
        other = await harness.member("consumer-b")
        await asyncio.sleep(IDLE * 2)

        entries = await other.stalled(min_idle=IDLE, count=10)
        claimed = await other.claim(entries, min_idle=IDLE)

        assert [message.id for message in claimed.messages] == [entry_id]
        assert claimed.messages[0].delivery_count == 2
        assert claimed.messages[0].claimed is True
        assert claimed.messages[0].field("n") == b"0"
        assert await other.pending_count(consumer="consumer-b") == 1
        assert await other.pending_count(consumer="consumer-a") == 0

    async def test_claiming_resets_the_idle_clock(self, harness: Harness) -> None:
        """Which is what stops a third consumer taking it straight back."""
        await publish_many(harness.group, 1)
        await harness.group.read(count=10, block=0)
        other = await harness.member("consumer-b")
        await asyncio.sleep(IDLE * 2)
        entries = await other.stalled(min_idle=IDLE, count=10)
        await other.claim(entries, min_idle=IDLE)

        assert await other.stalled(min_idle=IDLE, count=10) == ()

    async def test_a_claim_is_refused_when_the_entry_stopped_being_idle(
        self, harness: Harness
    ) -> None:
        """The condition is re-checked server-side, which is what keeps two
        consumers scanning at the same moment from both taking a message."""
        await publish_many(harness.group, 1)
        await harness.group.read(count=10, block=0)
        other = await harness.member("consumer-b")
        await asyncio.sleep(IDLE * 2)
        entries = await other.stalled(min_idle=IDLE, count=10)

        # Ten seconds of idleness is what a competing consumer that had just
        # re-read the message would leave: the entry is real, the condition is
        # not met, and nothing is claimed.
        claimed = await other.claim(entries, min_idle=10.0)

        assert claimed.messages == ()
        assert claimed.missing == tuple(entry.id for entry in entries)
        assert await other.pending_count(consumer="consumer-a") == 1

    async def test_claiming_an_acknowledged_entry_finds_nothing(
        self, harness: Harness
    ) -> None:
        ids = await publish_many(harness.group, 1)
        await harness.group.read(count=10, block=0)
        await asyncio.sleep(IDLE * 2)
        entries = await harness.group.stalled(min_idle=IDLE, count=10)
        await harness.group.ack(ids)

        claimed = await harness.group.claim(entries, min_idle=IDLE)

        assert claimed.messages == ()
        assert claimed.missing == tuple(ids)

    async def test_an_entry_deleted_from_under_the_pel_is_dropped_not_owed(
        self, harness: Harness
    ) -> None:
        """The dangling pending entry, which any capped stream reaches
        eventually: the message is gone, so the acknowledgement it will never
        get must not leave the group owing it forever."""
        (entry_id,) = await publish_many(harness.group, 1)
        await harness.group.read(count=10, block=0)
        await harness.delete(entry_id)
        await asyncio.sleep(IDLE * 2)
        entries = await harness.group.stalled(min_idle=IDLE, count=10)

        claimed = await harness.group.claim(entries, min_idle=IDLE)

        assert claimed.messages == ()
        assert claimed.missing == (entry_id,)
        assert await harness.group.pending_count() == 0

    async def test_a_stalled_scan_pages_from_a_start_id(self, harness: Harness) -> None:
        ids = await publish_many(harness.group, 3)
        await harness.group.read(count=10, block=0)
        await asyncio.sleep(IDLE * 2)

        first_page = await harness.group.stalled(min_idle=IDLE, count=2)
        second_page = await harness.group.stalled(
            min_idle=IDLE, count=2, start=first_page[-1].id
        )

        assert [entry.id for entry in first_page] == ids[:2]
        assert [entry.id for entry in second_page] == ids[2:]

    async def test_claiming_nothing_asks_the_server_nothing(
        self, harness: Harness
    ) -> None:
        claimed = await harness.group.claim([], min_idle=IDLE)

        assert claimed.messages == ()
        assert claimed.missing == ()

    async def test_acknowledging_nothing_is_zero(self, harness: Harness) -> None:
        assert await harness.group.ack([]) == 0


class TestLifecycleAndArguments:
    async def test_reading_before_start_is_refused(self, harness: Harness) -> None:
        """Not an implicit start: a connection pool built from inside whichever
        coroutine read first has a request's lifetime, not the process's."""
        await harness.group.stop()

        with pytest.raises(StreamLifecycleError):
            await harness.group.read(count=1, block=0)

    async def test_starting_twice_is_harmless(self, harness: Harness) -> None:
        await harness.group.start()
        await harness.group.start()

        assert await harness.group.read(count=10, block=0) == ()

    async def test_stopping_twice_is_harmless(self, harness: Harness) -> None:
        await harness.group.stop()
        await harness.group.stop()

    @pytest.mark.parametrize("count", [0, -1])
    async def test_a_read_of_no_messages_is_refused(
        self, harness: Harness, count: int
    ) -> None:
        with pytest.raises(ValueError, match="count must be at least 1"):
            await harness.group.read(count=count, block=0)

    async def test_a_stalled_scan_of_no_entries_is_refused(
        self, harness: Harness
    ) -> None:
        with pytest.raises(ValueError, match="count must be at least 1"):
            await harness.group.stalled(min_idle=IDLE, count=0)

    async def test_an_empty_message_is_refused(self, harness: Harness) -> None:
        """`XADD key *` with no field is a syntax error from the server,
        arriving as an opaque `ResponseError` wherever the empty dict was
        built."""
        with pytest.raises(ValueError, match="at least one field"):
            await harness.group.publish({})

    async def test_a_non_bytes_field_is_refused(self, harness: Harness) -> None:
        """redis-py would encode it, and the consumer would be handed a type
        nobody chose: `1` and `"1"` both arrive as `b"1"`."""
        with pytest.raises(TypeError, match="must be bytes"):
            await harness.group.publish({"n": 1})  # type: ignore[dict-item]

    async def test_it_satisfies_the_protocol(self, harness: Harness) -> None:
        assert isinstance(harness.group, StreamConsumerGroup)
