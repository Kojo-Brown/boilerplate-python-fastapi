"""The in-process model's own behaviour: the clock, the ids, and trimming.

The shared contract in `tests/test_redis_streams_contract.py` proves this is a
consumer group, against a real server as well as this one. What is left here is
what only this implementation has — a clock a test owns, so the idle arithmetic
can be checked at the boundary rather than approached with `sleep`, and the
trimming path, which is how a pending entry comes to point at a message that no
longer exists.
"""

from __future__ import annotations

import asyncio

import pytest

from src.redis_streams.base import StreamEntryId, StreamLifecycleError
from src.redis_streams.memory import InMemoryStreamGroup, InMemoryStreamServer

STREAM = "events"
GROUP = "workers"


class FakeClock:
    def __init__(self, now: float = 500.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def server(clock: FakeClock) -> InMemoryStreamServer:
    return InMemoryStreamServer(clock=clock)


async def started(
    server: InMemoryStreamServer, consumer: str = "c1", *, maxlen: int = 0
) -> InMemoryStreamGroup:
    group = server.group(stream=STREAM, group=GROUP, consumer=consumer, maxlen=maxlen)
    await group.start()
    return group


class TestIds:
    async def test_ids_stay_ordered_when_the_clock_does_not_move(
        self, server: InMemoryStreamServer
    ) -> None:
        """Which is the normal case under a pinned clock, and a real one when
        two messages land in the same millisecond."""
        first = server.append(STREAM, {"n": b"0"})
        second = server.append(STREAM, {"n": b"1"})

        assert first < second
        assert first.ms == second.ms
        assert second.seq == first.seq + 1

    async def test_the_sequence_restarts_when_the_clock_moves(
        self, server: InMemoryStreamServer, clock: FakeClock
    ) -> None:
        first = server.append(STREAM, {"n": b"0"})
        clock.advance(1.0)
        second = server.append(STREAM, {"n": b"1"})

        assert second.ms == first.ms + 1000
        assert second.seq == 0


class TestIdleness:
    async def test_an_entry_is_stalled_exactly_at_the_threshold(
        self, server: InMemoryStreamServer, clock: FakeClock
    ) -> None:
        """The boundary, which is what a `sleep`-based test cannot assert."""
        group = await started(server)
        await group.publish({"n": b"0"})
        await group.read(count=10, block=0)

        clock.advance(29.999)
        assert await group.stalled(min_idle=30.0, count=10) == ()

        clock.advance(0.001)
        (entry,) = await group.stalled(min_idle=30.0, count=10)
        assert entry.idle == pytest.approx(30.0)

    async def test_a_claim_resets_the_clock_it_was_selected_by(
        self, server: InMemoryStreamServer, clock: FakeClock
    ) -> None:
        group = await started(server)
        await group.publish({"n": b"0"})
        await group.read(count=10, block=0)
        clock.advance(60.0)
        entries = await group.stalled(min_idle=30.0, count=10)

        await group.claim(entries, min_idle=30.0)

        assert await group.stalled(min_idle=30.0, count=10) == ()


class TestTrimming:
    async def test_a_capped_stream_drops_its_oldest_entries(
        self, server: InMemoryStreamServer
    ) -> None:
        group = await started(server, maxlen=2)
        for index in range(5):
            await group.publish({"n": str(index).encode()})

        assert server.entry_count(STREAM) == 2
        assert [fields["n"] for _id, fields in server.entries(STREAM)] == [b"3", b"4"]

    async def test_trimming_does_not_consult_the_pending_list(
        self, server: InMemoryStreamServer, clock: FakeClock
    ) -> None:
        """The correctness edge on `REDIS_STREAMS_MAXLEN`: a cap low enough to
        overtake a slow consumer deletes messages it has not finished with, and
        the pending entry left behind is only resolved by a claim."""
        group = await started(server, maxlen=2)
        await group.publish({"n": b"0"})
        await group.read(count=10, block=0)
        assert await group.pending_count() == 1

        for index in range(1, 4):
            await group.publish({"n": str(index).encode()})
        clock.advance(60.0)

        entries = await group.stalled(min_idle=30.0, count=10)
        claimed = await group.claim(entries, min_idle=30.0)

        assert claimed.messages == ()
        assert len(claimed.missing) == 1
        assert await group.pending_count() == 0


class TestBlockingReads:
    async def test_a_blocking_read_wakes_on_a_publish(
        self, server: InMemoryStreamServer
    ) -> None:
        group = await started(server)

        async def publish_shortly() -> None:
            await asyncio.sleep(0.01)
            server.append(STREAM, {"n": b"late"})

        publisher = asyncio.create_task(publish_shortly())
        try:
            messages = await group.read(count=10, block=5.0)
        finally:
            await publisher

        assert [message.field("n") for message in messages] == [b"late"]

    async def test_a_waiter_is_removed_when_the_wait_times_out(
        self, server: InMemoryStreamServer
    ) -> None:
        """A waiter left behind is a leak per empty read, which for an idle
        consumer is one per cycle forever."""
        group = await started(server)

        assert await group.read(count=10, block=0.01) == ()
        assert await group.read(count=10, block=0.01) == ()

        assert server._streams[STREAM].waiters == []  # noqa: SLF001


class TestBookkeeping:
    async def test_it_lists_the_streams_it_holds(
        self, server: InMemoryStreamServer
    ) -> None:
        server.append("b", {"n": b"1"})
        server.append("a", {"n": b"1"})

        assert server.stream_names() == ("a", "b")

    async def test_deleting_an_entry_that_is_not_there_is_zero(
        self, server: InMemoryStreamServer
    ) -> None:
        assert server.delete(STREAM, StreamEntryId(1, 0)) == 0

    async def test_a_group_created_on_a_full_stream_starts_at_its_end(
        self, server: InMemoryStreamServer
    ) -> None:
        server.append(STREAM, {"n": b"history"})
        group = await started(server)

        assert await group.read(count=10, block=0) == ()

    async def test_claiming_before_start_is_refused(
        self, server: InMemoryStreamServer
    ) -> None:
        group = server.group(stream=STREAM, group=GROUP, consumer="c1")

        with pytest.raises(StreamLifecycleError):
            await group.claim([], min_idle=1.0)

    async def test_counting_before_start_is_refused(
        self, server: InMemoryStreamServer
    ) -> None:
        group = server.group(stream=STREAM, group=GROUP, consumer="c1")

        with pytest.raises(StreamLifecycleError):
            await group.pending_count()

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"stream": ""}, "stream must not be empty"),
            ({"group": ""}, "group must not be empty"),
            ({"consumer": ""}, "consumer must not be empty"),
            ({"maxlen": -1}, "maxlen cannot be negative"),
        ],
    )
    def test_a_nonsensical_group_is_refused_at_construction(
        self, server: InMemoryStreamServer, kwargs: dict[str, object], message: str
    ) -> None:
        fields: dict[str, object] = {
            "stream": STREAM,
            "group": GROUP,
            "consumer": "c1",
        }
        fields.update(kwargs)

        with pytest.raises(ValueError, match=message):
            server.group(**fields)  # type: ignore[arg-type]
