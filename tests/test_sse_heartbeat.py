"""The keepalive: when it fires, what it costs, and what it must never drop.

Intervals here are milliseconds rather than the production seconds, so the
suite spends single-digit milliseconds proving behaviour that ships at 15
seconds. Nothing sleeps for longer than it takes the loop to notice a timer.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator

import pytest

from src.sse.event import comment_frame
from src.sse.heartbeat import DEFAULT_HEARTBEAT_SECONDS, with_heartbeat

BEAT = comment_frame("keep-alive")
TICK = 0.02


async def frames(*items: bytes) -> AsyncGenerator[bytes, None]:
    """A source that yields `items` and ends."""
    for item in items:
        yield item


async def take(stream: AsyncIterator[bytes], count: int) -> list[bytes]:
    """Read exactly `count` frames."""
    collected: list[bytes] = []
    async for frame in stream:
        collected.append(frame)
        if len(collected) == count:
            break
    return collected


class TestPassingFramesThrough:
    async def test_a_busy_stream_emits_no_keepalives(self) -> None:
        """The timer measures silence, and there is none."""
        out = [f async for f in with_heartbeat(frames(b"a", b"b"), interval=10.0)]

        assert out == [b"a", b"b"]

    async def test_the_stream_ends_when_the_source_does(self) -> None:
        out = [f async for f in with_heartbeat(frames(), interval=10.0)]

        assert out == []

    async def test_frames_keep_their_order(self) -> None:
        source = frames(*(f"{i}".encode() for i in range(50)))

        out = [f async for f in with_heartbeat(source, interval=10.0)]

        assert out == [f"{i}".encode() for i in range(50)]


class TestKeepalives:
    async def test_silence_produces_a_keepalive(self) -> None:
        async def never() -> AsyncGenerator[bytes, None]:
            await asyncio.Event().wait()
            yield b"unreachable"  # pragma: no cover - the wait never returns

        stream = with_heartbeat(never(), interval=TICK)
        try:
            assert await take(stream, 1) == [BEAT]
        finally:
            await stream.aclose()

    async def test_silence_keeps_producing_them(self) -> None:
        """One keepalive is a fluke; the connection stays open on the rest."""

        async def never() -> AsyncGenerator[bytes, None]:
            await asyncio.Event().wait()
            yield b"unreachable"  # pragma: no cover - the wait never returns

        stream = with_heartbeat(never(), interval=TICK)
        try:
            assert await take(stream, 3) == [BEAT, BEAT, BEAT]
        finally:
            await stream.aclose()

    async def test_the_comment_body_is_configurable(self) -> None:
        async def never() -> AsyncGenerator[bytes, None]:
            await asyncio.Event().wait()
            yield b"unreachable"  # pragma: no cover - the wait never returns

        stream = with_heartbeat(never(), interval=TICK, comment="ping")
        try:
            assert await take(stream, 1) == [comment_frame("ping")]
        finally:
            await stream.aclose()

    async def test_a_keepalive_dispatches_no_event(self) -> None:
        """It is a comment: the client's listeners never see it."""
        assert BEAT.startswith(b":")

    async def test_the_timer_restarts_after_every_frame(self) -> None:
        """A slow-but-steady source under the interval is never interrupted."""

        async def steady() -> AsyncGenerator[bytes, None]:
            for i in range(4):
                await asyncio.sleep(TICK / 4)
                yield f"{i}".encode()

        out = [f async for f in with_heartbeat(steady(), interval=TICK)]

        assert out == [b"0", b"1", b"2", b"3"]


class TestNothingIsLostAcrossAKeepalive:
    """The reason the pending read survives the timer instead of being cancelled.

    A construction that cancelled the in-flight `__anext__` on every timeout
    would lose an event whenever the source produced one at the same moment —
    rarely, under load, and invisibly.
    """

    async def test_an_event_arriving_after_several_keepalives_is_delivered(
        self,
    ) -> None:
        async def late() -> AsyncGenerator[bytes, None]:
            await asyncio.sleep(TICK * 5)
            yield b"late"

        stream = with_heartbeat(late(), interval=TICK)
        out = [f async for f in stream]

        assert out.count(b"late") == 1
        assert out[-1] == b"late"
        assert set(out[:-1]) == {BEAT}

    async def test_nothing_is_dropped_when_the_source_races_the_timer(self) -> None:
        """Every event survives a source producing near the keepalive boundary."""

        async def racing() -> AsyncGenerator[bytes, None]:
            for i in range(20):
                await asyncio.sleep(TICK)
                yield f"{i}".encode()

        out = [f async for f in with_heartbeat(racing(), interval=TICK)]

        assert [f for f in out if f != BEAT] == [f"{i}".encode() for i in range(20)]


class TestCleanup:
    async def test_closing_the_stream_closes_the_source(self) -> None:
        """A client disconnect has to release whatever the source holds."""
        closed = asyncio.Event()

        async def source() -> AsyncGenerator[bytes, None]:
            try:
                yield b"first"
                await asyncio.Event().wait()
                yield b"unreachable"  # pragma: no cover - the wait never returns
            finally:
                closed.set()

        stream = with_heartbeat(source(), interval=10.0)
        assert await take(stream, 1) == [b"first"]
        await stream.aclose()

        assert closed.is_set()

    async def test_closing_mid_wait_leaves_no_pending_task(self) -> None:
        """The in-flight read is cancelled before the source is closed.

        `aclose()` on a generator with an `__anext__` still running raises
        RuntimeError, so the ordering in the `finally` is load-bearing rather
        than tidiness — and a task left behind would outlive the response.
        """

        async def never() -> AsyncGenerator[bytes, None]:
            await asyncio.Event().wait()
            yield b"unreachable"  # pragma: no cover - the wait never returns

        before = len(asyncio.all_tasks())
        stream = with_heartbeat(never(), interval=TICK)
        assert await take(stream, 1) == [BEAT]
        await stream.aclose()
        await asyncio.sleep(0)

        assert len(asyncio.all_tasks()) == before

    async def test_a_source_error_propagates(self) -> None:
        """A broken source is not silently turned into an endless keepalive."""

        async def broken() -> AsyncGenerator[bytes, None]:
            yield b"first"
            raise RuntimeError("source failed")

        stream = with_heartbeat(broken(), interval=10.0)

        with pytest.raises(RuntimeError, match="source failed"):
            [f async for f in stream]


class TestArguments:
    @pytest.mark.parametrize("interval", [0.0, -1.0])
    async def test_a_non_positive_interval_is_refused(self, interval: float) -> None:
        """It would mean a keepalive per loop iteration, forever."""
        stream = with_heartbeat(frames(b"a"), interval=interval)

        with pytest.raises(ValueError, match="must be positive"):
            await anext(stream)

    async def test_a_comment_with_a_line_break_is_refused(self) -> None:
        stream = with_heartbeat(frames(b"a"), interval=1.0, comment="one\ntwo")

        with pytest.raises(ValueError, match="line break"):
            await anext(stream)

    def test_the_default_interval_sits_under_a_typical_proxy_timeout(self) -> None:
        """60 seconds is the nginx and ELB default; anything near it is churn."""
        assert 0 < DEFAULT_HEARTBEAT_SECONDS <= 30.0
