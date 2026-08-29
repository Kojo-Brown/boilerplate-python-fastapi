"""The stream as a whole: preamble, keepalives, lifetime, and the disconnect log.

The `aborted` outcome is the one to read carefully. It is how a client
disconnect is recorded, and the only way this code learns about one is that
something closes the generator — so a test that asserts the log line is
asserting that the detection path exists at all.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, MutableMapping
from typing import Any

import pytest
from structlog.testing import capture_logs

from src.sse.event import ServerSentEvent, comment_frame, retry_frame
from src.sse.stream import LIFETIME_COMMENT, sse_stream

BEAT = comment_frame("keep-alive")
TICK = 0.02


async def events(*items: ServerSentEvent) -> AsyncGenerator[ServerSentEvent, None]:
    for item in items:
        yield item


async def take(stream: AsyncIterator[bytes], count: int) -> list[bytes]:
    collected: list[bytes] = []
    async for frame in stream:
        collected.append(frame)
        if len(collected) == count:
            break
    return collected


def closed_lines(
    logs: list[MutableMapping[str, Any]],
) -> list[MutableMapping[str, Any]]:
    return [line for line in logs if line["event"] == "sse.stream_closed"]


class TestBody:
    async def test_events_are_encoded_in_order(self) -> None:
        source = events(ServerSentEvent(data="a"), ServerSentEvent(data="b"))

        out = [f async for f in sse_stream(source, name="s", heartbeat=10.0)]

        assert out == [b"data: a\n\n", b"data: b\n\n"]

    async def test_the_retry_preamble_comes_first(self) -> None:
        """Advice about a reconnect has to arrive before the connection fails."""
        source = events(ServerSentEvent(data="a"))

        out = [f async for f in sse_stream(source, name="s", retry=3000)]

        assert out[0] == retry_frame(3000)

    async def test_no_preamble_when_no_retry_is_configured(self) -> None:
        source = events(ServerSentEvent(data="a"))

        out = [f async for f in sse_stream(source, name="s")]

        assert out == [b"data: a\n\n"]

    async def test_an_empty_source_produces_an_empty_body(self) -> None:
        out = [f async for f in sse_stream(events(), name="s")]

        assert out == []

    async def test_keepalives_are_injected_into_silence(self) -> None:
        async def quiet() -> AsyncGenerator[ServerSentEvent, None]:
            await asyncio.sleep(TICK * 3)
            yield ServerSentEvent(data="finally")

        out = [f async for f in sse_stream(quiet(), name="s", heartbeat=TICK)]

        assert BEAT in out
        assert out[-1] == b"data: finally\n\n"


class TestLifetime:
    async def test_the_stream_ends_after_the_ceiling(self) -> None:
        async def endless() -> AsyncGenerator[ServerSentEvent, None]:
            while True:
                await asyncio.sleep(TICK)
                yield ServerSentEvent(data="tick")

        out = [
            f
            async for f in sse_stream(
                endless(), name="s", heartbeat=TICK, max_seconds=TICK * 3
            )
        ]

        assert out[-1] == comment_frame(LIFETIME_COMMENT)

    async def test_it_ends_cleanly_rather_than_raising(self) -> None:
        """A `DeadlineExceeded` mid-body would be a reset with a traceback."""

        async def endless() -> AsyncGenerator[ServerSentEvent, None]:
            while True:
                await asyncio.sleep(TICK)
                yield ServerSentEvent(data="tick")

        stream = sse_stream(endless(), name="s", heartbeat=TICK, max_seconds=TICK * 2)

        # Exhausting it is the assertion: an exception would leave this loop
        # by raising rather than by ending.
        assert [f async for f in stream]

    async def test_a_quiet_stream_still_reaches_its_ceiling(self) -> None:
        """The keepalive is what resumes the generator to check the clock."""

        async def never() -> AsyncGenerator[ServerSentEvent, None]:
            await asyncio.Event().wait()
            yield ServerSentEvent(data="x")  # pragma: no cover - never reached

        out = [
            f
            async for f in sse_stream(
                never(), name="s", heartbeat=TICK, max_seconds=TICK * 2
            )
        ]

        assert out[-1] == comment_frame(LIFETIME_COMMENT)
        assert set(out[:-1]) == {BEAT}

    async def test_no_ceiling_means_the_source_decides(self) -> None:
        source = events(ServerSentEvent(data="a"))

        out = [f async for f in sse_stream(source, name="s", max_seconds=None)]

        assert out == [b"data: a\n\n"]


class TestOutcomeLogging:
    async def test_a_source_that_ends_logs_exhausted(self) -> None:
        with capture_logs() as logs:
            [f async for f in sse_stream(events(ServerSentEvent(data="a")), name="s")]

        (line,) = closed_lines(logs)
        assert line["outcome"] == "exhausted"
        assert line["events"] == 1
        assert line["stream"] == "s"

    async def test_reaching_the_ceiling_logs_lifetime(self) -> None:
        async def never() -> AsyncGenerator[ServerSentEvent, None]:
            await asyncio.Event().wait()
            yield ServerSentEvent(data="x")  # pragma: no cover - never reached

        with capture_logs() as logs:
            [
                f
                async for f in sse_stream(
                    never(), name="s", heartbeat=TICK, max_seconds=TICK
                )
            ]

        assert closed_lines(logs)[0]["outcome"] == "lifetime"

    async def test_closing_the_stream_early_logs_aborted(self) -> None:
        """This is the client disconnect, and the line that makes it countable."""

        async def never() -> AsyncGenerator[ServerSentEvent, None]:
            yield ServerSentEvent(data="first")
            await asyncio.Event().wait()
            yield ServerSentEvent(data="x")  # pragma: no cover - never reached

        stream = sse_stream(never(), name="s", heartbeat=10.0)
        with capture_logs() as logs:
            assert await take(stream, 1) == [b"data: first\n\n"]
            await stream.aclose()

        (line,) = closed_lines(logs)
        assert line["outcome"] == "aborted"
        assert line["events"] == 1

    async def test_keepalives_are_not_counted_as_events(self) -> None:
        """`events=0` on a long stream is the signal that it carried nothing."""

        async def never() -> AsyncGenerator[ServerSentEvent, None]:
            await asyncio.Event().wait()
            yield ServerSentEvent(data="x")  # pragma: no cover - never reached

        stream = sse_stream(never(), name="s", heartbeat=TICK)
        with capture_logs() as logs:
            assert await take(stream, 2) == [BEAT, BEAT]
            await stream.aclose()

        assert closed_lines(logs)[0]["events"] == 0


class TestCleanup:
    async def test_closing_the_stream_closes_the_source(self) -> None:
        """What releases the hub subscription when a client disconnects."""
        released = asyncio.Event()

        async def source() -> AsyncGenerator[ServerSentEvent, None]:
            try:
                yield ServerSentEvent(data="first")
                await asyncio.Event().wait()
                yield ServerSentEvent(data="x")  # pragma: no cover - never reached
            finally:
                released.set()

        stream = sse_stream(source(), name="s", heartbeat=10.0)
        assert await take(stream, 1) == [b"data: first\n\n"]
        await stream.aclose()

        assert released.is_set()

    async def test_the_source_is_closed_when_the_ceiling_ends_the_stream(self) -> None:
        released = asyncio.Event()

        async def source() -> AsyncGenerator[ServerSentEvent, None]:
            try:
                while True:
                    await asyncio.sleep(TICK)
                    yield ServerSentEvent(data="tick")
            finally:
                released.set()

        async for _ in sse_stream(
            source(), name="s", heartbeat=TICK, max_seconds=TICK * 2
        ):
            pass

        assert released.is_set()

    async def test_a_source_error_propagates(self) -> None:
        """Not swallowed: unlike an export, SSE has no terminal record to write.

        The connection ends, which is a disconnect the client reconnects from,
        and the traceback belongs in the logs rather than in the body.
        """

        async def broken() -> AsyncGenerator[ServerSentEvent, None]:
            yield ServerSentEvent(data="first")
            raise RuntimeError("source failed")

        with pytest.raises(RuntimeError, match="source failed"):
            [f async for f in sse_stream(broken(), name="s", heartbeat=10.0)]
