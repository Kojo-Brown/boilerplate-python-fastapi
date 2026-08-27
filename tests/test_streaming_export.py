"""`stream_ndjson_export`: the pipeline, and what it does about failing late.

The one behaviour worth stating twice: nothing raised by the record source
propagates out of this generator. By the time it could, a 200 has been sent and
the only honest place left to report a failure is the body.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Mapping
from contextlib import aclosing

import pytest

from src.exceptions import ForbiddenError
from src.streaming.export import stream_ndjson_export
from src.streaming.ndjson import TERMINAL_KEY


def _lines(body: bytes) -> list[dict[str, object]]:
    return [json.loads(line) for line in body.splitlines()]


async def _collect(stream: AsyncIterator[bytes]) -> bytes:
    return b"".join([chunk async for chunk in stream])


def _records(
    total: int,
    *,
    fail_after: int | None = None,
    error: Exception | None = None,
) -> AsyncIterator[Mapping[str, object]]:
    async def gen() -> AsyncIterator[Mapping[str, object]]:
        for i in range(total):
            if fail_after is not None and i == fail_after:
                raise error or RuntimeError("the cursor died")
            yield {"i": i}

    return gen()


class TestACompleteStream:
    async def test_every_record_then_a_terminal_record(self) -> None:
        body = await _collect(
            stream_ndjson_export(lambda: _records(3), name="t", chunk_bytes=1)
        )

        assert _lines(body) == [
            {"i": 0},
            {"i": 1},
            {"i": 2},
            {TERMINAL_KEY: "complete", "records": 3},
        ]

    async def test_an_empty_export_is_still_terminated(self) -> None:
        """Zero rows and a truncated stream must not look the same."""
        body = await _collect(stream_ndjson_export(lambda: _records(0), name="t"))

        assert _lines(body) == [{TERMINAL_KEY: "complete", "records": 0}]

    async def test_records_are_coalesced_rather_than_sent_one_by_one(self) -> None:
        chunks = [
            chunk
            async for chunk in stream_ndjson_export(
                lambda: _records(500), name="t", chunk_bytes=65536
            )
        ]

        assert len(chunks) < 500
        assert _lines(b"".join(chunks))[-1] == {
            TERMINAL_KEY: "complete",
            "records": 500,
        }


class TestFailingAfterTheFirstByte:
    async def test_the_failure_is_reported_in_the_body_not_raised(self) -> None:
        body = await _collect(
            stream_ndjson_export(
                lambda: _records(10, fail_after=4), name="t", chunk_bytes=1
            )
        )

        lines = _lines(body)
        assert lines[:4] == [{"i": i} for i in range(4)]
        assert lines[-1] == {
            TERMINAL_KEY: "failed",
            "records": 4,
            "error": "INTERNAL_ERROR",
            "message": "The export stopped before it finished.",
        }

    async def test_an_application_error_keeps_its_code(self) -> None:
        body = await _collect(
            stream_ndjson_export(
                lambda: _records(10, fail_after=1, error=ForbiddenError("nope")),
                name="t",
                chunk_bytes=1,
            )
        )

        assert _lines(body)[-1] == {
            TERMINAL_KEY: "failed",
            "records": 1,
            "error": "FORBIDDEN",
            "message": "nope",
        }

    async def test_a_spent_budget_is_reported_the_same_way(self) -> None:
        async def slow() -> AsyncIterator[Mapping[str, object]]:
            yield {"i": 0}
            await asyncio.sleep(10)
            yield {"i": 1}  # pragma: no cover - the budget expires first

        body = await _collect(
            stream_ndjson_export(slow, name="users-export", chunk_bytes=1, budget=0.05)
        )

        lines = _lines(body)
        assert lines[0] == {"i": 0}
        assert lines[-1][TERMINAL_KEY] == "failed"
        assert lines[-1]["error"] == "DEADLINE_EXCEEDED"
        assert lines[-1]["records"] == 1

    async def test_the_count_describes_the_body_above_it(self) -> None:
        body = await _collect(
            stream_ndjson_export(
                lambda: _records(50, fail_after=37), name="t", chunk_bytes=1
            )
        )

        lines = _lines(body)
        assert lines[-1]["records"] == len(lines) - 1

    async def test_a_record_that_cannot_be_encoded_fails_the_stream(self) -> None:
        """Not a truncation: a bad value is a `failed` terminal record too."""

        async def unencodable() -> AsyncIterator[Mapping[str, object]]:
            yield {"amount": float("nan")}

        body = await _collect(stream_ndjson_export(unencodable, name="t"))

        assert _lines(body) == [
            {
                TERMINAL_KEY: "failed",
                "records": 0,
                "error": "INTERNAL_ERROR",
                "message": "The export stopped before it finished.",
            }
        ]


class TestResources:
    async def test_stopping_early_releases_the_source(self) -> None:
        released = asyncio.Event()

        async def endless() -> AsyncIterator[Mapping[str, object]]:
            try:
                i = 0
                while True:
                    yield {"i": i}
                    i += 1
            finally:
                released.set()

        async with aclosing(
            stream_ndjson_export(endless, name="t", chunk_bytes=1)
        ) as stream:
            async for _ in stream:
                break

        assert released.is_set()
        assert not [
            task for task in asyncio.all_tasks() if "readahead" in task.get_name()
        ]

    @pytest.mark.parametrize("readahead", [1, 2, 8])
    async def test_the_result_does_not_depend_on_the_readahead(
        self, readahead: int
    ) -> None:
        body = await _collect(
            stream_ndjson_export(
                lambda: _records(20), name="t", chunk_bytes=1, readahead=readahead
            )
        )

        assert _lines(body)[-1] == {TERMINAL_KEY: "complete", "records": 20}
