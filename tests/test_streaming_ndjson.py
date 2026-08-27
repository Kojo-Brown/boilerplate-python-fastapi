"""NDJSON encoding, chunking, and the terminal record that ends every stream."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping

import pytest

from src.exceptions import NotFoundError
from src.streaming.ndjson import (
    DEFAULT_CHUNK_BYTES,
    TERMINAL_KEY,
    RecordCount,
    chunk_records,
    completion_record,
    encode_line,
    failure_record,
)
from src.structured.errors import DeadlineExceeded
from src.users.export import UserExportRecord


async def _records(
    *records: Mapping[str, object],
) -> AsyncIterator[Mapping[str, object]]:
    for record in records:
        yield record


class TestEncodeLine:
    def test_one_compact_line_terminated_by_a_newline(self) -> None:
        line = encode_line({"a": 1, "b": "two"})

        assert line == b'{"a":1,"b":"two"}\n'

    def test_non_ascii_is_written_as_utf8_rather_than_escaped(self) -> None:
        """An export of a real user table is mostly not ASCII."""
        line = encode_line({"name": "Zoë"})

        assert line.decode() == '{"name":"Zoë"}\n'
        assert b"\\u" not in line

    @pytest.mark.parametrize("separator", ["\u2028", "\u2029"])
    def test_unicode_line_separators_are_escaped(self, separator: str) -> None:
        """`str.splitlines()` breaks on these; `\\n` framing must not."""
        line = encode_line({"name": f"a{separator}b"})

        assert separator not in line.decode()
        assert len(line.decode().splitlines()) == 1
        assert json.loads(line)["name"] == f"a{separator}b"

    def test_a_newline_inside_a_value_cannot_break_the_framing(self) -> None:
        line = encode_line({"name": "a\nb"})

        assert line.count(b"\n") == 1
        assert json.loads(line)["name"] == "a\nb"

    @pytest.mark.parametrize("value", [float("nan"), float("inf")])
    def test_a_non_finite_float_is_refused_rather_than_emitted(
        self, value: float
    ) -> None:
        """`NaN` and `Infinity` are Python's invention, not JSON's."""
        with pytest.raises(ValueError):
            encode_line({"amount": value})


class TestChunking:
    async def test_lines_are_coalesced_up_to_the_threshold(self) -> None:
        counter = RecordCount()
        source = _records(*({"i": i} for i in range(10)))

        chunks = [
            chunk
            async for chunk in chunk_records(source, chunk_bytes=1024, counter=counter)
        ]

        assert len(chunks) == 1
        assert chunks[0].count(b"\n") == 10
        assert counter.emitted == 10

    async def test_a_chunk_is_flushed_once_it_passes_the_threshold(self) -> None:
        counter = RecordCount()
        source = _records(*({"i": i} for i in range(10)))

        chunks = [
            chunk
            async for chunk in chunk_records(source, chunk_bytes=1, counter=counter)
        ]

        assert len(chunks) == 10
        assert counter.emitted == 10

    async def test_a_chunk_never_splits_a_line(self) -> None:
        counter = RecordCount()
        source = _records(*({"i": i, "pad": "x" * 40} for i in range(20)))

        async for chunk in chunk_records(source, chunk_bytes=64, counter=counter):
            assert chunk.endswith(b"\n")
            for line in chunk.splitlines():
                json.loads(line)

    async def test_an_empty_source_produces_no_chunks(self) -> None:
        counter = RecordCount()

        assert [
            chunk
            async for chunk in chunk_records(
                _records(), chunk_bytes=64, counter=counter
            )
        ] == []
        assert counter.emitted == 0

    async def test_the_count_only_covers_records_that_reached_a_chunk(self) -> None:
        """The terminal record's `records` must describe the body above it."""
        counter = RecordCount()
        source = _records(*({"i": i} for i in range(100)))

        async for _ in chunk_records(source, chunk_bytes=1, counter=counter):
            if counter.emitted == 3:
                break

        assert counter.emitted == 3

    async def test_a_failing_source_flushes_what_it_had_buffered(self) -> None:
        """Records that were read and encoded must not be thrown away."""
        counter = RecordCount()

        async def failing() -> AsyncIterator[Mapping[str, object]]:
            yield {"i": 0}
            yield {"i": 1}
            raise RuntimeError("the cursor died")

        chunks: list[bytes] = []
        with pytest.raises(RuntimeError):
            async for chunk in chunk_records(
                failing(), chunk_bytes=65536, counter=counter
            ):
                chunks.append(chunk)

        assert b"".join(chunks).count(b"\n") == 2
        assert counter.emitted == 2

    async def test_a_chunk_size_below_one_is_refused(self) -> None:
        counter = RecordCount()

        with pytest.raises(ValueError, match="at least 1"):
            async for _ in chunk_records(_records(), chunk_bytes=0, counter=counter):
                pass  # pragma: no cover - the first __anext__ raises

    def test_the_default_chunk_is_64_kib(self) -> None:
        assert DEFAULT_CHUNK_BYTES == 65536


class TestTerminalRecords:
    def test_completion_states_how_many_records_went_out(self) -> None:
        assert completion_record(4200) == {TERMINAL_KEY: "complete", "records": 4200}

    def test_a_failure_carries_the_application_error_code(self) -> None:
        record = failure_record(41000, DeadlineExceeded("users-export", 300.0))

        assert record[TERMINAL_KEY] == "failed"
        assert record["records"] == 41000
        assert record["error"] == "DEADLINE_EXCEEDED"
        assert "users-export" in str(record["message"])

    def test_an_app_exceptions_own_message_is_used(self) -> None:
        record = failure_record(0, NotFoundError("No such export"))

        assert record["error"] == "NOT_FOUND"
        assert record["message"] == "No such export"

    def test_an_unexpected_failure_does_not_leak_its_message(self) -> None:
        """The response is already a 200 and the body is already public."""
        record = failure_record(7, RuntimeError("connection to 10.0.0.4 refused"))

        assert record["error"] == "INTERNAL_ERROR"
        assert "10.0.0.4" not in str(record["message"])

    def test_every_terminal_record_encodes(self) -> None:
        for record in (
            completion_record(1),
            failure_record(1, RuntimeError("x")),
            failure_record(1, NotFoundError()),
        ):
            assert json.loads(encode_line(record))[TERMINAL_KEY] in {
                "complete",
                "failed",
            }


class TestTheReservedKey:
    def test_no_export_schema_declares_it(self) -> None:
        """A data record that used `_export` would be read as the terminator."""
        assert TERMINAL_KEY not in UserExportRecord.model_fields
