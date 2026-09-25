"""The processor, and the one property that cannot be tested from inside it.

Redaction working is the easy half. The half worth the fixtures below is
*position*: `configure_logging` builds a chain with two sinks on it, and a
redactor that sits between the event and stdout while sitting behind the OTLP
forwarder produces a clean log and an exported original. Both `TestTheChain`
cases exist because that mistake is invisible in any test that only reads
stdout — which is every obvious test to write here.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import pytest
import structlog
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import (
    InMemoryLogRecordExporter,
    SimpleLogRecordProcessor,
)
from structlog.types import EventDict

from src.config import Settings
from src.logging_config import configure_logging
from src.observability.logs import log_forwarder
from src.redaction.keys import KeyPolicy
from src.redaction.processor import (
    FAILURE_KEY,
    STRUCTURAL_KEYS,
    RedactionProcessor,
    build_redaction_processor,
)
from src.redaction.values import REDACTED


def settings_with(**overrides: object) -> Settings:
    return Settings(
        DATABASE_URL="postgresql+asyncpg://fake:fake@localhost/fake",
        SECRET_KEY="mock-secret-key-not-a-real-one",
        **overrides,  # type: ignore[arg-type]
    )


def an_event(**fields: object) -> EventDict:
    event: EventDict = {
        "event": "something.happened",
        "level": "info",
        "timestamp": "2026-01-01T00:00:00Z",
    }
    event.update(fields)
    return event


@pytest.fixture
def processor() -> RedactionProcessor:
    return RedactionProcessor(KeyPolicy())


@pytest.fixture
def structlog_restored() -> Iterator[None]:
    """Put structlog's configuration back, so the chain built here is not the
    chain the rest of the suite runs under."""
    saved = structlog.get_config()
    try:
        yield
    finally:
        structlog.configure(**saved)


class TestTheEventDict:
    def test_a_sensitive_field_is_redacted(self, processor: RedactionProcessor) -> None:
        event = processor(None, "info", an_event(user_email="ada@example.com"))
        assert event["user_email"] == REDACTED

    def test_the_message_itself_is_scrubbed(
        self, processor: RedactionProcessor
    ) -> None:
        event = processor(None, "info", an_event(event="mailed ada@example.com"))
        assert event["event"] == f"mailed {REDACTED}"

    def test_structural_keys_are_left_alone(
        self, processor: RedactionProcessor
    ) -> None:
        # `exc_info` is the renderer's instruction, not a field. Rewriting it
        # would turn an exception into the string "True".
        event = processor(None, "error", an_event(exc_info=True))
        assert event["exc_info"] is True
        assert event["level"] == "info"
        assert event["timestamp"] == "2026-01-01T00:00:00Z"
        assert STRUCTURAL_KEYS == frozenset({"level", "timestamp", "exc_info"})

    def test_a_clean_event_comes_out_identical(
        self, processor: RedactionProcessor
    ) -> None:
        event = an_event(request_id="7f3a", status_code=200, path="/v1/users")
        assert processor(None, "info", dict(event)) == event


class TestFailingClosed:
    def test_a_redaction_error_drops_the_fields_rather_than_passing_them(
        self, processor: RedactionProcessor
    ) -> None:
        class Hostile(dict[str, str]):
            def items(self) -> Any:
                raise TypeError("nope")

        event = processor(
            None, "info", an_event(payload=Hostile(email="ada@example.com"))
        )
        assert "payload" not in event
        assert event[FAILURE_KEY] == "TypeError"
        assert event["event"] == "log.redaction_failed"
        # The structural keys survive, because they are what makes the dropped
        # line findable at all.
        assert event["level"] == "info"
        assert event["timestamp"] == "2026-01-01T00:00:00Z"


class TestConfiguration:
    def test_extra_keys_widen_the_policy(self) -> None:
        processor = build_redaction_processor(
            settings_with(LOG_REDACTION_EXTRA_KEYS="employeeNumber")
        )
        event = processor(None, "info", an_event(employee_number="E-1"))
        assert event["employee_number"] == REDACTED

    def test_the_default_policy_is_already_on(self) -> None:
        processor = build_redaction_processor(settings_with())
        event = processor(None, "info", an_event(password="hunter2"))
        assert event["password"] == REDACTED


class TestTheChain:
    def test_redaction_runs_before_the_otlp_forwarder(
        self, structlog_restored: None
    ) -> None:
        configure_logging("INFO")
        chain = structlog.get_config()["processors"]
        redactors = [
            index
            for index, step in enumerate(chain)
            if isinstance(step, RedactionProcessor)
        ]
        assert len(redactors) == 1
        assert redactors[0] < chain.index(log_forwarder)

    def test_the_exported_record_carries_the_redaction_not_the_original(
        self, structlog_restored: None, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # The whole point of the ordering above: stdout being clean proves
        # nothing about the copy that leaves the process over OTLP.
        exporter = InMemoryLogRecordExporter()  # type: ignore[no-untyped-call]
        provider = LoggerProvider()
        provider.add_log_record_processor(SimpleLogRecordProcessor(exporter))
        configure_logging("INFO")
        log_forwarder.bind(provider.get_logger(__name__))
        try:
            structlog.get_logger().info(
                "request.started", query="email=ada@example.com&page=2"
            )
        finally:
            log_forwarder.unbind()
            provider.shutdown()

        written = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
        assert written["query"] == f"email={REDACTED}&page=2"

        (record,) = exporter.get_finished_logs()
        attributes = record.log_record.attributes or {}
        assert attributes["query"] == f"email={REDACTED}&page=2"
        assert "ada@example.com" not in str(attributes)
