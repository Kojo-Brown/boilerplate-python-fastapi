"""The structlog processor, and where it has to sit.

**Position is the whole of the wiring decision.** `configure_logging` builds one
chain and it has two sinks on it, not one: the renderer that writes JSON to
stdout, and `log_forwarder`, which mirrors every event into the OpenTelemetry
logs pipeline (`src/observability/logs.py`). A redactor placed after the
forwarder cleans the copy you were already reading and ships the original to the
collector, which is the worst of the three possible orders — it looks like it
works, and the evidence that it does not is in a system nobody greps. So this
processor goes immediately *before* `structlog_processors()`.

It also goes after `structlog.contextvars.merge_contextvars`, because the
request-scoped fields bound in `src/middleware/request_id.py` are merged there
and a processor upstream of the merge would never see them. `query=` is one of
those, and a raw query string is the most reliably sensitive thing this service
logs.

Two keys are skipped. `exc_info` is not a field, it is the renderer's
instruction — and by the time it matters, the forwarder has turned it into an
`exception` on the OTLP record, which is downstream of every processor and out
of reach from here. `timestamp` and `level` are skipped because they are
structural and because scanning them is work with no possible finding.

**The failure path drops rather than passes.** If redaction raises — a mapping
whose keys are unhashable after a rebuild, a `__str__` that raises something
outside `Exception`, a value nobody anticipated — the event is replaced by its
structural keys plus `redaction_failed`, and every caller-supplied field is
gone. Letting the original through on error would mean the redactor stops
working on precisely the input strange enough to be worth hiding, and the line
would look completely ordinary.
"""

from __future__ import annotations

from typing import Final

from structlog.types import EventDict, WrappedLogger

from src.config import Settings
from src.redaction.keys import KeyPolicy
from src.redaction.walk import Redactor

#: Keys the processor leaves alone. See the module docstring.
STRUCTURAL_KEYS: Final[frozenset[str]] = frozenset({"level", "timestamp", "exc_info"})

#: Set on an event whose redaction raised. Alert on it: it means log lines are
#: being dropped, and it means the redactor has met something it cannot read.
FAILURE_KEY: Final[str] = "redaction_failed"


class RedactionProcessor:
    """structlog processor that redacts an event in place of the caller.

    A class rather than a closure so that `configure_logging` can build one from
    `Settings` and a test can build another from different settings without
    either reaching for a module-level global. The compiled key policy lives on
    the instance, which is also where its cache lives.
    """

    def __init__(self, keys: KeyPolicy) -> None:
        self._redactor = Redactor(keys)

    def __call__(
        self, _logger: WrappedLogger, _method_name: str, event_dict: EventDict
    ) -> EventDict:
        try:
            return {
                key: (
                    value
                    if key in STRUCTURAL_KEYS
                    else self._redactor.field(key, value)
                )
                for key, value in event_dict.items()
            }
        except Exception as exc:  # noqa: BLE001 - see the module docstring
            safe: EventDict = {
                key: value
                for key, value in event_dict.items()
                if key in STRUCTURAL_KEYS
            }
            safe["event"] = "log.redaction_failed"
            safe[FAILURE_KEY] = type(exc).__name__
            return safe


def build_redaction_processor(settings: Settings) -> RedactionProcessor:
    """The processor `configure_logging` installs, widened by configuration.

    `LOG_REDACTION_EXTRA_KEYS` can only add names. There is no setting that
    removes one and none that disables redaction, which is deliberate and is
    explained in `src/redaction/keys.py`.
    """
    return RedactionProcessor(
        KeyPolicy().widened_with(settings.LOG_REDACTION_EXTRA_KEYS)
    )


__all__ = [
    "FAILURE_KEY",
    "STRUCTURAL_KEYS",
    "RedactionProcessor",
    "build_redaction_processor",
]
