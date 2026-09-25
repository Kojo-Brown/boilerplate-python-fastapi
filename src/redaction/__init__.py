"""PII redaction for the structured log pipeline.

Three pieces, in the order an event meets them:

`keys`
    Which field *names* are sensitive, matched on contiguous word runs.

`values`
    Which *strings* are sensitive, by shape, with a real check behind every
    detector.

`walk`
    How far into a value the first two are applied, and what happens to the
    things that are neither a scalar nor a mapping.

`processor` binds them into the structlog processor that `configure_logging`
installs, and `docs/pii-redaction.md` is the prose version of all of it.

Redaction is the last line and not a licence: a passport number typed into a
free-text `note` field passes straight through, because nothing about it is
checkable. Do not log what you do not need.
"""

from src.redaction.keys import SENSITIVE_PHRASES, KeyPolicy, words
from src.redaction.processor import (
    FAILURE_KEY,
    STRUCTURAL_KEYS,
    RedactionProcessor,
    build_redaction_processor,
)
from src.redaction.values import REDACTED, ValuePolicy
from src.redaction.walk import CIRCULAR, MAX_DEPTH, TOO_DEEP, UNRENDERABLE, Redactor

__all__ = [
    "CIRCULAR",
    "FAILURE_KEY",
    "MAX_DEPTH",
    "REDACTED",
    "SENSITIVE_PHRASES",
    "STRUCTURAL_KEYS",
    "TOO_DEEP",
    "UNRENDERABLE",
    "KeyPolicy",
    "RedactionProcessor",
    "Redactor",
    "ValuePolicy",
    "build_redaction_processor",
    "words",
]
