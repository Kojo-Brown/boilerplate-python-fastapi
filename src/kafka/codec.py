"""JSON on the wire, with the two refusals that matter.

Kafka carries bytes and has no opinion about them, so every deployment picks an
encoding. This is the one this codebase uses for its own topics; a topic owned
by somebody else is read with whatever that owner chose, which is why the
protocols in `base.py` speak `bytes` and this module is a convenience rather
than a layer.

**`NaN` and `Infinity` are refused.** `json.dumps` emits them by default and no
other language's parser accepts them, so a Python producer and a Java consumer
of the same topic disagree about whether the topic is valid JSON — and the
disagreement surfaces in the consumer's log, days later, as a parse error on a
record whose producer is long gone.

**Decoding refuses a tombstone rather than returning `None`.** A null value is
a delete instruction on a compacted topic; handing back `None` from a decoder
whose return type is a mapping would push that distinction into every handler's
`if`, and the ones that forget would treat a delete as an empty update.
"""

from __future__ import annotations

import json
from typing import Any

from src.kafka.base import (
    ConsumedMessage,
    MessageNotDecodableError,
    MessageNotSerializableError,
)

#: Compact separators: a topic is storage, and the space after every comma is
#: paid for on every replica of every record.
_SEPARATORS = (",", ":")


def encode_json(payload: object) -> bytes:
    """Serialise `payload` to the bytes of a record value.

    `sort_keys` so that two encodings of the same mapping are the same bytes,
    which is what makes a record comparable in a test and deduplicable
    downstream without parsing it.
    """
    try:
        text = json.dumps(
            payload,
            allow_nan=False,
            sort_keys=True,
            separators=_SEPARATORS,
            ensure_ascii=False,
        )
    except (TypeError, ValueError) as exc:
        raise MessageNotSerializableError(
            f"Payload is not JSON-encodable: {exc}"
        ) from exc
    return text.encode("utf-8")


def decode_json(message: ConsumedMessage) -> Any:
    """Parse a record's value, or raise `MessageNotDecodableError`.

    Takes the message rather than the bytes so the error can name the record.
    An error that says "invalid JSON" without a topic, partition and offset
    cannot be acted on: nobody can go and look at the record it means.
    """
    if message.value is None:
        raise MessageNotDecodableError(
            f"Record {message.partition}@{message.offset} is a tombstone "
            "and has no JSON body."
        )
    try:
        return json.loads(message.value.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise MessageNotDecodableError(
            f"Record {message.partition}@{message.offset} is not UTF-8: {exc}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise MessageNotDecodableError(
            f"Record {message.partition}@{message.offset} is not valid JSON: {exc}"
        ) from exc


__all__ = ["decode_json", "encode_json"]
