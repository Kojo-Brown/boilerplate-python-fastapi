"""Wire format for a stored record.

Kept out of `base.py` so the contract stays a contract, and out of
`redis_store.py` because any store that persists bytes — Redis today, a
Postgres table when the same key has to outlive a cache flush — needs exactly
this encoding. `memory.py` does not use it: holding frozen dataclasses in a
dict is already a faithful representation, and serialising them there would
test the codec instead of the store.

JSON rather than pickle: the payload is written by one process and read by
another, possibly running a different release, and a pickle in a shared cache
is a remote-code-execution primitive waiting for someone to gain write access
to Redis.
"""

from __future__ import annotations

import base64
import json
from typing import Any, Final

from src.idempotency.base import (
    IdempotencyRecord,
    IdempotencyStoreUnavailableError,
    StoredResponse,
)

# Bumped when the payload shape changes. A record written by an older release
# is discarded rather than misread — at worst one retry re-executes, which is
# the behaviour the caller already has to tolerate on a cache miss.
SCHEMA_VERSION: Final[int] = 1


def encode_record(record: IdempotencyRecord) -> bytes:
    """Serialise a record to the bytes a store persists."""
    payload: dict[str, Any] = {
        "v": SCHEMA_VERSION,
        "fingerprint": record.fingerprint,
        "response": None,
    }
    if record.response is not None:
        payload["response"] = {
            "status_code": record.response.status_code,
            "headers": [list(pair) for pair in record.response.headers],
            # Response bodies are arbitrary bytes — an image, a gzip stream —
            # and JSON only carries text, so base64 rather than a decode that
            # would fail on the first non-UTF-8 byte.
            "body": base64.b64encode(record.response.body).decode("ascii"),
        }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def decode_record(raw: bytes) -> IdempotencyRecord | None:
    """Parse bytes written by `encode_record`.

    Returns `None` for a payload from a different schema version, which the
    caller treats as a miss. Raises `IdempotencyStoreUnavailableError` for
    anything unparseable: that is not an old record, it is something else
    writing into this namespace, and quietly re-executing the request would
    hide a misconfiguration that affects every key at once.
    """
    try:
        payload = json.loads(raw)
    except (ValueError, UnicodeDecodeError) as exc:
        raise IdempotencyStoreUnavailableError(
            "Idempotency record is not valid JSON."
        ) from exc

    if not isinstance(payload, dict):
        raise IdempotencyStoreUnavailableError(
            "Idempotency record is not an object.",
        )

    if payload.get("v") != SCHEMA_VERSION:
        return None

    try:
        fingerprint = str(payload["fingerprint"])
        raw_response = payload["response"]
        response: StoredResponse | None = None
        if raw_response is not None:
            response = StoredResponse(
                status_code=int(raw_response["status_code"]),
                headers=tuple(
                    (str(name), str(value)) for name, value in raw_response["headers"]
                ),
                body=base64.b64decode(raw_response["body"]),
            )
    except (KeyError, TypeError, ValueError) as exc:
        raise IdempotencyStoreUnavailableError(
            "Idempotency record is missing or malformed fields.",
        ) from exc

    return IdempotencyRecord(fingerprint=fingerprint, response=response)
