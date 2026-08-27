"""`/api/v1/exports` — bulk reads that never exist in memory all at once.

Admin-only, and not as a formality: this endpoint returns every account in the
system in one request, which is a different thing from the ability to read any
one of them. `require_role("admin")` resolves before the response starts, so an
unauthorised caller gets an ordinary 403 envelope rather than a 200 whose body
turns out to be an error — the ordering that everything else about this route is
arranged to preserve.

Nothing here decides what a user record contains (`src/users/export.py`), how
bytes are framed (`src/streaming/ndjson.py`), or how far the producer may run
ahead (`src/streaming/backpressure.py`). The handler's whole job is to build
the record source and hand it over, which is why it fits on a screen: a route
that assembled the pipeline inline would be the only place any of those
decisions were written down.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from typing import Annotated

from fastapi import APIRouter, Depends, Query

from src.auth.dependencies import require_role
from src.config import settings
from src.dependencies import UserExportSourceDep
from src.models.user import User
from src.streaming.export import stream_ndjson_export
from src.streaming.response import NDJSON_MEDIA_TYPE, NDJSONStreamingResponse
from src.users.export import UserExportSource

router = APIRouter(prefix="/exports", tags=["exports"])

_STREAM_NAME = "users-export"

_EXPORT_DESCRIPTION = """
Streams every user account as newline-delimited JSON (`application/x-ndjson`),
one record per line, in ascending primary-key order.

The response is chunked and has no `Content-Length`: the size is not known
until the last row is read. **The last line of a complete stream is always a
terminal record** — `{"_export": "complete", "records": N}` — and a stream
whose last line is not an `_export` record was truncated, whatever the HTTP
status said. A failure after the first byte cannot change the status code, so
it arrives as `{"_export": "failed", "records": N, "error": "..."}` instead,
carrying the same error codes as this API's JSON error envelope.
"""


async def _records(
    source: UserExportSource,
    *,
    active_only: bool,
) -> AsyncIterator[Mapping[str, object]]:
    """Adapt export records to the mappings the NDJSON encoder takes.

    `mode="json"` is what turns the UUID and the two timestamps into strings a
    JSON encoder accepts; without it the first record raises inside the
    producer and the export's only output is a `failed` terminal record.
    """
    async for record in source.stream_export(
        batch_size=settings.EXPORT_BATCH_ROWS,
        active_only=active_only,
    ):
        yield record.model_dump(mode="json")


@router.get(
    "/users",
    response_class=NDJSONStreamingResponse,
    summary="Stream every user account as NDJSON",
    description=_EXPORT_DESCRIPTION,
    responses={
        200: {
            "content": {NDJSON_MEDIA_TYPE: {}},
            "description": (
                "A chunked NDJSON stream ending in an `_export` terminal record."
            ),
        },
        403: {"description": "The caller is not an administrator."},
    },
)
async def export_users(
    source: UserExportSourceDep,
    _admin: Annotated[User, Depends(require_role("admin"))],
    active_only: Annotated[
        bool,
        Query(description="Skip deactivated accounts."),
    ] = False,
) -> NDJSONStreamingResponse:
    """Stream the user table.

    The source is a factory rather than an already-started iterator, because
    the producer task in `with_readahead` is what has to create it: an iterator
    built here and never started would be finalized by the garbage collector,
    and one built here and started in another task would open its cursor
    outside the scope that owns it.
    """

    def records() -> AsyncIterator[Mapping[str, object]]:
        return _records(source, active_only=active_only)

    return NDJSONStreamingResponse(
        stream_ndjson_export(
            records,
            name=_STREAM_NAME,
            chunk_bytes=settings.EXPORT_CHUNK_BYTES,
            readahead=settings.EXPORT_READAHEAD_CHUNKS,
            budget=settings.EXPORT_DEADLINE_SECONDS,
        ),
        filename="users.ndjson",
    )
