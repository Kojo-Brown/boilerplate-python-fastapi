"""Optimistic concurrency control: entity tags and the `If-Match` precondition.

The problem this solves is the lost update. Two clients `GET` the same row,
both edit it, both `PATCH` it back; the second write silently overwrites the
first, and nothing in the exchange ever looked like an error. Optimistic
concurrency makes the second write *fail* instead: every response carries an
entity tag derived from the row's version, an unsafe request has to echo that
tag back in `If-Match`, and a tag that no longer describes the row is a 412.

Nothing here knows about SQLAlchemy or about any particular model. `EntityTag`
and `IfMatch` are the HTTP half; the storage half is
`User.__mapper_args__["version_id_col"]`, and `src/users/service.py` is where
the two meet. See `docs/optimistic-concurrency.md`.
"""

from src.concurrency.etag import (
    EntityTag,
    IfMatch,
    MalformedPreconditionError,
    resource_version_tag,
)

__all__ = [
    "EntityTag",
    "IfMatch",
    "MalformedPreconditionError",
    "resource_version_tag",
]
