"""Optimistic concurrency control: entity tags and the `If-Match` precondition.

The problem this solves is the lost update. Two clients `GET` the same row,
both edit it, both `PATCH` it back; the second write silently overwrites the
first, and nothing in the exchange ever looked like an error. Optimistic
concurrency makes the second write *fail* instead: every response carries an
entity tag derived from the row's version, an unsafe request has to echo that
tag back in `If-Match`, and a tag that no longer describes the row is a 412.

The same tag answers the read's question. `IfNoneMatch` is the other half of
the protocol: a client that holds a tag asks "still current?" and gets a 304
with no body instead of a representation it already has. See
`docs/conditional-get.md`.

Nothing here knows about SQLAlchemy or about any particular model. `EntityTag`
and the two precondition types are the HTTP half; the storage half is
`User.__mapper_args__["version_id_col"]`, and `src/users/service.py` is where
the two meet. See `docs/optimistic-concurrency.md`.
"""

from src.concurrency.conditional import ConditionalOutcome, IfNoneMatch
from src.concurrency.etag import IfMatch, resource_version_tag
from src.concurrency.tags import EntityTag, MalformedPreconditionError

__all__ = [
    "ConditionalOutcome",
    "EntityTag",
    "IfMatch",
    "IfNoneMatch",
    "MalformedPreconditionError",
    "resource_version_tag",
]
