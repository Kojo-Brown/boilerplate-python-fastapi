"""`If-None-Match` and the conditional GET, per RFC 9110 §13.1.2.

Not yet implemented — see `tests/test_conditional_get.py` for the cases this
module has to satisfy. The stubs exist so that the failing commit fails its
*tests* rather than the import, which keeps the step independently checkable.
"""

from __future__ import annotations

from dataclasses import dataclass

from src.concurrency.etag import EntityTag


@dataclass(frozen=True, slots=True)
class IfNoneMatch:
    """A parsed `If-None-Match` header, including the case where there wasn't one."""

    present: bool
    wildcard: bool = False
    tags: tuple[EntityTag, ...] = ()

    @classmethod
    def absent(cls) -> IfNoneMatch:
        """The value for a request that carried no `If-None-Match`."""
        raise NotImplementedError

    @classmethod
    def parse(cls, raw: str | None) -> IfNoneMatch:
        """Parse a header value, or `None` for a request that omitted it."""
        raise NotImplementedError

    def matches(self, current: EntityTag) -> bool:
        """Whether this field names the current representation."""
        raise NotImplementedError
