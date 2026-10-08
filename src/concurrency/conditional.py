"""`If-None-Match` and the conditional GET, per RFC 9110 §13.1.2.

The companion to `If-Match` in `src/concurrency/etag.py`, and the asymmetry
between them is the whole content of this module:

**`If-None-Match` uses the weak comparison function** (§8.8.3.2). `If-Match`
guards a write, where "semantically equivalent" is not good enough to overwrite
a row on. This field guards a *read*, and semantic equivalence is exactly the
question a cache is asking: may I keep what I have? So `W/"7"` and `"7"` name
the same representation here, and the weak flag is ignored on both sides.

The grammar is the one `If-Match` uses, so the scanner is shared — a comma is a
legal `etagc`, which is why neither field may be split on commas.
"""

from __future__ import annotations

from dataclasses import dataclass

from src.concurrency.etag import EntityTag, MalformedPreconditionError, _parse_tag_list

_OWS = " \t"


@dataclass(frozen=True, slots=True)
class IfNoneMatch:
    """A parsed `If-None-Match` header, including the case where there wasn't one.

    Absence is represented rather than signalled with `None`, for the same
    reason `IfMatch` does it: a caller testing for `None` first is the shape
    that eventually grows a branch where a missing header means "no
    precondition, carry on" — which is the correct reading here, but only
    because it is stated once rather than re-derived at each call site.
    """

    present: bool
    wildcard: bool = False
    tags: tuple[EntityTag, ...] = ()

    @classmethod
    def absent(cls) -> IfNoneMatch:
        """The value for a request that carried no `If-None-Match`."""
        return cls(present=False)

    @classmethod
    def parse(cls, raw: str | None) -> IfNoneMatch:
        """Parse a header value, or `None` for a request that omitted it.

        Repeated field lines should be joined with commas by the caller before
        they get here (RFC 9110 §5.3).
        """
        if raw is None:
            return cls.absent()

        if raw.strip(_OWS) == "*":
            return cls(present=True, wildcard=True)

        tags = _parse_tag_list(raw)
        if not tags:
            raise MalformedPreconditionError(
                "If-None-Match must be '*' or a non-empty list of entity tags"
            )
        return cls(present=True, tags=tags)

    def matches(self, current: EntityTag) -> bool:
        """Whether this field names the current representation.

        A wildcard names any representation that exists, and having a current
        tag to compare against means this one does (§13.1.2).
        """
        if self.wildcard:
            return True
        return any(tag.weakly_matches(current) for tag in self.tags)
