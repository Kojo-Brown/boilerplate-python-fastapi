"""The `If-Match` precondition, parsed and compared the way RFC 9110 says.

The grammar and `EntityTag` itself live in `src/concurrency/tags.py`, shared
with `If-None-Match`. What is specific to this field is the comparison it uses
and what a failure means:

**`If-Match` uses the strong comparison function** (RFC 9110 §13.1.1). A weak
tag — `W/"7"` — never satisfies it, even against `"7"`. Weakness is a claim
that two representations are *semantically* equivalent, which is a useful thing
to say about a cached copy and a useless thing to say about a row you are about
to overwrite: "equivalent enough to read" is not "unchanged since I read it".
`If-None-Match` is the field for which the other answer is right.

**A malformed `If-Match` is a 400, not a shrug.** The obvious alternative is to
ignore a header we cannot parse, which turns the precondition off at exactly
the moment the client believed it was on, and turns a lost update into a
success. Failing loudly costs a client one visible bug; ignoring it costs
someone else's edit.

    If-Match = "*" / #entity-tag
"""

from __future__ import annotations

from dataclasses import dataclass

from src.concurrency.tags import (
    OWS,
    EntityTag,
    MalformedPreconditionError,
    parse_entity_tag_list,
)
from src.exceptions import PreconditionFailedError, PreconditionRequiredError


def resource_version_tag(resource_id: object, version: int) -> EntityTag:
    """Build the strong tag for a versioned row.

    The identifier is folded in alongside the version on purpose. Without it,
    the tag for `/api/v1/users/me` is just a small integer, and every user's
    row is at version 1 the moment it is created — so a tag one client obtained
    would compare equal to a completely different row at the same version. That
    matters for exactly one resource shape, but it is the shape this API has:
    `/me` is a different resource per bearer token behind a single URI, which
    is also why those responses are marked `Cache-Control: private, no-store`.

    Values are opaque to clients, so nothing depends on the format, and the id
    is already in the body of any response that carries the tag.
    """
    return EntityTag(f"{resource_id}.{version}")


@dataclass(frozen=True, slots=True)
class IfMatch:
    """A parsed `If-Match` header, including the case where there wasn't one.

    Absence is represented rather than signalled with `None` so that a route
    can state its policy in one call — `require_match` — instead of testing for
    `None` first and then evaluating, which is the shape that eventually grows
    a path where a missing header means "no precondition to check, carry on".
    """

    present: bool
    wildcard: bool = False
    tags: tuple[EntityTag, ...] = ()

    @classmethod
    def absent(cls) -> IfMatch:
        return cls(present=False)

    @classmethod
    def parse(cls, raw: str | None) -> IfMatch:
        """Parse a header value, or `None` for a request that omitted it.

        Repeated `If-Match` field lines should be joined with commas by the
        caller before they get here (RFC 9110 §5.3); `get_if_match` in
        `src/dependencies.py` does that.
        """
        if raw is None:
            return cls.absent()

        if raw.strip(OWS) == "*":
            return cls(present=True, wildcard=True)

        # Passed unstripped: the scanner already skips OWS at both ends of
        # every element, and trimming here first would mean two places
        # deciding what whitespace is allowed where.
        tags = parse_entity_tag_list(raw, field="If-Match")
        if not tags:
            # Syntactically a list, semantically nothing: `If-Match: ,` asks
            # for the update to succeed if the row matches none of no tags,
            # which cannot be satisfied and is far more likely a client bug.
            raise MalformedPreconditionError(
                "If-Match must be '*' or a non-empty list of entity tags"
            )
        return cls(present=True, tags=tags)

    def matches(self, current: EntityTag) -> bool:
        """Whether this precondition is satisfied by the current tag.

        A wildcard matches any tag: `If-Match: *` asks only that the resource
        exist, and having a current tag to compare against means it does.
        """
        if self.wildcard:
            return True
        return any(tag.strongly_matches(current) for tag in self.tags)

    def require_match(self, current: EntityTag) -> None:
        """Enforce the precondition, or raise the status that describes why not.

        - Header absent → 428, per RFC 6585: the request would otherwise be a
          blind overwrite, and 428 is the response that tells the client to
          retry it conditionally rather than leaving it to guess.
        - Header present, nothing matches → 412, carrying the current `ETag` so
          a client that wants to re-read, merge and retry has the tag already.
        """
        if not self.present:
            raise PreconditionRequiredError(
                "This request must be made conditional with an If-Match header "
                "carrying the ETag of the version you are updating"
            )
        if not self.matches(current):
            raise PreconditionFailedError(
                "The resource has changed since the version your If-Match refers to",
                headers={"ETag": current.serialize()},
            )
