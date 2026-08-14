"""Entity tags and `If-Match`, parsed and compared the way RFC 9110 says.

Two rules here are easy to get wrong and expensive to get wrong, so they are
stated once, in code, rather than left to each route:

**`If-Match` uses the strong comparison function** (RFC 9110 §13.1.1). A weak
tag — `W/"7"` — never satisfies it, even against `"7"`. Weakness is a claim
that two representations are *semantically* equivalent, which is a useful thing
to say about a cached copy and a useless thing to say about a row you are about
to overwrite: "equivalent enough to read" is not "unchanged since I read it".

**A malformed `If-Match` is a 400, not a shrug.** The obvious alternative is to
ignore a header we cannot parse, which turns the precondition off at exactly
the moment the client believed it was on, and turns a lost update into a
success. Failing loudly costs a client one visible bug; ignoring it costs
someone else's edit.

The grammar (§8.8.3, §13.1.1):

    If-Match   = "*" / #entity-tag
    entity-tag = [ weak ] opaque-tag
    weak       = %s"W/"
    opaque-tag = DQUOTE *etagc DQUOTE
    etagc      = %x21 / %x23-7E / obs-text

Note what `etagc` admits: a comma is `%x2C`, so `If-Match: "a,b"` is one tag
and not two, and splitting the header on commas is wrong. Hence the scanner
below rather than `header.split(",")`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from src.exceptions import (
    BadRequestError,
    PreconditionFailedError,
    PreconditionRequiredError,
)

# `[ weak ] opaque-tag`, anchored by the caller with `.match(raw, pos)`.
_ENTITY_TAG = re.compile(r'(W/)?"([\x21\x23-\x7e\x80-\xff]*)"')
_OWS = " \t"


class MalformedPreconditionError(BadRequestError):
    """Raised when a precondition header does not parse.

    A `BadRequestError` subclass rather than its own status: the request is
    malformed, which is what 400 means. It carries a distinct `error_code` so a
    client can tell "your If-Match is not a valid entity tag" apart from every
    other 400 this API can return, without parsing prose.
    """

    error_code = "MALFORMED_PRECONDITION"


@dataclass(frozen=True, slots=True)
class EntityTag:
    """One entity tag: an opaque string plus the weak/strong distinction.

    `value` is the *unquoted* opaque part. Constructing one with a value
    containing a character `etagc` forbids raises, because the alternative is
    emitting a header no conforming client can parse — and the caller who chose
    the value is the only one who can fix it.
    """

    value: str
    weak: bool = False

    def __post_init__(self) -> None:
        if not _ENTITY_TAG.fullmatch(f'"{self.value}"'):
            raise ValueError(
                f"entity-tag value contains characters RFC 9110 forbids: {self.value!r}"
            )

    def serialize(self) -> str:
        """Render as it appears in an `ETag` or `If-Match` header."""
        return f'W/"{self.value}"' if self.weak else f'"{self.value}"'

    def strongly_matches(self, other: EntityTag) -> bool:
        """RFC 9110 §8.8.3.2 strong comparison: both strong, values equal."""
        return not self.weak and not other.weak and self.value == other.value


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

        if raw.strip(_OWS) == "*":
            return cls(present=True, wildcard=True)

        # Passed unstripped: the scanner already skips OWS at both ends of
        # every element, and trimming here first would mean two places
        # deciding what whitespace is allowed where.
        tags = _parse_tag_list(raw)
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


def _parse_tag_list(raw: str) -> tuple[EntityTag, ...]:
    """Scan `#entity-tag`, raising `MalformedPreconditionError` on anything else.

    Empty list elements are skipped rather than rejected: RFC 9110 §5.6.1.2
    requires recipients to tolerate them, and they come from clients that build
    the header by joining a list that had a hole in it.
    """
    tags: list[EntityTag] = []
    pos = 0
    length = len(raw)

    while pos < length:
        while pos < length and raw[pos] in _OWS:
            pos += 1
        if pos < length and raw[pos] == ",":
            pos += 1
            continue
        if pos >= length:
            break

        match = _ENTITY_TAG.match(raw, pos)
        if match is None:
            raise MalformedPreconditionError(
                f"If-Match is not a valid entity-tag list at offset {pos}: {raw!r}"
            )
        tags.append(EntityTag(match.group(2), weak=match.group(1) is not None))
        pos = match.end()

        while pos < length and raw[pos] in _OWS:
            pos += 1
        if pos < length:
            if raw[pos] != ",":
                raise MalformedPreconditionError(
                    f"If-Match entity tags must be comma-separated: {raw!r}"
                )
            pos += 1

    return tuple(tags)
