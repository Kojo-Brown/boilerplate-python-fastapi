"""The entity-tag grammar, shared by every precondition that carries one.

This was part of `etag.py` while `If-Match` was the only field that needed it.
Two fields need it now — `If-Match` guards a write, `If-None-Match` guards a
read — and neither module is the right place for a grammar the other also
depends on: whichever one owned it would have a docstring describing its own
precondition and a scanner serving somebody else's.

The grammar (RFC 9110 §8.8.3, §13.1.1):

    entity-tag = [ weak ] opaque-tag
    weak       = %s"W/"
    opaque-tag = DQUOTE *etagc DQUOTE
    etagc      = %x21 / %x23-7E / obs-text

Note what `etagc` admits: a comma is `%x2C`, so `"a,b"` is one tag and not two,
and splitting either header on commas is wrong. Hence the scanner below rather
than `header.split(",")`.

§8.8.3.2 defines two comparison functions, and which one a field uses is a
property of the field rather than of the tag — see `strongly_matches` and
`weakly_matches`, and the two call sites in `etag.py` and `conditional.py`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from src.exceptions import BadRequestError

# `[ weak ] opaque-tag`, anchored by the caller with `.match(raw, pos)`.
_ENTITY_TAG = re.compile(r'(W/)?"([\x21\x23-\x7e\x80-\xff]*)"')

#: Optional whitespace, which the list grammar allows around every element.
OWS = " \t"


class MalformedPreconditionError(BadRequestError):
    """Raised when a precondition header does not parse.

    A `BadRequestError` subclass rather than its own status: the request is
    malformed, which is what 400 means. It carries a distinct `error_code` so a
    client can tell "your precondition is not a valid entity tag" apart from
    every other 400 this API can return, without parsing prose.
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
        """Render as it appears in an `ETag`, `If-Match` or `If-None-Match`."""
        return f'W/"{self.value}"' if self.weak else f'"{self.value}"'

    def strongly_matches(self, other: EntityTag) -> bool:
        """RFC 9110 §8.8.3.2 strong comparison: both strong, values equal."""
        return not self.weak and not other.weak and self.value == other.value

    def weakly_matches(self, other: EntityTag) -> bool:
        """RFC 9110 §8.8.3.2 weak comparison: values equal, weakness ignored.

        Used by `If-None-Match` and not by `If-Match` — see
        `src/concurrency/conditional.py` for why the distinction matters.
        """
        return self.value == other.value


def parse_entity_tag_list(raw: str, *, field: str) -> tuple[EntityTag, ...]:
    """Scan `#entity-tag`, raising `MalformedPreconditionError` on anything else.

    `field` is the header name the message should blame, and it is required: a
    400 that names the wrong header sends a client to read the wrong half of
    its own code, and a default here is how one module's name ends up on the
    other's errors.

    Empty list elements are skipped rather than rejected: RFC 9110 §5.6.1.2
    requires recipients to tolerate them, and they come from clients that build
    the header by joining a list that had a hole in it.
    """
    tags: list[EntityTag] = []
    pos = 0
    length = len(raw)

    while pos < length:
        while pos < length and raw[pos] in OWS:
            pos += 1
        if pos < length and raw[pos] == ",":
            pos += 1
            continue
        if pos >= length:
            break

        match = _ENTITY_TAG.match(raw, pos)
        if match is None:
            raise MalformedPreconditionError(
                f"{field} is not a valid entity-tag list at offset {pos}: {raw!r}"
            )
        tags.append(EntityTag(match.group(2), weak=match.group(1) is not None))
        pos = match.end()

        while pos < length and raw[pos] in OWS:
            pos += 1
        if pos < length:
            if raw[pos] != ",":
                raise MalformedPreconditionError(
                    f"{field} entity tags must be comma-separated: {raw!r}"
                )
            pos += 1

    return tuple(tags)
