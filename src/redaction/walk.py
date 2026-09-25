"""Walking a log event, conservatively.

The realistic leak is not `logger.info("x", password=secret)` — that one is
visible in review. It is `logger.error("x", error=exc, request=payload)`, where
the interesting value is three levels down inside something nobody wrote out by
hand. So the walk descends into structures rather than only looking at the top
level.

What it does **not** do is rewrite the log. Every rule here is chosen so that a
line with nothing sensitive in it comes out byte-identical to the line this
service writes today:

*Containers* — mappings, and sequences and sets that are not strings — are
rebuilt with redacted members, keeping their type where the type is one the
renderer already handles.

*Scalars that are not strings* — `int`, `float`, `bool`, `None` — are returned
untouched. There is no shape to check on a number and no key-independent way to
tell a user id from an account number, so numbers are covered by their field
name or not at all. A `_PAN` detector that ran on integers would be redacting
the `size`, `offset` and `status_code` fields this service is read through.

*Anything else* — a `UUID`, a `datetime`, an exception, an ORM object, a
connection pool — is left as the object it was, unless the string it renders to
trips a detector, in which case the redacted string replaces it. The renderer
is going to call `str()` on it a few processors later regardless, so this costs
nothing that was not already going to be paid, and it closes the one hole that
would otherwise be wide open: `error=exc` is the single most common way a
secret reaches this codebase's logs, and an exception is not a `str`.

Three bounds keep a pathological value from turning one log line into an
incident of its own. `MAX_DEPTH` stops the descent; a cycle is caught against
the *ancestors* of the current value rather than everything already seen, so a
list that appears twice under different keys still renders both times instead of
the second one being falsely called circular; and `str()` on a foreign object is
wrapped, because `__str__` is other people's code running inside a log call.

The whole thing fails **closed**. If redaction raises, the caller replaces the
event's fields rather than passing the originals through — see
`src/redaction/processor.py`. A redactor whose error path emits the unredacted
dict is a redactor that stops working on precisely the malformed input that was
worth looking at.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence, Set
from typing import Final

from src.redaction.keys import KeyPolicy
from src.redaction.values import REDACTED, ValuePolicy

#: How far down the walk goes before it stops describing and starts marking.
#: Deeper than any log this service emits, shallow enough that a self-similar
#: structure cannot exhaust the stack.
MAX_DEPTH: Final[int] = 12

#: Stand-ins that say why a value is missing, so that a reader can tell a
#: redaction from a truncation from a cycle.
TOO_DEEP: Final[str] = "[max-depth]"
CIRCULAR: Final[str] = "[circular]"
UNRENDERABLE: Final[str] = "[unrenderable]"


class Redactor:
    """Key policy plus value policy, applied to a whole value graph."""

    def __init__(self, keys: KeyPolicy) -> None:
        self._keys = keys
        self._values = ValuePolicy(keys)

    def field(self, key: str, value: object) -> object:
        """One top-level field of a log event, redacted.

        A sensitive *name* redacts the whole value without looking at it. That
        is deliberate: `password={"old": ..., "new": ...}` is sensitive in every
        member, and a walk that descended would have to get every one of them
        right to arrive at the answer the name already gave.
        """
        if self._keys.is_sensitive(key):
            return REDACTED
        return self._value(value, depth=0, ancestors=())

    def _value(self, value: object, depth: int, ancestors: tuple[int, ...]) -> object:
        if isinstance(value, str):
            return self._values.scrub(value)
        if value is None or isinstance(value, bool | int | float):
            return value
        if isinstance(value, Mapping | Sequence | Set) and not isinstance(
            value, str | bytes | bytearray
        ):
            return self._container(value, depth, ancestors)
        return self._foreign(value)

    def _container(
        self,
        value: Mapping[object, object] | Sequence[object] | Set[object],
        depth: int,
        ancestors: tuple[int, ...],
    ) -> object:
        if depth >= MAX_DEPTH:
            return TOO_DEEP
        if id(value) in ancestors:
            return CIRCULAR
        inside = (*ancestors, id(value))
        if isinstance(value, Mapping):
            return {
                key: (
                    REDACTED
                    if isinstance(key, str) and self._keys.is_sensitive(key)
                    else self._value(member, depth + 1, inside)
                )
                for key, member in value.items()
            }
        members = [self._value(member, depth + 1, inside) for member in value]
        if isinstance(value, tuple):
            return tuple(members)
        # Everything else — lists, and sets — comes back as a list. A set is not
        # rebuilt as a set on purpose: redaction collapses distinct members onto
        # the same marker, and a set would then swallow the duplicates, turning
        # "three tokens were logged here" into "one was".
        return members

    def _foreign(self, value: object) -> object:
        """A value that is neither a scalar nor a container we descend into.

        Returned unchanged unless rendering it trips a detector, which keeps
        every benign object — a `UUID`, a `datetime`, an `Enum` — rendering
        exactly as it does today.
        """
        try:
            rendered = str(value)
        except Exception:
            return UNRENDERABLE
        scrubbed = self._values.scrub(rendered)
        return value if scrubbed == rendered else scrubbed


__all__ = ["CIRCULAR", "MAX_DEPTH", "TOO_DEEP", "UNRENDERABLE", "Redactor"]
