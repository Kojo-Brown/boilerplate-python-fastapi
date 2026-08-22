"""An immutable mapping, and the reason one is needed at all.

Python's three immutability tools each guard exactly one edge, and the gap
between them is where this module lives.

`Final` stops *rebinding*. `CURRENCY_EXPONENTS: Final[dict[str, int]] = {...}`
makes `CURRENCY_EXPONENTS = {}` a type error and leaves
`CURRENCY_EXPONENTS["JPY"] = 2` untouched — mypy accepts it, the interpreter
performs it, and every request served by that process afterwards converts yen
wrongly. A module-level constant is the widest-shared mutable state a process
has; `Final` alone does not make it a constant.

`@dataclass(frozen=True)` stops *attribute assignment*. It does not copy what
it is handed, so the caller keeps a reference to any container passed in:

    metadata = {"order": "1"}
    request = ChargeRequest(..., metadata=metadata)
    metadata["order"] = "2"        # request.metadata is now {"order": "2"}

and it does not stop `request.metadata["order"] = "2"` either. The dataclass is
frozen; the value object is not.

`frozen=True` on a Pydantic model has both of the same limits, plus one more of
its own: validation *replaces* the container, so a `dict[str, str]` field holds
a dict Pydantic built and mutation through it is unimpeded.

`FrozenDict` closes all three by being a mapping that copies on construction
and exposes no mutator. Because it is also hashable and picklable, one type
serves every context this codebase needs — a module-level constant, a frozen
dataclass field, and a Pydantic model field — instead of `MappingProxyType`
for the first (unhashable, and `pickle` refuses it outright) and something else
for the rest.

What it deliberately does not do is guarantee *deep* immutability. A
`FrozenDict[str, list[int]]` hands out the same lists to everyone; it is
`hash()` that will complain, and only when someone asks. Freezing a value
object means every field being immutable, which is a property of the whole
graph and not something one container type can enforce for you.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import TYPE_CHECKING, Any, cast, overload

if TYPE_CHECKING:  # pragma: no cover
    from pydantic import GetCoreSchemaHandler
    from pydantic_core import CoreSchema


class FrozenDict[K, V](Mapping[K, V]):
    """A hashable, picklable mapping with no mutators.

    Construction copies, so the mapping cannot be reached through whatever it
    was built from:

        >>> source = {"a": "1"}
        >>> frozen = FrozenDict(source)
        >>> source["a"] = "2"
        >>> frozen["a"]
        '1'

    Equality is `Mapping`'s, so a `FrozenDict` compares equal to a plain dict
    with the same pairs. That is what keeps it a drop-in for a constant nobody
    should be assigning to: existing lookups and comparisons are unchanged, and
    only the writes that were never meant to happen become errors.

    `__hash__` is computed on first use and cached. It raises `TypeError` if
    any *value* is unhashable — the same rule as a tuple, and for the same
    reason: a mapping of lists cannot promise its hash will not move.
    """

    __slots__ = ("_data", "_hash")

    _data: dict[K, V]
    _hash: int | None

    # Overloaded rather than declared once over the union, because mypy will
    # not push a *union* expected-type into a literal argument. Written as one
    # signature, `FrozenDict[str, PaymentStatus]({"COMPLETED": "succeeded"})`
    # infers the literal as `dict[str, str]` and then rejects it, which would
    # push every constant in `src/payments` towards a cast. The mapping
    # overload gives the literal a single concrete context, exactly as
    # typeshed does for `dict` itself.
    @overload
    def __init__(self, data: Mapping[K, V] = ..., /, **kwargs: V) -> None: ...

    @overload
    def __init__(self, data: Iterable[tuple[K, V]], /, **kwargs: V) -> None: ...

    def __init__(
        self, data: Mapping[K, V] | Iterable[tuple[K, V]] = (), /, **kwargs: V
    ) -> None:
        # `dict(...)` is the copy. Passing the caller's mapping straight through
        # would leave this object aliased to it, which is the whole failure this
        # class exists to prevent.
        merged = dict(data)
        if kwargs:
            merged.update(cast("Mapping[K, V]", kwargs))
        self._data = merged
        self._hash = None

    def __getitem__(self, key: K) -> V:
        return self._data[key]

    def __iter__(self) -> Iterator[K]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __hash__(self) -> int:
        if self._hash is None:
            # `frozenset` of the pairs rather than a tuple of them, so two
            # mappings built in different insertion orders hash alike — which
            # they must, since `Mapping.__eq__` already calls them equal.
            self._hash = hash(frozenset(self._data.items()))
        return self._hash

    def __or__(self, other: Mapping[K, V]) -> FrozenDict[K, V]:
        """Return a new mapping with `other`'s pairs applied over this one.

        The functional update. Without it the only way to derive a changed
        mapping is `dict(frozen) | changes`, which produces a mutable dict and
        quietly puts one back into whatever field it came from.
        """
        return FrozenDict({**self._data, **other})

    def __ror__(self, other: Mapping[K, V]) -> FrozenDict[K, V]:
        return FrozenDict({**other, **self._data})

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._data!r})"

    def __reduce__(self) -> tuple[Callable[..., FrozenDict[K, V]], tuple[dict[K, V]]]:
        # `__slots__` leaves no `__dict__` for pickle's default protocol to
        # copy, and the class takes a mapping positionally, so rebuilding it is
        # a one-argument call. This is what `MappingProxyType` cannot do, and
        # why it is not the type used for these fields: a `Notification` on a
        # Celery queue or a `ChargeRequest` crossing into a worker process has
        # to survive a round trip.
        return (type(self), (dict(self._data),))

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Teach Pydantic to validate into a `FrozenDict` and dump out of one.

        Imported lazily so that this module stays free of Pydantic: it is used
        by `src/payments/base.py` and `src/notifications/base.py`, which are
        contract modules that deliberately import nothing from the web layer.

        The inner schema is a `dict` schema over the parameters, so validation,
        coercion and the generated OpenAPI document are all Pydantic's own —
        only the final construction and the serialisation are ours.
        """
        from pydantic_core import core_schema

        args: tuple[Any, ...] = getattr(source_type, "__args__", ())
        if len(args) == 2:
            inner = core_schema.dict_schema(
                handler.generate_schema(args[0]), handler.generate_schema(args[1])
            )
        else:
            inner = core_schema.dict_schema()

        return core_schema.no_info_after_validator_function(
            cls,
            inner,
            serialization=core_schema.plain_serializer_function_ser_schema(
                dict, return_schema=inner, when_used="always"
            ),
        )


def freeze_mapping[K, V](value: Mapping[K, V]) -> FrozenDict[K, V]:
    """Return `value` as a `FrozenDict`, without copying one that already is.

    The normaliser for a frozen dataclass's `__post_init__`:

        object.__setattr__(self, "metadata", freeze_mapping(self.metadata))

    which is the only place the copy can happen. Doing it in the caller instead
    means every call site is one omission away from the aliasing bug, and the
    call sites are not where the invariant is written down.
    """
    if isinstance(value, FrozenDict):
        return cast("FrozenDict[K, V]", value)
    return FrozenDict(value)


EMPTY_MAPPING: FrozenDict[Any, Any] = FrozenDict()
"""A shared empty mapping, safe as a default because it cannot be written to.

`field(default_factory=dict)` builds a fresh mutable dict per instance, which
is correct only because the default is mutable. Once it is not, one instance
does for every value object that omitted the field.
"""
