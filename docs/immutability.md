# Immutability

Value objects in this codebase are frozen, module-level constants are actually
constant, and the two rules are enforced by a test rather than by review.

This document is about what each of Python's immutability tools *does not*
cover, because that gap is where the bugs are. Everything below is asserted in
`tests/test_immutability_gate.py` and `tests/test_immutable.py`; if a claim
here stops being true, those fail.

## What each tool actually guards

| Tool | Refuses | Permits |
| --- | --- | --- |
| `Final` | rebinding the **name** | mutating the **object** |
| `@dataclass(frozen=True)` | assigning to an **attribute** | mutating a field's contents; keeping the caller's container |
| Pydantic `frozen=True` | assigning to a **field** | mutating a field's contents |
| `FrozenDict` | every write to the mapping | mutating the **values** inside it |

Nothing in that table gives deep immutability, and no single mechanism can:
"this object cannot change" is a property of a whole graph. What the rules
below buy is that every *edge* in the graph has been decided deliberately.

### `Final` is about the name

```python
CURRENCY_EXPONENTS: Final[dict[str, int]] = {"JPY": 0, "USD": 2}

CURRENCY_EXPONENTS = {}           # error — mypy refuses to rebind
CURRENCY_EXPONENTS["JPY"] = 2     # fine — and wrong for the rest of the process
```

The second line type-checks, runs, and silently changes how every later yen
amount converts, in a process that will not restart for hours. A module-level
constant is the widest-shared mutable state a Python process has, and `Final`
does nothing about it.

Twelve `Final` names in `src/` held a writable dict, and `__all__` in
`src/events/subscribers.py` held a writable list. The `DEFAULT_STRATEGIES` /
`DEFAULT_BACKENDS` / `DEFAULT_GATEWAYS` registries were the sharpest of them:
each registry's `reset()` restores `dict(DEFAULT_*)`, so one write to the
module constant would have poisoned the escape hatch itself, permanently and
for every test that ran afterwards.

### `frozen=True` is about the attribute

```python
metadata = {"order": "1"}
request = ChargeRequest(..., metadata=metadata)

request.metadata = {}          # AttributeError — frozen
metadata["order"] = "2"        # no error, and request.metadata now says "2"
request.metadata["order"] = "3"  # no error either
```

A frozen dataclass does not copy what it is handed, so construction leaves the
caller holding a live reference into the "immutable" object. For
`ChargeRequest` that reference reaches the provider: both adapters send
`metadata` upstream, so an edit between construction and the HTTP call puts
something in Stripe's or PayPal's records that the validated request never
contained.

The fix is one line in `__post_init__`:

```python
object.__setattr__(self, "metadata", freeze_mapping(self.metadata))
```

`object.__setattr__` is how a frozen dataclass normalises a field — the
generated `__setattr__` refuses, and this is the documented way past it during
`__post_init__`. Putting the copy here rather than at the call sites is the
point: the invariant lives with the type that has it.

### Pydantic adds one more gap

`frozen=True` on a model refuses field assignment, and Pydantic *replaces* the
container during validation, so the model holds a dict it built. Mutating
through it is unimpeded. Where that matters — `PresignedUploadResponse.fields`
carries the S3 POST policy's signed form fields — the field is typed
`FrozenDict[str, str]` so the field is as frozen as the model claiming to hold
it.

`CursorPage.items` is the same problem with a different container, solved the
plain way: it is a `tuple[T, ...]`. A page is a snapshot whose `next_cursor`
was computed from its last element, so an appended item leaves the envelope
describing a page it no longer contains. JSON has one array type, so the
serialised body is identical.

## `FrozenDict`

`src/immutable.py`. A `Mapping` that copies on construction and exposes no
mutator.

```python
from src.immutable import EMPTY_MAPPING, FrozenDict, freeze_mapping

CURRENCY_EXPONENTS: Final[FrozenDict[str, int]] = FrozenDict({"JPY": 0})

frozen = FrozenDict({"a": 1})
frozen["a"] = 2          # TypeError
frozen | {"b": 2}        # a new FrozenDict; the original is unchanged
```

Three properties decide its shape:

**It is not a `dict` subclass.** Inheriting from `dict` means inheriting
`__setitem__`, `update`, `pop`, `popitem`, `clear` and `setdefault`, each of
which then has to be overridden to raise — and `dict.update` in CPython does
not route through `__setitem__`, so the overrides would not even be enough.
Subclassing `collections.abc.Mapping` starts from nothing writable.

**It is hashable.** A frozen dataclass with a plain-dict field is *not*
hashable: `hash(Notification(...))` raises, so such a value object cannot be a
set member, a dict key, or an argument to a `@cached` function —
`src/decorators/cache.py` rejects unhashable arguments by design. The hash is
computed on first use, cached, and raises `TypeError` if any value is
unhashable, exactly as a tuple does.

**It is picklable.** `MappingProxyType` is the obvious standard-library
answer and `pickle` refuses it outright. `Notification` goes onto a Celery
queue and `src/parallel` pickles arguments into worker processes, so a frozen
mapping that cannot cross a process boundary is not usable as a field type
here. `__reduce__` rebuilds it from a plain dict.

It also carries a Pydantic core schema, so one type covers module constants,
dataclass fields and model fields. The schema wraps Pydantic's own `dict`
schema, so validation, coercion and the generated OpenAPI document are
unchanged — a client generated from `/openapi.json` cannot tell.

`EMPTY_MAPPING` is a shared empty instance used as a field default.
`field(default_factory=dict)` exists because sharing a *mutable* default is how
one instance's metadata turns up on another's; once the default cannot be
written to, one instance does for every value object that omitted the field.

## The settings singleton

`Settings` is `frozen=True`. Several subsystems already assume configuration
does not move: `get_strategy` is `@cache`d per channel, `StorageFactory` and
`PaymentGatewayRegistry` build from a `Settings` at first use, and the
idempotency and lock backends are constructed once in the lifespan. Under a
mutable singleton, `settings.PAYMENT_GATEWAY = "paypal"` type-checks, appears
to work, and takes effect for whatever has not been built yet and nothing else
— the worst failure shape available, because every individual piece behaves
exactly as designed.

A test that needs different configuration constructs its own `Settings(...)`
and passes it in; every factory takes one for that reason. That is a better
seam than mutating a global, because it cannot leak into the next test.

## The gate

`tests/test_immutability_gate.py` walks every module under `src/` and enforces
three rules:

1. A module-level `Final` name must not hold a `dict`, `list`, `set` or
   `bytearray`.
2. Every dataclass must be `frozen=True`.
3. Every Pydantic model must have `frozen=True` in its `model_config`.

Each has an exemption table mapping a qualified name to a *reason*. The tables
are the mechanism, not an escape from it: a mutable value is sometimes right —
`_Entry` in the cache and `_Held` in the in-memory lock backend are storage,
not values — and the way to say so is to write it down where the next reader
will find it. Two further tests keep the tables honest: an entry naming
something that no longer exists fails, and so does an entry with an empty
reason.

The gate imports every module rather than reading the AST, because `frozen` and
`model_config` exist only on the built class. It reads the AST for rule 1,
because most modules here use `from __future__ import annotations` and
resolving those strings would require every `TYPE_CHECKING`-only import to be
importable, which by construction they are not.

## When not to freeze

Freezing is for **values** — things defined entirely by what they contain, that
two callers can share because neither can tell the difference. It is wrong for
**state**: a cache entry exists to be replaced, a held lease exists to be
renewed. Adding `frozen=True` to one of those does not make it safer, it makes
the code that maintains it construct a replacement object per update and hide
the mutation one level up.

The question to ask is not "could this be frozen" but "does anything care which
instance it has". If nothing does, it is a value.
