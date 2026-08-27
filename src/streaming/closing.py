"""`contextlib.aclosing`, for iterators that are not necessarily generators.

`aclosing` requires an object with `aclose()`, which every async generator has
and the `AsyncIterator` protocol does not promise. That distinction is not
pedantry here: the ports in this package take `AsyncIterator` on purpose, so
that a fake can be a small class with `__aiter__`/`__anext__` and not have to
be a generator to satisfy them. Typing the parameters as `AsyncGenerator`
instead would push that requirement onto every implementation to save this
module.

Closing still matters for the ones that *are* generators, which in production
is all of them. An async generator abandoned mid-iteration keeps its frame —
and whatever that frame holds open, here a server-side cursor on a pooled
connection — until the garbage collector finalizes it, which is not a schedule
a connection pool can be sized against.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager


@asynccontextmanager
async def closing_iterator[T](
    items: AsyncIterator[T],
) -> AsyncGenerator[AsyncIterator[T], None]:
    """Yield `items`, calling `aclose()` on the way out if it has one."""
    try:
        yield items
    finally:
        # Structural: `AsyncGenerator`'s ABC hook matches anything with the
        # generator methods, so this also covers a fake that implements
        # `aclose` without inheriting from anything.
        if isinstance(items, AsyncGenerator):
            await items.aclose()


__all__ = ["closing_iterator"]
