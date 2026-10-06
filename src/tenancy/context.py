"""Which tenant the current piece of work belongs to.

A `ContextVar`, not a request attribute and not an argument threaded through
every call. The reason is the one that decides the whole design of this
package: the thing that has to read the tenant is not a route handler but the
`begin` event on the database engine (`src/tenancy/binding.py`), which fires
somewhere underneath SQLAlchemy with no access to the request. Anything that
has to be *passed* can be forgotten, and a forgotten tenant here is not a
missing field in a response — it is a query that runs under the previous
request's tenant.

`ContextVar` also gives the isolation that a module-level global would not.
`asyncio` copies the context when a task is created, so a tenant set inside
`asyncio.create_task` is not visible to its parent or its siblings, and two
requests served concurrently on one event loop cannot see each other's. That
property is asserted directly in `tests/test_tenancy_context.py`, because it
is load-bearing rather than incidental.

The variable holds `None` rather than being unset-by-default, and `None` means
*no tenant*, never *every tenant*. Everything downstream reads it that way:
`bind_tenant` sends no setting, the database setting stays absent, and the
row-level security policies compare `tenant_id` against `NULL`, which is never
true. An unscoped query therefore returns nothing. That is the one failure
direction worth having — a feature that stops working is reported in minutes,
and a feature that quietly returns another customer's rows is not.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

from src.tenancy.errors import TenantRequiredError

#: The tenant every database statement on this task runs as, or `None`.
#:
#: Private: the three functions below are the whole interface. A caller
#: holding the variable could `.set()` without keeping the token, which leaks
#: the tenant into whatever the task does next — the exact failure
#: `tenant_scope` exists to make unrepresentable.
_current_tenant_id: ContextVar[uuid.UUID | None] = ContextVar(
    "app_tenant_id", default=None
)


def current_tenant_id() -> uuid.UUID | None:
    """The tenant in scope, or `None` if the work is unscoped."""
    return _current_tenant_id.get()


def require_tenant_id() -> uuid.UUID:
    """The tenant in scope, or raise.

    For the callers that cannot do anything sensible without one — minting a
    token that has to carry a `tid`, say. Code that merely *queries* should
    use `current_tenant_id` and let the database answer: an unscoped read
    returning nothing is already the right behaviour, and raising here would
    only move the same outcome earlier at the cost of a second place that has
    to agree about what a tenant is for.

    Raises:
        TenantRequiredError: nothing established a tenant for this task.
    """
    tenant_id = _current_tenant_id.get()
    if tenant_id is None:
        raise TenantRequiredError()
    return tenant_id


@contextmanager
def tenant_scope(tenant_id: uuid.UUID | None) -> Iterator[None]:
    """Run the block as `tenant_id`, restoring whatever was in scope before.

    Restoring rather than clearing, and via the token `set` returns rather
    than by remembering the old value: under `asyncio` the context this exits
    into is not guaranteed to be the one it entered from, and `ContextVar`
    tokens are the only mechanism that gets that right. The practical case is
    a worker that serves two tenants in turn on one task — resetting to `None`
    instead of to the enclosing value would work right up until these nest.

    Passing `None` is meaningful and not a no-op: it runs the block unscoped,
    which is how a background job deliberately steps outside a tenant.
    """
    token = _current_tenant_id.set(tenant_id)
    try:
        yield
    finally:
        _current_tenant_id.reset(token)


__all__ = ["current_tenant_id", "require_tenant_id", "tenant_scope"]
