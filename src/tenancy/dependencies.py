"""The tenant, for routes that need to name it rather than merely be inside it.

Most routes need neither of these. The tenant reaches the database through
`TenantContextMiddleware` and the `begin` listener, so an ordinary handler
runs tenant-scoped without mentioning tenancy at all — which is the point of
doing this with row-level security instead of a `WHERE` clause per query.

What is left is the small set of handlers that have to *say* the tenant: one
that mints a token carrying a `tid`, one that renders it into a response, one
that refuses early rather than returning an empty page. They take
`CurrentTenantDep` and get a 400 when there is no tenant in scope.
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import Depends

from src.tenancy.context import current_tenant_id, require_tenant_id


def get_current_tenant_id() -> uuid.UUID:
    """The request's tenant, or a 400 if it did not resolve to one.

    Takes no parameters because the tenant is not in the signature — the
    middleware put it in the context before dependency resolution began. That
    makes this trivially overridable in a test: `app.dependency_overrides` can
    replace it with a lambda, where a `Request`-reading version would need a
    whole request to be faked.
    """
    return require_tenant_id()


def get_optional_tenant_id() -> uuid.UUID | None:
    """The request's tenant, or `None`, for handlers that tolerate both."""
    return current_tenant_id()


CurrentTenantDep = Annotated[uuid.UUID, Depends(get_current_tenant_id)]
OptionalTenantDep = Annotated[uuid.UUID | None, Depends(get_optional_tenant_id)]

__all__ = [
    "CurrentTenantDep",
    "OptionalTenantDep",
    "get_current_tenant_id",
    "get_optional_tenant_id",
]
