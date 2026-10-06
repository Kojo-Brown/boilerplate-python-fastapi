"""Multi-tenancy: one database, one schema, and a boundary the database owns.

`src/tenancy/context.py` holds which tenant the current task is,
`src/tenancy/binding.py` puts it on the connection, `src/tenancy/sql.py` is
the SQL the boundary is made of, `src/tenancy/resolver.py` decides where the
tenant came from, `src/tenancy/middleware.py` establishes it per request and
`src/tenancy/isolation.py` checks a live database still enforces it.
`docs/multi-tenancy.md` is the operator's view: the roles to create, what
breaks if you get them wrong, and how migrations work once `FORCE ROW LEVEL
SECURITY` is on.

The one-paragraph version: every tenant-scoped table carries a `tenant_id`
and a row-level security policy comparing it against `app_current_tenant_id()`,
which reads a per-transaction setting. Application code never writes a tenant
filter, which is the entire argument for doing it this way — a filter that is
written four hundred times is a filter that is forgotten once.
"""

from src.tenancy.binding import apply_tenant, bind_tenant_on_begin
from src.tenancy.context import current_tenant_id, require_tenant_id, tenant_scope
from src.tenancy.dependencies import (
    CurrentTenantDep,
    OptionalTenantDep,
    get_current_tenant_id,
    get_optional_tenant_id,
)
from src.tenancy.errors import (
    InvalidTenantError,
    TenantMismatchError,
    TenantRequiredError,
)
from src.tenancy.middleware import TenantContextMiddleware
from src.tenancy.resolver import TenantResolution, resolve_tenant
from src.tenancy.sql import TENANT_SCOPED_TABLES, TENANT_SETTING

__all__ = [
    "TENANT_SCOPED_TABLES",
    "TENANT_SETTING",
    "CurrentTenantDep",
    "InvalidTenantError",
    "OptionalTenantDep",
    "TenantContextMiddleware",
    "TenantMismatchError",
    "TenantRequiredError",
    "TenantResolution",
    "apply_tenant",
    "bind_tenant_on_begin",
    "current_tenant_id",
    "get_current_tenant_id",
    "get_optional_tenant_id",
    "require_tenant_id",
    "resolve_tenant",
    "tenant_scope",
]
