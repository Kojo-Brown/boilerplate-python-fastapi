"""Deciding which tenant a request belongs to, before it is authenticated.

## Why the token is not enough on its own

The natural answer is "the tenant is a claim in the access token", and for
every authenticated request it is. The problem is the requests that come
*before* one exists: `POST /api/v1/auth/login` has to find a user row by
email, and under row-level security that read returns nothing unless the
connection is already bound to a tenant. Authentication cannot be what
establishes the tenant when authentication is itself a tenant-scoped read.

So there are two sources, with a strict precedence:

1. **The `tid` claim of a verified access token.** Authoritative wherever it
   exists. It was signed by this API when the user logged in, so it cannot be
   chosen by the caller.
2. **The `X-Tenant-ID` header.** The fallback, and the only source available
   to an unauthenticated request. It *is* chosen by the caller.

## What the header can and cannot do

Naming a tenant is not being admitted to it. A header-resolved request can
reach exactly what an anonymous caller may reach in that tenant: it can
attempt a login, and it will fail unless it also presents a password that
matches a row in that tenant. Treating the header as a credential is the
mistake to avoid here; treating it as routing information — the equivalent of
the `Host` a multi-tenant deployment would otherwise use — is what it is.

`TENANCY_TRUST_HEADER=false` turns the fallback off for deployments that
derive the tenant some other way (a per-tenant hostname terminated at the
proxy, a path prefix) and want no second route into the setting.

## Disagreement is a refusal, never a preference

When a token carries a tenant and the header names a different one, the
request is refused. The tempting alternative — use the token's, ignore the
header — is wrong for a reason worth naming: a client that sends the wrong
tenant is either broken or probing, and answering it successfully with the
*right* tenant's data tells neither of them apart, hides the bug from the
first and gives the second a working request. 403 tells both.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Literal

from src.auth.utils import InvalidAccessTokenError, verify_access_token
from src.tenancy.errors import InvalidTenantError, TenantMismatchError

#: Where a resolved tenant came from. `"none"` is a successful resolution of
#: no tenant — the request is unscoped, and the database will treat it that
#: way — rather than a failure, which is always an exception.
TenantSource = Literal["token", "header", "none"]


@dataclass(frozen=True, slots=True)
class TenantResolution:
    """What this request resolved to, and on whose say-so.

    The source is carried rather than discarded because it is what the logs
    need to answer "was this tenant asserted by us or by the caller" after the
    fact, and because `tests/test_tenancy_resolver.py` asserts precedence
    rather than merely the resulting id.
    """

    tenant_id: uuid.UUID | None
    source: TenantSource


def parse_tenant_id(raw: str) -> uuid.UUID:
    """Parse a caller-supplied tenant identifier.

    Raises:
        InvalidTenantError: `raw` is not a UUID.
    """
    try:
        return uuid.UUID(raw.strip())
    except ValueError as exc:
        raise InvalidTenantError() from exc


def bearer_token(authorization: str | None) -> str | None:
    """The credential out of an `Authorization: Bearer <token>` header.

    Returns `None` for an absent header and for any other scheme, rather than
    raising: this module's job is to find a tenant, and a request whose
    authorization it cannot read is simply one with no token-borne tenant.
    Refusing the credential is `get_current_user`'s job, and doing it here too
    would mean two places that have to agree about what a 401 is.
    """
    if not authorization:
        return None
    scheme, _, credential = authorization.partition(" ")
    if scheme.lower() != "bearer" or not credential.strip():
        return None
    return credential.strip()


def tenant_from_token(token: str | None) -> uuid.UUID | None:
    """The `tid` claim of `token`, or `None` if there is not one to be had.

    A token that does not verify yields `None` rather than an error. It will
    be refused with a 401 by `get_current_user` a few microseconds later, and
    raising here would answer an expired token with a tenant error — which is
    both wrong and the kind of misleading status code that costs an afternoon.
    """
    if token is None:
        return None
    try:
        return verify_access_token(token).tenant_id
    except InvalidAccessTokenError:
        return None


def resolve_tenant(
    *,
    header_value: str | None,
    authorization: str | None,
    trust_header: bool,
) -> TenantResolution:
    """Resolve the tenant for one request.

    Raises:
        InvalidTenantError: the header is present but is not a UUID.
        TenantMismatchError: the header and the token name different tenants.
    """
    from_header = parse_tenant_id(header_value) if header_value else None
    from_token = tenant_from_token(bearer_token(authorization))

    if from_token is not None:
        # Checked even when the header is not trusted. An untrusted header is
        # one this API will not *route* by; it is not one it should quietly
        # accept a contradiction from, and a client sending both wants to know
        # it disagrees with itself either way.
        if from_header is not None and from_header != from_token:
            raise TenantMismatchError()
        return TenantResolution(tenant_id=from_token, source="token")

    if from_header is not None and trust_header:
        return TenantResolution(tenant_id=from_header, source="header")

    return TenantResolution(tenant_id=None, source="none")


__all__ = [
    "TenantResolution",
    "TenantSource",
    "bearer_token",
    "parse_tenant_id",
    "resolve_tenant",
    "tenant_from_token",
]
