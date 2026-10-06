"""Where the tenant comes from, and what happens when the two sources disagree.

The precedence here is a security boundary rather than a convenience: the
token's `tid` was signed by this API, and the header was chosen by whoever
sent the request. Every test below is about keeping that asymmetry visible.
"""

from __future__ import annotations

import uuid

import pytest

from src.auth.utils import create_access_token, create_refresh_token
from src.tenancy.errors import InvalidTenantError, TenantMismatchError
from src.tenancy.resolver import (
    bearer_token,
    parse_tenant_id,
    resolve_tenant,
    tenant_from_token,
)

ALPHA = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")
BETA = uuid.UUID("bbbbbbbb-0000-0000-0000-000000000002")
USER = uuid.UUID("11111111-2222-3333-4444-555555555555")


def _token(tenant_id: uuid.UUID | None) -> str:
    return create_access_token(str(USER), "user@example.com", "user", tenant_id)


def _auth(tenant_id: uuid.UUID | None) -> str:
    return f"Bearer {_token(tenant_id)}"


class TestParsing:
    def test_a_uuid_parses(self) -> None:
        assert parse_tenant_id(str(ALPHA)) == ALPHA

    def test_surrounding_whitespace_is_tolerated(self) -> None:
        """Header values pick it up from proxies and hand-written clients."""
        assert parse_tenant_id(f"  {ALPHA}  ") == ALPHA

    @pytest.mark.parametrize("raw", ["", "not-a-uuid", "1", "' OR 1=1 --"])
    def test_anything_else_is_refused(self, raw: str) -> None:
        with pytest.raises(InvalidTenantError) as exc:
            parse_tenant_id(raw)
        assert exc.value.status_code == 400


class TestBearerToken:
    def test_it_reads_the_credential(self) -> None:
        assert bearer_token("Bearer abc") == "abc"

    def test_the_scheme_is_case_insensitive(self) -> None:
        """RFC 9110 §11.1 — `bearer` and `Bearer` are the same scheme."""
        assert bearer_token("bearer abc") == "abc"

    @pytest.mark.parametrize(
        "header", [None, "", "Basic abc", "Bearer", "Bearer   ", "abc"]
    )
    def test_anything_it_cannot_read_is_simply_no_token(
        self, header: str | None
    ) -> None:
        """Refusing the credential is `get_current_user`'s job, not this one's."""
        assert bearer_token(header) is None


class TestTenantFromToken:
    def test_a_signed_token_yields_its_tid(self) -> None:
        assert tenant_from_token(_token(ALPHA)) == ALPHA

    def test_a_token_without_a_tid_yields_none(self) -> None:
        assert tenant_from_token(_token(None)) is None

    def test_no_token_yields_none(self) -> None:
        assert tenant_from_token(None) is None

    def test_a_token_that_does_not_verify_yields_none_rather_than_raising(self) -> None:
        """It becomes a 401 a moment later; a tenant error here would mislead."""
        assert tenant_from_token("not.a.token") is None

    def test_a_refresh_token_yields_none(self) -> None:
        """Signed by the same key, and still not an access token."""
        refresh, _ = create_refresh_token(str(USER), "jti")
        assert tenant_from_token(refresh) is None


class TestResolution:
    def test_the_token_wins_when_there_is_one(self) -> None:
        resolution = resolve_tenant(
            header_value=None, authorization=_auth(ALPHA), trust_header=True
        )
        assert (resolution.tenant_id, resolution.source) == (ALPHA, "token")

    def test_the_header_is_used_when_there_is_no_token_tenant(self) -> None:
        resolution = resolve_tenant(
            header_value=str(ALPHA), authorization=None, trust_header=True
        )
        assert (resolution.tenant_id, resolution.source) == (ALPHA, "header")

    def test_a_header_agreeing_with_the_token_resolves_to_the_token(self) -> None:
        resolution = resolve_tenant(
            header_value=str(ALPHA), authorization=_auth(ALPHA), trust_header=True
        )
        assert (resolution.tenant_id, resolution.source) == (ALPHA, "token")

    def test_disagreement_is_refused_rather_than_resolved(self) -> None:
        with pytest.raises(TenantMismatchError) as exc:
            resolve_tenant(
                header_value=str(BETA), authorization=_auth(ALPHA), trust_header=True
            )
        assert exc.value.status_code == 403

    def test_disagreement_is_refused_even_when_the_header_is_not_trusted(self) -> None:
        """Not trusting the header means not routing by it, not ignoring it."""
        with pytest.raises(TenantMismatchError):
            resolve_tenant(
                header_value=str(BETA), authorization=_auth(ALPHA), trust_header=False
            )

    def test_an_untrusted_header_resolves_to_nothing(self) -> None:
        resolution = resolve_tenant(
            header_value=str(ALPHA), authorization=None, trust_header=False
        )
        assert (resolution.tenant_id, resolution.source) == (None, "none")

    def test_nothing_at_all_resolves_to_nothing(self) -> None:
        """Which the database then treats as no rows, rather than as all rows."""
        resolution = resolve_tenant(
            header_value=None, authorization=None, trust_header=True
        )
        assert (resolution.tenant_id, resolution.source) == (None, "none")

    def test_a_malformed_header_is_refused_before_the_token_is_read(self) -> None:
        with pytest.raises(InvalidTenantError):
            resolve_tenant(
                header_value="nope", authorization=_auth(ALPHA), trust_header=True
            )

    def test_an_unverifiable_token_falls_back_to_the_header(self) -> None:
        """The request still gets routed; authentication still refuses it."""
        resolution = resolve_tenant(
            header_value=str(ALPHA), authorization="Bearer rubbish", trust_header=True
        )
        assert (resolution.tenant_id, resolution.source) == (ALPHA, "header")
