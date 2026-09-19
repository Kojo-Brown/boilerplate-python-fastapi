"""The documentation pages, and the nonce that lets them keep a strict CSP.

The gate this file exists for is `test_every_inline_script_is_nonced`: the
markup comes from FastAPI, so a future release is free to change it, and the
failure that change produces in a browser — a blocked script and a blank
documentation page — is not one a test suite would otherwise see.
"""

import re
from collections.abc import AsyncGenerator

import pytest
from fastapi import Request
from httpx import ASGITransport, AsyncClient

from src.docs import (
    DOCS_URL,
    DOCUMENTATION_PATHS,
    OAUTH2_REDIRECT_URL,
    REDOC_URL,
    _nonce_of,
    add_script_nonce,
)
from src.main import app
from src.middleware.security_headers import CSP_HEADER

#: Every opening `<script` tag, so the assertions below can classify each one
#: rather than trusting the same pattern the implementation uses.
_SCRIPT_TAG = re.compile(r"<script\b[^>]*>", re.IGNORECASE)


@pytest.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


def nonce_from_csp(csp: str) -> str:
    match = re.search(r"'nonce-([^']+)'", csp)
    assert match is not None, f"no nonce in {csp!r}"
    return match.group(1)


class TestAddScriptNonce:
    def test_an_inline_script_gets_the_nonce(self) -> None:
        assert add_script_nonce(b"<script>x()</script>", "abc") == (
            b'<script nonce="abc">x()</script>'
        )

    def test_existing_attributes_are_preserved(self) -> None:
        assert add_script_nonce(b'<script type="module">x</script>', "abc") == (
            b'<script nonce="abc" type="module">x</script>'
        )

    def test_a_sourced_script_is_left_alone(self) -> None:
        # It is covered by the `https://cdn.jsdelivr.net` source in the policy,
        # and a nonce on it would be noise.
        markup = b'<script src="https://cdn.example/x.js"></script>'
        assert add_script_nonce(markup, "abc") == markup

    def test_a_sourced_script_with_other_attributes_is_left_alone(self) -> None:
        markup = b'<script defer src="/x.js" crossorigin></script>'
        assert add_script_nonce(markup, "abc") == markup


class TestNonceLookup:
    def test_missing_state_yields_an_empty_string(self) -> None:
        request = Request({"type": "http", "path": "/docs", "headers": []})
        assert _nonce_of(request) == ""

    def test_a_non_string_value_is_ignored(self) -> None:
        request = Request(
            {"type": "http", "path": "/docs", "headers": [], "state": {"csp_nonce": 7}}
        )
        assert _nonce_of(request) == ""


class TestRenderedPages:
    @pytest.mark.parametrize("path", sorted(DOCUMENTATION_PATHS))
    async def test_the_page_renders(self, client: AsyncClient, path: str) -> None:
        response = await client.get(path)
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/html")

    @pytest.mark.parametrize("path", sorted(DOCUMENTATION_PATHS))
    async def test_every_inline_script_is_nonced(
        self, client: AsyncClient, path: str
    ) -> None:
        response = await client.get(path)
        nonce = nonce_from_csp(response.headers[CSP_HEADER])
        tags = _SCRIPT_TAG.findall(response.text)
        assert tags, f"{path} renders no script at all"
        for tag in tags:
            if "src=" in tag:
                continue
            assert f'nonce="{nonce}"' in tag, (
                f"inline script in {path} carries no nonce: {tag!r}. FastAPI's "
                "documentation markup has changed shape; src/docs.py needs to "
                "follow it."
            )

    async def test_the_nonce_in_the_markup_matches_the_header(
        self, client: AsyncClient
    ) -> None:
        response = await client.get(DOCS_URL)
        nonce = nonce_from_csp(response.headers[CSP_HEADER])
        assert f'nonce="{nonce}"' in response.text

    async def test_each_request_gets_a_fresh_nonce(self, client: AsyncClient) -> None:
        first = nonce_from_csp((await client.get(DOCS_URL)).headers[CSP_HEADER])
        second = nonce_from_csp((await client.get(DOCS_URL)).headers[CSP_HEADER])
        assert first != second

    async def test_the_documentation_policy_allows_what_the_pages_load(
        self, client: AsyncClient
    ) -> None:
        csp = (await client.get(REDOC_URL)).headers[CSP_HEADER]
        assert "https://cdn.jsdelivr.net" in csp
        assert "https://fonts.googleapis.com" in csp
        assert "https://fonts.gstatic.com" in csp
        assert "worker-src 'self' blob:" in csp
        assert "frame-ancestors 'none'" in csp

    async def test_script_src_has_no_unsafe_inline(self, client: AsyncClient) -> None:
        # The whole point of the nonce. A nonce plus `'unsafe-inline'` would
        # also work in a modern browser — the nonce makes it ignored — but it
        # would keep the pages open to injected script on anything older.
        csp = (await client.get(DOCS_URL)).headers[CSP_HEADER]
        script_src = next(
            part for part in csp.split("; ") if part.startswith("script-src ")
        )
        assert "'unsafe-inline'" not in script_src

    async def test_swagger_points_at_the_oauth2_redirect_route(
        self, client: AsyncClient
    ) -> None:
        # The redirect page is re-registered here too, so the URL Swagger UI is
        # configured with has to be the one this router serves.
        assert OAUTH2_REDIRECT_URL in (await client.get(DOCS_URL)).text

    async def test_the_pages_are_absent_from_the_schema(
        self, client: AsyncClient
    ) -> None:
        paths = (await client.get("/openapi.json")).json()["paths"]
        assert DOCUMENTATION_PATHS.isdisjoint(paths)
