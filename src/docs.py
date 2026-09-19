"""The OpenAPI documentation pages, re-registered so they can carry a nonce.

FastAPI mounts `/docs`, `/docs/oauth2-redirect` and `/redoc` itself, and each
of those pages contains an inline `<script>` — the Swagger UI bootstrap, the
OAuth2 redirect handler, and (for ReDoc) an inline `<style>`. An inline script
is exactly what a content-security policy exists to refuse, so the built-in
pages leave a choice between breaking them and writing `'unsafe-inline'` into
`script-src`, which is the same as not having a policy at all.

The third option is a nonce, and taking it means owning the routes: the value
has to be minted per response, written into the markup and into the header,
and it cannot be either if FastAPI renders the page. So `src/main.py` builds
the app with `docs_url=None` / `redoc_url=None` and includes this router
instead. The HTML is still FastAPI's — `get_swagger_ui_html` and friends are
called unchanged — and the only edit is a `nonce` attribute added to every
inline `<script>` tag.

`style-src` is **not** covered by the nonce and deliberately keeps
`'unsafe-inline'`; see `src/middleware/security_headers.py` for why attaching
a nonce to the `<style>` block would make the docs pages *less* safe rather
than more.
"""

import re

from fastapi import APIRouter, Request
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.responses import HTMLResponse

#: Where the three pages live. Declared here rather than derived from the app,
#: because `SecurityHeadersPolicy` has to know them before any request arrives
#: and `src/exception_handlers.py` needs them without an app in hand.
DOCS_URL = "/docs"
OAUTH2_REDIRECT_URL = "/docs/oauth2-redirect"
REDOC_URL = "/redoc"

DOCUMENTATION_PATHS = frozenset({DOCS_URL, OAUTH2_REDIRECT_URL, REDOC_URL})

#: Matches an opening `<script>` tag that carries no `src` attribute — that is,
#: one whose body is inline and therefore needs the nonce. Deliberately not a
#: literal `b"<script>"` replacement: FastAPI is free to emit
#: `<script type="module">` in a future release, and a literal match would skip
#: it silently, leaving a page whose script the browser then refuses to run.
#: `tests/test_docs_pages.py` asserts the invariant this expresses — every
#: inline script in every rendered page ends up with a nonce — so a change in
#: that markup fails the suite rather than the documentation UI.
_INLINE_SCRIPT_TAG = re.compile(rb"<script(?![^>]*\bsrc\s*=)([^>]*)>", re.IGNORECASE)


def add_script_nonce(html: bytes, nonce: str) -> bytes:
    """Add `nonce="<nonce>"` to every inline `<script>` tag in `html`."""
    attribute = f' nonce="{nonce}"'.encode()

    def _replace(match: re.Match[bytes]) -> bytes:
        return b"<script" + attribute + match.group(1) + b">"

    return _INLINE_SCRIPT_TAG.sub(_replace, html)


def _nonce_of(request: Request) -> str:
    """The nonce `SecurityHeadersMiddleware` minted for this request.

    Empty when the middleware is disabled, which is the one case where these
    pages render without a policy to satisfy. An empty `nonce=""` attribute is
    ignored by browsers, so the markup stays valid either way.
    """
    nonce = getattr(request.state, "csp_nonce", "")
    return nonce if isinstance(nonce, str) else ""


def build_docs_router(*, title: str, openapi_url: str) -> APIRouter:
    """The three documentation routes, rendered with a per-response nonce."""
    router = APIRouter(include_in_schema=False)

    @router.get(DOCS_URL)
    async def swagger_ui(request: Request) -> HTMLResponse:
        page = get_swagger_ui_html(
            openapi_url=openapi_url,
            title=f"{title} - Swagger UI",
            oauth2_redirect_url=OAUTH2_REDIRECT_URL,
        )
        return HTMLResponse(add_script_nonce(bytes(page.body), _nonce_of(request)))

    @router.get(OAUTH2_REDIRECT_URL)
    async def swagger_ui_oauth2_redirect(request: Request) -> HTMLResponse:
        page = get_swagger_ui_oauth2_redirect_html()
        return HTMLResponse(add_script_nonce(bytes(page.body), _nonce_of(request)))

    @router.get(REDOC_URL)
    async def redoc(request: Request) -> HTMLResponse:
        page = get_redoc_html(
            openapi_url=openapi_url,
            title=f"{title} - ReDoc",
            # The default pulls Google Fonts in through a `<link>` that ReDoc
            # writes itself; keeping it means `font-src` and a second
            # `style-src` origin in the documentation policy, which is priced
            # in there rather than silently dropped here.
            with_google_fonts=True,
        )
        return HTMLResponse(add_script_nonce(bytes(page.body), _nonce_of(request)))

    return router
