"""Response security headers: CSP, HSTS, nosniff, referrer and frame policy.

Four headers, one of which is conditional and none of which mean anything if
they are only *usually* present — which is the whole difficulty. The three
decisions worth reading about:

**The policy is applied to every response, including the ones the application
did not produce.** A 404 from the router, a 429 from the rate limiter, a 409
short-circuited by `IdempotencyMiddleware` and a 500 all leave through this
middleware's `send`, because it is added last in `src/main.py` and Starlette
runs the last-added middleware outermost. One response escapes even that:
`ServerErrorMiddleware` sits *outside* the user middleware stack, so the 500
envelope rendered for an unhandled exception is written by a handler this
middleware never sees. That is why `SecurityHeadersPolicy` is a value with a
`headers_for()` method rather than logic inside `__call__` — the handler in
`src/exception_handlers.py` stamps its own response with the same policy. The
alternative, a strict policy on 99% of responses, is the failure shape nobody
notices: the missing one is the error page, which is where reflected content
is most likely to appear.

**HSTS is sent over HTTPS and only over HTTPS.** RFC 6797 §7.2 requires a user
agent to *ignore* the header when it arrives over plain HTTP, so sending it
there is at best noise; worse, a local `http://localhost:8000` that answers
with `Strict-Transport-Security` pins a developer's browser to HTTPS for
localhost, across every other project on that machine, for a year. The
condition is `scope["scheme"]`, which behind a load balancer is set from
`X-Forwarded-Proto` by uvicorn's `--proxy-headers` (on by default) — so a
deployment that terminates TLS at the edge and forgets to trust its proxy
headers gets no HSTS at all, silently. `docs/security-headers.md` says how to
check that from outside.

**`preload` is refused unless it can actually be preloaded.** The browser
preload list requires `includeSubDomains` and `max-age` of at least a year,
and a submission that lacks either is rejected — but the *header* is still
well-formed, so nothing in the running system says so. `HSTSPolicy` raises at
construction instead, which turns a year-long misconfiguration into a failed
start-up.

The default content-security policy is `default-src 'none'` and that is not an
approximation: a JSON API loads nothing, embeds nothing, and submits no forms,
so every fetch directive falls through to a default that forbids everything.
The documentation pages are the exception and they are handled by a nonce
rather than by weakening the policy — see `src/docs.py`.
"""

import secrets
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from functools import cache

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from src.config import Settings, settings
from src.docs import DOCUMENTATION_PATHS

CSP_HEADER = "content-security-policy"
HSTS_HEADER = "strict-transport-security"
CONTENT_TYPE_OPTIONS_HEADER = "x-content-type-options"
REFERRER_POLICY_HEADER = "referrer-policy"
FRAME_OPTIONS_HEADER = "x-frame-options"

#: The shortest `max-age` the browser preload list accepts (one year).
PRELOAD_MIN_MAX_AGE_SECONDS = 31_536_000

#: Bytes of entropy per nonce. The CSP specification asks for at least 128
#: bits; `token_urlsafe` reports its argument in bytes, so 16 is that floor and
#: 32 is a cheap margin over it.
NONCE_BYTES = 32

#: Substituted with the request's nonce. `str.replace` rather than `format`,
#: because the policy itself is full of `'none'` and `'self'` — braces are not
#: special in CSP, but a policy string that has to escape them would be a trap
#: for whoever edits it next.
NONCE_PLACEHOLDER = "{nonce}"

#: What the documentation pages need, directive by directive, measured against
#: the markup FastAPI actually emits rather than guessed:
#:
#: - `script-src` — the Swagger UI and ReDoc bundles come from jsDelivr; the
#:   bootstrap and OAuth2-redirect scripts are inline and carry the nonce.
#: - `style-src` — Swagger's stylesheet is on jsDelivr, ReDoc's font stylesheet
#:   on Google Fonts, and both render through inline `style=` attributes and
#:   `<style>` elements that ReDoc's renderer creates at run time. Those cannot
#:   be nonced (nothing in the page writes them) and this is the one directive
#:   that stays loose. Note that it is loose *because* it carries no nonce:
#:   under CSP Level 3 a nonce or hash in a directive makes the browser ignore
#:   `'unsafe-inline'` in that same directive, so noncing the one `<style>`
#:   block ReDoc ships in its markup would break every style it adds later.
#: - `font-src` — Montserrat and Roboto, served from gstatic by the stylesheet
#:   above.
#: - `img-src` — the favicon, plus `data:`/`blob:` for what the two renderers
#:   inline.
#: - `connect-src 'self'` — the `XMLHttpRequest` for `/openapi.json`.
#: - `worker-src blob:` — ReDoc builds its search index in a worker created
#:   from a blob URL.
DOCUMENTATION_CSP = (
    "default-src 'none'; "
    f"script-src 'self' https://cdn.jsdelivr.net 'nonce-{NONCE_PLACEHOLDER}'; "
    "style-src 'self' https://cdn.jsdelivr.net https://fonts.googleapis.com "
    "'unsafe-inline'; "
    "font-src 'self' https://fonts.gstatic.com; "
    "img-src 'self' data: blob: https://fastapi.tiangolo.com; "
    "connect-src 'self'; "
    "worker-src 'self' blob:; "
    "frame-ancestors 'none'; "
    "base-uri 'none'; "
    "form-action 'none'"
)


@dataclass(frozen=True, slots=True)
class HSTSPolicy:
    """`Strict-Transport-Security`, validated against what it claims.

    `max_age_seconds=0` is legal and meaningful: it tells a browser to forget
    the pin, which is how a host backs out of HSTS. It is therefore not treated
    as "disabled" — `SecurityHeadersPolicy.hsts = None` is.
    """

    max_age_seconds: int = PRELOAD_MIN_MAX_AGE_SECONDS
    include_subdomains: bool = True
    preload: bool = False

    def __post_init__(self) -> None:
        if self.max_age_seconds < 0:
            raise ValueError("HSTS max-age must not be negative")
        if self.preload and not self.include_subdomains:
            raise ValueError(
                "HSTS preload requires includeSubDomains; without it the "
                "browser preload list rejects the submission while the header "
                "still looks correct"
            )
        if self.preload and self.max_age_seconds < PRELOAD_MIN_MAX_AGE_SECONDS:
            raise ValueError(
                "HSTS preload requires a max-age of at least "
                f"{PRELOAD_MIN_MAX_AGE_SECONDS} seconds (one year); "
                f"got {self.max_age_seconds}"
            )

    @property
    def header_value(self) -> str:
        parts = [f"max-age={self.max_age_seconds}"]
        if self.include_subdomains:
            parts.append("includeSubDomains")
        if self.preload:
            parts.append("preload")
        return "; ".join(parts)


@dataclass(frozen=True, slots=True)
class SecurityHeadersPolicy:
    """The headers to add, and the two things that vary per request.

    Those two are the transport (HSTS is HTTPS-only) and the path (the
    documentation pages get their own content-security policy). Everything
    else is fixed at start-up, which is why this is a frozen value that both
    the middleware and the unhandled-exception handler can hold.
    """

    content_security_policy: str
    referrer_policy: str = "strict-origin-when-cross-origin"
    # Superseded by `frame-ancestors`, which every current browser honours, and
    # kept because it costs one header and is the only clickjacking defence
    # that reaches a browser too old to implement CSP Level 2. Set it to "" to
    # drop it.
    frame_options: str = "DENY"
    content_type_options: str = "nosniff"
    hsts: HSTSPolicy | None = field(default_factory=HSTSPolicy)
    documentation_csp: str = DOCUMENTATION_CSP
    documentation_paths: frozenset[str] = DOCUMENTATION_PATHS

    def needs_nonce(self, path: str) -> bool:
        """Whether a response for `path` will have a nonce to spend."""
        return (
            path in self.documentation_paths
            and NONCE_PLACEHOLDER in self.documentation_csp
        )

    def csp_for(self, path: str, nonce: str | None) -> str:
        if path not in self.documentation_paths:
            return self.content_security_policy
        return self.documentation_csp.replace(NONCE_PLACEHOLDER, nonce or "")

    def headers_for(
        self, *, path: str, secure: bool, nonce: str | None = None
    ) -> tuple[tuple[str, str], ...]:
        """The headers to add to a response for `path`.

        `secure` is whether the request arrived over TLS, and is the only thing
        that decides whether HSTS is present.
        """
        headers: list[tuple[str, str]] = [
            (CSP_HEADER, self.csp_for(path, nonce)),
            (CONTENT_TYPE_OPTIONS_HEADER, self.content_type_options),
            (REFERRER_POLICY_HEADER, self.referrer_policy),
        ]
        if self.frame_options:
            headers.append((FRAME_OPTIONS_HEADER, self.frame_options))
        if secure and self.hsts is not None:
            headers.append((HSTS_HEADER, self.hsts.header_value))
        return tuple((name, value) for name, value in headers if value)


def build_security_headers_policy(config: Settings) -> SecurityHeadersPolicy | None:
    """Map `Settings` onto a policy, or `None` when the headers are off."""
    if not config.SECURITY_HEADERS_ENABLED:
        return None
    hsts = (
        HSTSPolicy(
            max_age_seconds=config.SECURITY_HSTS_MAX_AGE_SECONDS,
            include_subdomains=config.SECURITY_HSTS_INCLUDE_SUBDOMAINS,
            preload=config.SECURITY_HSTS_PRELOAD,
        )
        if config.SECURITY_HSTS_ENABLED
        else None
    )
    return SecurityHeadersPolicy(
        content_security_policy=config.SECURITY_CSP,
        referrer_policy=config.SECURITY_REFERRER_POLICY,
        frame_options=config.SECURITY_FRAME_OPTIONS,
        hsts=hsts,
    )


@cache
def get_security_headers_policy() -> SecurityHeadersPolicy | None:
    """The process-wide policy, built once from the frozen global settings.

    Cached for the same reason every other factory here is: `Settings` does not
    move, so rebuilding it per request would only burn the validation in
    `HSTSPolicy`. A test that wants different configuration builds its own with
    `build_security_headers_policy(Settings(...))`.
    """
    return build_security_headers_policy(settings)


def merge_headers(
    existing: Sequence[tuple[bytes, bytes]],
    additions: Iterable[tuple[str, str]],
) -> list[tuple[bytes, bytes]]:
    """Add `additions` to `existing`, leaving any header already set alone.

    A route that sets its own `Content-Security-Policy` — one serving user
    content under a sandbox policy, say — has a reason the middleware does not
    know, so the policy here is a floor rather than an override.
    """
    merged = list(existing)
    present = {name.lower() for name, _ in merged}
    for name, value in additions:
        encoded = name.lower().encode("latin-1")
        if encoded in present:
            continue
        merged.append((encoded, value.encode("latin-1")))
        present.add(encoded)
    return merged


class SecurityHeadersMiddleware:
    """Stamps every HTTP response with `policy`.

    Non-HTTP scopes pass straight through. A WebSocket has no response to put
    these on: the handshake's headers are the ASGI server's, none of the five
    apply to a socket, and `HSTSPolicy` in particular is a directive about
    *navigations* the browser will make later, not about this connection.
    """

    def __init__(self, app: ASGIApp, policy: SecurityHeadersPolicy) -> None:
        self.app = app
        self.policy = policy

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path: str = scope["path"]
        nonce: str | None = None
        if self.policy.needs_nonce(path):
            nonce = secrets.token_urlsafe(NONCE_BYTES)
            # `scope["state"]` is what `request.state` reads, and the routes in
            # `src/docs.py` are the only thing that looks for this key. Set
            # before the application runs, so the page is rendered with the
            # same value the header below announces.
            scope.setdefault("state", {})["csp_nonce"] = nonce

        secure = scope.get("scheme") == "https"

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                additions = self.policy.headers_for(
                    path=path, secure=secure, nonce=nonce
                )
                message = {
                    **message,
                    "headers": merge_headers(message.get("headers", []), additions),
                }
            await send(message)

        await self.app(scope, receive, send_with_headers)


__all__ = [
    "CONTENT_TYPE_OPTIONS_HEADER",
    "CSP_HEADER",
    "DOCUMENTATION_CSP",
    "FRAME_OPTIONS_HEADER",
    "HSTS_HEADER",
    "REFERRER_POLICY_HEADER",
    "HSTSPolicy",
    "SecurityHeadersMiddleware",
    "SecurityHeadersPolicy",
    "build_security_headers_policy",
    "get_security_headers_policy",
    "merge_headers",
]
