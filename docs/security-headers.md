# Response security headers

Four headers on every response, one of which is conditional:

| Header | Value | Why |
| --- | --- | --- |
| `Content-Security-Policy` | `default-src 'none'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'` | nothing a JSON response returns may load, frame, rebase or submit |
| `X-Content-Type-Options` | `nosniff` | a JSON body that a browser decides is HTML is a stored XSS |
| `Referrer-Policy` | `strict-origin-when-cross-origin` | path and query — which in an API carry ids — never leave the origin |
| `X-Frame-Options` | `DENY` | the clickjacking defence for browsers older than CSP Level 2 |
| `Strict-Transport-Security` | `max-age=31536000; includeSubDomains` | **HTTPS responses only** — see below |

The code is `src/middleware/security_headers.py` (`SecurityHeadersPolicy`, the
value; `SecurityHeadersMiddleware`, which applies it) and `src/docs.py` (the
documentation pages, re-registered so they can carry a nonce). Everything in
the table is configurable — `SECURITY_*` in `src/config.py` — except the
documentation policy, which is not a deployment choice.

## "It's a JSON API, it doesn't need a CSP"

Half right, and the half it gets wrong is the half that matters. It is true
that `default-src` has nothing to do on a response nobody renders. The other
three directives are not about loading subresources at all:

- `frame-ancestors 'none'` decides whether **this origin** can be framed. An
  API that sets a session cookie and answers a `GET` has a clickjacking
  surface whether or not it returns HTML.
- `base-uri 'none'` stops an injected `<base href>` from re-pointing every
  relative URL on a page — the one directive that survives a successful
  injection of markup you thought could not be rendered.
- `form-action 'none'` stops an injected form posting somewhere else.

And `default-src 'none'` costs nothing on the responses that do not need it,
while covering the ones you forget: a file download, an error page, an HTML
fragment some future endpoint returns.

## Every response, including the ones the application did not write

`SecurityHeadersMiddleware` is added **last** in `src/main.py`, and Starlette
runs the last-added middleware outermost. So a 404 that never reached a
handler, a 429 from the rate limiter, and a replay refused by
`IdempotencyMiddleware` all leave through its `send`.

One response escapes even that, and it is the one most worth covering.
Starlette's stack is:

```
ServerErrorMiddleware  →  user middleware  →  ExceptionMiddleware  →  router
```

A handler registered for bare `Exception` — `unhandled_exception_handler` here
— is installed on `ServerErrorMiddleware`, which is *outside* every middleware
`add_middleware` can reach. Its 500 envelope therefore never passes through
this middleware, however early the middleware is added. That is why
`SecurityHeadersPolicy` is a frozen value with a `headers_for()` method rather
than logic inside `__call__`: `src/exception_handlers.py` stamps that one
response from the same policy object.

`tests/test_security_headers_app.py` asserts the headers on a 200, a 404, a
401, a documentation page and that 500. Neutering the handler's call leaves
the other seven green.

## HSTS is conditional, and the condition is a scope key

RFC 6797 §7.2 requires a user agent to **ignore** `Strict-Transport-Security`
arriving over plain HTTP, so sending it there achieves nothing — and on
`http://localhost:8000` it would be actively harmful, pinning a developer's
browser to HTTPS for `localhost` across every other project on the machine,
for a year, with no UI to undo it.

The condition is `scope["scheme"] == "https"`. Behind a TLS-terminating load
balancer the application never sees TLS, so that key comes from
`X-Forwarded-Proto` via uvicorn's `--proxy-headers` (on by default, and
`--forwarded-allow-ips` decides which peers are trusted to set it). A
deployment that turns proxy headers off, or does not trust its ingress, gets
**no HSTS at all** and nothing says so. Check it from outside:

```console
$ curl -sI https://api.example.com/health | grep -i strict-transport
strict-transport-security: max-age=31536000; includeSubDomains
```

### Why `preload` raises instead of warning

The browser preload list requires `includeSubDomains` and a `max-age` of at
least one year. A header missing either is still *well-formed* — browsers
honour it, `curl` shows it, and nothing in the running system objects. The
only feedback is a rejected submission, weeks later, by a person.

`HSTSPolicy.__post_init__` refuses both combinations, so the misconfiguration
is a failed start-up instead. It also accepts `max-age=0`, which is not a
mistake: it is how a host tells browsers to *forget* the pin, and it is the
only way back out of HSTS.

## The documentation pages and the nonce

`/docs`, `/docs/oauth2-redirect` and `/redoc` are HTML, and each contains an
inline `<script>` — the Swagger UI bootstrap, the OAuth2 redirect handler.
Inline script is the thing a CSP exists to refuse, which leaves three options:

1. Let the pages have no meaningful policy (`script-src 'unsafe-inline'`).
2. Turn the pages off in production and accept that they are unprotected
   everywhere they are on.
3. Mint a nonce per response, put it in the markup and in the header.

This repository takes the third, and doing so means owning the routes: the
value has to exist before the page is rendered, so FastAPI cannot be the one
rendering it. `src/main.py` builds the app with `docs_url=None` /
`redoc_url=None` and includes `build_docs_router()` instead. The HTML is still
FastAPI's — `get_swagger_ui_html` and friends are called unchanged — and the
only edit is a `nonce` attribute added to every inline `<script>` tag.

The resulting policy, one directive per thing the pages actually load:

```
default-src 'none';
script-src 'self' https://cdn.jsdelivr.net 'nonce-<per response>';
style-src  'self' https://cdn.jsdelivr.net https://fonts.googleapis.com 'unsafe-inline';
font-src   'self' https://fonts.gstatic.com;
img-src    'self' data: blob: https://fastapi.tiangolo.com;
connect-src 'self';
worker-src 'self' blob:;
frame-ancestors 'none'; base-uri 'none'; form-action 'none'
```

`style-src` is the one loose directive, and it is loose **because** it carries
no nonce. Under CSP Level 3 a nonce or hash in a directive makes the browser
ignore `'unsafe-inline'` in that same directive — so noncing the single
`<style>` block ReDoc ships in its markup would break every style its renderer
adds afterwards, which is most of them. Swagger UI's inline `style=`
attributes are in the same position. Either the directive allows inline styles
or the pages do not render; it does not allow inline *script*, which is what
the nonce buys.

### The gate

`src/docs.py` adds the nonce with a regex over `<script` tags that have no
`src`, rather than a literal `b"<script>"` replacement, so
`<script type="module">` in a future FastAPI release is still covered. The
invariant is asserted directly:
`tests/test_docs_pages.py::test_every_inline_script_is_nonced` renders all
three pages, reads the nonce out of the response's own CSP header, and fails
if any inline script lacks it. A FastAPI upgrade that changes that markup
breaks the test suite rather than the documentation UI.

## What is deliberately not here

- **`Cross-Origin-Resource-Policy` / `Cross-Origin-Opener-Policy`.** Both are
  worth setting on an origin that serves documents; on an API consumed
  cross-origin, `CORP: same-origin` also blocks embedding patterns some
  clients legitimately use, and the call depends on who the consumers are.
  Add them per deployment.
- **`Permissions-Policy`.** It governs browser features on a *document*. The
  three documents this application serves are its own documentation pages,
  and none of them ask for a camera.
- **CORS.** A different mechanism answering a different question — who may
  read a response — and not a response-hardening header. Add
  `CORSMiddleware` when there is a browser origin to allow; it belongs
  outside `SecurityHeadersMiddleware` so preflight responses are covered too.
- **A report-only mode.** `Content-Security-Policy-Report-Only` earns its
  keep when an existing site is being migrated onto a policy. Here the policy
  ships with the first response and there is nothing to migrate.
