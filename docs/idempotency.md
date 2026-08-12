# Idempotent requests

A client that never hears back from a `POST` has no safe move. Retrying may
charge the card twice; not retrying may lose the order. `Idempotency-Key` is
the client's half of the fix — a token it keeps stable across retries of *one*
logical request — and `IdempotencyMiddleware` is the server's half: the first
request carrying a key executes and has its response stored, and every later
request carrying the same key is answered from that store without the route
running again.

- Contract and helpers: `src/idempotency/base.py`
- Backends: `src/idempotency/memory.py`, `src/idempotency/redis_store.py`
- Wire format: `src/idempotency/codec.py`
- Selection: `src/idempotency/factory.py`
- Middleware: `src/middleware/idempotency.py`

## Using it as a client

```http
POST /api/v1/orders HTTP/1.1
Idempotency-Key: 3f1c2b7e-2c19-4a4f-8a1b-1d9f0c7f5f11
Content-Type: application/json

{"sku": "abc", "quantity": 2}
```

Retry with the *same* key and the same body. The replay carries
`Idempotency-Replayed: true`; both the original and the replay echo
`Idempotency-Key`. Generate a fresh UUID per logical request, not per attempt —
a key reused for a different payload is refused, and a new key per attempt
defeats the whole mechanism.

## What happens

| Situation | Result |
| --- | --- |
| No `Idempotency-Key` header | Request runs normally. Nothing is stored. |
| Safe method, or a path outside `/api/` | Ignored — see *Scope* below. |
| Malformed key | `400 IDEMPOTENCY_KEY_INVALID` |
| First request with this key | Runs. Response stored and returned. |
| Identical request, first still running | `409 IDEMPOTENCY_KEY_IN_PROGRESS` + `Retry-After: 1` |
| Identical request, first finished | Stored response replayed, `Idempotency-Replayed: true` |
| Same key, different method/path/query/body/content-type | `422 IDEMPOTENCY_KEY_REUSED` |
| Store unreachable | `503 IDEMPOTENCY_STORE_UNAVAILABLE`, or served undeduplicated if `IDEMPOTENCY_FAIL_OPEN` |

The 409 is a refusal rather than a queued wait: holding the second request open
until the first finishes turns a double-submit into two occupied workers, and
the client must handle a retry-later answer anyway.

The 422 for reuse follows the `Idempotency-Key` header draft. Answering the
second request with the first one's response would hand a caller a result for a
payload it never sent, which is worse than an error.

## Scope

`POST`, `PATCH`, `PUT` and `DELETE` under `/api/`. `GET`/`HEAD`/`OPTIONS` are
already idempotent, and storing their responses would be an HTTP cache — a
different feature with different invalidation rules. Health probes and the
OAuth redirect handlers sit outside `/api/` and are untouched.

The header is *optional*. A route that must not be retried without one should
enforce that itself, as a dependency; a middleware that required it would break
every existing client at once.

## Requests are matched on a fingerprint, not just the key

The fingerprint is a SHA-256 over method, path, query string, `Content-Type`
and body, each length-prefixed so `POST /a/b` with an empty body cannot collide
with `POST /a` whose body starts with `/b`.

Other headers are deliberately excluded. A retry through a different proxy,
with a fresh `X-Request-ID` or a re-issued bearer token, is still the same
request; fingerprinting the whole header set would turn every such retry into a
422. `Content-Type` is included because it changes how the same bytes are
parsed — identical bytes read as JSON and as form data are two requests.

## Keys are namespaced per caller

Keys are chosen by clients, so two callers will eventually pick the same one.
Records are stored under `sha256(Authorization)[:32] + ":" + key`, so one
caller can never be handed another's stored response. Hashing rather than
storing the credential keeps bearer tokens out of Redis keys and out of any log
line that echoes one.

Requests with no `Authorization` header share an `anon` namespace. They are
still protected by the fingerprint — a replay requires an identical method,
path, query, content type and body — but two genuinely different anonymous
callers who pick the same key *and* send byte-identical requests would share a
response. Anything that must not be shared that way needs a credential, which
every non-public route here already requires.

## What is not stored

- **5xx responses** and **408 / 425 / 429**. Storing one would pin a transient
  failure to the key for the whole record TTL, so the retry it invites would be
  answered with the very error being retried. These release the reservation
  instead, so the retry actually runs.
- **An exception from the handler.** The reservation is released before the
  exception propagates, including on `CancelledError` from a client that
  disconnected mid-flight. Otherwise the retry would meet a reservation nothing
  will ever complete.
- **`Set-Cookie`.** A session cookie is a credential minted for one caller;
  under the shared `anon` namespace a replay could hand it to another. Nothing
  here sets cookies on a keyed method — the OAuth routes that do are `GET`s.
- **Responses over `IDEMPOTENCY_MAX_BODY_BYTES`.** Returned normally, not
  stored, reservation released, so a retry re-executes.

2xx *and* deterministic 4xx are stored. A 404 or a 409 is an outcome, and
replaying it is the point.

## Requests larger than the cap

The body has to be buffered to be fingerprinted, so the cap is a memory bound.
A request over it is passed straight through **unprotected** rather than
rejected — refusing a large upload because it carried an optional header would
be a worse failure than not deduplicating it. The event is logged as
`idempotency.request_too_large`. Raise `IDEMPOTENCY_MAX_BODY_BYTES` if large
requests genuinely need protection, and remember that the ceiling is per
in-flight request.

## Two TTLs

`IDEMPOTENCY_RESERVATION_TTL_SECONDS` (60s) bounds how long a *reservation* may
sit unfinished. If the worker holding one is killed mid-request, nothing will
ever complete or release that key, and a 24h TTL would answer every retry with
409 until it expired. Set it above the slowest request this API serves — a
reservation that expires under a still-running request lets a retry execute
alongside it.

`IDEMPOTENCY_TTL_SECONDS` (24h) is how long a *completed* response stays
replayable. That is a question about the client's retry window, not the
server's.

## When the store is down

`IDEMPOTENCY_FAIL_OPEN` is `false` by default: the request is refused with 503
rather than executed without protection, because for anything that moves money
a double charge is worse than an outage. Set it to `true` where losing the
request is worse than executing it twice.

One case is deliberately *not* a failure: if `complete` fails after the
response has already been sent, the client keeps its normal response and the
next retry re-executes. That is the pre-idempotency behaviour, not a new
failure mode, and there is nothing left to fail on a response already on the
wire.

## Position in the middleware stack

Starlette runs the last-added middleware outermost. This one is added *before*
`RequestIDMiddleware` in `src/main.py`, so it runs **inside** it:

```
RequestIDMiddleware  →  IdempotencyMiddleware  →  SessionMiddleware  →  routes
```

Both consequences are intended. Idempotency log lines carry the *replaying*
request's `request_id`, so a replay is traceable to the retry that asked for
it. And `X-Request-ID` is stamped after this middleware returns, so a replayed
response carries a fresh id rather than pointing a support engineer at the
original request. That header is therefore never part of a stored record.

## Backends

`IDEMPOTENCY_BACKEND=redis` in any real deployment. Redis's
`SET key value NX EX ttl` is one round trip that both claims a key and refuses
to claim it twice, which is exactly the atomic reservation the contract needs;
a `get`-then-`set` pair would let two simultaneous retries both find nothing
and both execute.

`memory` exists for tests and single-process development. It is per-process: a
retry served by another worker executes a second time. The factory logs a
warning when it is selected outside `test`/`development`.

Redis is shared with Celery here and separated by key prefix
(`idempotency:…`). `IDEMPOTENCY_REDIS_URL` overrides `REDIS_URL` for
deployments that want a different instance or database number.

## Adding a backend

Implement the five methods of `IdempotencyStore` — `reserve`, `complete`,
`release`, `get`, `close` — register it in `src/idempotency/factory.py`, and
add its name to the `store` fixture params in
`tests/test_idempotency_contract.py`. That suite is what enforces the
atomicity promise; a backend that only passes its own tests has only proved
things about itself.

## What this is not

Idempotency is not a transaction. The response is stored *after* the handler
returns, so a handler that commits and then crashes before the middleware
stores anything will re-execute on retry. Making the effect itself idempotent —
a unique constraint on a client-supplied reference, an upsert — is what closes
that gap, and this middleware is what makes the retry that hits it cheap and
observable. The transactional outbox later in Phase 7 is the durable half of
the same story.
