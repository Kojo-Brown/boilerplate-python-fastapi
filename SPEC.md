# Spec: boilerplate-python-fastapi

> Spec-driven. Mark `[x]` only after pushing.

## Phase 0 — Green Baseline (blocks all feature work)
- [x] Verify every pinned version exists on PyPI, then commit a resolved `uv.lock`
- [x] Get `uv sync`, `ruff check`, `mypy`, and `pytest` all passing locally from a clean clone
- [x] Promote `workflow-templates/ci.yml` to `.github/workflows/ci.yml` and confirm it runs green on a PR
- [x] Confirm the app imports and starts against a real Postgres in CI

## Phase 1 — Foundation
- [x] FastAPI 0.138 + Python 3.14 project with `uv` package manager
- [x] Pydantic v2 settings with `.env` validation
- [x] SQLAlchemy 2.0 async engine + Alembic migrations
- [x] PostgreSQL schema: User, RefreshToken models
- [x] Structured logging with `structlog`

## Phase 2 — Auth
- [x] JWT auth: register, login, access + refresh token rotation (python-jose)
- [x] Argon2 password hashing (`argon2-cffi`)
- [x] OAuth 2.0 Google flow (authlib)
- [x] Rate limiting with `slowapi`
- [x] Dependency injectors: `get_current_user`, `require_role`

## Phase 3 — API Design
- [x] Versioned router: `/api/v1/`
- [x] Generic `Page[T]` cursor pagination response model
- [x] Custom exception handler → consistent JSON errors
- [x] Request ID middleware + structured request logging

## Phase 4 — Data Layer
- [x] Repository pattern: `BaseRepository[T]` with async SQLAlchemy
- [x] Async background tasks with `asyncio` + FastAPI `BackgroundTasks`
- [x] Celery + Redis task queue example (email sending)
- [x] S3 file upload helper with presigned URLs (boto3)

## Phase 5 — Testing & DevOps
- [x] Pytest + HTTPX async test client
- [x] Factory fixtures with `faker` + `pytest-factoryboy`
- [x] Coverage: 80% threshold
- [x] GitHub Actions: lint (ruff) → typecheck (mypy) → test → Docker push
- [x] Multi-stage Dockerfile + docker-compose

## Phase 6 — SOLID & Design Patterns
- [x] SOLID audit with before/after refactors documented in `docs/solid.md` — audit in `docs/solid.md`; the LSP/ISP finding it turned up (bare `ValueError` as the universal auth failure signal) fixed by routing auth failures through domain exceptions (PR #25)
- [x] Factory pattern: `StorageFactory` returning S3/local/memory backends via `Protocol` — `StorageBackend` protocol in `src/storage/base.py` with no boto3, filesystem or settings import; `S3Storage`/`LocalStorage`/`MemoryStorage` implement it structurally and one parametrised contract suite runs against all three. Presigned URLs are deliberately left off the protocol — only S3 can mint them, so putting them there would trade an OCP violation for an LSP one — and stay on `S3Storage`, which is why `/api/v1/uploads` is unchanged. Closes finding 5 of `docs/solid.md` (PR #26)
- [x] Strategy pattern: pluggable `NotificationStrategy` chosen per user preference — `NotificationStrategy` protocol in `src/notifications/base.py` over `Notification`/`Recipient`/`NotificationResult`, free of SQLAlchemy, httpx and settings; `email`, `webhook` and `none` implement it structurally and one parametrised contract suite runs against all three. `Recipient` is deliberately not the `User` row — `recipient_from_user` is the only code aware of both, so a webhook body can never be assembled from an object carrying the password hash. The channel comes from `users.notification_channel` (migration 0003): a plain string rather than an enum, so adding a channel stays a `register` call instead of a migration plus a deploy-order problem; unknown values raise rather than silently defaulting, empty ones fall back. Webhook deliveries are HMAC-signed over `{timestamp}.{body}` so a capture cannot be replayed by rewriting the header, and user-supplied URLs are SSRF-checked — the one gap, a hostname that resolves privately, is documented and asserted by a test rather than implied away (PR #27)
- [x] Decorator pattern: `@cached`, `@retry`, `@timed` decorators preserving signatures via `ParamSpec` — in `src/decorators`, documented in `docs/decorators.md`, 100% line coverage. Signatures survive both halves of the round trip: a `ParamSpec` on every `__call__` for mypy, `functools.update_wrapper` for `inspect.signature`, which is how FastAPI builds a route's parameters. Each is a factory that must be called (`@timed()`, not `@timed`) — the bare form needs an implementation signature loose enough to take a function or nothing, which erases the `ParamSpec` link. `@retry` re-raises the original exception rather than a `RetryError`, because this API derives status codes from exception types and a wrapper would turn a 409 into a 500 on the third attempt; it also refuses to retry `CancelledError` even under `on=BaseException`, and checks exhaustion before `should_retry` so caller code is never run for a decision that no longer exists. `@cached` collapses concurrent misses behind a refcounted per-key lock and never caches a failure; it is per-process, and the docs say so before they say anything else. Two bugs the composition tests found were fixed rather than documented around: `@retry` over `@cached` took the sync branch and silently retried nothing (an `AsyncCachedFunction` is a callable object, not a coroutine function), and decorating a `functools.partial` raised `AttributeError` for a missing `__qualname__` (PR #28)
- [x] Observer pattern: async domain event bus with typed subscribers — `src/events`, documented in `docs/events.md`, 100% line coverage. `subscribe(event_type, handler)` is checked by mypy in both directions, and dispatch walks the MRO, so an audit log subscribing to `UserEvent` is an ordinary subscriber rather than a special case in `publish`. `publish` awaits its subscribers: `create_task` would drop exceptions nothing holds and outlive the request scope whose context it borrowed, so work that should not be paid for inline is enqueued to Celery from *inside* a subscriber. Subscribers run concurrently and fail independently — a broken mail queue cannot fail a registration that already committed — while cancellation propagates rather than being recorded as eight spurious failures, and a handler that merely overran its `timeout` is a failure. Every publish in `AuthService` sits after the commit so nothing reacts to a transaction that then rolls back, and nested publishes are counted in a `ContextVar` because a cycle here is a request that never returns. Subscribers are registered from the lifespan, not at import, so importing a module never starts sending mail. Two bugs the tests found were fixed rather than documented around: a `Subscription` compared field-wise, letting one from a *different* bus unsubscribe a local handler (identity handles now), and structlog reserves the `event` kwarg, so every publish raised `TypeError` until the log keys became `event_name`. The bus is in-process and forgets; the transactional outbox in Phase 7 is the durable answer (PR #29)
- [x] Adapter pattern: `PaymentGateway` protocol with two concrete adapters — `PaymentGateway` protocol in `src/payments/base.py`, free of httpx, any provider SDK and settings; `StripeGateway` and `PayPalGateway` translate it to two APIs that agree on almost nothing (form encoding vs JSON, integer minor units vs decimal strings, a static key vs a cached OAuth2 token, `Idempotency-Key` vs `PayPal-Request-Id`, a 402 `card_error` vs a 422 `INSTRUMENT_DECLINED`), and one parametrised contract suite runs against both. This is the adapter pattern rather than another strategy: the two are not one idea implemented twice but existing APIs this codebase does not control, so translation *is* the work. `Money` is integer minor units plus a currency-exponent table and refuses an unlisted currency rather than defaulting to two decimals — the default is wrong in both directions at once, turning the same ¥1000 into ¥10 at Stripe and ¥100,000 at PayPal. A decline is a 402 distinct from the 503 that means retry is safe, and the reverse mistake is tested too, since reporting our own malformed request as a declined card sends shoppers to their bank over our bug. PayPal's `charge` returns the *capture* id, not the order id the response leads with, because only a capture can be refunded — an order id would work all the way through checkout and fail at the first refund — and capture status wins over order status, an order reading COMPLETED while its capture is still PENDING being the difference between money that exists and money that does not. `ChargeRequest.reference` is required rather than optional because an optional idempotency key is one nobody passes. No retry or circuit breaker (Phase 9 owns that; the idempotency keys are what make adding it safe), no vaulting/subscriptions/webhook verification on the protocol (Stripe HMACs the raw body, PayPal fetches a certificate chain — adapter-specific, as presigned URLs are on `S3Storage`), and no route, which is the next item (PR #30)
- [x] Dependency inversion via FastAPI `Depends` + protocol-typed providers, overridable in tests — `AuthService` now takes four protocols (`UserStore`, `RefreshTokenStore`, `UnitOfWork`, `EventPublisher`) instead of building `UserRepository(db)` and holding a whole `AsyncSession` for two methods, which closes findings 4 (DIP) and 6 (ISP) of `docs/solid.md` — the same seam, cut once. `src/dependencies.py` is the composition root: every provider is annotated with the protocol and returns the implementation, so mypy fails the build the day one drifts, and the concrete names appear nowhere else in the request path — `src/auth/router.py` no longer imports `get_db` at all. `Depends` alone injects without inverting; the protocols are what make the substitution possible. There is no unit-of-work adapter class because `AsyncSession` already satisfies `UnitOfWork` structurally, and no `rollback` on it because nothing calls one. `EventPublisher.publish` returns `object` rather than `PublishResult`, since every caller here discards it on purpose. The three DB-backed providers each take the session through `Depends(get_db)`, which FastAPI caches per request: load-bearing rather than an optimisation, since three sessions would have the unit of work committing a transaction the repositories never wrote to, and a registration answering 201 while persisting nothing. The protocols still name the SQLAlchemy models, a bounded compromise documented in place — the cost of a concrete model is that a fake must construct one, where the cost of a concrete repository was that a fake had to *be* a session. `tests/fakes.py` is the payoff: the auth, OAuth and event suites now run the real service over in-memory stores, seeded by what exists rather than by the order the service happens to query in — the stub they replaced answered `execute()` from a `side_effect` list, so adding a lookup shifted every later answer onto the wrong question in silence. A fitness function parses the imports of `src/auth/service.py` and `src/auth/router.py`, so the one-line regression that reintroduces finding 4 fails a test instead of passing review. No payments route: charging a card needs the `Idempotency-Key` middleware that opens Phase 7, and a charge endpoint that dedupes nothing turns a retried request into a second charge — `PaymentGatewayDep` is wired and waiting for it. Documented in `docs/dependency-injection.md` (PR #31)

## Phase 7 — Concurrency & Data Integrity
- [x] Idempotency middleware: `Idempotency-Key` + Redis dedupe with response replay — `IdempotencyStore` protocol in `src/idempotency/base.py` with Redis and in-memory backends behind one contract suite, and the ASGI middleware in `src/middleware/idempotency.py`; documented in `docs/idempotency.md`, 100% line coverage on every new module bar the protocol stubs. Middleware rather than a dependency because a dependency runs inside the route — it can see the request but not the response, and the response is what has to be replayed — and cannot cover the endpoint someone forgets to decorate, which is the one that will double-charge. Redis's `SET NX EX` is the reservation: a `get`-then-`set` pair lets two simultaneous retries both find nothing and both execute, so the contract suite measures that with 20 concurrent reservations against a real server rather than asserting it. Two TTLs, because one value cannot be both — a reservation expires in 60s so a worker killed mid-request cannot answer its own retries with 409 for a day, while a completed record lives 24h, which is a question about the client's retry window. Keys are namespaced by `sha256(Authorization)`: keys are client-chosen, so two callers will eventually pick the same one and the second would otherwise be handed the first's response. 5xx, 408, 425 and 429 release the reservation instead of being stored, since pinning a transient failure to a key answers the retry it invites with the very error being retried; an exception releases it too, `CancelledError` included. `Set-Cookie` is stripped from stored responses. Reuse is checked before in-progress so a client bug is a 422 rather than 409 or 422 depending on timing, and the store failing is a 503 by default because serving the request anyway is the double execution the caller asked to be protected from. Added *before* `RequestIDMiddleware` so it runs inside it: idempotency logs carry the replaying request's id and a replay is stamped with a fresh `X-Request-ID`, asserted from outside. `render_app_exception` was split out of `app_exception_handler` because exception handlers are installed inside the middleware stack, so a short-circuit never reaches them and would otherwise grow a second error envelope. No route requires a key yet — that is a per-route dependency, not middleware policy — and idempotency is not a transaction: a handler that commits and then dies before the response is stored still re-executes, which the transactional outbox later in this phase is the durable answer to (PR #32)
- [x] Optimistic concurrency: SQLAlchemy `version_id_col` + `If-Match`/ETag, 412 on conflict — `src/concurrency` holds the HTTP half (`EntityTag`, `IfMatch`, `resource_version_tag`, free of SQLAlchemy), `User.__mapper_args__["version_id_col"]` the storage half, and `/api/v1/users/me` is where they meet; documented in `docs/optimistic-concurrency.md`, 100% line coverage on every new module. The precondition is checked twice deliberately: `require_match` is the check that fires in practice and is the only one that can name the current tag, while the `WHERE version = :loaded` SQLAlchemy appends is the one that is *sound* — between the comparison and the write there is a window only the database can adjudicate, and keeping just the first leaves a lost-update race no test issuing one request at a time would ever reveal. `If-Match` uses strong comparison (RFC 9110 §13.1.1), so `W/"…"` never satisfies it; a comma is a legal `etagc`, so the header is scanned against the grammar rather than split on commas; an unparseable precondition is a 400 rather than a silent pass, because ignoring it switches the protection off exactly when the client believed it was on; a missing one is 428 rather than a blind overwrite. The tag carries the row id as well as the version — every row is at version 1 when created and `/users/me` is one URI naming a different row per bearer token, so a bare `"1"` from one client would compare equal to someone else's row — and those responses are `Cache-Control: private, no-store` for the same reason. 412 is kept distinct from 409: one means re-read and retry, the other means repeating the request fails identically. `tests/test_optimistic_concurrency_db.py` runs two real transactions at one row against the CI Postgres (skipped only when `DATABASE_URL` is unreachable, as the Redis leg of the idempotency contract suite is) and found a real bug while being written: logging `user.version` inside the `StaleDataError` handler reloaded an expired attribute through the session the failed flush had already killed, turning the 412 into a 500. No `If-None-Match`/304 — that is a caching question, not a correctness one — and no decorator wrapping the pattern, since *which* tag is current differs per resource and a wrong guess fails open (PR #33)
- [ ] Pessimistic locking with `with_for_update()` and a deadlock-retry wrapper
- [ ] Distributed lock via Redis with fencing tokens, exposed as an async context manager
- [ ] GIL-bound work offloaded to `ProcessPoolExecutor`; IO-bound to `asyncio.gather` with semaphores
- [ ] Immutability: frozen Pydantic models, `Final`, and frozen dataclasses for value objects
- [ ] Transactional outbox: event row in the same session, async relay publisher
- [ ] `asyncio` structured concurrency: TaskGroup, cancellation, and timeout patterns

## Phase 8 — Streaming & Messaging
- [ ] `StreamingResponse` for large exports with backpressure-aware async generators
- [ ] Server-Sent Events endpoint with heartbeat and client disconnect detection
- [ ] WebSocket endpoint with JWT auth, rooms, and per-connection rate limiting
- [ ] Kafka producer/consumer (aiokafka) with consumer groups and manual commits
- [ ] Dead-letter queue with an exponential-backoff retry ladder
- [ ] Redis Streams consumer group with stalled-message claiming

## Phase 9 — Resilience & Observability
- [ ] Circuit breaker + retry with jitter on outbound httpx calls
- [ ] Bulkhead semaphores per dependency with hard timeouts
- [ ] OpenTelemetry traces/metrics/logs with W3C context propagation
- [ ] Prometheus RED metrics + a checked-in Grafana dashboard
- [ ] Liveness vs readiness probes with real dependency checks
- [ ] N+1 detection in tests + `selectinload`/`joinedload` tuning guide

## Phase 10 — Security Hardening
- [ ] Security headers middleware: CSP, HSTS, `X-Content-Type-Options`, referrer policy
- [ ] Refresh-token reuse detection with family revocation
- [ ] Field-level encryption at rest (AES-256-GCM) via a SQLAlchemy TypeDecorator
- [ ] PII redaction processor in the structlog pipeline
- [ ] HMAC-signed webhooks with constant-time compare and replay windows
- [ ] OWASP API Top 10 checklist with a test per mitigation
- [ ] Multi-tenancy with PostgreSQL row-level security and a tenant-scoped session

## Phase 11 — TDD & Advanced Testing
- [ ] TDD kata: one feature built red→green→refactor, one commit per step
- [ ] Mutation testing with `mutmut` + a CI threshold
- [ ] Property-based tests with Hypothesis for serializers and pagination
- [ ] Testcontainers-backed integration tests against real Postgres + Redis
- [ ] Locust load test with a latency budget asserted in CI
