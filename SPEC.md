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
- [ ] Adapter pattern: `PaymentGateway` protocol with two concrete adapters
- [ ] Dependency inversion via FastAPI `Depends` + protocol-typed providers, overridable in tests

## Phase 7 — Concurrency & Data Integrity
- [ ] Idempotency middleware: `Idempotency-Key` + Redis dedupe with response replay
- [ ] Optimistic concurrency: SQLAlchemy `version_id_col` + `If-Match`/ETag, 412 on conflict
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
