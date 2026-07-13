# Spec: boilerplate-python-fastapi

> Spec-driven. Mark `[x]` only after pushing.

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
- [ ] Factory fixtures with `faker` + `pytest-factoryboy`
- [ ] Coverage: 80% threshold
- [ ] GitHub Actions: lint (ruff) → typecheck (mypy) → test → Docker push
- [ ] Multi-stage Dockerfile + docker-compose
