# boilerplate-python-fastapi

> FastAPI 0.138 · Python 3.14 · SQLAlchemy 2.0 · PostgreSQL · Alembic · Pydantic v2

Async Python API starter with full auth, migrations, and DevOps.

## Stack

| Layer | Tech |
|-------|------|
| Framework | FastAPI 0.138 |
| Language | Python 3.14 |
| ORM | SQLAlchemy 2.0 (async) |
| Migrations | Alembic |
| Auth | JWT + OAuth 2.0 (python-jose, authlib) |
| Hashing | Argon2 (argon2-cffi) |
| Validation | Pydantic v2 |
| Package mgr | uv |
| Testing | Pytest + HTTPX |

## Quick Start

```bash
git clone https://github.com/Kojo-Brown/boilerplate-python-fastapi.git
cd boilerplate-python-fastapi
uv sync
cp .env.example .env
docker-compose up postgres -d
uv run alembic upgrade head
uv run fastapi dev src/main.py  # http://localhost:8000/docs
```

## Spec Progress
See [SPEC.md](./SPEC.md).
