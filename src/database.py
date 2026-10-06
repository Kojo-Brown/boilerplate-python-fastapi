from collections.abc import AsyncGenerator

from sqlalchemy import MetaData, event, text
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from src.config import settings
from src.tenancy.binding import bind_tenant_on_begin
from src.tenancy.sql import CREATE_CURRENT_TENANT_FUNCTION

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.ENVIRONMENT == "development",
    pool_pre_ping=True,
)

AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


# Every transaction this engine opens carries the task's tenant as its first
# statement, when there is one. Registered here rather than in the lifespan so
# that importing the module is enough — a Celery worker, a one-off script and
# the test suite all reach this engine without ever running `lifespan`, and an
# unbound transaction is not an error that surfaces, it is a query that
# silently returns nothing. See src/tenancy/binding.py.
bind_tenant_on_begin(engine)


class Base(DeclarativeBase):
    pass


# `users.tenant_id` and `refresh_tokens.tenant_id` declare
# `server_default=text("app_current_tenant_id()")`, so the function has to
# exist before `create_all` emits those `CREATE TABLE`s. On a migrated
# database this is redundant — Alembic 0008 creates the same function, and
# `CREATE OR REPLACE` makes running both harmless — but a schema built
# straight from the mappers would otherwise fail on an undefined function, and
# the failure would be in a test fixture rather than anywhere near the cause.
@event.listens_for(Base.metadata, "before_create")
def _create_current_tenant_function(
    target: MetaData, connection: Connection, **_: object
) -> None:
    # Guarded on the dialect because the statement is Postgres-specific and a
    # SQLite metadata (a doc example, a scratch script) should not fail on it.
    if connection.dialect.name != "postgresql":
        return
    connection.execute(text(CREATE_CURRENT_TENANT_FUNCTION))


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session
