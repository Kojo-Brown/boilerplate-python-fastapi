from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from slowapi.errors import RateLimitExceeded
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.sessions import SessionMiddleware

from src.config import settings
from src.distributed_lock.factory import get_lock_backend
from src.events.bus import event_bus
from src.events.subscribers import register_default_subscribers
from src.exception_handlers import (
    app_exception_handler,
    http_exception_handler,
    rate_limit_exceeded_handler,
    unhandled_exception_handler,
    validation_exception_handler,
)
from src.exceptions import AppException
from src.health import router as health_router
from src.idempotency.factory import get_idempotency_store
from src.limiter import limiter
from src.logging_config import configure_logging
from src.middleware.idempotency import IdempotencyConfig, IdempotencyMiddleware
from src.middleware.request_id import RequestIDMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    configure_logging(settings.LOG_LEVEL)
    # At start-up rather than at import, so that importing a module never
    # turns on a side effect and a unit test gets an empty bus by default.
    register_default_subscribers()
    yield
    # Nothing is in flight by the time this runs — publish awaits its
    # subscribers — so dropping the registrations is all shutdown needs.
    event_bus.clear()
    # The idempotency store owns a connection pool. Closing it here rather than
    # leaving it to garbage collection keeps a reload from leaking sockets.
    await get_idempotency_store().close()
    # Same for the distributed lock backend. Building it here when nothing has
    # asked for one yet costs nothing: redis-py connects lazily, so an unused
    # backend closes a pool that never opened a socket.
    await get_lock_backend().close()


app = FastAPI(
    title="boilerplate-python-fastapi",
    version="0.1.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, rate_limit_exceeded_handler)  # type: ignore[arg-type]
app.add_exception_handler(AppException, app_exception_handler)  # type: ignore[arg-type]
app.add_exception_handler(StarletteHTTPException, http_exception_handler)  # type: ignore[arg-type]
app.add_exception_handler(RequestValidationError, validation_exception_handler)  # type: ignore[arg-type]
app.add_exception_handler(Exception, unhandled_exception_handler)
app.add_middleware(SessionMiddleware, secret_key=settings.SECRET_KEY)
# Added before RequestIDMiddleware, which means it runs *inside* it: Starlette
# runs the last-added middleware outermost. That ordering is deliberate — see
# the module docstring in src/middleware/idempotency.py — so that idempotency
# logs carry the replaying request's id and a replayed response is stamped with
# a fresh X-Request-ID rather than the original request's.
app.add_middleware(
    IdempotencyMiddleware,
    store=get_idempotency_store(),
    config=IdempotencyConfig(
        max_request_body_bytes=settings.IDEMPOTENCY_MAX_BODY_BYTES,
        max_response_body_bytes=settings.IDEMPOTENCY_MAX_BODY_BYTES,
        fail_open=settings.IDEMPOTENCY_FAIL_OPEN,
        enabled=settings.IDEMPOTENCY_ENABLED,
    ),
)
app.add_middleware(RequestIDMiddleware)


app.include_router(health_router)

from src.api.v1.router import v1_router  # noqa: E402

app.include_router(v1_router)
