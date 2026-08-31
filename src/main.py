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
from src.outbox.factory import get_outbox_relay
from src.parallel.factory import get_cpu_pool
from src.sse.hub import event_stream_hub
from src.ws.rooms import room_registry


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    configure_logging(settings.LOG_LEVEL)
    # At start-up rather than at import, so that importing a module never
    # turns on a side effect and a unit test gets an empty bus by default.
    register_default_subscribers()
    # Builds the executor; the worker processes themselves are spawned lazily on
    # the first offload, so a deployment that never uses it pays nothing and the
    # first health check is not queued behind several interpreter start-ups.
    get_cpu_pool().start()
    # The relay drains committed outbox rows into the bus above, so it starts
    # after the subscribers are registered — a batch delivered to an empty bus
    # would count as delivered and be deleted. Off in a deployment that runs
    # its relays as separate processes; see src/config.py.
    if settings.OUTBOX_RELAY_ENABLED:
        get_outbox_relay().start()
    yield
    # First, and before the bus is cleared: the relay is the only thing still
    # publishing by now, and an event dispatched to a bus with no subscribers
    # is a *successful* delivery whose row is then deleted. Stopping it here
    # also lets its in-flight batch roll back, which releases the row locks and
    # leaves those events for the next process to claim.
    if settings.OUTBOX_RELAY_ENABLED:
        await get_outbox_relay().stop()
    event_bus.clear()
    # After the bus, so nothing is still fanning out into streams that are
    # being closed. Ending each one is a clean end of body the client
    # reconnects from — to another replica, if this one is being drained —
    # rather than the connection reset it gets when the process exits with the
    # sockets still open.
    event_stream_hub.close()
    # And the same for rooms, which are the other thing a publisher can still
    # be fanning out into. Emptying the registry does not close the sockets —
    # uvicorn's own shutdown does that, and each connection's `finally` handles
    # its own membership — but it stops a broadcast in flight from queueing
    # messages into connections that are being torn down.
    room_registry.close()
    # The idempotency store owns a connection pool. Closing it here rather than
    # leaving it to garbage collection keeps a reload from leaking sockets.
    await get_idempotency_store().close()
    # Same for the distributed lock backend. Building it here when nothing has
    # asked for one yet costs nothing: redis-py connects lazily, so an unused
    # backend closes a pool that never opened a socket.
    await get_lock_backend().close()
    # Last, and waiting: the pool owns child processes rather than sockets, and
    # a child killed mid-call leaves a half-written result nobody reads. Waiting
    # here is what makes SIGTERM an orderly drain instead of a truncation, and
    # it costs nothing when nothing is in flight.
    await get_cpu_pool().shutdown()


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
