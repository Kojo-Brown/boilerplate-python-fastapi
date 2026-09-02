"""Protocol-typed providers — the one place concrete classes get named.

This module is the composition root. Every provider below is annotated with a
*protocol* and returns an *implementation*, which is what inverts the
dependency: `src/auth/router.py` asks for an `AuthService`, `AuthService` asks
for a `UserStore`, and neither of them mentions `UserRepository`, `AsyncSession`
or `EventBus`. Those names appear here and nowhere else in the request path, so
substituting one is an edit to a single file — or, in a test, to
`app.dependency_overrides`.

The annotated aliases (`UserStoreDep`, `AuthServiceDep`, …) are the form route
handlers should use:

    @router.post("/register")
    async def register(data: RegisterRequest, service: AuthServiceDep) -> UserResponse:
        return await service.register(data)

`Annotated[X, Depends(f)]` beats `x: X = Depends(f)` for the ordinary reason
that a parameter with a default cannot precede one without, so the older style
forces every handler to order its parameters around FastAPI instead of around
what it takes.

## Overriding in tests

FastAPI resolves overrides by the *callable* used in `Depends`, so the target is
the provider function, not the alias:

    app.dependency_overrides[get_user_store] = lambda: InMemoryUserStore()

Override the narrowest provider that removes what you are trying to avoid.
Replacing `get_auth_service` skips the service's own wiring and tests the route;
replacing `get_user_store` and `get_unit_of_work` keeps the real service and
removes only the database, which is what most of the auth tests want. See
`tests/fakes.py` for stores that need neither a session nor an event loop.

## One session per request

`get_user_store`, `get_refresh_token_store`, `get_unit_of_work` and
`get_event_publisher` each declare `Depends(get_db)`, and all four receive the
*same* session: FastAPI caches a dependency's result for the duration of a
request, keyed on the callable. That caching is load-bearing rather than an
optimisation — separate sessions would mean the unit of work committing a
transaction that the repositories never wrote to, so a registration would return
201 and persist nothing. The publisher joined that list when events became
outbox rows, and for it the stake is the same one in a different place: a row
written through a second session is a notification that commits separately from
the change it describes, which is the entire failure the outbox exists to
remove. Any provider added here that touches the database must take it through
`get_db` for the same reason.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Header
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.dependencies import get_current_user
from src.auth.service import AuthService
from src.concurrency import IfMatch
from src.database import get_db
from src.distributed_lock.base import LockBackend
from src.distributed_lock.factory import get_lock_backend
from src.events.base import EventPublisher
from src.kafka.base import MessagePublisher
from src.kafka.factory import get_message_publisher
from src.models.user import User
from src.outbox.publisher import OutboxPublisher
from src.parallel.cpu import CpuPool
from src.parallel.factory import get_cpu_pool
from src.payments.base import PaymentGateway
from src.payments.registry import get_payment_gateway
from src.repositories.protocols import RefreshTokenStore, UserStore
from src.repositories.refresh_token import RefreshTokenRepository
from src.repositories.user import UserRepository
from src.sse.hub import EventStreamHub, event_stream_hub
from src.storage.base import StorageBackend
from src.storage.factory import get_storage
from src.unit_of_work import UnitOfWork
from src.users.export import UserExportSource
from src.users.service import ProfileService

DbSession = Annotated[AsyncSession, Depends(get_db)]


def get_user_store(db: DbSession) -> UserStore:
    """The user store backed by this request's session."""
    return UserRepository(db)


def get_refresh_token_store(db: DbSession) -> RefreshTokenStore:
    """The refresh-token store backed by this request's session."""
    return RefreshTokenRepository(db)


def get_unit_of_work(db: DbSession) -> UnitOfWork:
    """This request's transaction boundary.

    Returned unwrapped: `AsyncSession` satisfies `UnitOfWork` structurally, so
    an adapter here would forward two calls and hide which object the
    repositories are sharing.
    """
    return db


def get_event_publisher(db: DbSession) -> EventPublisher:
    """Publishing writes a row in *this request's* transaction.

    The session is the reason this provider takes a dependency at all. An
    outbox row is only worth anything if it commits with the state change that
    caused it, so the publisher has to write through the same session the
    repositories do — which it does for the same reason they share one, namely
    that FastAPI caches `Depends(get_db)` per request.

    The process-wide `EventBus` is no longer what a route publishes to. It is
    what the *relay* dispatches to once a row has committed, which is what
    keeps a subscriber from ever reacting to a transaction that rolled back
    while also keeping the reaction from being lost if this process dies. See
    `docs/outbox.md`.

    Tests that want to assert on published events override this with
    `CollectingPublisher`, or with a real `OutboxPublisher` over a sink of
    their own.
    """
    return OutboxPublisher(db)


def get_if_match(
    if_match: Annotated[list[str] | None, Header(alias="If-Match")] = None,
) -> IfMatch:
    """Parse the request's `If-Match`, or record that there wasn't one.

    Declared as a list because a header field may legally arrive as several
    field lines, which RFC 9110 §5.3 says to treat as one comma-separated
    value. Taking a `str` here would silently read only the first of them and
    ignore tags the client sent — the sort of bug that shows up as an
    unexplained 412 for one client library and never for the others.

    A malformed value raises `MalformedPreconditionError` (400) from inside
    dependency resolution, which reaches the same `AppException` handler as
    everything else, so the error envelope is the usual one.
    """
    if not if_match:
        return IfMatch.absent()
    return IfMatch.parse(", ".join(if_match))


def get_user_export_source(db: DbSession) -> UserExportSource:
    """Bulk reads of the user table, over this request's session.

    A second, narrower port onto the same repository as `get_user_store`. They
    are separate because their consumers are: authentication wants one user at
    a time and an export wants all of them, and a fake for either should not
    have to implement the other's method. See `src/users/export.py`.

    It shares the request's session by way of `Depends(get_db)`, so the cursor
    it opens lives exactly as long as the request does — which for a streaming
    response is until the last chunk has been sent, since FastAPI's exit stack
    is closed by a middleware wrapped around the whole ASGI call rather than
    around the handler. `tests/test_export_users.py` asserts that ordering
    directly, because the entire export breaks quietly if it ever changes.
    """
    return UserRepository(db)


def get_event_stream_hub() -> EventStreamHub:
    """The process-wide fan-out hub open SSE streams subscribe to.

    Returns the module singleton rather than building one: the whole point of a
    hub is that the publisher and the subscriber find the same registry, and
    two of them in one process is a stream that never receives anything.
    """
    return event_stream_hub


def get_profile_service(
    uow: Annotated[UnitOfWork, Depends(get_unit_of_work)],
) -> ProfileService:
    """Profile reads and conditional writes over this request's transaction."""
    return ProfileService(uow)


def get_auth_service(
    users: Annotated[UserStore, Depends(get_user_store)],
    tokens: Annotated[RefreshTokenStore, Depends(get_refresh_token_store)],
    uow: Annotated[UnitOfWork, Depends(get_unit_of_work)],
    events: Annotated[EventPublisher, Depends(get_event_publisher)],
) -> AuthService:
    """Assemble the authentication service for this request."""
    return AuthService(users=users, tokens=tokens, uow=uow, events=events)


CurrentUserDep = Annotated[User, Depends(get_current_user)]
IfMatchDep = Annotated[IfMatch, Depends(get_if_match)]
ProfileServiceDep = Annotated[ProfileService, Depends(get_profile_service)]
UserStoreDep = Annotated[UserStore, Depends(get_user_store)]
UserExportSourceDep = Annotated[UserExportSource, Depends(get_user_export_source)]
RefreshTokenStoreDep = Annotated[RefreshTokenStore, Depends(get_refresh_token_store)]
UnitOfWorkDep = Annotated[UnitOfWork, Depends(get_unit_of_work)]
EventPublisherDep = Annotated[EventPublisher, Depends(get_event_publisher)]
AuthServiceDep = Annotated[AuthService, Depends(get_auth_service)]

# Storage and payments already had provider functions next to their factories,
# where the caching lives. Aliased here so a handler has one import for its
# dependencies, and so the set of things a route may ask for is visible in one
# place. Both are `lru_cache`d and process-wide, not per-request: they own HTTP
# connection pools, and rebuilding one per request would throw the pool away.
StorageDep = Annotated[StorageBackend, Depends(get_storage)]
PaymentGatewayDep = Annotated[PaymentGateway, Depends(get_payment_gateway)]
# The *backend*, not a lock: which name to take, for how long, and whether to
# wait for it are properties of the section being protected, so a handler
# builds its own `DistributedLock` around this. A provider that returned an
# already-acquired lock would have to take the lock during dependency
# resolution, which is before the handler exists to be protected.
LockBackendDep = Annotated[LockBackend, Depends(get_lock_backend)]
# Process-wide and started by the lifespan, so this is the same instance for
# every request — see `get_cpu_pool` for why one per request would be a bug
# rather than an inefficiency. A handler asks for the pool and offloads its own
# call; there is no provider that returns a *result*, because what to run and
# how long to allow it are properties of the work, not of the pool.
CpuPoolDep = Annotated[CpuPool, Depends(get_cpu_pool)]
# Process-wide, and it has to be: a hub per request would give every stream its
# own registry, so a published event would reach nobody. Provided through a
# dependency rather than imported into the route so a test can override it with
# its own hub instead of publishing into the global one and hoping the teardown
# runs — the same reason `EventBus` can be constructed per test.
EventStreamHubDep = Annotated[EventStreamHub, Depends(get_event_stream_hub)]
# Process-wide and started by the lifespan when KAFKA_ENABLED is on. One
# publisher per request would pay a metadata fetch and a connection handshake
# per record and would batch nothing, which is why this is cached rather than
# built here. There is no consumer dependency beside it: a consumer is a
# background loop with a lifetime of its own, not something a request borrows.
MessagePublisherDep = Annotated[MessagePublisher, Depends(get_message_publisher)]

__all__ = [
    "AuthServiceDep",
    "CpuPoolDep",
    "EventStreamHubDep",
    "CurrentUserDep",
    "DbSession",
    "EventPublisherDep",
    "IfMatchDep",
    "LockBackendDep",
    "MessagePublisherDep",
    "PaymentGatewayDep",
    "ProfileServiceDep",
    "RefreshTokenStoreDep",
    "StorageDep",
    "UnitOfWorkDep",
    "UserExportSourceDep",
    "UserStoreDep",
    "get_auth_service",
    "get_cpu_pool",
    "get_event_publisher",
    "get_if_match",
    "get_message_publisher",
    "get_profile_service",
    "get_refresh_token_store",
    "get_unit_of_work",
    "get_user_export_source",
    "get_user_store",
]
