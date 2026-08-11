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

`get_user_store`, `get_refresh_token_store` and `get_unit_of_work` each declare
`Depends(get_db)`, and all three receive the *same* session: FastAPI caches a
dependency's result for the duration of a request, keyed on the callable. That
caching is load-bearing rather than an optimisation — three separate sessions
would mean the unit of work committing a transaction that the repositories never
wrote to, so a registration would return 201 and persist nothing. Any provider
added here that touches the database must take it through `get_db` for the same
reason.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.service import AuthService
from src.database import get_db
from src.events.base import EventPublisher
from src.events.bus import event_bus
from src.payments.base import PaymentGateway
from src.payments.registry import get_payment_gateway
from src.repositories.protocols import RefreshTokenStore, UserStore
from src.repositories.refresh_token import RefreshTokenRepository
from src.repositories.user import UserRepository
from src.storage.base import StorageBackend
from src.storage.factory import get_storage
from src.unit_of_work import UnitOfWork

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


def get_event_publisher() -> EventPublisher:
    """The process-wide bus.

    Not cached per request and not built here: subscribers are registered once
    from the lifespan, and a bus constructed per request would have none of
    them. Tests that want to assert on published events override this with
    their own `EventBus` rather than registering against the global one and
    racing every other test.
    """
    return event_bus


def get_auth_service(
    users: Annotated[UserStore, Depends(get_user_store)],
    tokens: Annotated[RefreshTokenStore, Depends(get_refresh_token_store)],
    uow: Annotated[UnitOfWork, Depends(get_unit_of_work)],
    events: Annotated[EventPublisher, Depends(get_event_publisher)],
) -> AuthService:
    """Assemble the authentication service for this request."""
    return AuthService(users=users, tokens=tokens, uow=uow, events=events)


UserStoreDep = Annotated[UserStore, Depends(get_user_store)]
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

__all__ = [
    "AuthServiceDep",
    "DbSession",
    "EventPublisherDep",
    "PaymentGatewayDep",
    "RefreshTokenStoreDep",
    "StorageDep",
    "UnitOfWorkDep",
    "UserStoreDep",
    "get_auth_service",
    "get_event_publisher",
    "get_refresh_token_store",
    "get_unit_of_work",
    "get_user_store",
]
