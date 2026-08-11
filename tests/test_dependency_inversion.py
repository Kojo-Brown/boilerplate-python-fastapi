"""The seam itself: what the providers hand over, and what tests can replace.

Everything else in the suite benefits from dependency inversion. This file is
what fails when it stops working — a provider that starts returning a concrete
class the protocol does not cover, a second session appearing in one request, or
an import creeping back into `AuthService` that ties policy to SQLAlchemy again.
"""

from __future__ import annotations

import ast
import pathlib
from collections.abc import AsyncGenerator, Callable
from typing import Annotated, Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from src.auth.service import AuthService
from src.database import get_db
from src.dependencies import (
    RefreshTokenStoreDep,
    UnitOfWorkDep,
    UserStoreDep,
    get_auth_service,
    get_event_publisher,
    get_refresh_token_store,
    get_unit_of_work,
    get_user_store,
)
from src.events.base import EventPublisher
from src.events.bus import event_bus
from src.limiter import limiter
from src.main import app
from src.repositories.protocols import RefreshTokenStore, UserStore
from src.repositories.refresh_token import RefreshTokenRepository
from src.repositories.user import UserRepository
from src.unit_of_work import UnitOfWork
from tests.fakes import (
    CollectingPublisher,
    InMemoryRefreshTokenStore,
    InMemoryUserStore,
    RecordingUnitOfWork,
)

SRC = pathlib.Path(__file__).resolve().parent.parent / "src"


@pytest.fixture(autouse=True)
def reset_limiter() -> None:
    """Clear rate-limit counters so registering here cannot spend another
    test's budget, or have its own spent by one."""
    storage = limiter._limiter
    if hasattr(storage, "storage") and hasattr(storage.storage, "reset"):
        storage.storage.reset()


# --- conformance ---------------------------------------------------------
#
# `runtime_checkable` verifies that the methods exist and nothing about their
# signatures, so these are smoke tests. The signatures are checked statically:
# every provider is annotated with its protocol and returns an implementation,
# so mypy fails the build if one drifts. That check runs in CI, not here.


def test_the_repositories_satisfy_the_store_protocols() -> None:
    session = AsyncMock(spec=AsyncSession)

    assert isinstance(UserRepository(session), UserStore)
    assert isinstance(RefreshTokenRepository(session), RefreshTokenStore)


def test_the_fakes_satisfy_the_same_protocols() -> None:
    """A fake that no longer implements the port would fail tests for reasons
    that look like application bugs."""
    assert isinstance(InMemoryUserStore(), UserStore)
    assert isinstance(InMemoryRefreshTokenStore(), RefreshTokenStore)
    assert isinstance(RecordingUnitOfWork(), UnitOfWork)
    assert isinstance(CollectingPublisher(), EventPublisher)


def test_a_session_is_already_a_unit_of_work() -> None:
    """Which is why `get_unit_of_work` returns one unwrapped: an adapter class
    here would exist only to forward `flush` and `commit`."""
    assert isinstance(AsyncMock(spec=AsyncSession), UnitOfWork)


def test_the_event_publisher_is_the_process_wide_bus() -> None:
    """Subscribers are registered once from the lifespan. A bus built per
    request would have none of them, and nothing would notice until a welcome
    email stopped being sent."""
    assert get_event_publisher() is event_bus


# --- wiring --------------------------------------------------------------


async def test_one_request_gets_one_session() -> None:
    """The stores and the unit of work must share a session.

    Three separate sessions would mean committing a transaction the repositories
    never wrote to: a registration would answer 201 and persist nothing. FastAPI
    caches `Depends(get_db)` per request, which is what makes this hold — this
    test is here because that is a property of the framework's behaviour, not of
    anything visible in `src/dependencies.py`.
    """
    probe = FastAPI()
    session = AsyncMock(spec=AsyncSession)

    async def _override_db() -> AsyncGenerator[AsyncSession, None]:
        yield session

    probe.dependency_overrides[get_db] = _override_db

    seen: dict[str, object] = {}

    @probe.get("/probe")
    async def _probe(
        users: UserStoreDep,
        tokens: RefreshTokenStoreDep,
        uow: UnitOfWorkDep,
    ) -> dict[str, bool]:
        # Reaching for `.session` is reaching past the protocol on purpose: the
        # question is which concrete object the wiring handed over, and only the
        # implementation can answer it.
        seen["users"] = users.session  # type: ignore[attr-defined]
        seen["tokens"] = tokens.session  # type: ignore[attr-defined]
        seen["uow"] = uow
        return {"ok": True}

    async with AsyncClient(
        transport=ASGITransport(app=probe), base_url="http://test"
    ) as client:
        assert (await client.get("/probe")).status_code == 200

    assert seen["users"] is session
    assert seen["tokens"] is session
    assert seen["uow"] is session


async def test_the_providers_build_the_real_implementations() -> None:
    session = AsyncMock(spec=AsyncSession)

    assert isinstance(get_user_store(session), UserRepository)
    assert isinstance(get_refresh_token_store(session), RefreshTokenRepository)
    assert get_unit_of_work(session) is session


# --- overriding ----------------------------------------------------------


async def test_a_route_runs_the_real_service_over_fakes(
    fake_backed_client: AsyncClient,
    user_store: InMemoryUserStore,
    uow: RecordingUnitOfWork,
) -> None:
    """The payoff. `AuthService` is the production class; only what it talks to
    is substituted, and no database is involved at any point."""
    response = await fake_backed_client.post(
        "/api/v1/auth/register",
        json={"email": "new@example.com", "password": "password123"},
    )

    assert response.status_code == 201
    assert response.json()["email"] == "new@example.com"
    assert [u.email for u in user_store.users] == ["new@example.com"]
    assert uow.commits == 1


async def test_overriding_the_service_never_touches_the_database() -> None:
    """`get_db` opens a real connection pool. If overriding `get_auth_service`
    left it in the resolved tree, these tests would be quietly talking to
    whatever `DATABASE_URL` points at."""
    service = AuthService(
        users=InMemoryUserStore(),
        tokens=InMemoryRefreshTokenStore(),
        uow=RecordingUnitOfWork(),
        events=CollectingPublisher(),
    )

    async def _explode() -> AsyncGenerator[AsyncSession, None]:
        raise AssertionError("get_db was resolved despite the service override")
        yield  # pragma: no cover - unreachable, keeps this a generator

    app.dependency_overrides[get_auth_service] = lambda: service
    app.dependency_overrides[get_db] = _explode

    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post(
                "/api/v1/auth/register",
                json={"email": "nodb@example.com", "password": "password123"},
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 201


def _override_stores_with(store: InMemoryUserStore) -> None:
    """Point the app's own `get_auth_service` at in-memory collaborators.

    Every override is a lambda, including the ones that would work as bare
    classes today. FastAPI inspects whatever it is handed as a dependency
    callable, so overriding with a class makes its `__init__` parameters into
    request parameters — `InMemoryUserStore(users=...)` becomes a query field
    of type `list[User] | None`, and the app fails to build its response model
    rather than failing the assertion you wrote.
    """
    app.dependency_overrides[get_user_store] = lambda: store
    app.dependency_overrides[get_refresh_token_store] = lambda: (
        InMemoryRefreshTokenStore()
    )
    app.dependency_overrides[get_unit_of_work] = lambda: RecordingUnitOfWork()
    app.dependency_overrides[get_event_publisher] = lambda: CollectingPublisher()


async def test_a_store_can_be_swapped_under_the_real_service() -> None:
    """The narrower override: keep the app's own `get_auth_service`, replace
    only the store beneath it. This is what lets a test exercise the wiring and
    the policy together while still holding the seeded data."""
    store = InMemoryUserStore()

    _override_stores_with(store)

    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post(
                "/api/v1/auth/register",
                json={"email": "swapped@example.com", "password": "password123"},
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 201
    assert [u.email for u in store.users] == ["swapped@example.com"]


async def test_the_dependency_is_resolved_per_request_not_captured() -> None:
    """An override installed after the app was built must still take effect.

    `Annotated[..., Depends(f)]` records the callable and resolves it on every
    request, so this holds. Capturing a provider's *result* at import time —
    a default argument, say — would make the first override silently the last.
    """
    first, second = InMemoryUserStore(), InMemoryUserStore()

    async def _register(store: InMemoryUserStore) -> int:
        _override_stores_with(store)
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post(
                "/api/v1/auth/register",
                json={"email": "twice@example.com", "password": "password123"},
            )
        return response.status_code

    try:
        assert await _register(first) == 201
        assert await _register(second) == 201
    finally:
        app.dependency_overrides.clear()

    assert len(first.users) == 1
    assert len(second.users) == 1


async def test_an_unoverridden_provider_still_reaches_the_session() -> None:
    """The inverse of the override tests: with only `get_db` replaced, the real
    providers are what run, so a stub session still drives the whole path."""
    session = AsyncMock(spec=AsyncSession)
    # A `SELECT` that finds nothing: `revoke` then answers False, and the
    # service commits regardless, which is the call this test is about.
    empty = MagicMock()
    empty.scalar_one_or_none.return_value = None
    session.execute = AsyncMock(return_value=empty)

    async def _override_db() -> AsyncGenerator[AsyncSession, None]:
        yield session

    app.dependency_overrides[get_db] = _override_db

    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post(
                "/api/v1/auth/logout", json={"refresh_token": "mock-refresh-token"}
            )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 204
    session.execute.assert_awaited_once()
    session.commit.assert_awaited_once()


# --- the seam stays open -------------------------------------------------


def _imported_modules(path: pathlib.Path) -> set[str]:
    tree = ast.parse(path.read_text())
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    return modules


@pytest.mark.parametrize("module", ["auth/service.py", "auth/router.py"])
def test_policy_does_not_import_persistence(module: str) -> None:
    """A fitness function, not a style rule.

    The finding this item closes was `AuthService` constructing
    `UserRepository(db)` — one line, easily written again, and nothing else in
    the suite would fail if it were. Both files are allowed to name the
    protocols and `src.dependencies`; neither may name SQLAlchemy or a concrete
    repository.
    """
    forbidden = {
        "sqlalchemy",
        "sqlalchemy.ext.asyncio",
        "src.database",
        "src.repositories.base",
        "src.repositories.user",
        "src.repositories.refresh_token",
    }
    assert _imported_modules(SRC / module) & forbidden == set()


def test_the_composition_root_is_the_only_place_that_names_them() -> None:
    """`src/dependencies.py` is where the concrete classes are allowed to
    appear, and the test above is only meaningful if they still appear
    somewhere — otherwise a rename could empty the seam and pass both."""
    named = _imported_modules(SRC / "dependencies.py")

    assert "src.repositories.user" in named
    assert "src.repositories.refresh_token" in named
    assert "src.database" in named


def test_annotated_dependencies_are_exported_for_handlers() -> None:
    """Route handlers should reach for the alias, not rebuild `Depends(...)`.

    An alias is only a shorthand if it stays in step with its provider, so this
    pins each one to the callable it wraps.
    """
    pairs: list[tuple[Any, Callable[..., object]]] = [
        (UserStoreDep, get_user_store),
        (RefreshTokenStoreDep, get_refresh_token_store),
        (UnitOfWorkDep, get_unit_of_work),
        (Annotated[AuthService, Depends(get_auth_service)], get_auth_service),
    ]
    for alias, provider in pairs:
        depends = alias.__metadata__[0]
        assert isinstance(depends, type(Depends(provider)))
        assert depends.dependency is provider
