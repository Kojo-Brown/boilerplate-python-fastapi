"""The two providers, and the token claim routes depend on.

Most of this package is invisible to a route handler by design, so what is
left to test here is small: the dependency that turns "no tenant" into a 400
rather than an empty page, and the fact that a login mints a token carrying
the tenant of the row it authenticated rather than of the request that asked.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from jose import jwt

from src.auth.service import AuthService
from src.auth.utils import (
    InvalidAccessTokenError,
    create_access_token,
    hash_password,
    verify_access_token,
)
from src.config import settings
from src.exception_handlers import app_exception_handler
from src.exceptions import AppException
from src.models.tenant import DEFAULT_TENANT_ID
from src.models.user import User
from src.tenancy.context import tenant_scope
from src.tenancy.dependencies import (
    CurrentTenantDep,
    OptionalTenantDep,
    get_current_tenant_id,
    get_optional_tenant_id,
)
from src.tenancy.errors import TenantRequiredError
from src.tenancy.middleware import TenantContextMiddleware
from tests.fakes import (
    CollectingPublisher,
    InMemoryRefreshTokenStore,
    InMemoryUserStore,
    RecordingUnitOfWork,
)

ALPHA = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")
HEADER = "X-Tenant-ID"


class TestProviders:
    def test_the_required_provider_returns_the_tenant_in_scope(self) -> None:
        with tenant_scope(ALPHA):
            assert get_current_tenant_id() == ALPHA

    def test_the_required_provider_raises_outside_one(self) -> None:
        with tenant_scope(None), pytest.raises(TenantRequiredError):
            get_current_tenant_id()

    def test_the_optional_provider_returns_none_outside_one(self) -> None:
        with tenant_scope(None):
            assert get_optional_tenant_id() is None


def _app() -> FastAPI:
    app = FastAPI()
    app.add_exception_handler(AppException, app_exception_handler)  # type: ignore[arg-type]
    app.add_middleware(TenantContextMiddleware, header_name=HEADER, trust_header=True)

    @app.get("/required")
    async def required(tenant_id: CurrentTenantDep) -> dict[str, str]:
        return {"tenant_id": str(tenant_id)}

    @app.get("/optional")
    async def optional(tenant_id: OptionalTenantDep) -> dict[str, str | None]:
        return {"tenant_id": str(tenant_id) if tenant_id else None}

    return app


class TestThroughARoute:
    async def test_a_scoped_request_reaches_the_handler(self) -> None:
        async with AsyncClient(
            transport=ASGITransport(app=_app()), base_url="http://test"
        ) as client:
            response = await client.get("/required", headers={HEADER: str(ALPHA)})
        assert response.status_code == 200
        assert response.json() == {"tenant_id": str(ALPHA)}

    async def test_an_unscoped_request_is_a_400_in_the_usual_envelope(self) -> None:
        """Raised during dependency resolution, so it reaches the handler for
        `AppException` like any other failure."""
        async with AsyncClient(
            transport=ASGITransport(app=_app()), base_url="http://test"
        ) as client:
            response = await client.get("/required")
        assert response.status_code == 400
        assert response.json()["error"] == "TENANT_REQUIRED"

    async def test_the_optional_provider_tolerates_an_unscoped_request(self) -> None:
        async with AsyncClient(
            transport=ASGITransport(app=_app()), base_url="http://test"
        ) as client:
            response = await client.get("/optional")
        assert response.status_code == 200
        assert response.json() == {"tenant_id": None}

    async def test_a_provider_is_overridable_without_faking_a_request(self) -> None:
        """Which is the reason it takes no parameters — see its docstring."""
        app = _app()
        app.dependency_overrides[get_current_tenant_id] = lambda: ALPHA
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get("/required")
        assert response.json() == {"tenant_id": str(ALPHA)}


class TestTheTokenClaim:
    def test_a_minted_token_round_trips_its_tenant(self) -> None:
        subject = str(uuid.uuid4())
        token = create_access_token(subject, "u@example.com", "user", DEFAULT_TENANT_ID)
        assert verify_access_token(token).tenant_id == DEFAULT_TENANT_ID

    def test_a_token_minted_without_one_carries_no_tid(self) -> None:
        token = create_access_token(str(uuid.uuid4()), "u@example.com", "user")
        assert verify_access_token(token).tenant_id is None

    @pytest.mark.parametrize("tid", ["not-a-uuid", 42, ["a"]])
    def test_a_signed_token_with_an_unusable_tid_is_refused(self, tid: object) -> None:
        """Refused rather than read as absent.

        Falling back to `None` would send the resolver to the caller-supplied
        header, which is the one case where a malformed claim *widens* what
        the request can reach — so a token this API signed and cannot route is
        an authentication failure instead.
        """
        token = jwt.encode(
            {
                "sub": str(uuid.uuid4()),
                "type": "access",
                "exp": datetime.now(UTC) + timedelta(minutes=5),
                "tid": tid,
            },
            settings.SECRET_KEY,
            algorithm=settings.ALGORITHM,
        )
        with pytest.raises(InvalidAccessTokenError):
            verify_access_token(token)

    async def test_login_puts_the_row_s_tenant_in_the_token(self) -> None:
        """Not the request's tenant.

        The row is the only value that is true by construction: under the
        policy it could not have been read from any other tenant. The service
        is driven directly, with a user whose `tenant_id` is deliberately
        *not* the one in scope, so that reading the ambient tenant instead
        would produce a different claim and fail this.
        """
        # Deliberately unmistakable rather than realistic. A plausible-looking
        # password in a fixture is a secret scanner's finding on every pull
        # request that touches the file, and the test needs a string the
        # hasher accepts, not a strong one.
        password = "mock-password-not-a-real-credential"
        user = User(
            id=uuid.uuid4(),
            tenant_id=ALPHA,
            email="claim@example.com",
            hashed_password=hash_password(password),
            is_active=True,
            is_verified=True,
            role="user",
        )
        service = AuthService(
            users=InMemoryUserStore([user]),
            tokens=InMemoryRefreshTokenStore(),
            uow=RecordingUnitOfWork(),
            events=CollectingPublisher(),
        )

        with tenant_scope(DEFAULT_TENANT_ID):
            tokens = await service.login("claim@example.com", password)

        assert verify_access_token(tokens.access_token).tenant_id == ALPHA
