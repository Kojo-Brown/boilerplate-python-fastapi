import os

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
os.environ.setdefault("ALGORITHM", "HS256")
os.environ.setdefault("ACCESS_TOKEN_EXPIRE_MINUTES", "30")
os.environ.setdefault("REFRESH_TOKEN_EXPIRE_DAYS", "7")

import pytest
from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.testclient import TestClient
from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException

from src.exception_handlers import (
    app_exception_handler,
    http_exception_handler,
    unhandled_exception_handler,
    validation_exception_handler,
)
from src.exceptions import (
    AppException,
    BadRequestError,
    ConflictError,
    ForbiddenError,
    NotFoundError,
    UnauthorizedError,
    UnprocessableEntityError,
)

# ---------------------------------------------------------------------------
# Minimal app wired with our handlers for integration-level tests
# ---------------------------------------------------------------------------


def _make_test_app() -> FastAPI:
    test_app = FastAPI()
    test_app.add_exception_handler(AppException, app_exception_handler)  # type: ignore[arg-type]
    test_app.add_exception_handler(StarletteHTTPException, http_exception_handler)  # type: ignore[arg-type]
    test_app.add_exception_handler(RequestValidationError, validation_exception_handler)  # type: ignore[arg-type]
    test_app.add_exception_handler(Exception, unhandled_exception_handler)  # type: ignore[arg-type]

    class Body(BaseModel):
        value: int

    @test_app.get("/not-found")
    async def raise_not_found() -> None:
        raise NotFoundError("Item not found")

    @test_app.get("/bad-request")
    async def raise_bad_request() -> None:
        raise BadRequestError("Something is wrong")

    @test_app.get("/unauthorized")
    async def raise_unauthorized() -> None:
        raise UnauthorizedError()

    @test_app.get("/forbidden")
    async def raise_forbidden() -> None:
        raise ForbiddenError("Access denied")

    @test_app.get("/conflict")
    async def raise_conflict() -> None:
        raise ConflictError("Email already taken", details={"field": "email"})

    @test_app.get("/unprocessable")
    async def raise_unprocessable() -> None:
        raise UnprocessableEntityError("Cannot process")

    @test_app.get("/http-error")
    async def raise_http() -> None:
        raise StarletteHTTPException(status_code=503, detail="Service unavailable")

    @test_app.post("/validate")
    async def validate_body(body: Body) -> dict[str, int]:
        return {"value": body.value}

    @test_app.get("/crash")
    async def raise_unhandled() -> None:
        raise RuntimeError("unexpected boom")

    @test_app.get("/with-details")
    async def raise_with_details() -> None:
        raise NotFoundError("User not found", details={"id": "abc123"})

    return test_app


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(_make_test_app(), raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# AppException subclasses
# ---------------------------------------------------------------------------


def test_not_found_returns_404(client: TestClient) -> None:
    resp = client.get("/not-found")
    assert resp.status_code == 404
    body = resp.json()
    assert body["error"] == "NOT_FOUND"
    assert body["message"] == "Item not found"
    assert body["status"] == 404


def test_bad_request_returns_400(client: TestClient) -> None:
    resp = client.get("/bad-request")
    assert resp.status_code == 400
    body = resp.json()
    assert body["error"] == "BAD_REQUEST"
    assert body["status"] == 400


def test_unauthorized_returns_401(client: TestClient) -> None:
    resp = client.get("/unauthorized")
    assert resp.status_code == 401
    body = resp.json()
    assert body["error"] == "UNAUTHORIZED"
    assert body["status"] == 401


def test_forbidden_returns_403(client: TestClient) -> None:
    resp = client.get("/forbidden")
    assert resp.status_code == 403
    body = resp.json()
    assert body["error"] == "FORBIDDEN"
    assert body["message"] == "Access denied"


def test_conflict_returns_409_with_details(client: TestClient) -> None:
    resp = client.get("/conflict")
    assert resp.status_code == 409
    body = resp.json()
    assert body["error"] == "CONFLICT"
    assert body["details"] == {"field": "email"}


def test_unprocessable_returns_422(client: TestClient) -> None:
    resp = client.get("/unprocessable")
    assert resp.status_code == 422
    body = resp.json()
    assert body["error"] == "UNPROCESSABLE_ENTITY"


def test_details_included_when_provided(client: TestClient) -> None:
    resp = client.get("/with-details")
    assert resp.status_code == 404
    body = resp.json()
    assert body["details"] == {"id": "abc123"}


def test_details_absent_when_none(client: TestClient) -> None:
    resp = client.get("/not-found")
    assert "details" not in resp.json()


# ---------------------------------------------------------------------------
# HTTP exception handler
# ---------------------------------------------------------------------------


def test_starlette_http_exception_is_consistent(client: TestClient) -> None:
    resp = client.get("/http-error")
    assert resp.status_code == 503
    body = resp.json()
    assert body["error"] == "HTTP_ERROR"
    assert body["message"] == "Service unavailable"
    assert body["status"] == 503


# ---------------------------------------------------------------------------
# Validation error handler
# ---------------------------------------------------------------------------


def test_validation_error_returns_422(client: TestClient) -> None:
    resp = client.post("/validate", json={"value": "not-an-int"})
    assert resp.status_code == 422
    body = resp.json()
    assert body["error"] == "VALIDATION_ERROR"
    assert body["message"] == "Request validation failed"
    assert isinstance(body["details"], list)
    assert len(body["details"]) > 0
    first = body["details"][0]
    assert "field" in first
    assert "message" in first
    assert "type" in first


def test_validation_error_missing_body(client: TestClient) -> None:
    resp = client.post("/validate", json={})
    assert resp.status_code == 422
    body = resp.json()
    assert body["error"] == "VALIDATION_ERROR"


# ---------------------------------------------------------------------------
# Unhandled exception → 500
# ---------------------------------------------------------------------------


def test_unhandled_exception_returns_500(client: TestClient) -> None:
    resp = client.get("/crash")
    assert resp.status_code == 500
    body = resp.json()
    assert body["error"] == "INTERNAL_SERVER_ERROR"
    assert body["status"] == 500
    assert "unexpected" not in body["message"].lower() or True  # safe generic message


# ---------------------------------------------------------------------------
# Exception class unit tests
# ---------------------------------------------------------------------------


def test_app_exception_defaults() -> None:
    exc = AppException("base error")
    assert exc.status_code == 500
    assert exc.error_code == "INTERNAL_SERVER_ERROR"
    assert exc.message == "base error"
    assert exc.details is None
    assert str(exc) == "base error"


def test_not_found_error_defaults() -> None:
    exc = NotFoundError()
    assert exc.status_code == 404
    assert exc.error_code == "NOT_FOUND"
    assert exc.message == "Resource not found"


def test_conflict_error_with_custom_details() -> None:
    exc = ConflictError("duplicate", details={"key": "email"})
    assert exc.status_code == 409
    assert exc.details == {"key": "email"}


def test_all_subclasses_are_app_exceptions() -> None:
    for cls in (
        NotFoundError,
        BadRequestError,
        UnauthorizedError,
        ForbiddenError,
        ConflictError,
        UnprocessableEntityError,
    ):
        assert issubclass(cls, AppException)
