"""`GET /api/v1/exports/users`, as an administrator's client sees it.

The source is faked, so what these measure is the route: who may call it, what
the framing looks like, which fields leave the building, and — the one that
would break everything else quietly — that the request's database session is
still open while the body is being produced.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncGenerator, AsyncIterator
from datetime import UTC, datetime
from typing import Annotated, Any
from unittest.mock import AsyncMock

import pytest
from fastapi import Depends
from httpx import ASGITransport, AsyncClient

from src.auth.dependencies import get_current_user
from src.config import settings
from src.database import get_db
from src.dependencies import get_user_export_source
from src.main import app
from src.models.user import User
from src.repositories.user import UserRepository
from src.streaming.ndjson import TERMINAL_KEY
from src.streaming.response import NDJSON_MEDIA_TYPE
from src.users.export import UserExportRecord, UserExportSource

ENDPOINT = "/api/v1/exports/users"


def _record(index: int, *, is_active: bool = True) -> UserExportRecord:
    return UserExportRecord(
        id=uuid.UUID(int=index),
        email=f"user{index}@example.com",
        role="user",
        is_active=is_active,
        is_verified=True,
        notification_channel="email",
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 2, tzinfo=UTC),
    )


class FakeExportSource:
    """A `UserExportSource` that needs no database, and can fail on cue."""

    def __init__(
        self,
        records: list[UserExportRecord],
        *,
        fail_after: int | None = None,
        error: Exception | None = None,
        events: list[str] | None = None,
    ) -> None:
        self.records = records
        self.calls: list[dict[str, object]] = []
        self._fail_after = fail_after
        self._error = error
        self._events = events if events is not None else []

    async def stream_export(
        self,
        *,
        batch_size: int,
        active_only: bool,
    ) -> AsyncIterator[UserExportRecord]:
        self.calls.append({"batch_size": batch_size, "active_only": active_only})
        for index, record in enumerate(self.records):
            if self._fail_after is not None and index == self._fail_after:
                raise self._error or RuntimeError("the cursor died")
            if active_only and not record.is_active:
                continue
            self._events.append(f"row {index}")
            yield record


def _lines(body: bytes) -> list[dict[str, Any]]:
    return [json.loads(line) for line in body.splitlines()]


def _asgi_scope() -> dict[str, Any]:
    """A GET of the export endpoint, for tests that watch the ASGI messages."""
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": ENDPOINT,
        "raw_path": ENDPOINT.encode(),
        "root_path": "",
        "query_string": b"",
        "headers": [(b"host", b"test")],
        "client": ("127.0.0.1", 1234),
        "server": ("test", 80),
    }


async def _no_receive() -> dict[str, Any]:  # pragma: no cover - never awaited
    await asyncio.Event().wait()
    raise AssertionError("unreachable")


@pytest.fixture
def source() -> FakeExportSource:
    return FakeExportSource([_record(i) for i in range(3)])


@pytest.fixture
async def export_client(
    source: FakeExportSource,
    mock_admin: User,
    admin_headers: dict[str, str],
    mock_db: AsyncMock,
) -> AsyncGenerator[AsyncClient, None]:
    """An admin client whose export source is `source`, over a stubbed session.

    `get_user_export_source` is overridden with a provider that still declares
    `Depends(get_db)`, so the session is opened and closed exactly as it would
    be in production — which is what makes the lifetime test below mean
    something.
    """

    async def _override_db() -> AsyncGenerator[AsyncMock, None]:
        yield mock_db

    def _override_source(
        _db: Annotated[AsyncMock, Depends(get_db)],
    ) -> UserExportSource:
        return source

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[get_current_user] = lambda: mock_admin
    app.dependency_overrides[get_user_export_source] = _override_source

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers=admin_headers,
    ) as client:
        yield client

    app.dependency_overrides.clear()


class TestWhoMayCallIt:
    async def test_an_anonymous_caller_is_refused(
        self, async_client: AsyncClient
    ) -> None:
        assert (await async_client.get(ENDPOINT)).status_code == 401

    async def test_an_ordinary_user_is_refused(
        self, authenticated_client: AsyncClient
    ) -> None:
        """Reading your own profile and reading everyone's are not the same right."""
        response = await authenticated_client.get(ENDPOINT)

        assert response.status_code == 403
        assert response.json()["error"] == "FORBIDDEN"

    async def test_the_refusal_is_a_json_envelope_not_a_stream(
        self, authenticated_client: AsyncClient
    ) -> None:
        """Authorisation resolves before the response starts, so it can be a 403."""
        response = await authenticated_client.get(ENDPOINT)

        assert response.headers["content-type"].startswith("application/json")


class TestTheStream:
    async def test_it_is_an_ndjson_attachment(self, export_client: AsyncClient) -> None:
        response = await export_client.get(ENDPOINT)

        assert response.status_code == 200
        assert response.headers["content-type"].startswith(NDJSON_MEDIA_TYPE)
        assert (
            response.headers["content-disposition"]
            == 'attachment; filename="users.ndjson"'
        )
        assert response.headers["cache-control"] == "no-store"
        assert response.headers["x-accel-buffering"] == "no"

    async def test_every_record_then_the_terminal_record(
        self, export_client: AsyncClient
    ) -> None:
        lines = _lines((await export_client.get(ENDPOINT)).content)

        assert len(lines) == 4
        assert [line["email"] for line in lines[:3]] == [
            "user0@example.com",
            "user1@example.com",
            "user2@example.com",
        ]
        assert lines[-1] == {TERMINAL_KEY: "complete", "records": 3}

    async def test_an_empty_table_still_gets_a_terminal_record(
        self, export_client: AsyncClient, source: FakeExportSource
    ) -> None:
        source.records = []

        lines = _lines((await export_client.get(ENDPOINT)).content)

        assert lines == [{TERMINAL_KEY: "complete", "records": 0}]

    async def test_the_record_is_exactly_the_published_schema(
        self, export_client: AsyncClient
    ) -> None:
        first = _lines((await export_client.get(ENDPOINT)).content)[0]

        assert set(first) == set(UserExportRecord.model_fields)

    async def test_no_password_hash_leaves_the_building(
        self, export_client: AsyncClient
    ) -> None:
        """An export gets emailed around; the `users` row must not go with it."""
        body = (await export_client.get(ENDPOINT)).content

        assert b"hashed_password" not in body
        assert b"oauth_sub" not in body

    async def test_uuids_and_timestamps_are_json_scalars(
        self, export_client: AsyncClient
    ) -> None:
        first = _lines((await export_client.get(ENDPOINT)).content)[0]

        assert first["id"] == str(uuid.UUID(int=0))
        assert first["created_at"].startswith("2026-01-01T")

    async def test_the_body_arrives_in_more_than_one_piece(
        self, export_client: AsyncClient, source: FakeExportSource
    ) -> None:
        """A stream that is assembled before the first byte is not a stream.

        Driven as an ASGI call rather than through the client, because
        `httpx.ASGITransport` joins the body messages before handing them back
        — so no assertion made through it can tell a stream from a buffer.
        """
        source.records = [_record(i) for i in range(2000)]
        sent: list[bytes] = []

        async def send(message: dict[str, Any]) -> None:
            if message["type"] == "http.response.body" and message.get("body"):
                sent.append(message["body"])

        await app(_asgi_scope(), _no_receive, send)

        assert len(sent) > 1
        assert all(chunk.endswith(b"\n") for chunk in sent)
        assert _lines(b"".join(sent))[-1]["records"] == 2000


class TestQueryParameters:
    async def test_the_batch_size_comes_from_settings(
        self, export_client: AsyncClient, source: FakeExportSource
    ) -> None:
        await export_client.get(ENDPOINT)

        assert source.calls == [
            {"batch_size": settings.EXPORT_BATCH_ROWS, "active_only": False}
        ]

    async def test_active_only_reaches_the_source(
        self, export_client: AsyncClient, source: FakeExportSource
    ) -> None:
        await export_client.get(ENDPOINT, params={"active_only": "true"})

        assert source.calls == [
            {"batch_size": settings.EXPORT_BATCH_ROWS, "active_only": True}
        ]

    async def test_a_non_boolean_is_rejected_before_the_stream_starts(
        self, export_client: AsyncClient
    ) -> None:
        response = await export_client.get(ENDPOINT, params={"active_only": "maybe"})

        assert response.status_code == 422
        assert response.headers["content-type"].startswith("application/json")


class TestFailingHalfway:
    async def test_the_status_stays_200_and_the_body_says_what_happened(
        self, export_client: AsyncClient, source: FakeExportSource
    ) -> None:
        source.records = [_record(i) for i in range(10)]
        source._fail_after = 4

        response = await export_client.get(ENDPOINT)

        assert response.status_code == 200
        lines = _lines(response.content)
        assert len(lines) == 5
        assert lines[-1] == {
            TERMINAL_KEY: "failed",
            "records": 4,
            "error": "INTERNAL_ERROR",
            "message": "The export stopped before it finished.",
        }

    async def test_a_client_can_tell_the_two_endings_apart(
        self, export_client: AsyncClient, source: FakeExportSource
    ) -> None:
        source.records = [_record(i) for i in range(10)]
        complete = _lines((await export_client.get(ENDPOINT)).content)[-1]
        source._fail_after = 2
        failed = _lines((await export_client.get(ENDPOINT)).content)[-1]

        assert complete[TERMINAL_KEY] == "complete"
        assert failed[TERMINAL_KEY] == "failed"


class TestResourceLifetime:
    async def test_the_session_is_closed_after_the_last_record_not_before(
        self, source: FakeExportSource, mock_admin: User, admin_headers: dict[str, str]
    ) -> None:
        """The contract the whole export rests on.

        FastAPI closes a `yield` dependency from an exit stack wrapped around
        the *ASGI call*, not around the handler, so the session outlives a
        streaming response's body. If that ever changes, every row after the
        first would be read through a closed session — so it is asserted here
        rather than assumed.
        """
        events: list[str] = []
        source._events = events
        source.records = [_record(i) for i in range(5)]

        async def _override_db() -> AsyncGenerator[AsyncMock, None]:
            events.append("session open")
            try:
                yield AsyncMock()
            finally:
                events.append("session closed")

        def _override_source(
            _db: Annotated[AsyncMock, Depends(get_db)],
        ) -> UserExportSource:
            return source

        app.dependency_overrides[get_db] = _override_db
        app.dependency_overrides[get_current_user] = lambda: mock_admin
        app.dependency_overrides[get_user_export_source] = _override_source
        try:
            async with AsyncClient(
                transport=ASGITransport(app=app),
                base_url="http://test",
                headers=admin_headers,
            ) as client:
                await client.get(ENDPOINT)
        finally:
            app.dependency_overrides.clear()

        assert events[0] == "session open"
        assert events[-1] == "session closed"
        assert events.count("row 4") == 1
        assert events.index("row 4") < events.index("session closed")

    async def test_no_producer_task_survives_the_request(
        self, export_client: AsyncClient
    ) -> None:
        await export_client.get(ENDPOINT)

        assert not [
            task for task in asyncio.all_tasks() if "readahead" in task.get_name()
        ]


class TestConformance:
    def test_the_repository_satisfies_the_port(self) -> None:
        assert isinstance(UserRepository(AsyncMock()), UserExportSource)

    def test_the_fake_satisfies_the_same_port(self) -> None:
        assert isinstance(FakeExportSource([]), UserExportSource)

    def test_the_media_type_is_documented(self) -> None:
        """A client generator has to know this is not `application/json`."""
        schema = app.openapi()["paths"][ENDPOINT]["get"]

        assert NDJSON_MEDIA_TYPE in schema["responses"]["200"]["content"]
