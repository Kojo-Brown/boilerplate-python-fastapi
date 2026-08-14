"""`/api/v1/users/me`: the conditional-update protocol as clients see it.

Everything here runs against a stubbed session, so it measures *policy* — which
status code, which headers, whether the write was attempted at all. The claim
these tests cannot make is that the database really refuses a stale UPDATE;
that one needs a database, and lives in
`tests/test_optimistic_concurrency_db.py`.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest
from httpx import AsyncClient
from sqlalchemy.orm.exc import StaleDataError

from src.concurrency import resource_version_tag
from src.models.user import User

ENDPOINT = "/api/v1/users/me"


def tag_for(user: User, version: int | None = None) -> str:
    """The serialized ETag for `user`, optionally at a different version."""
    return resource_version_tag(
        user.id, user.version if version is None else version
    ).serialize()


class TestReadProfile:
    async def test_returns_the_callers_own_profile(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        response = await authenticated_client.get(ENDPOINT)

        assert response.status_code == 200
        assert response.json() == {
            "id": str(mock_user.id),
            "email": mock_user.email,
            "role": "user",
            "is_active": True,
            "is_verified": True,
            "notification_channel": "email",
            "notification_webhook_url": None,
        }

    async def test_carries_the_etag_to_edit_with(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        response = await authenticated_client.get(ENDPOINT)

        assert response.headers["etag"] == tag_for(mock_user)

    async def test_forbids_shared_caching(
        self, authenticated_client: AsyncClient
    ) -> None:
        """One URI, a different resource per token: no shared cache may keep it."""
        response = await authenticated_client.get(ENDPOINT)

        assert response.headers["cache-control"] == "private, no-store"

    async def test_requires_authentication(self, async_client: AsyncClient) -> None:
        assert (await async_client.get(ENDPOINT)).status_code == 401


class TestConditionalUpdate:
    async def test_applies_the_change_when_the_tag_is_current(
        self,
        authenticated_client: AsyncClient,
        mock_user: User,
        mock_db: AsyncMock,
    ) -> None:
        response = await authenticated_client.patch(
            ENDPOINT,
            json={"notification_channel": "none"},
            headers={"If-Match": tag_for(mock_user)},
        )

        assert response.status_code == 200
        assert response.json()["notification_channel"] == "none"
        assert mock_user.notification_channel == "none"
        mock_db.commit.assert_awaited_once()

    async def test_response_carries_the_tag_for_the_next_edit(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        response = await authenticated_client.patch(
            ENDPOINT,
            json={"notification_channel": "none"},
            headers={"If-Match": tag_for(mock_user)},
        )

        assert response.headers["etag"] == tag_for(mock_user)
        assert response.headers["cache-control"] == "private, no-store"

    async def test_wildcard_is_accepted(
        self, authenticated_client: AsyncClient, mock_db: AsyncMock
    ) -> None:
        response = await authenticated_client.patch(
            ENDPOINT,
            json={"notification_channel": "none"},
            headers={"If-Match": "*"},
        )

        assert response.status_code == 200
        mock_db.commit.assert_awaited_once()

    async def test_a_list_containing_the_current_tag_is_accepted(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        response = await authenticated_client.patch(
            ENDPOINT,
            json={"notification_channel": "none"},
            headers={"If-Match": f'"stale", {tag_for(mock_user)}'},
        )

        assert response.status_code == 200

    async def test_clearing_a_nullable_field_uses_an_explicit_null(
        self,
        authenticated_client: AsyncClient,
        mock_user: User,
    ) -> None:
        mock_user.notification_channel = "webhook"
        mock_user.notification_webhook_url = "https://hooks.example.com/u/1"

        response = await authenticated_client.patch(
            ENDPOINT,
            json={"notification_webhook_url": None},
            headers={"If-Match": tag_for(mock_user)},
        )

        assert response.status_code == 200
        assert response.json()["notification_webhook_url"] is None
        assert mock_user.notification_webhook_url is None

    async def test_sets_a_webhook_address(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        response = await authenticated_client.patch(
            ENDPOINT,
            json={
                "notification_channel": "webhook",
                "notification_webhook_url": "https://hooks.example.com/u/7",
            },
            headers={"If-Match": tag_for(mock_user)},
        )

        assert response.status_code == 200
        assert response.json() == {
            "id": str(mock_user.id),
            "email": mock_user.email,
            "role": "user",
            "is_active": True,
            "is_verified": True,
            "notification_channel": "webhook",
            "notification_webhook_url": "https://hooks.example.com/u/7",
        }

    async def test_omitting_a_field_leaves_it_alone(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        mock_user.notification_webhook_url = "https://hooks.example.com/u/1"

        response = await authenticated_client.patch(
            ENDPOINT,
            json={"notification_channel": "webhook"},
            headers={"If-Match": tag_for(mock_user)},
        )

        assert response.status_code == 200
        assert mock_user.notification_webhook_url == "https://hooks.example.com/u/1"


class TestPreconditionFailures:
    async def test_missing_if_match_is_428_and_writes_nothing(
        self, authenticated_client: AsyncClient, mock_db: AsyncMock
    ) -> None:
        response = await authenticated_client.patch(
            ENDPOINT, json={"notification_channel": "none"}
        )

        assert response.status_code == 428
        assert response.json()["error"] == "PRECONDITION_REQUIRED"
        mock_db.commit.assert_not_awaited()

    async def test_stale_tag_is_412_and_writes_nothing(
        self,
        authenticated_client: AsyncClient,
        mock_user: User,
        mock_db: AsyncMock,
    ) -> None:
        response = await authenticated_client.patch(
            ENDPOINT,
            json={"notification_channel": "none"},
            headers={"If-Match": tag_for(mock_user, version=mock_user.version - 1)},
        )

        assert response.status_code == 412
        assert response.json()["error"] == "PRECONDITION_FAILED"
        assert mock_user.notification_channel == "email"
        mock_db.commit.assert_not_awaited()

    async def test_412_names_the_current_tag_so_a_retry_needs_no_extra_read(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        response = await authenticated_client.patch(
            ENDPOINT,
            json={"notification_channel": "none"},
            headers={"If-Match": '"stale"'},
        )

        assert response.headers["etag"] == tag_for(mock_user)

    async def test_another_rows_tag_at_the_same_version_is_rejected(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        """What folding the id into the tag buys.

        Both rows are at version 1; a tag of `"1"` would have matched.
        """
        someone_else = resource_version_tag(uuid.uuid4(), mock_user.version)

        response = await authenticated_client.patch(
            ENDPOINT,
            json={"notification_channel": "none"},
            headers={"If-Match": someone_else.serialize()},
        )

        assert response.status_code == 412

    async def test_weak_tag_is_rejected(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        response = await authenticated_client.patch(
            ENDPOINT,
            json={"notification_channel": "none"},
            headers={"If-Match": f"W/{tag_for(mock_user)}"},
        )

        assert response.status_code == 412

    async def test_malformed_tag_is_400_rather_than_ignored(
        self, authenticated_client: AsyncClient, mock_db: AsyncMock
    ) -> None:
        response = await authenticated_client.patch(
            ENDPOINT,
            json={"notification_channel": "none"},
            headers={"If-Match": "not-a-tag"},
        )

        assert response.status_code == 400
        assert response.json()["error"] == "MALFORMED_PRECONDITION"
        mock_db.commit.assert_not_awaited()

    async def test_a_lost_race_at_write_time_is_also_412(
        self,
        authenticated_client: AsyncClient,
        mock_user: User,
        mock_db: AsyncMock,
    ) -> None:
        """The second line of defence, standing in for the database's answer.

        `StaleDataError` is what SQLAlchemy raises when the versioned UPDATE
        matches no rows. Here it is injected; that it genuinely happens under
        concurrency is `tests/test_optimistic_concurrency_db.py`.
        """
        mock_db.commit.side_effect = StaleDataError("UPDATE matched 0 rows")

        response = await authenticated_client.patch(
            ENDPOINT,
            json={"notification_channel": "none"},
            headers={"If-Match": tag_for(mock_user)},
        )

        assert response.status_code == 412
        assert response.json()["error"] == "PRECONDITION_FAILED"
        # No ETag: the session is unusable after a failed flush, so the only
        # tag this handler could name is the one it already knows is stale.
        assert "etag" not in response.headers


class TestRequestValidation:
    async def test_empty_patch_is_rejected(
        self, authenticated_client: AsyncClient, mock_user: User, mock_db: AsyncMock
    ) -> None:
        response = await authenticated_client.patch(
            ENDPOINT, json={}, headers={"If-Match": tag_for(mock_user)}
        )

        assert response.status_code == 422
        assert response.json()["error"] == "UNPROCESSABLE_ENTITY"
        mock_db.commit.assert_not_awaited()

    async def test_unknown_field_is_rejected_rather_than_silently_dropped(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        response = await authenticated_client.patch(
            ENDPOINT,
            json={"role": "admin"},
            headers={"If-Match": tag_for(mock_user)},
        )

        assert response.status_code == 422
        assert response.json()["error"] == "VALIDATION_ERROR"

    async def test_unknown_notification_channel_is_rejected(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        response = await authenticated_client.patch(
            ENDPOINT,
            json={"notification_channel": "carrier-pigeon"},
            headers={"If-Match": tag_for(mock_user)},
        )

        assert response.status_code == 422
        details = response.json()["details"]
        assert details[0]["field"] == "notification_channel"
        assert "expected one of" in details[0]["message"]

    async def test_null_channel_is_rejected_since_the_column_is_not_nullable(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        response = await authenticated_client.patch(
            ENDPOINT,
            json={"notification_channel": None},
            headers={"If-Match": tag_for(mock_user)},
        )

        assert response.status_code == 422
        assert "omit the field" in response.json()["details"][0]["message"]

    @pytest.mark.parametrize(
        "url",
        ["ftp://example.com/hook", "javascript:alert(1)", "https://" + "a" * 2048],
    )
    async def test_webhook_url_must_be_a_bounded_http_url(
        self, authenticated_client: AsyncClient, mock_user: User, url: str
    ) -> None:
        response = await authenticated_client.patch(
            ENDPOINT,
            json={"notification_webhook_url": url},
            headers={"If-Match": tag_for(mock_user)},
        )

        assert response.status_code == 422

    async def test_requires_authentication(self, async_client: AsyncClient) -> None:
        response = await async_client.patch(
            ENDPOINT, json={"notification_channel": "none"}, headers={"If-Match": "*"}
        )

        assert response.status_code == 401
