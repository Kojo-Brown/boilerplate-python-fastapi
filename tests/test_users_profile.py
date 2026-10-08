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


class TestConditionalRead:
    """`If-None-Match` on the read, which is what the served `ETag` is *for*.

    Until this feature the route minted a validator on every response and then
    accepted it only on a write. A client holding the tag had no way to ask
    "still current?" — it could only re-fetch and compare, which is the thing
    the tag exists to avoid.
    """

    async def test_the_current_tag_is_304(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        response = await authenticated_client.get(
            ENDPOINT, headers={"If-None-Match": tag_for(mock_user)}
        )

        assert response.status_code == 304

    async def test_the_304_carries_the_tag_it_confirmed(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        """§15.4.5: a 304 sends the `ETag` the client's copy may keep using."""
        response = await authenticated_client.get(
            ENDPOINT, headers={"If-None-Match": tag_for(mock_user)}
        )

        assert response.headers["etag"] == tag_for(mock_user)

    async def test_the_304_has_no_body(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        """§15.4.5: a 304 MUST NOT carry content — that is the entire saving."""
        response = await authenticated_client.get(
            ENDPOINT, headers={"If-None-Match": tag_for(mock_user)}
        )

        assert response.content == b""
        assert "content-length" not in response.headers

    async def test_the_304_repeats_the_caching_policy(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        """A revalidation that dropped it would leave the client's stored copy
        governed by whatever it remembered from the original response."""
        response = await authenticated_client.get(
            ENDPOINT, headers={"If-None-Match": tag_for(mock_user)}
        )

        assert response.headers["cache-control"] == "private, no-cache"

    async def test_the_read_permits_revalidation(
        self, authenticated_client: AsyncClient
    ) -> None:
        """`no-store` and a conditional GET cannot both be meant.

        `no-store` (RFC 9111 §5.2.2.5) forbids the client to keep the
        representation at all, so there is nothing for an `If-None-Match` to
        revalidate and the tag this route serves is decoration. `no-cache`
        (§5.2.2.4) is the directive that was wanted all along: store it, and
        never reuse it without asking the origin first. `private` is untouched,
        so no shared cache may hold it either way.
        """
        response = await authenticated_client.get(ENDPOINT)

        assert response.headers["cache-control"] == "private, no-cache"

    async def test_a_stale_tag_gets_the_full_representation(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        response = await authenticated_client.get(
            ENDPOINT, headers={"If-None-Match": tag_for(mock_user, version=1)}
        )

        assert response.status_code == 200
        assert response.json()["email"] == mock_user.email

    async def test_a_wildcard_is_304_because_the_profile_exists(
        self, authenticated_client: AsyncClient
    ) -> None:
        response = await authenticated_client.get(
            ENDPOINT, headers={"If-None-Match": "*"}
        )

        assert response.status_code == 304

    async def test_a_weak_form_of_the_current_tag_is_304(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        """The weak comparison function, end to end through the route."""
        response = await authenticated_client.get(
            ENDPOINT, headers={"If-None-Match": f"W/{tag_for(mock_user)}"}
        )

        assert response.status_code == 304

    async def test_a_list_containing_the_current_tag_is_304(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        response = await authenticated_client.get(
            ENDPOINT,
            headers={"If-None-Match": f'"stale", {tag_for(mock_user)}'},
        )

        assert response.status_code == 304

    async def test_another_accounts_tag_at_the_same_version_is_not_304(
        self, authenticated_client: AsyncClient, mock_user: User
    ) -> None:
        """The tag carries the row id, so one account's tag cannot confirm
        another's copy — a 304 here would be a cache serving the wrong user."""
        other = resource_version_tag(uuid.uuid4(), mock_user.version).serialize()

        response = await authenticated_client.get(
            ENDPOINT, headers={"If-None-Match": other}
        )

        assert response.status_code == 200

    async def test_a_malformed_field_is_400_rather_than_ignored(
        self, authenticated_client: AsyncClient
    ) -> None:
        """Ignoring it would answer 200 to a client that believed it had asked
        conditionally — a silent waste rather than a reported bug."""
        response = await authenticated_client.get(
            ENDPOINT, headers={"If-None-Match": "notatag"}
        )

        assert response.status_code == 400
        assert response.json()["error_code"] == "MALFORMED_PRECONDITION"
        assert "If-None-Match" in response.json()["message"]

    async def test_authentication_is_decided_before_the_precondition(
        self, async_client: AsyncClient
    ) -> None:
        """A 304 to an unauthenticated caller would confirm that a guessed tag
        describes somebody's current profile."""
        response = await async_client.get(ENDPOINT, headers={"If-None-Match": "*"})

        assert response.status_code == 401


class TestPreconditionPrecedence:
    """§13.2.2 step 3: `If-None-Match` is evaluated on a write too.

    A request carrying both fields is asking for two contradictory things —
    "only if unchanged" and "only if absent" — and the RFC resolves it by
    evaluating `If-Match` first and then still evaluating `If-None-Match`. On
    an unsafe method a match there is a 412, not a 304: answering 304 would
    tell the client its write had succeeded and changed nothing.
    """

    async def test_if_none_match_matching_on_a_patch_is_412(
        self,
        authenticated_client: AsyncClient,
        mock_user: User,
        mock_db: AsyncMock,
    ) -> None:
        response = await authenticated_client.patch(
            ENDPOINT,
            json={"notification_channel": "none"},
            headers={
                "If-Match": tag_for(mock_user),
                "If-None-Match": tag_for(mock_user),
            },
        )

        assert response.status_code == 412
        mock_db.commit.assert_not_awaited()

    async def test_a_patch_is_unaffected_by_a_stale_if_none_match(
        self,
        authenticated_client: AsyncClient,
        mock_user: User,
        mock_db: AsyncMock,
    ) -> None:
        response = await authenticated_client.patch(
            ENDPOINT,
            json={"notification_channel": "none"},
            headers={
                "If-Match": tag_for(mock_user),
                "If-None-Match": '"someone-elses-tag"',
            },
        )

        assert response.status_code == 200
        mock_db.commit.assert_awaited_once()

    async def test_a_failing_if_match_is_decided_first(
        self,
        authenticated_client: AsyncClient,
        mock_user: User,
        mock_db: AsyncMock,
    ) -> None:
        """Both fields refuse this request; `If-Match` owns the 412 (step 1),
        so the message names the field whose precondition actually failed."""
        response = await authenticated_client.patch(
            ENDPOINT,
            json={"notification_channel": "none"},
            headers={
                "If-Match": '"stale"',
                "If-None-Match": tag_for(mock_user),
            },
        )

        assert response.status_code == 412
        assert "If-Match" in response.json()["message"]
        mock_db.commit.assert_not_awaited()
