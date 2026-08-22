"""Request and response models for `/api/v1/users/me`."""

from __future__ import annotations

import uuid

from pydantic import BaseModel, ConfigDict, field_validator

from src.notifications.registry import NotificationStrategyRegistry

_WEBHOOK_URL_MAX_LENGTH = 2048
_ALLOWED_WEBHOOK_SCHEMES = ("http://", "https://")


class UserProfileResponse(BaseModel):
    """The profile as the owner sees it.

    `updated_at` is deliberately absent. It is maintained by `onupdate` in the
    database, so the value held in memory immediately after a write is the
    *previous* timestamp — returning it would mean either shipping a stale
    field or spending a round trip refreshing the row for a value nothing here
    needs. The `ETag` is the freshness signal this resource actually offers.

    `version` is absent for a related reason: it is an implementation detail of
    how the tag is computed, and putting it in the body invites clients to
    build their own `If-Match` out of it rather than echoing back the opaque
    tag they were given.
    """

    id: uuid.UUID
    email: str
    role: str
    is_active: bool
    is_verified: bool
    notification_channel: str
    notification_webhook_url: str | None

    model_config = ConfigDict(from_attributes=True, frozen=True)


class ProfileUpdateRequest(BaseModel):
    """A partial update: every field is optional, and omission means "leave it".

    The distinction between *omitted* and *explicitly null* is the whole
    difficulty of PATCH, and it is resolved here by Pydantic's own rules rather
    than by a sentinel. Defaults are not validated, so a validator that runs at
    all is looking at a value the client sent; `model_dump(exclude_unset=True)`
    in the service then applies exactly the fields that were present. Clearing
    the webhook URL is `{"notification_webhook_url": null}`; leaving it alone is
    not mentioning it.

    `extra="forbid"` because the opposite — accepting `{"emial": "..."}` with a
    200 and changing nothing — is the worst answer available.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    notification_channel: str | None = None
    notification_webhook_url: str | None = None

    @field_validator("notification_channel")
    @classmethod
    def _validate_channel(cls, value: str | None) -> str:
        if value is None:
            raise ValueError(
                "notification_channel cannot be null; omit the field to leave "
                "it unchanged"
            )
        available = NotificationStrategyRegistry.available()
        if value not in available:
            raise ValueError(
                f"unknown notification channel {value!r}; expected one of "
                f"{', '.join(available)}"
            )
        return value

    @field_validator("notification_webhook_url")
    @classmethod
    def _validate_webhook_url(cls, value: str | None) -> str | None:
        # Null is meaningful here — it clears the address — so unlike the
        # channel above, this validator accepts it.
        if value is None:
            return None
        if not value.startswith(_ALLOWED_WEBHOOK_SCHEMES):
            raise ValueError("notification_webhook_url must be an http(s) URL")
        if len(value) > _WEBHOOK_URL_MAX_LENGTH:
            raise ValueError(
                f"notification_webhook_url must be at most "
                f"{_WEBHOOK_URL_MAX_LENGTH} characters"
            )
        # Whether the host is one this service is allowed to call is settled at
        # delivery time by `WebhookNotificationStrategy`, which resolves the
        # name and rejects private addresses. Duplicating that check here would
        # be a second, weaker copy of it that goes stale on its own schedule —
        # and a DNS name that is public now can point at 169.254.169.254 by the
        # time anything is sent to it, so the check that matters is the late one.
        return value
