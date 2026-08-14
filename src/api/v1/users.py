"""`/api/v1/users/me` — the one resource a user is always allowed to edit.

Both handlers stamp the response with the profile's entity tag, because a
client cannot make a conditional request without one and `PATCH` returning the
*new* tag is what lets it make a second edit without a round trip in between.

`Cache-Control: private, no-store` is not incidental. This URI names a
different resource for every bearer token, so a shared cache holding one user's
representation and serving it — or its tag — to another is a real hazard rather
than a theoretical one. The tag itself carries the user id for the same reason
(see `resource_version_tag`), which is belt and braces on purpose: these
responses contain someone's email address and account state.
"""

from fastapi import APIRouter, Response, status

from src.dependencies import CurrentUserDep, IfMatchDep, ProfileServiceDep
from src.models.user import User
from src.users.schemas import ProfileUpdateRequest, UserProfileResponse
from src.users.service import profile_etag

router = APIRouter(prefix="/users", tags=["users"])

# Documented on the routes so the generated OpenAPI describes the conditional
# protocol rather than only its happy path. A client that has read the schema
# and handles 412 correctly is the entire point of serving the tag.
_CONDITIONAL_RESPONSES: dict[int | str, dict[str, str]] = {
    status.HTTP_412_PRECONDITION_FAILED: {
        "description": (
            "The If-Match tag does not describe the current state of the "
            "profile. Re-read it, reapply the change, and retry."
        )
    },
    status.HTTP_428_PRECONDITION_REQUIRED: {
        "description": "The request carried no If-Match header."
    },
}


def _stamp(response: Response, user: User) -> None:
    response.headers["ETag"] = profile_etag(user).serialize()
    response.headers["Cache-Control"] = "private, no-store"


@router.get(
    "/me",
    response_model=UserProfileResponse,
    summary="Read the authenticated user's profile",
)
async def read_profile(
    response: Response,
    current_user: CurrentUserDep,
) -> UserProfileResponse:
    """Return the caller's own profile and the `ETag` to edit it with."""
    _stamp(response, current_user)
    return UserProfileResponse.model_validate(current_user)


@router.patch(
    "/me",
    response_model=UserProfileResponse,
    responses=_CONDITIONAL_RESPONSES,
    summary="Update the authenticated user's profile (requires If-Match)",
)
async def update_profile(
    response: Response,
    changes: ProfileUpdateRequest,
    current_user: CurrentUserDep,
    precondition: IfMatchDep,
    service: ProfileServiceDep,
) -> UserProfileResponse:
    """Apply a partial update, but only to the version the client last saw.

    `PATCH` rather than `PUT`: the row holds fields the owner may not set —
    `role`, `is_verified`, the password hash — so a request body that replaced
    the whole representation would have to be half-ignored, and a `PUT` whose
    response differs from what was sent is a worse contract than a `PATCH` that
    only ever mentions what changed.
    """
    updated = await service.update(current_user, changes, precondition)
    _stamp(response, updated)
    return UserProfileResponse.model_validate(updated)
