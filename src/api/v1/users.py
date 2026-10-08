"""`/api/v1/users/me` — the one resource a user is always allowed to edit.

Both handlers stamp the response with the profile's entity tag, because a
client cannot make a conditional request without one and `PATCH` returning the
*new* tag is what lets it make a second edit without a round trip in between.

`Cache-Control: private, no-cache` is not incidental, and neither directive is
the obvious one. `private` is the hazard this URI actually has: it names a
different resource for every bearer token, so a shared cache holding one user's
representation — or its tag — and serving it to another is a real problem
rather than a theoretical one. The tag itself carries the user id for the same
reason (see `resource_version_tag`), which is belt and braces on purpose: these
responses contain someone's email address and account state.

`no-cache` rather than `no-store` because this route serves a validator and
honours it. `no-store` (RFC 9111 §5.2.2.5) forbids a client to keep the
representation at all, which leaves nothing for an `If-None-Match` to
revalidate and makes the `ETag` decoration on a read. `no-cache` (§5.2.2.4)
permits the client to store it and forbids reusing it without asking the origin
first, which is the property that was wanted: a stale `role` or `is_active` is
never served from a cache, and a client that has asked is answered in one
round trip with no body. The trade is that the representation may now sit in a
private cache where `no-store` kept it out of one — see
`docs/conditional-get.md` for when to take `no-store` back.
"""

from fastapi import APIRouter, Request, Response, status

from src.concurrency import ConditionalOutcome, EntityTag
from src.dependencies import (
    CurrentUserDep,
    IfMatchDep,
    IfNoneMatchDep,
    ProfileServiceDep,
)
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
            "profile, or an If-None-Match tag does. Re-read it, reapply the "
            "change, and retry."
        )
    },
    status.HTTP_428_PRECONDITION_REQUIRED: {
        "description": "The request carried no If-Match header."
    },
}


#: Served on every response from this router, 200 and 304 alike. A 304 that
#: dropped it would leave the client's stored copy governed by whatever it
#: remembered from the response it was revalidating.
_CACHE_CONTROL = "private, no-cache"


def _conditional_headers(tag: EntityTag) -> dict[str, str]:
    """The headers RFC 9110 §15.4.5 wants on a 304: the validator and the policy."""
    return {"ETag": tag.serialize(), "Cache-Control": _CACHE_CONTROL}


def _stamp(response: Response, user: User) -> None:
    response.headers["ETag"] = profile_etag(user).serialize()
    response.headers["Cache-Control"] = _CACHE_CONTROL


@router.get(
    "/me",
    response_model=UserProfileResponse,
    responses={
        status.HTTP_304_NOT_MODIFIED: {
            "description": (
                "The caller's `If-None-Match` names the current profile. No "
                "body; the `ETag` confirms the copy the client already holds."
            )
        },
        status.HTTP_400_BAD_REQUEST: {
            "description": "The If-None-Match header is not a valid entity-tag list."
        },
    },
    summary="Read the authenticated user's profile",
)
async def read_profile(
    request: Request,
    response: Response,
    current_user: CurrentUserDep,
    precondition: IfNoneMatchDep,
) -> UserProfileResponse | Response:
    """Return the caller's own profile, or confirm the copy it already has.

    `request.method` rather than a literal `"GET"`, which is a smaller point
    than it first looks. FastAPI's `APIRoute` does *not* add `HEAD` alongside
    `GET` the way Starlette's plain `Route` does, so a `HEAD` of this URI is a
    405 naming `Allow: GET` and the method here is always `"GET"` today —
    `test_head_is_not_routed_here` pins that. Reading it off the request
    anyway keeps the handler from asserting anything about which methods were
    routed to it: §13.1.2 turns on the method, and if `HEAD` is ever added the
    branch is already right.

    Returning a bare `Response` is how a handler with a `response_model`
    answers without a body — FastAPI passes it through untouched, and
    Starlette omits `Content-Length` for a 304, which §15.4.5 requires.
    """
    tag = profile_etag(current_user)
    if precondition.evaluate(tag, method=request.method) is (
        ConditionalOutcome.NOT_MODIFIED
    ):
        return Response(
            status_code=status.HTTP_304_NOT_MODIFIED,
            headers=_conditional_headers(tag),
        )

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
    none_match: IfNoneMatchDep,
    service: ProfileServiceDep,
) -> UserProfileResponse:
    """Apply a partial update, but only to the version the client last saw.

    `PATCH` rather than `PUT`: the row holds fields the owner may not set —
    `role`, `is_verified`, the password hash — so a request body that replaced
    the whole representation would have to be half-ignored, and a `PUT` whose
    response differs from what was sent is a worse contract than a `PATCH` that
    only ever mentions what changed.
    """
    updated = await service.update(
        current_user, changes, precondition, none_match=none_match
    )
    _stamp(response, updated)
    return UserProfileResponse.model_validate(updated)
