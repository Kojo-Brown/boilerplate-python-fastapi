"""`/api/v1/uploads` — presigned URLs for direct-to-S3 transfer.

Neither route moves bytes. Both mint a *capability*: a URL that S3 will honour
on its own, without consulting this application again. That is the whole reason
the authorisation decision has to be complete before the URL is returned —
there is no second checkpoint at the fetch, and a URL handed to the wrong caller
cannot be recalled inside its TTL.

Which is why both handlers bind the authenticated user rather than merely
requiring one. `get_current_user` answers "is there a caller"; the key's
namespace answers "is this *their* object", and only the second is object-level
authorisation. The download route had the first and not the second, so any
authenticated account could name any key in the bucket and be handed a signed
URL for it — see `owner_key_prefix` in `src/storage/base.py` for how the key
itself came to carry the answer.
"""

from fastapi import APIRouter

from src.dependencies import CurrentUserDep
from src.storage.s3 import generate_presigned_download, generate_presigned_upload
from src.storage.schemas import (
    PresignedDownloadRequest,
    PresignedDownloadResponse,
    PresignedUploadRequest,
    PresignedUploadResponse,
)

router = APIRouter(prefix="/uploads", tags=["uploads"])


@router.post(
    "/presigned-upload",
    response_model=PresignedUploadResponse,
    summary="Request a presigned S3 POST URL for direct client upload",
    responses={
        400: {"description": "The content type or folder is not permitted."},
    },
)
async def request_presigned_upload(
    body: PresignedUploadRequest,
    current_user: CurrentUserDep,
) -> PresignedUploadResponse:
    """Mint an upload capability into the caller's own key namespace.

    `folder` is still the client's to choose, but only *within* that namespace:
    it is a suffix of the owner prefix, never a replacement for it.
    """
    result = generate_presigned_upload(
        folder=body.folder,
        filename=body.filename,
        content_type=body.content_type,
        owner_id=current_user.id,
    )
    return PresignedUploadResponse(**result)


@router.post(
    "/presigned-download",
    response_model=PresignedDownloadResponse,
    summary="Request a presigned S3 GET URL for one of the caller's objects",
    responses={
        400: {"description": "The key is not a well-formed object key."},
        403: {"description": "The key belongs to another account."},
    },
)
async def request_presigned_download(
    body: PresignedDownloadRequest,
    current_user: CurrentUserDep,
) -> PresignedDownloadResponse:
    """Mint a download capability, but only for an object the caller owns.

    The 403 is deliberately indifferent to whether the object exists: nothing
    has looked by the time the key is refused, so this route cannot be used to
    probe another account's bucket contents.
    """
    result = generate_presigned_download(key=body.key, owner_id=current_user.id)
    return PresignedDownloadResponse(**result)
