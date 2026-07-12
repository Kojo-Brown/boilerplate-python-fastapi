from fastapi import APIRouter, Depends

from src.auth.dependencies import get_current_user
from src.models.user import User
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
)
async def request_presigned_upload(
    body: PresignedUploadRequest,
    _: User = Depends(get_current_user),
) -> PresignedUploadResponse:
    result = generate_presigned_upload(
        folder=body.folder,
        filename=body.filename,
        content_type=body.content_type,
    )
    return PresignedUploadResponse(**result)


@router.post(
    "/presigned-download",
    response_model=PresignedDownloadResponse,
    summary="Request a presigned S3 GET URL to download an object",
)
async def request_presigned_download(
    body: PresignedDownloadRequest,
    _: User = Depends(get_current_user),
) -> PresignedDownloadResponse:
    result = generate_presigned_download(key=body.key)
    return PresignedDownloadResponse(**result)
