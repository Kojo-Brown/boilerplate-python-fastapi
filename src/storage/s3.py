from __future__ import annotations

import uuid
from functools import lru_cache
from typing import TYPE_CHECKING, Final, TypedDict

import boto3
from botocore.exceptions import ClientError

from src.config import settings
from src.exceptions import AppException, BadRequestError

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

ALLOWED_CONTENT_TYPES: Final[frozenset[str]] = frozenset(
    {
        "image/jpeg",
        "image/png",
        "image/gif",
        "image/webp",
        "application/pdf",
        "text/plain",
        "text/csv",
    }
)

MAX_FILE_SIZE_BYTES: Final[int] = 10 * 1024 * 1024  # 10 MiB


class StorageError(AppException):
    status_code = 500
    error_code = "STORAGE_ERROR"

    def __init__(
        self, message: str = "Storage operation failed", details: object = None
    ) -> None:
        super().__init__(message, details)


class PresignedUploadResult(TypedDict):
    key: str
    url: str
    fields: dict[str, str]
    expires_in: int


class PresignedDownloadResult(TypedDict):
    url: str
    expires_in: int


@lru_cache(maxsize=1)
def _get_s3_client() -> S3Client:
    return boto3.client(
        "s3",
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID or None,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY or None,
        region_name=settings.AWS_REGION,
    )


def _build_object_key(folder: str, filename: str) -> str:
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    unique = str(uuid.uuid4())
    return f"{folder}/{unique}.{ext}" if ext else f"{folder}/{unique}"


def generate_presigned_upload(
    *,
    folder: str,
    filename: str,
    content_type: str,
    expiry: int | None = None,
) -> PresignedUploadResult:
    """Return a presigned POST payload for direct S3 upload."""
    if content_type not in ALLOWED_CONTENT_TYPES:
        raise BadRequestError(
            f"Content type '{content_type}' is not allowed.",
            details={"allowed": sorted(ALLOWED_CONTENT_TYPES)},
        )

    key = _build_object_key(folder, filename)
    ttl = expiry if expiry is not None else settings.AWS_S3_PRESIGNED_URL_EXPIRY

    try:
        result = _get_s3_client().generate_presigned_post(
            Bucket=settings.AWS_S3_BUCKET,
            Key=key,
            Fields={"Content-Type": content_type},
            Conditions=[
                {"Content-Type": content_type},
                ["content-length-range", 1, MAX_FILE_SIZE_BYTES],
            ],
            ExpiresIn=ttl,
        )
    except ClientError as exc:
        raise StorageError("Failed to generate presigned upload URL.") from exc

    return PresignedUploadResult(
        key=key,
        url=result["url"],
        fields=result["fields"],
        expires_in=ttl,
    )


def generate_presigned_download(
    *,
    key: str,
    expiry: int | None = None,
) -> PresignedDownloadResult:
    """Return a presigned GET URL for an existing S3 object."""
    ttl = expiry if expiry is not None else settings.AWS_S3_PRESIGNED_URL_EXPIRY

    try:
        url: str = _get_s3_client().generate_presigned_url(
            "get_object",
            Params={"Bucket": settings.AWS_S3_BUCKET, "Key": key},
            ExpiresIn=ttl,
        )
    except ClientError as exc:
        raise StorageError("Failed to generate presigned download URL.") from exc

    return PresignedDownloadResult(url=url, expires_in=ttl)


def delete_s3_object(key: str) -> None:
    """Delete an object from S3."""
    try:
        _get_s3_client().delete_object(Bucket=settings.AWS_S3_BUCKET, Key=key)
    except ClientError as exc:
        raise StorageError(f"Failed to delete object '{key}'.") from exc
