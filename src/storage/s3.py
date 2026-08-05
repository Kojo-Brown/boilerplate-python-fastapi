from __future__ import annotations

import asyncio
from functools import lru_cache
from typing import TYPE_CHECKING, TypedDict

import boto3
import structlog
from botocore.exceptions import ClientError

from src.config import settings
from src.exceptions import BadRequestError
from src.storage.base import (
    ALLOWED_CONTENT_TYPES,
    MAX_FILE_SIZE_BYTES,
    ObjectNotFoundError,
    StorageError,
    StoredObject,
    build_object_key,
    validate_object_key,
    validate_upload,
)

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = structlog.get_logger(__name__)

# The status codes S3 uses to say "no such object". `head_object` answers 404
# with a bare `ClientError` rather than the `NoSuchKey` code `get_object`
# returns, so both have to be recognised.
_NOT_FOUND_CODES = frozenset({"404", "NoSuchKey", "NoSuchBucket"})


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


def _is_not_found(exc: ClientError) -> bool:
    code = str(exc.response.get("Error", {}).get("Code", ""))
    return code in _NOT_FOUND_CODES


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

    key = build_object_key(folder, filename)
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


class S3Storage:
    """`StorageBackend` over an S3 bucket.

    boto3 is synchronous and releases the GIL around its socket I/O, so every
    call is dispatched with `asyncio.to_thread` rather than blocking the event
    loop for the length of an AWS round-trip. The client itself is thread-safe
    for the operations used here.

    Beyond the protocol it also mints presigned URLs, which no other backend
    can do; the module-level `generate_presigned_*` functions remain the entry
    point for that and are what `/api/v1/uploads` uses.
    """

    def __init__(self, bucket: str, *, client: S3Client | None = None) -> None:
        if not bucket:
            raise StorageError(
                "S3 storage requires a bucket name; set AWS_S3_BUCKET.",
            )
        self._bucket = bucket
        self._client = client

    @property
    def name(self) -> str:
        return "s3"

    @property
    def bucket(self) -> str:
        return self._bucket

    @property
    def client(self) -> S3Client:
        # Resolved lazily so constructing the backend never opens a session —
        # the factory builds it at import time in some deployments.
        return self._client if self._client is not None else _get_s3_client()

    async def put(self, key: str, data: bytes, *, content_type: str) -> StoredObject:
        validate_object_key(key)
        validate_upload(content_type=content_type, size=len(data))

        try:
            await asyncio.to_thread(
                self.client.put_object,
                Bucket=self._bucket,
                Key=key,
                Body=data,
                ContentType=content_type,
            )
        except ClientError as exc:
            raise StorageError(
                f"Failed to write object '{key}'.", details={"key": key}
            ) from exc

        logger.info("storage.put", backend=self.name, key=key, size=len(data))
        return StoredObject(key=key, size=len(data), content_type=content_type)

    async def get(self, key: str) -> bytes:
        validate_object_key(key)

        try:
            response = await asyncio.to_thread(
                self.client.get_object, Bucket=self._bucket, Key=key
            )
            body: bytes = await asyncio.to_thread(response["Body"].read)
        except ClientError as exc:
            if _is_not_found(exc):
                raise ObjectNotFoundError(key) from exc
            raise StorageError(
                f"Failed to read object '{key}'.", details={"key": key}
            ) from exc

        return body

    async def delete(self, key: str) -> None:
        validate_object_key(key)

        # S3 `delete_object` succeeds on a key that never existed, so the
        # protocol's "raise if absent" contract needs the head first.
        if not await self.exists(key):
            raise ObjectNotFoundError(key)

        try:
            await asyncio.to_thread(
                self.client.delete_object, Bucket=self._bucket, Key=key
            )
        except ClientError as exc:
            raise StorageError(
                f"Failed to delete object '{key}'.", details={"key": key}
            ) from exc

        logger.info("storage.delete", backend=self.name, key=key)

    async def exists(self, key: str) -> bool:
        validate_object_key(key)

        try:
            await asyncio.to_thread(
                self.client.head_object, Bucket=self._bucket, Key=key
            )
        except ClientError as exc:
            if _is_not_found(exc):
                return False
            raise StorageError(
                f"Failed to stat object '{key}'.", details={"key": key}
            ) from exc

        return True

    def _list_sync(self, prefix: str) -> list[str]:
        paginator = self.client.get_paginator("list_objects_v2")
        keys = [
            obj["Key"]
            for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix)
            for obj in page.get("Contents", [])
            if "Key" in obj
        ]
        return sorted(keys)

    async def list_keys(self, prefix: str = "") -> list[str]:
        try:
            return await asyncio.to_thread(self._list_sync, prefix)
        except ClientError as exc:
            raise StorageError(
                f"Failed to list objects under '{prefix}'.",
                details={"prefix": prefix},
            ) from exc
