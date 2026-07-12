from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError
from pydantic import ValidationError

from src.exceptions import BadRequestError
from src.storage.s3 import (
    ALLOWED_CONTENT_TYPES,
    MAX_FILE_SIZE_BYTES,
    StorageError,
    _build_object_key,
    delete_s3_object,
    generate_presigned_download,
    generate_presigned_upload,
)
from src.storage.schemas import (
    PresignedDownloadRequest,
    PresignedUploadRequest,
    PresignedUploadResponse,
)


# --- _build_object_key ---


def test_build_object_key_includes_extension() -> None:
    key = _build_object_key("uploads", "photo.jpg")
    assert key.startswith("uploads/")
    assert key.endswith(".jpg")
    uuid_part = key[len("uploads/"):].rsplit(".", 1)[0]
    assert len(uuid_part) == 36


def test_build_object_key_without_extension() -> None:
    key = _build_object_key("docs", "myfile")
    assert key.startswith("docs/")
    assert "." not in key.split("/")[1]


def test_build_object_key_is_unique() -> None:
    assert _build_object_key("uploads", "same.jpg") != _build_object_key("uploads", "same.jpg")


# --- generate_presigned_upload ---


def test_generate_presigned_upload_disallowed_type() -> None:
    with pytest.raises(BadRequestError, match="not allowed"):
        generate_presigned_upload(
            folder="uploads",
            filename="virus.exe",
            content_type="application/octet-stream",
        )


@patch("src.storage.s3._get_s3_client")
def test_generate_presigned_upload_success(mock_get_client: MagicMock) -> None:
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    mock_client.generate_presigned_post.return_value = {
        "url": "https://s3.example.com/upload",
        "fields": {"key": "uploads/abc.jpg", "Content-Type": "image/jpeg"},
    }

    result = generate_presigned_upload(
        folder="uploads",
        filename="photo.jpg",
        content_type="image/jpeg",
        expiry=600,
    )

    assert result["url"] == "https://s3.example.com/upload"
    assert result["expires_in"] == 600
    assert result["key"].startswith("uploads/")
    call_kwargs = mock_client.generate_presigned_post.call_args[1]
    assert call_kwargs["ExpiresIn"] == 600
    assert call_kwargs["Fields"] == {"Content-Type": "image/jpeg"}
    assert ["content-length-range", 1, MAX_FILE_SIZE_BYTES] in call_kwargs["Conditions"]


@patch("src.storage.s3._get_s3_client")
def test_generate_presigned_upload_uses_default_expiry(mock_get_client: MagicMock) -> None:
    from src.config import settings

    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    mock_client.generate_presigned_post.return_value = {
        "url": "https://s3.example.com/upload",
        "fields": {},
    }

    result = generate_presigned_upload(
        folder="uploads",
        filename="doc.pdf",
        content_type="application/pdf",
    )

    assert result["expires_in"] == settings.AWS_S3_PRESIGNED_URL_EXPIRY


@patch("src.storage.s3._get_s3_client")
def test_generate_presigned_upload_client_error_raises_storage_error(
    mock_get_client: MagicMock,
) -> None:
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    mock_client.generate_presigned_post.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}},
        "GeneratePresignedPost",
    )

    with pytest.raises(StorageError, match="Failed to generate presigned upload URL"):
        generate_presigned_upload(
            folder="uploads",
            filename="photo.jpg",
            content_type="image/jpeg",
        )


# --- generate_presigned_download ---


@patch("src.storage.s3._get_s3_client")
def test_generate_presigned_download_success(mock_get_client: MagicMock) -> None:
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    mock_client.generate_presigned_url.return_value = "https://s3.example.com/file?sig=abc"

    result = generate_presigned_download(key="uploads/abc.jpg", expiry=300)

    assert result["url"] == "https://s3.example.com/file?sig=abc"
    assert result["expires_in"] == 300
    call_args = mock_client.generate_presigned_url.call_args
    assert call_args[0][0] == "get_object"
    assert call_args[1]["ExpiresIn"] == 300
    assert call_args[1]["Params"]["Key"] == "uploads/abc.jpg"


@patch("src.storage.s3._get_s3_client")
def test_generate_presigned_download_client_error_raises_storage_error(
    mock_get_client: MagicMock,
) -> None:
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    mock_client.generate_presigned_url.side_effect = ClientError(
        {"Error": {"Code": "NoSuchKey", "Message": "Not Found"}}, "GetObject"
    )

    with pytest.raises(StorageError, match="Failed to generate presigned download URL"):
        generate_presigned_download(key="missing/file.jpg")


# --- delete_s3_object ---


@patch("src.storage.s3._get_s3_client")
def test_delete_s3_object_success(mock_get_client: MagicMock) -> None:
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client

    delete_s3_object("uploads/to-delete.jpg")

    mock_client.delete_object.assert_called_once()


@patch("src.storage.s3._get_s3_client")
def test_delete_s3_object_client_error_raises_storage_error(
    mock_get_client: MagicMock,
) -> None:
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    mock_client.delete_object.side_effect = ClientError(
        {"Error": {"Code": "NoSuchBucket", "Message": "No such bucket"}}, "DeleteObject"
    )

    with pytest.raises(StorageError, match="Failed to delete object"):
        delete_s3_object("some/key.jpg")


# --- Schema validation ---


def test_presigned_upload_request_valid_folder() -> None:
    req = PresignedUploadRequest(
        filename="file.jpg",
        content_type="image/jpeg",
        folder="user-uploads/avatars",
    )
    assert req.folder == "user-uploads/avatars"


def test_presigned_upload_request_invalid_folder_rejected() -> None:
    with pytest.raises(ValidationError):
        PresignedUploadRequest(
            filename="file.jpg",
            content_type="image/jpeg",
            folder="INVALID FOLDER!",
        )


def test_presigned_upload_request_default_folder() -> None:
    req = PresignedUploadRequest(filename="doc.pdf", content_type="application/pdf")
    assert req.folder == "uploads"


def test_presigned_download_request_rejects_empty_key() -> None:
    with pytest.raises(ValidationError):
        PresignedDownloadRequest(key="")


# --- Constants ---


def test_allowed_content_types_is_frozen() -> None:
    assert isinstance(ALLOWED_CONTENT_TYPES, frozenset)
    assert "image/jpeg" in ALLOWED_CONTENT_TYPES
    assert "application/octet-stream" not in ALLOWED_CONTENT_TYPES


def test_max_file_size_is_10_mib() -> None:
    assert MAX_FILE_SIZE_BYTES == 10 * 1024 * 1024


def test_presigned_upload_response_fields_typed() -> None:
    resp = PresignedUploadResponse(
        key="uploads/abc.jpg",
        url="https://s3.amazonaws.com/bucket",
        fields={"Content-Type": "image/jpeg", "key": "uploads/abc.jpg"},
        expires_in=3600,
    )
    assert resp.expires_in == 3600
    assert isinstance(resp.fields, dict)
