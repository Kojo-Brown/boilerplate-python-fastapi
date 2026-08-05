"""S3-specific behaviour the shared contract suite cannot express.

The contract suite proves `S3Storage` behaves like the other backends. These
cover what is peculiar to S3: the two different shapes a "missing object" takes
on the wire, and the failures that must surface as `StorageError` rather than
being mistaken for absence.
"""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from src.storage.base import ObjectNotFoundError, StorageError
from src.storage.s3 import S3Storage


def client_error(code: str, operation: str = "HeadObject") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


def make_backend() -> tuple[S3Storage, MagicMock]:
    client = MagicMock()
    return S3Storage("test-bucket", client=cast(Any, client)), client


async def test_put_translates_client_error() -> None:
    backend, client = make_backend()
    client.put_object.side_effect = client_error("AccessDenied", "PutObject")

    with pytest.raises(StorageError, match="Failed to write object"):
        await backend.put("uploads/a.png", b"x", content_type="image/png")


async def test_put_sends_the_content_type_as_object_metadata() -> None:
    backend, client = make_backend()

    await backend.put("uploads/a.png", b"payload", content_type="image/png")

    kwargs = client.put_object.call_args[1]
    assert kwargs == {
        "Bucket": "test-bucket",
        "Key": "uploads/a.png",
        "Body": b"payload",
        "ContentType": "image/png",
    }


@pytest.mark.parametrize("code", ["NoSuchKey", "404", "NoSuchBucket"])
async def test_get_maps_every_not_found_code(code: str) -> None:
    backend, client = make_backend()
    client.get_object.side_effect = client_error(code, "GetObject")

    with pytest.raises(ObjectNotFoundError):
        await backend.get("uploads/a.png")


async def test_get_does_not_swallow_other_failures() -> None:
    backend, client = make_backend()
    client.get_object.side_effect = client_error("SlowDown", "GetObject")

    with pytest.raises(StorageError, match="Failed to read object"):
        await backend.get("uploads/a.png")


async def test_exists_does_not_report_a_throttled_head_as_absent() -> None:
    # Returning False here would let a caller conclude the object is gone and
    # delete or re-upload on the strength of a transient 503.
    backend, client = make_backend()
    client.head_object.side_effect = client_error("SlowDown")

    with pytest.raises(StorageError, match="Failed to stat object"):
        await backend.exists("uploads/a.png")


async def test_delete_heads_before_deleting() -> None:
    backend, client = make_backend()

    await backend.delete("uploads/a.png")

    client.head_object.assert_called_once_with(
        Bucket="test-bucket", Key="uploads/a.png"
    )
    client.delete_object.assert_called_once_with(
        Bucket="test-bucket", Key="uploads/a.png"
    )


async def test_delete_translates_client_error() -> None:
    backend, client = make_backend()
    client.delete_object.side_effect = client_error("AccessDenied", "DeleteObject")

    with pytest.raises(StorageError, match="Failed to delete object"):
        await backend.delete("uploads/a.png")


async def test_list_keys_translates_client_error() -> None:
    backend, client = make_backend()
    client.get_paginator.side_effect = client_error("AccessDenied", "ListObjectsV2")

    with pytest.raises(StorageError, match="Failed to list objects"):
        await backend.list_keys("uploads/")


async def test_client_defaults_to_the_shared_module_level_client() -> None:
    with patch("src.storage.s3._get_s3_client") as get_client:
        backend = S3Storage("test-bucket")
        assert backend.client is get_client.return_value

    # Constructing the backend must not have opened a session by itself.
    get_client.assert_called_once()
