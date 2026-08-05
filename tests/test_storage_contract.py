"""One suite, run against every backend.

A factory is only worth having if the things it returns are interchangeable, so
the behaviour that callers rely on is asserted once and parametrised over the
implementations rather than written three times with three sets of assumptions.
`S3Storage` participates through an in-test fake of the boto3 client surface it
uses — enough to prove this module's translation of S3 semantics (delete of a
missing key, 404 on head) without a bucket.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast

import pytest
from botocore.exceptions import ClientError

from src.exceptions import BadRequestError
from src.storage.base import (
    MAX_FILE_SIZE_BYTES,
    ObjectNotFoundError,
    StorageBackend,
    StoredObject,
)
from src.storage.local import LocalStorage
from src.storage.memory import MemoryStorage
from src.storage.s3 import S3Storage

PNG = "image/png"


class FakeS3Client:
    """The five boto3 calls `S3Storage` makes, over a dict."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.content_types: dict[str, str] = {}

    @staticmethod
    def _not_found(operation: str) -> ClientError:
        return ClientError(
            {"Error": {"Code": "404", "Message": "Not Found"}}, operation
        )

    def put_object(
        self, *, Bucket: str, Key: str, Body: bytes, ContentType: str
    ) -> dict[str, Any]:
        self.objects[Key] = Body
        self.content_types[Key] = ContentType
        return {}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        if Key not in self.objects:
            raise self._not_found("GetObject")
        payload = self.objects[Key]
        return {"Body": _FakeBody(payload)}

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        if Key not in self.objects:
            raise self._not_found("HeadObject")
        return {"ContentLength": len(self.objects[Key])}

    def delete_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.objects.pop(Key, None)
        self.content_types.pop(Key, None)
        return {}

    def get_paginator(self, operation: str) -> _FakePaginator:
        assert operation == "list_objects_v2"
        return _FakePaginator(self)


class _FakeBody:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload


class _FakePaginator:
    def __init__(self, client: FakeS3Client) -> None:
        self._client = client

    def paginate(self, *, Bucket: str, Prefix: str) -> Iterator[dict[str, Any]]:
        contents = [
            {"Key": key}
            for key in sorted(self._client.objects)
            if key.startswith(Prefix)
        ]
        # Two pages, so the pagination loop is actually exercised.
        midpoint = len(contents) // 2
        yield {"Contents": contents[:midpoint]}
        yield {"Contents": contents[midpoint:]}


@pytest.fixture(params=["memory", "local", "s3"])
def backend(request: pytest.FixtureRequest, tmp_path: Path) -> StorageBackend:
    if request.param == "memory":
        return MemoryStorage()
    if request.param == "local":
        return LocalStorage(tmp_path / "objects")
    return S3Storage("test-bucket", client=cast(Any, FakeS3Client()))


# --- The contract ---


async def test_backend_satisfies_the_protocol(backend: StorageBackend) -> None:
    assert isinstance(backend, StorageBackend)
    assert backend.name in {"memory", "local", "s3"}


async def test_put_then_get_round_trips_bytes(backend: StorageBackend) -> None:
    stored = await backend.put("uploads/a.png", b"binary\x00payload", content_type=PNG)

    assert stored == StoredObject(
        key="uploads/a.png", size=len(b"binary\x00payload"), content_type=PNG
    )
    assert await backend.get("uploads/a.png") == b"binary\x00payload"


async def test_put_overwrites_an_existing_key(backend: StorageBackend) -> None:
    await backend.put("uploads/a.png", b"first", content_type=PNG)
    await backend.put("uploads/a.png", b"second", content_type=PNG)

    assert await backend.get("uploads/a.png") == b"second"
    assert await backend.list_keys() == ["uploads/a.png"]


async def test_exists_reflects_put_and_delete(backend: StorageBackend) -> None:
    assert await backend.exists("uploads/a.png") is False

    await backend.put("uploads/a.png", b"x", content_type=PNG)
    assert await backend.exists("uploads/a.png") is True

    await backend.delete("uploads/a.png")
    assert await backend.exists("uploads/a.png") is False


async def test_get_missing_key_raises_object_not_found(backend: StorageBackend) -> None:
    with pytest.raises(ObjectNotFoundError) as excinfo:
        await backend.get("uploads/missing.png")

    assert excinfo.value.status_code == 404
    assert excinfo.value.error_code == "OBJECT_NOT_FOUND"
    assert excinfo.value.details == {"key": "uploads/missing.png"}


async def test_delete_missing_key_raises_object_not_found(
    backend: StorageBackend,
) -> None:
    with pytest.raises(ObjectNotFoundError):
        await backend.delete("uploads/missing.png")


async def test_list_keys_is_sorted_and_prefix_filtered(
    backend: StorageBackend,
) -> None:
    for key in ("b/2.png", "a/1.png", "b/1.png"):
        await backend.put(key, b"x", content_type=PNG)

    assert await backend.list_keys() == ["a/1.png", "b/1.png", "b/2.png"]
    assert await backend.list_keys("b/") == ["b/1.png", "b/2.png"]
    assert await backend.list_keys("nothing/") == []


async def test_list_keys_on_an_empty_backend(backend: StorageBackend) -> None:
    assert await backend.list_keys() == []


@pytest.mark.parametrize(
    "key",
    [
        "",
        "/absolute.png",
        "../escape.png",
        "uploads/../../escape.png",
        "uploads//double.png",
        "uploads\\windows.png",
        "uploads/nul\x00.png",
        "uploads/.hidden-leading-dot.png",
        "x" * 1025,
    ],
)
async def test_unsafe_keys_are_rejected(backend: StorageBackend, key: str) -> None:
    with pytest.raises(BadRequestError):
        await backend.put(key, b"x", content_type=PNG)


async def test_disallowed_content_type_is_rejected(backend: StorageBackend) -> None:
    with pytest.raises(BadRequestError, match="not allowed"):
        await backend.put(
            "uploads/a.exe", b"MZ", content_type="application/octet-stream"
        )

    assert await backend.exists("uploads/a.exe") is False


async def test_empty_payload_is_rejected(backend: StorageBackend) -> None:
    with pytest.raises(BadRequestError, match="empty object"):
        await backend.put("uploads/a.png", b"", content_type=PNG)


async def test_oversized_payload_is_rejected(backend: StorageBackend) -> None:
    with pytest.raises(BadRequestError, match="exceeds"):
        await backend.put(
            "uploads/a.png", b"x" * (MAX_FILE_SIZE_BYTES + 1), content_type=PNG
        )
