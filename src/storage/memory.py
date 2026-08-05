"""In-process storage backend.

The backend tests inject when they want the real code path without a bucket or
a temp directory. It is not shared between workers and does not survive a
restart, so it is never a production choice.
"""

from __future__ import annotations

import asyncio

from src.storage.base import (
    ObjectNotFoundError,
    StoredObject,
    validate_object_key,
    validate_upload,
)


class MemoryStorage:
    """Holds objects in a dict, guarded by an `asyncio.Lock`.

    The lock is not about the dict — `dict` mutation is already atomic under
    the GIL — but about `exists`-then-`delete` style pairs staying consistent
    when concurrent tasks touch the same key.
    """

    def __init__(self) -> None:
        self._objects: dict[str, StoredObject] = {}
        self._data: dict[str, bytes] = {}
        self._lock = asyncio.Lock()

    @property
    def name(self) -> str:
        return "memory"

    async def put(self, key: str, data: bytes, *, content_type: str) -> StoredObject:
        validate_object_key(key)
        validate_upload(content_type=content_type, size=len(data))

        stored = StoredObject(key=key, size=len(data), content_type=content_type)
        async with self._lock:
            self._data[key] = data
            self._objects[key] = stored
        return stored

    async def get(self, key: str) -> bytes:
        validate_object_key(key)
        async with self._lock:
            try:
                return self._data[key]
            except KeyError as exc:
                raise ObjectNotFoundError(key) from exc

    async def delete(self, key: str) -> None:
        validate_object_key(key)
        async with self._lock:
            if key not in self._data:
                raise ObjectNotFoundError(key)
            del self._data[key]
            del self._objects[key]

    async def exists(self, key: str) -> bool:
        validate_object_key(key)
        async with self._lock:
            return key in self._data

    async def list_keys(self, prefix: str = "") -> list[str]:
        async with self._lock:
            return sorted(key for key in self._data if key.startswith(prefix))

    async def stat(self, key: str) -> StoredObject:
        """Return the metadata recorded at `put` time.

        Not part of `StorageBackend` — the other two backends cannot serve
        `content_type` back — but it is what makes this backend useful as a
        test double for code that asserts on what was uploaded.
        """
        validate_object_key(key)
        async with self._lock:
            try:
                return self._objects[key]
            except KeyError as exc:
                raise ObjectNotFoundError(key) from exc

    async def clear(self) -> None:
        """Drop every object. For test teardown between cases."""
        async with self._lock:
            self._data.clear()
            self._objects.clear()
