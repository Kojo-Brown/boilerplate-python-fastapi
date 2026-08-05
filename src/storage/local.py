"""Filesystem storage backend.

For local development and for tests that want real bytes on a real disk without
an AWS account or a network round-trip.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import structlog

from src.storage.base import (
    ObjectNotFoundError,
    StorageError,
    StoredObject,
    validate_object_key,
    validate_upload,
)

logger = structlog.get_logger(__name__)


class LocalStorage:
    """Stores objects as files under a single root directory.

    Every blocking filesystem call is pushed to a worker thread with
    `asyncio.to_thread`, because these methods are awaited from request
    handlers running on the event loop.
    """

    def __init__(self, root: Path | str) -> None:
        self._root = Path(root).expanduser().resolve()

    @property
    def name(self) -> str:
        return "local"

    @property
    def root(self) -> Path:
        return self._root

    def _path_for(self, key: str) -> Path:
        """Map a key to an absolute path that is provably inside the root.

        `validate_object_key` already rejects traversal, but the containment
        check is repeated on the resolved path so a symlink planted inside the
        root cannot redirect a write outside it either.
        """
        path = (self._root / validate_object_key(key)).resolve()
        if self._root not in path.parents:
            raise StorageError(
                "Resolved object path escapes the storage root.",
                details={"key": key},
            )
        return path

    def _put_sync(self, path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        # Write to a sibling temp file and rename, so a reader never observes a
        # half-written object and a failed write leaves the previous one intact.
        tmp = path.with_name(f".{path.name}.tmp")
        try:
            tmp.write_bytes(data)
            tmp.replace(path)
        finally:
            tmp.unlink(missing_ok=True)

    async def put(self, key: str, data: bytes, *, content_type: str) -> StoredObject:
        validate_upload(content_type=content_type, size=len(data))
        path = self._path_for(key)

        try:
            await asyncio.to_thread(self._put_sync, path, data)
        except OSError as exc:
            raise StorageError(
                f"Failed to write object '{key}'.", details={"key": key}
            ) from exc

        logger.info("storage.put", backend=self.name, key=key, size=len(data))
        return StoredObject(key=key, size=len(data), content_type=content_type)

    async def get(self, key: str) -> bytes:
        path = self._path_for(key)

        try:
            return await asyncio.to_thread(path.read_bytes)
        except FileNotFoundError as exc:
            raise ObjectNotFoundError(key) from exc
        except OSError as exc:
            raise StorageError(
                f"Failed to read object '{key}'.", details={"key": key}
            ) from exc

    async def delete(self, key: str) -> None:
        path = self._path_for(key)

        try:
            await asyncio.to_thread(path.unlink)
        except FileNotFoundError as exc:
            raise ObjectNotFoundError(key) from exc
        except OSError as exc:
            raise StorageError(
                f"Failed to delete object '{key}'.", details={"key": key}
            ) from exc

        logger.info("storage.delete", backend=self.name, key=key)

    async def exists(self, key: str) -> bool:
        path = self._path_for(key)
        return await asyncio.to_thread(path.is_file)

    def _list_sync(self, prefix: str) -> list[str]:
        if not self._root.is_dir():
            return []
        keys = [
            path.relative_to(self._root).as_posix()
            for path in self._root.rglob("*")
            if path.is_file()
        ]
        return sorted(key for key in keys if key.startswith(prefix))

    async def list_keys(self, prefix: str = "") -> list[str]:
        return await asyncio.to_thread(self._list_sync, prefix)
