"""Backend-agnostic storage contract.

Everything here is free of boto3, the filesystem and the settings object, so a
backend can be written against it without inheriting any of the three. The
concrete backends live in `s3.py`, `local.py` and `memory.py`; `factory.py`
chooses between them.
"""

from __future__ import annotations

import posixpath
import re
import uuid
from dataclasses import dataclass
from typing import Final, Protocol, runtime_checkable

from src.exceptions import AppException, BadRequestError, NotFoundError

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

MAX_KEY_LENGTH: Final[int] = 1024

# S3 accepts far more than this in a key, and so does ext4. The point of the
# restriction is that the same key has to be safe as an S3 key *and* as a path
# segment under a local root, so the contract takes the intersection: lowercase
# alphanumerics, dot, dash, underscore, and `/` as the only separator.
_KEY_SEGMENT_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class StorageError(AppException):
    status_code = 500
    error_code = "STORAGE_ERROR"

    def __init__(
        self, message: str = "Storage operation failed", details: object = None
    ) -> None:
        super().__init__(message, details)


class ObjectNotFoundError(NotFoundError):
    """Raised by `get`/`delete` when the key does not exist.

    A missing object is a 404 whichever backend is behind it, so the contract
    names one exception rather than letting `ClientError`, `FileNotFoundError`
    and `KeyError` leak out of three implementations.
    """

    error_code = "OBJECT_NOT_FOUND"

    def __init__(self, key: str) -> None:
        super().__init__(f"No stored object with key '{key}'.", details={"key": key})


class UnknownStorageBackendError(StorageError):
    """Raised when the configured backend name has no registered builder."""

    error_code = "UNKNOWN_STORAGE_BACKEND"

    def __init__(self, name: str, available: tuple[str, ...]) -> None:
        super().__init__(
            f"Unknown storage backend '{name}'.",
            details={"requested": name, "available": list(available)},
        )


@dataclass(frozen=True, slots=True)
class StoredObject:
    """What a backend reports back about an object it just accepted."""

    key: str
    size: int
    content_type: str


@runtime_checkable
class StorageBackend(Protocol):
    """The operations every backend supports.

    Deliberately narrow. Presigned URLs are *not* here: only S3 can mint a
    credential that a browser POSTs to directly, and a protocol method that two
    of three backends could not honour would be a lie the type checker accepts.
    `S3Storage` keeps that capability as its own surface (see `s3.py`).

    `content_type` is validated on `put` by every backend and echoed on the
    returned `StoredObject`, but only S3 persists it as object metadata —
    `LocalStorage` writes bytes to a file and `MemoryStorage` holds them in a
    dict, so neither can serve it back on a later `get`.
    """

    @property
    def name(self) -> str:
        """Short backend identifier, e.g. `"s3"`. Used in logs and errors."""
        ...

    async def put(
        self, key: str, data: bytes, *, content_type: str
    ) -> StoredObject: ...

    async def get(self, key: str) -> bytes:
        """Return the object's bytes, or raise `ObjectNotFoundError`."""
        ...

    async def delete(self, key: str) -> None:
        """Remove the object, or raise `ObjectNotFoundError` if it is absent."""
        ...

    async def exists(self, key: str) -> bool: ...

    async def list_keys(self, prefix: str = "") -> list[str]:
        """Return every key starting with `prefix`, in lexicographic order."""
        ...


def validate_object_key(key: str) -> str:
    """Return `key` unchanged if it is safe for every backend, else raise.

    Rejects what would let a caller-supplied key escape a local storage root or
    address something other than a plain object: absolute paths, `.`/`..`
    segments, backslashes, empty segments and control characters.
    """
    if not key:
        raise BadRequestError("Object key must not be empty.")

    if len(key) > MAX_KEY_LENGTH:
        raise BadRequestError(
            f"Object key must be at most {MAX_KEY_LENGTH} characters.",
            details={"length": len(key)},
        )

    if key.startswith("/") or "\\" in key or "\x00" in key:
        raise BadRequestError(
            "Object key must be a relative, slash-separated path.",
            details={"key": key},
        )

    segments = key.split("/")
    if any(not _KEY_SEGMENT_RE.match(segment) for segment in segments):
        raise BadRequestError(
            "Object key segments must be non-empty and contain only letters, "
            "digits, '.', '-' and '_'.",
            details={"key": key},
        )

    return key


def validate_upload(*, content_type: str, size: int) -> None:
    """Enforce the upload policy shared by every backend and the presigned path.

    S3 enforces the same two rules inside the presigned POST conditions, where
    the browser uploads without touching this process. Server-side `put` has to
    apply them itself or the policy would depend on which route was used.
    """
    if content_type not in ALLOWED_CONTENT_TYPES:
        raise BadRequestError(
            f"Content type '{content_type}' is not allowed.",
            details={"allowed": sorted(ALLOWED_CONTENT_TYPES)},
        )

    if size <= 0:
        raise BadRequestError("Refusing to store an empty object.")

    if size > MAX_FILE_SIZE_BYTES:
        raise BadRequestError(
            f"Object exceeds the {MAX_FILE_SIZE_BYTES} byte limit.",
            details={"size": size, "limit": MAX_FILE_SIZE_BYTES},
        )


def build_object_key(folder: str, filename: str) -> str:
    """Derive a collision-free key from a caller-supplied folder and filename.

    Only the extension is taken from `filename`; the stem is replaced by a
    UUID4 so a client cannot choose where its bytes land or overwrite another
    tenant's object by guessing a name.
    """
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    unique = str(uuid.uuid4())
    key = posixpath.join(folder, f"{unique}.{ext}" if ext else unique)
    return validate_object_key(key)
