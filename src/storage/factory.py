"""Backend selection.

The point of the factory is that nothing outside this module needs to know
which backend is in use, or how to build one. Callers depend on
`StorageBackend`; configuration decides the implementation.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import lru_cache
from typing import ClassVar, Final, Literal

import structlog

from src.config import Settings, settings
from src.immutable import FrozenDict
from src.storage.base import StorageBackend, UnknownStorageBackendError
from src.storage.local import LocalStorage
from src.storage.memory import MemoryStorage
from src.storage.s3 import S3Storage

logger = structlog.get_logger(__name__)

StorageBackendName = Literal["s3", "local", "memory"]

BackendBuilder = Callable[[Settings], StorageBackend]

DEFAULT_BACKENDS: Final[FrozenDict[str, BackendBuilder]] = FrozenDict[
    str, BackendBuilder
](
    {
        "s3": lambda config: S3Storage(config.AWS_S3_BUCKET),
        "local": lambda config: LocalStorage(config.STORAGE_LOCAL_ROOT),
        "memory": lambda _: MemoryStorage(),
    }
)


class StorageFactory:
    """Builds a `StorageBackend` from a name and a settings object.

    Extension does not mean editing this class: `register` adds a builder, so a
    GCS or Azure backend is a new module plus one registration call, and every
    existing caller keeps working untouched.
    """

    _builders: ClassVar[dict[str, BackendBuilder]] = dict(DEFAULT_BACKENDS)

    @classmethod
    def register(cls, name: str, builder: BackendBuilder) -> None:
        """Add or replace the builder for `name`."""
        cls._builders[name] = builder

    @classmethod
    def unregister(cls, name: str) -> None:
        """Remove a builder. No-op if it was never registered."""
        cls._builders.pop(name, None)

    @classmethod
    def reset(cls) -> None:
        """Restore the built-in registry. For tests that call `register`."""
        cls._builders = dict(DEFAULT_BACKENDS)

    @classmethod
    def available(cls) -> tuple[str, ...]:
        return tuple(sorted(cls._builders))

    @classmethod
    def create(
        cls,
        backend: str | None = None,
        *,
        config: Settings | None = None,
    ) -> StorageBackend:
        """Return a new backend instance.

        `backend` defaults to `STORAGE_BACKEND` and `config` to the process
        settings, so `StorageFactory.create()` is the configured backend and
        both arguments exist for tests and for scripts that need a second
        backend alongside the configured one (a migration, say).
        """
        resolved_config = config if config is not None else settings
        name = backend if backend is not None else resolved_config.STORAGE_BACKEND

        try:
            builder = cls._builders[name]
        except KeyError as exc:
            raise UnknownStorageBackendError(name, cls.available()) from exc

        instance = builder(resolved_config)
        logger.debug("storage.backend_created", backend=name)
        return instance


@lru_cache(maxsize=1)
def get_storage() -> StorageBackend:
    """FastAPI dependency returning the process-wide configured backend.

    Cached because building an `S3Storage` per request would be wasteful and
    because `LocalStorage` and `MemoryStorage` are only useful when every
    caller sees the same instance — an in-memory store rebuilt per request
    would lose every object it was handed. Call `get_storage.cache_clear()`
    after changing `STORAGE_BACKEND` in a test.
    """
    return StorageFactory.create()
