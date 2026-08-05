from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from src.config import Settings
from src.storage.base import StorageBackend, StorageError, UnknownStorageBackendError
from src.storage.factory import StorageFactory, get_storage
from src.storage.local import LocalStorage
from src.storage.memory import MemoryStorage
from src.storage.s3 import S3Storage


def make_settings(**overrides: object) -> Settings:
    """A Settings instance that ignores the ambient .env and environment."""
    base: dict[str, object] = {
        "DATABASE_URL": "postgresql+asyncpg://fake:fake@localhost/fake",
        "SECRET_KEY": "not-a-real-secret-key-for-tests-only",
        "AWS_S3_BUCKET": "test-bucket",
    }
    return Settings(_env_file=None, **{**base, **overrides})  # type: ignore[arg-type]


@pytest.fixture(autouse=True)
def restore_registry() -> Iterator[None]:
    """Undo `register`/`unregister` and the `get_storage` singleton per test."""
    yield
    StorageFactory.reset()
    get_storage.cache_clear()


# --- create ---


def test_create_returns_the_configured_backend() -> None:
    backend = StorageFactory.create(config=make_settings(STORAGE_BACKEND="memory"))

    assert isinstance(backend, MemoryStorage)
    assert backend.name == "memory"


def test_create_explicit_name_overrides_configuration() -> None:
    config = make_settings(STORAGE_BACKEND="memory")

    backend = StorageFactory.create("s3", config=config)

    assert isinstance(backend, S3Storage)
    assert backend.bucket == "test-bucket"


def test_create_local_uses_the_configured_root(tmp_path: Path) -> None:
    config = make_settings(
        STORAGE_BACKEND="local", STORAGE_LOCAL_ROOT=str(tmp_path / "objects")
    )

    backend = StorageFactory.create(config=config)

    assert isinstance(backend, LocalStorage)
    assert backend.root == (tmp_path / "objects").resolve()


def test_create_returns_a_new_instance_each_call() -> None:
    config = make_settings(STORAGE_BACKEND="memory")

    assert StorageFactory.create(config=config) is not StorageFactory.create(
        config=config
    )


def test_create_unknown_backend_raises_with_the_available_names() -> None:
    with pytest.raises(UnknownStorageBackendError) as excinfo:
        StorageFactory.create("gcs", config=make_settings())

    error = excinfo.value
    assert error.status_code == 500
    assert error.error_code == "UNKNOWN_STORAGE_BACKEND"
    assert error.details == {
        "requested": "gcs",
        "available": ["local", "memory", "s3"],
    }


def test_s3_backend_without_a_bucket_fails_loudly() -> None:
    # An empty AWS_S3_BUCKET would otherwise surface as a confusing boto3
    # ParamValidationError on the first upload, long after the misconfiguration.
    with pytest.raises(StorageError, match="AWS_S3_BUCKET"):
        StorageFactory.create(config=make_settings(AWS_S3_BUCKET=""))


def test_available_lists_the_built_in_backends() -> None:
    assert StorageFactory.available() == ("local", "memory", "s3")


# --- register / unregister ---


def test_register_adds_a_backend_without_editing_the_factory() -> None:
    sentinel = MemoryStorage()
    StorageFactory.register("gcs", lambda _: sentinel)

    assert "gcs" in StorageFactory.available()
    assert StorageFactory.create("gcs", config=make_settings()) is sentinel


def test_register_replaces_an_existing_builder() -> None:
    sentinel = MemoryStorage()
    StorageFactory.register("s3", lambda _: sentinel)

    assert StorageFactory.create("s3", config=make_settings()) is sentinel


def test_unregister_removes_a_backend() -> None:
    StorageFactory.unregister("memory")

    assert "memory" not in StorageFactory.available()
    with pytest.raises(UnknownStorageBackendError):
        StorageFactory.create("memory", config=make_settings())


def test_unregister_is_a_no_op_for_an_unknown_name() -> None:
    StorageFactory.unregister("never-registered")

    assert StorageFactory.available() == ("local", "memory", "s3")


def test_reset_restores_the_built_in_registry() -> None:
    StorageFactory.register("gcs", lambda _: MemoryStorage())
    StorageFactory.unregister("s3")

    StorageFactory.reset()

    assert StorageFactory.available() == ("local", "memory", "s3")


# --- get_storage ---


def test_get_storage_is_a_singleton() -> None:
    StorageFactory.register("s3", lambda _: MemoryStorage())
    get_storage.cache_clear()

    assert get_storage() is get_storage()


def test_get_storage_cache_clear_picks_up_a_new_backend() -> None:
    first = MemoryStorage()
    second = MemoryStorage()
    StorageFactory.register("s3", lambda _: first)
    get_storage.cache_clear()
    assert get_storage() is first

    StorageFactory.register("s3", lambda _: second)
    assert get_storage() is first, "still cached until cache_clear()"

    get_storage.cache_clear()
    assert get_storage() is second


def test_get_storage_returns_something_satisfying_the_protocol() -> None:
    StorageFactory.register("s3", lambda _: MemoryStorage())
    get_storage.cache_clear()

    backend: StorageBackend = get_storage()

    assert isinstance(backend, StorageBackend)
