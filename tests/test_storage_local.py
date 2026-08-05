"""Filesystem-specific behaviour: containment, atomicity, and OS failures.

The contract suite covers what `LocalStorage` shares with the other backends.
What is left is what only a real filesystem can get wrong.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from src.storage.base import ObjectNotFoundError, StorageError
from src.storage.local import LocalStorage

PNG = "image/png"


async def test_put_creates_nested_directories(tmp_path: Path) -> None:
    backend = LocalStorage(tmp_path / "objects")

    await backend.put("a/b/c/file.png", b"payload", content_type=PNG)

    assert (tmp_path / "objects/a/b/c/file.png").read_bytes() == b"payload"


async def test_put_leaves_no_temp_file_behind(tmp_path: Path) -> None:
    backend = LocalStorage(tmp_path / "objects")

    await backend.put("uploads/file.png", b"payload", content_type=PNG)

    children = sorted(p.name for p in (tmp_path / "objects/uploads").iterdir())
    assert children == ["file.png"]


async def test_root_is_resolved_and_expanded(tmp_path: Path) -> None:
    backend = LocalStorage(tmp_path / "objects" / ".." / "objects")

    assert backend.root == (tmp_path / "objects").resolve()


async def test_a_symlink_cannot_redirect_a_write_outside_the_root(
    tmp_path: Path,
) -> None:
    root = tmp_path / "objects"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    (root / "escape").symlink_to(outside, target_is_directory=True)
    backend = LocalStorage(root)

    with pytest.raises(StorageError, match="escapes the storage root"):
        await backend.put("escape/pwned.png", b"x", content_type=PNG)

    assert list(outside.iterdir()) == []


async def test_put_translates_an_os_error(tmp_path: Path) -> None:
    # A regular file where the root directory should be: mkdir cannot proceed.
    root = tmp_path / "objects"
    root.write_text("not a directory")
    backend = LocalStorage(root)

    with pytest.raises(StorageError, match="Failed to write object"):
        await backend.put("uploads/file.png", b"x", content_type=PNG)


async def test_get_translates_an_os_error_that_is_not_absence(
    tmp_path: Path,
) -> None:
    root = tmp_path / "objects"
    backend = LocalStorage(root)
    (root / "uploads/file.png").mkdir(parents=True)

    with pytest.raises(StorageError, match="Failed to read object"):
        await backend.get("uploads/file.png")


async def test_delete_translates_an_os_error_that_is_not_absence(
    tmp_path: Path,
) -> None:
    root = tmp_path / "objects"
    backend = LocalStorage(root)
    (root / "uploads/file.png").mkdir(parents=True)

    with pytest.raises(StorageError, match="Failed to delete object"):
        await backend.delete("uploads/file.png")


async def test_exists_is_false_for_a_directory(tmp_path: Path) -> None:
    root = tmp_path / "objects"
    backend = LocalStorage(root)
    (root / "uploads/file.png").mkdir(parents=True)

    assert await backend.exists("uploads/file.png") is False


async def test_list_keys_on_a_root_that_does_not_exist(tmp_path: Path) -> None:
    backend = LocalStorage(tmp_path / "never-created")

    assert await backend.list_keys() == []


async def test_get_after_delete_raises_object_not_found(tmp_path: Path) -> None:
    backend = LocalStorage(tmp_path / "objects")
    await backend.put("uploads/file.png", b"x", content_type=PNG)
    await backend.delete("uploads/file.png")

    with pytest.raises(ObjectNotFoundError):
        await backend.get("uploads/file.png")
