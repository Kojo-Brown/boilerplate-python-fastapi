"""What `MemoryStorage` offers beyond the shared contract.

It is the backend tests inject, so the extras that make it usable as a double —
recorded metadata and teardown — are themselves worth testing.
"""

from __future__ import annotations

import asyncio

import pytest

from src.storage.base import ObjectNotFoundError, StoredObject
from src.storage.memory import MemoryStorage

PNG = "image/png"


async def test_stat_returns_the_metadata_recorded_at_put() -> None:
    backend = MemoryStorage()
    await backend.put("uploads/a.png", b"payload", content_type=PNG)

    assert await backend.stat("uploads/a.png") == StoredObject(
        key="uploads/a.png", size=7, content_type=PNG
    )


async def test_stat_reflects_the_latest_put() -> None:
    backend = MemoryStorage()
    await backend.put("uploads/a.png", b"payload", content_type=PNG)
    await backend.put("uploads/a.png", b"x", content_type="text/plain")

    stored = await backend.stat("uploads/a.png")
    assert stored.size == 1
    assert stored.content_type == "text/plain"


async def test_stat_missing_key_raises_object_not_found() -> None:
    backend = MemoryStorage()

    with pytest.raises(ObjectNotFoundError):
        await backend.stat("uploads/missing.png")


async def test_clear_drops_every_object() -> None:
    backend = MemoryStorage()
    await backend.put("uploads/a.png", b"x", content_type=PNG)
    await backend.put("uploads/b.png", b"y", content_type=PNG)

    await backend.clear()

    assert await backend.list_keys() == []
    with pytest.raises(ObjectNotFoundError):
        await backend.stat("uploads/a.png")


async def test_delete_removes_the_metadata_too() -> None:
    backend = MemoryStorage()
    await backend.put("uploads/a.png", b"x", content_type=PNG)
    await backend.delete("uploads/a.png")

    with pytest.raises(ObjectNotFoundError):
        await backend.stat("uploads/a.png")


async def test_concurrent_puts_all_land() -> None:
    backend = MemoryStorage()

    await asyncio.gather(
        *(
            backend.put(f"uploads/{index}.png", b"x", content_type=PNG)
            for index in range(50)
        )
    )

    assert len(await backend.list_keys()) == 50
