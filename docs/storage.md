# Object storage

Application code depends on the `StorageBackend` protocol, never on boto3.
Configuration picks the implementation; `StorageFactory` builds it.

```python
from src.storage import StorageBackend, get_storage

async def save_avatar(storage: StorageBackend, key: str, data: bytes) -> None:
    await storage.put(key, data, content_type="image/png")
```

In a route, take it as a dependency so a test can swap it:

```python
from fastapi import Depends
from src.storage import StorageBackend, get_storage

@router.post("/avatar")
async def upload_avatar(storage: StorageBackend = Depends(get_storage)) -> None:
    ...
```

## The contract

| Method | Behaviour |
|---|---|
| `put(key, data, *, content_type)` | Stores bytes, overwriting any object at `key`. Returns `StoredObject`. |
| `get(key)` | Returns the bytes, or raises `ObjectNotFoundError` (404). |
| `delete(key)` | Removes the object, or raises `ObjectNotFoundError` (404). |
| `exists(key)` | `True`/`False`. Raises `StorageError` if the backend could not answer. |
| `list_keys(prefix="")` | Every matching key, lexicographically sorted. |
| `name` | `"s3"`, `"local"` or `"memory"` — for logs and error messages. |

Two rules hold on every backend, and are enforced by `validate_upload` before
any I/O:

- `content_type` must be in `ALLOWED_CONTENT_TYPES`.
- `0 < len(data) <= MAX_FILE_SIZE_BYTES` (10 MiB).

They are the same two conditions the presigned POST policy carries, so a client
uploading directly to S3 and a server calling `put` are held to one policy
rather than two.

Keys are validated by `validate_object_key`: relative, slash-separated, each
segment starting with a letter or digit and otherwise limited to letters,
digits, `.`, `-` and `_`, at most 1024 characters. That is the intersection of
what S3 accepts and what is safe to append to a local root — `..`, absolute
paths, backslashes and NUL are rejected with a 400 rather than being silently
rewritten. Use `build_object_key(folder, filename)` to derive a key from
user input; it keeps only the extension and replaces the stem with a UUID4, so
a client cannot choose where its bytes land.

### What the protocol deliberately leaves out

**Presigned URLs.** Only S3 can hand a browser a credential to upload directly.
Putting `presigned_upload` on the protocol would mean two of three backends
raising `NotImplementedError`, which is a lie the type checker would accept.
`generate_presigned_upload` / `generate_presigned_download` stay in
`src/storage/s3.py` as S3's own surface, and `/api/v1/uploads` uses them
directly.

**Reading `content_type` back.** Every backend validates and echoes it on
`put`, but only S3 persists it as object metadata. `LocalStorage` writes bytes
to a file and `MemoryStorage` holds them in a dict. If you need the content
type later, store it in your own table alongside the key — that is what a
production app should do regardless of backend.

## Backends

### `s3` (default)

`S3Storage`, over `AWS_S3_BUCKET`. boto3 is synchronous, so every call is
dispatched through `asyncio.to_thread` rather than blocking the event loop for
an AWS round-trip.

Two S3 behaviours are normalised here. `delete_object` succeeds on a key that
never existed, so `delete` issues a `head_object` first to honour the
"raise if absent" contract. And "missing" arrives as `NoSuchKey` from
`get_object` but as a bare `404` from `head_object`, so both are recognised —
while a throttled or forbidden response becomes a `StorageError`, never a
`False` from `exists` that would let a caller conclude the object is gone.

A missing `AWS_S3_BUCKET` fails at construction with a message naming the
setting, instead of surfacing as a boto3 `ParamValidationError` on the first
upload.

### `local`

`LocalStorage`, writing files under `STORAGE_LOCAL_ROOT`. For development and
for tests that want real bytes on a real disk.

Writes go to a temp file and are renamed into place, so a reader never sees a
half-written object and a failed write leaves the previous one intact. The
resolved path is checked for containment inside the root *after* resolution, so
a symlink planted in the tree cannot redirect a write outside it.

### `memory`

`MemoryStorage`, a dict guarded by an `asyncio.Lock`. For tests. It is
per-process and does not survive a restart, so it is never correct for a
deployment with more than one worker.

It adds two methods outside the protocol that make it useful as a double:
`stat(key)` returns the `StoredObject` recorded at `put` time (including the
content type the other backends cannot serve back), and `clear()` empties it
between cases.

## Configuration

| Setting | Default | Meaning |
|---|---|---|
| `STORAGE_BACKEND` | `s3` | `s3` \| `local` \| `memory` |
| `STORAGE_LOCAL_ROOT` | `./var/storage` | Root directory for the `local` backend |
| `AWS_S3_BUCKET` | — | Bucket for the `s3` backend |

`STORAGE_BACKEND` is a `Literal`, so an unrecognised value fails at settings
validation on boot rather than at the first upload.

## Adding a backend

Adding one does not mean editing `StorageFactory`:

```python
from src.storage import StorageFactory

class GCSStorage:
    ...  # implement the six members of StorageBackend

StorageFactory.register("gcs", lambda config: GCSStorage(config.GCS_BUCKET))
```

`StorageBackend` is a `Protocol`, so a new backend does not inherit from
anything — implementing the members is the whole requirement, and mypy checks
it structurally at the point the factory returns it.

Then extend the `STORAGE_BACKEND` literal in `src/config.py`, add the backend to
the parametrised fixture in `tests/test_storage_contract.py`, and the shared
contract suite runs against it unchanged. That suite existing is the reason
"interchangeable" is a checked claim rather than a hope.

## Testing against storage

```python
from src.storage import MemoryStorage, StorageFactory, get_storage

def test_something():
    backend = MemoryStorage()
    StorageFactory.register("s3", lambda _: backend)
    get_storage.cache_clear()
    ...
```

`get_storage` is `lru_cache`d — it is a process-wide singleton, which is what
makes an in-memory backend usable at all — so call `get_storage.cache_clear()`
after changing the registry or the setting, and `StorageFactory.reset()` in
teardown.
