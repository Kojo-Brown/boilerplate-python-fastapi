"""What an exported user is, and what storage has to offer to produce one.

## The record

`UserExportRecord` is the published shape of the export, and it is a
declaration of what is *left out* as much as what is included. `users` carries
a password hash, an OAuth subject claim and an optimistic-concurrency counter,
none of which belong in a file that gets emailed around, dropped in a bucket,
or loaded into a warehouse. Listing the fields explicitly — rather than dumping
the row and deleting a few keys — means a column added to the model later is
absent from the export until somebody decides it should be there, which is the
direction that fails safe. `tests/test_export_users.py` asserts the hash is not
in the output, so the decision is checked rather than remembered.

## The port

`UserExportSource` is one method, and it is not `UserStore`: `src/repositories/
protocols.py` describes what *authentication* needs, and adding a bulk read to
it would charge every in-memory fake in the suite for a method none of them
uses. This is a second, narrower port over the same table, which is what
protocols are for.

The method is a plain `def` returning an `AsyncIterator`, not an `async def`.
That is what an async generator function's type actually is — calling it
returns the iterator without an `await` — and declaring it `async def` here
would make every implementation fail to match.

## Why the adapter does not select ORM entities

`UserRepository.stream_export` (`src/repositories/user.py`) selects the eight
columns above and builds these records directly, rather than streaming `User`
objects and reading attributes off them.

The reason is the field list, not memory: `select(User)` emits `SELECT
users.hashed_password, ...`, so an export that must not contain the password
hash would be fetching one per row and discarding it in the application. The
hash crosses the network, sits in the driver's buffers for the life of the
batch, and is one careless `model_dump()` away from the file. Asking for the
published columns means it never leaves Postgres, which is a stronger
guarantee than remembering to drop a key.

Memory is explicitly *not* the argument, because the obvious version of it is
false: SQLAlchemy's identity map holds weak references, so streamed entities
are collected as they go out of scope rather than accumulating. What entities
do cost per row is instrumentation, identity-map bookkeeping and — for `User`,
which declares a `version_id_col` — the optimistic-concurrency machinery, all
of it for values that are serialised and dropped a moment later.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict


class UserExportRecord(BaseModel):
    """One line of a user export.

    Frozen for the reason everything else here is (`docs/immutability.md`): a
    record is a value, and the producer hands it to an encoder several stages
    away.
    """

    model_config = ConfigDict(frozen=True)

    id: uuid.UUID
    email: str
    role: str
    is_active: bool
    is_verified: bool
    notification_channel: str
    created_at: datetime
    updated_at: datetime


@runtime_checkable
class UserExportSource(Protocol):
    """Bulk, ordered reads of the user table, for export."""

    def stream_export(
        self,
        *,
        batch_size: int,
        active_only: bool,
    ) -> AsyncIterator[UserExportRecord]:
        """Yield every user, oldest primary key first.

        Args:
            batch_size: How many rows the implementation should fetch per
                round trip. A knob for round trips, not for memory: the caller
                bounds memory with the read-ahead depth in
                `src/streaming/backpressure.py`.
            active_only: Skip deactivated accounts.
        """
        ...


__all__ = ["UserExportRecord", "UserExportSource"]
