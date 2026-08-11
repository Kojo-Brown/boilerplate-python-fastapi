"""What authentication policy needs from storage, stated as protocols.

`AuthService` used to name `UserRepository` and `RefreshTokenRepository`
directly and build them itself (`docs/solid.md` finding 4). Policy naming its
own concrete collaborators is the dependency-inversion complaint in its
textbook form: the interesting half of the codebase pointed at the boring half,
so nothing could stand in for the database and every service test had to fake a
SQLAlchemy session convincingly enough that `flush()` populated column defaults.

These protocols are the seam. They are declared here rather than in
`base.py` because that module holds `BaseRepository`, which is an
implementation — the port and the adapter should not share a file.

Two properties are deliberate:

**They describe a store, not a table.** Each method is one thing the service
asks for, with the arguments it actually passes. `create` spells its fields out
instead of inheriting `BaseRepository.create(**kwargs: Any)`, so a fake cannot
accept a misspelled column name that the real repository would reject at
INSERT, and mypy checks the call sites.

**They stop at what the service uses.** `update`, `delete`, `list`, `count`,
`deactivate` and `revoke_all_for_user` all exist on the concrete repositories
and none of them appear here. A protocol is a bill for every implementer;
adding a method nothing calls charges the fakes for it.

They do still traffic in the SQLAlchemy models — `User`, `RefreshToken` — and
that is a real, bounded compromise. Those classes are this codebase's domain
entities as well as its rows; splitting them would be a much larger change than
this one, and it would not buy what the seam is for. The cost of a concrete
model is that a fake has to construct one, which needs no session, no engine
and no event loop. The cost of a concrete *repository* was that a fake had to
be a session, which needed all three.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Protocol, runtime_checkable

from src.models.refresh_token import RefreshToken
from src.models.user import User


@runtime_checkable
class UserStore(Protocol):
    """Lookup and creation of users, as authentication uses them."""

    async def get(self, id: uuid.UUID) -> User | None:
        """Return the user with this primary key, or `None`."""
        ...

    async def get_by_email(self, email: str) -> User | None:
        """Return the user registered with this address, or `None`."""
        ...

    async def get_by_oauth(self, provider: str, sub: str) -> User | None:
        """Return the user linked to this provider identity, or `None`.

        `sub` is the provider's subject claim, which is stable for an account;
        matching on email instead would hand over an account to anyone who can
        get a provider to assert an address.
        """
        ...

    async def exists_by_email(self, email: str) -> bool:
        """Whether an address is already registered."""
        ...

    async def create(
        self,
        *,
        email: str,
        hashed_password: str | None,
        is_active: bool = ...,
        is_verified: bool = ...,
        oauth_provider: str | None = ...,
        oauth_sub: str | None = ...,
    ) -> User:
        """Insert a user and return it, populated as a flush would leave it.

        `hashed_password` is required and nullable rather than optional: an
        OAuth-only account has none, and defaulting the parameter would let a
        password registration that forgot to pass one create an account nobody
        can log into and anyone can claim by registering the same address.
        """
        ...


@runtime_checkable
class RefreshTokenStore(Protocol):
    """The refresh-token half of the session lifecycle."""

    async def get_by_token(self, token: str) -> RefreshToken | None:
        """Return the stored record for this token string, or `None`."""
        ...

    async def create(
        self,
        *,
        token: str,
        user_id: uuid.UUID,
        expires_at: datetime,
    ) -> RefreshToken:
        """Store a newly issued refresh token."""
        ...

    async def revoke(self, token: str) -> bool:
        """Mark a token unusable. `False` if it was never stored.

        A logout for a token that does not exist is not an error — the caller
        wanted it gone and it is gone — which is why this answers with a bool
        rather than raising.
        """
        ...
