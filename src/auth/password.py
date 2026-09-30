import secrets
from functools import cache

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError

from src.config import settings

_ph = PasswordHasher(
    time_cost=settings.ARGON2_TIME_COST,
    memory_cost=settings.ARGON2_MEMORY_COST,
    parallelism=settings.ARGON2_PARALLELISM,
    hash_len=settings.ARGON2_HASH_LEN,
    salt_len=settings.ARGON2_SALT_LEN,
)


def hash_password(plain: str) -> str:
    return _ph.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    try:
        _ph.verify(hashed, plain)
        return True
    except (VerifyMismatchError, VerificationError, InvalidHashError):
        return False


def needs_rehash(hashed: str) -> bool:
    return _ph.check_needs_rehash(hashed)


@cache
def decoy_hash() -> str:
    """An argon2 hash for the branch that has no real hash to check against.

    Verifying a password costs ~75ms here by design; *not* verifying one costs
    nothing. So a login that short-circuits on an unknown address answers
    measurably sooner than one that reaches the hasher, and the difference is
    three orders of magnitude — far outside network jitter, readable in a single
    sample, and a working "is this address registered" oracle for anyone who can
    time a request. `verify_password(password, decoy_hash())` is how the absent
    branch spends the same work as the present one.

    Derived from the live hasher rather than written out as a constant, because
    argon2 reads its cost parameters from the *hash string* it is verifying: a
    literal committed here would keep verifying at whatever `ARGON2_TIME_COST`
    was when it was generated, so tuning the setting up would quietly reopen the
    gap it closes. Hashing a random value rather than a fixed one costs nothing
    and means no password can ever match it, so this cannot become a credential.

    Cached, so the hash is computed once per process. `prime_password_hasher()`
    exists to get that one computation out of the way before it lands on a
    request — see there.
    """
    return _ph.hash(secrets.token_urlsafe(32))


def prime_password_hasher() -> None:
    """Pay `decoy_hash()`'s one-off cost now rather than on a request.

    Without this the first login against an unknown address pays a *hash* plus a
    verify while every later one pays only the verify, so that single request is
    about twice as slow as the branch it is supposed to be indistinguishable
    from. One sample at the very start of a process is not much of an oracle,
    which is the honest description of what this prevents — it is called from the
    lifespan because it costs one argon2 hash at start-up and removes the
    question entirely.
    """
    decoy_hash()
