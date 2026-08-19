"""Redis-backed lock backend: one Lua script per operation.

Every operation here is a compare-then-act, and every one of them is a bug if
the compare and the act are two round trips. So each is a Lua script, which
Redis runs to completion without interleaving another client's commands — the
same property `SET NX` gives `src/idempotency/redis_store.py` for free, bought
here with `EVAL` because minting the token and claiming the key have to happen
together.

Two keys per lock name:

`{namespace}:lock:{name}` holds the live claim, with a `PX` expiry. Its value
is `{token}:{owner}` — the token alone would be enough to identify the lease
(the counter never repeats), and the owner is there so a log line and a 409 can
say *who* holds it rather than only that somebody does.

`{namespace}:fence:{name}` is the token counter, `INCR`ed on each successful
acquisition. **It has no TTL, and that is load-bearing.** If it disappears, the
next `INCR` returns 1 and the store starts handing out tokens a resource has
already accepted and moved past — at which point the fencing check silently
stops rejecting the writers it exists to reject. Two consequences for how the
server is configured, both of which the docs repeat:

* `maxmemory-policy` must not be one of the `allkeys-*` policies, which evict
  keys that have no expiry set. `noeviction` or a `volatile-*` policy is
  required; a lock store that can evict its own counters is not one.
* A failover to a replica that had not yet received the last `INCR`s replays
  tokens for the same reason. Redis replication is asynchronous, so this is a
  property of the deployment, not a misconfiguration to be fixed — see
  `docs/distributed-locking.md` for what it costs and when it matters.

The scripts are registered through `Redis.register_script`, which sends
`EVALSHA` and falls back to a full `EVAL` on `NOSCRIPT`. That fallback is why a
`SCRIPT FLUSH`, a restart or a failover to a server that never saw these
scripts is a slower first call rather than an outage.
"""

from __future__ import annotations

import math
from typing import Any, Final

import structlog
from redis.asyncio import Redis
from redis.commands.core import AsyncScript
from redis.exceptions import RedisError

from src.decorators.base import DEFAULT_CLOCK, Clock
from src.distributed_lock.base import (
    Lease,
    LockBackendUnavailableError,
    LockState,
    ReleaseOutcome,
    validate_lock_name,
)

logger = structlog.get_logger(__name__)

# KEYS[1] lock key, KEYS[2] fence counter. ARGV[1] owner, ARGV[2] ttl in ms.
#
# Both branches return the lock's *value* in slot 2 and a millisecond TTL in
# slot 3, so the caller parses one shape either way. Slot 1 is the flag.
#
# A Lua nil inside a returned table truncates the reply at that point, turning
# a three-element answer into a one-element one — hence no nils here, and hence
# the held branch reading PTTL rather than returning nothing for it.
_ACQUIRE_LUA: Final[str] = """
local current = redis.call('GET', KEYS[1])
if current then
  return {0, current, redis.call('PTTL', KEYS[1])}
end
local token = redis.call('INCR', KEYS[2])
local value = token .. ':' .. ARGV[1]
redis.call('SET', KEYS[1], value, 'PX', ARGV[2])
return {1, value, tonumber(ARGV[2])}
"""

# KEYS[1] lock key. ARGV[1] the value we expect to find, ARGV[2] ttl in ms.
#
# The comparison is the whole point. `PEXPIRE` on a key we no longer own would
# extend somebody else's lease, which is worse than failing to extend our own.
_EXTEND_LUA: Final[str] = """
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
if current ~= ARGV[1] then return -1 end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
"""

# KEYS[1] lock key. ARGV[1] the value we expect to find.
#
# A bare `DEL` here is the classic Redis-lock bug: after our lease expired and
# another holder took the name, an unconditional delete frees *their* lock, and
# a third caller walks straight into the section both of them are in.
_RELEASE_LUA: Final[str] = """
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
if current ~= ARGV[1] then return -1 end
redis.call('DEL', KEYS[1])
return 1
"""

_RELEASE_OUTCOMES: Final[dict[int, ReleaseOutcome]] = {
    0: ReleaseOutcome.EXPIRED,
    -1: ReleaseOutcome.NOT_OWNER,
    1: ReleaseOutcome.RELEASED,
}


def _decode(raw: object) -> str:
    """Redis replies as bytes here; Lua string returns arrive the same way."""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "replace")
    return str(raw)


def split_lock_value(value: str) -> tuple[int, str]:
    """Split a `{token}:{owner}` lock value.

    Splits on the *first* colon only: owners are arbitrary strings and a
    hostname-and-pid owner will contain colons of its own, where the token
    never does.

    Raises `ValueError` for anything that is not this backend's encoding —
    which in practice means a key an operator wrote by hand, since nothing else
    writes to this namespace.
    """
    token_text, separator, owner = value.partition(":")
    if not separator:
        raise ValueError(f"Malformed lock value: {value!r}")
    return int(token_text), owner


class RedisLockBackend:
    """`LockBackend` over a Redis server.

    The client is injected rather than built here so a caller can hand in a
    pool it already owns, and so tests can point at a throwaway namespace. Use
    `from_url` for the ordinary case.
    """

    def __init__(
        self,
        client: Redis,
        *,
        namespace: str = "dlock",
        clock: Clock = DEFAULT_CLOCK,
    ) -> None:
        self._client = client
        self._namespace = namespace
        self._clock = clock
        self._acquire: AsyncScript = client.register_script(_ACQUIRE_LUA)
        self._extend: AsyncScript = client.register_script(_EXTEND_LUA)
        self._release: AsyncScript = client.register_script(_RELEASE_LUA)

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        namespace: str = "dlock",
        clock: Clock = DEFAULT_CLOCK,
    ) -> RedisLockBackend:
        """Build a backend owning its own connection pool."""
        return cls(
            Redis.from_url(url, decode_responses=False),
            namespace=namespace,
            clock=clock,
        )

    @property
    def name(self) -> str:
        return "redis"

    def _lock_key(self, name: str) -> str:
        return f"{self._namespace}:lock:{name}"

    def _fence_key(self, name: str) -> str:
        return f"{self._namespace}:fence:{name}"

    async def acquire(
        self, name: str, *, owner: str, ttl_seconds: float
    ) -> Lease | None:
        validate_lock_name(name)
        ttl_ms = _ttl_milliseconds(ttl_seconds)
        # Before the call, not after. The lease is already ticking while the
        # request is in flight, and a deadline taken from the reply would hand
        # the caller a round trip of time the server has no record of.
        started = self._clock()
        try:
            reply: Any = await self._acquire(
                keys=[self._lock_key(name), self._fence_key(name)],
                args=[owner, ttl_ms],
            )
        except RedisError as exc:
            raise LockBackendUnavailableError(
                "Could not reach the distributed lock store."
            ) from exc

        claimed, value, _ttl = int(reply[0]), _decode(reply[1]), int(reply[2])
        if not claimed:
            return None

        token, _owner = split_lock_value(value)
        return Lease(
            name=name,
            token=token,
            owner=owner,
            ttl_seconds=ttl_seconds,
            expires_at=started + ttl_seconds,
        )

    async def extend(self, lease: Lease, *, ttl_seconds: float) -> Lease | None:
        ttl_ms = _ttl_milliseconds(ttl_seconds)
        started = self._clock()
        try:
            reply: Any = await self._extend(
                keys=[self._lock_key(lease.name)],
                args=[_lock_value(lease), ttl_ms],
            )
        except RedisError as exc:
            raise LockBackendUnavailableError(
                "Could not extend the distributed lock lease."
            ) from exc

        if int(reply) != 1:
            return None
        return lease.renewed_for(
            ttl_seconds=ttl_seconds, expires_at=started + ttl_seconds
        )

    async def release(self, lease: Lease) -> ReleaseOutcome:
        try:
            reply: Any = await self._release(
                keys=[self._lock_key(lease.name)], args=[_lock_value(lease)]
            )
        except RedisError as exc:
            raise LockBackendUnavailableError(
                "Could not release the distributed lock."
            ) from exc

        return _RELEASE_OUTCOMES[int(reply)]

    async def inspect(self, name: str) -> LockState | None:
        pipeline = self._client.pipeline(transaction=True)
        pipeline.get(self._lock_key(name))
        pipeline.pttl(self._lock_key(name))
        try:
            raw_value, raw_ttl = await pipeline.execute()
        except RedisError as exc:
            raise LockBackendUnavailableError(
                "Could not read the distributed lock store."
            ) from exc

        if raw_value is None:
            return None

        try:
            token, owner = split_lock_value(_decode(raw_value))
        except ValueError:
            # Something that is not one of ours is sitting on the key. Reported
            # as "free" would be a lie the caller would act on, so say what is
            # there instead, with a token that cannot pass a fencing check.
            logger.warning("distributed_lock.foreign_value", key=self._lock_key(name))
            return LockState(name=name, token=0, owner="unknown", ttl_seconds=None)

        # PTTL answers -1 for a key with no expiry and -2 for one that is gone.
        # Neither is reachable through this backend — every write sets PX, and
        # the pipeline is a MULTI/EXEC so the key cannot vanish between the two
        # commands — but an operator can SET a key by hand, and reporting -1 as
        # "one millisecond left" would be a strange way to find that out.
        ttl_ms = int(raw_ttl)
        return LockState(
            name=name,
            token=token,
            owner=owner,
            ttl_seconds=None if ttl_ms < 0 else ttl_ms / 1000.0,
        )

    async def close(self) -> None:
        """Close the client and its pool.

        Swallows `RedisError`: this runs during shutdown, and a server that is
        already gone is not a reason to fail a clean exit.
        """
        try:
            await self._client.aclose()
        except RedisError:  # pragma: no cover - shutdown against a dead server
            logger.warning("distributed_lock.close_failed", backend=self.name)


def _lock_value(lease: Lease) -> str:
    """The exact string the store must hold for `lease` to still be ours."""
    return f"{lease.token}:{lease.owner}"


def _ttl_milliseconds(ttl_seconds: float) -> int:
    """Convert a TTL to the integer milliseconds `PX`/`PEXPIRE` require.

    Rounded *up*, and floored at one: `PX 0` is an error, and truncating a
    sub-millisecond TTL to zero would turn a very short lease into a failed
    command rather than a very short lease. Callers get at most a millisecond
    more than they asked for, which is the harmless direction.
    """
    if ttl_seconds <= 0:
        raise ValueError("ttl_seconds must be positive.")
    return max(1, math.ceil(ttl_seconds * 1000))
