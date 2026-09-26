"""Remembering deliveries, so that a valid one is only valid once.

A tolerance window bounds how long a captured delivery stays replayable. It does
not stop the replay: inside the window the capture is byte-for-byte a delivery
this server will accept, and it will accept it as many times as it is sent.
Narrowing the window trades that against every sender whose clock is off and
every retry that queued behind an outage, and no width makes the problem go
away.

What closes it is remembering. The guard is a set of delivery fingerprints with
a TTL — `claim()` both asks and answers, in one atomic operation, because a
`get`-then-`set` pair lets two copies of the same delivery arriving together
both find nothing and both proceed, which is the case a replay guard exists for.

## The TTL is not a free parameter

A record has to outlive the window its delivery is acceptable in, or the guard
forgets a delivery that can still be used — and it does so silently, because
nothing distinguishes "never seen" from "seen and expired". A delivery stamped
`T` is acceptable while the clock reads within `tolerance` of `T`, so at the
earliest it can arrive at `T - tolerance` and at the latest it is still good at
`T + tolerance`: the record must live `2 * tolerance`, not `tolerance`.
`WebhookVerifier` refuses to be built with a guard whose TTL is shorter, because
the failure is otherwise invisible in every test that does not wait.
"""

from __future__ import annotations

import asyncio
import time
from typing import Final, Protocol, runtime_checkable


@runtime_checkable
class ReplayGuard(Protocol):
    """The one operation a verifier needs, plus what it needs to check it.

    No `get`, no `forget`, no listing. A fingerprint that has been claimed must
    stay claimed for its TTL or the guard is not a guard, so there is nothing
    for a caller to do with a wider surface except get it wrong.
    """

    @property
    def name(self) -> str:
        """Short identifier, e.g. `"redis"`. Used in logs."""
        ...

    @property
    def ttl_seconds(self) -> float:
        """How long a claim is remembered.

        Part of the contract rather than an implementation detail: the verifier
        compares it against its own tolerance window at construction, which is
        the only moment the mismatch can be caught before it matters.
        """
        ...

    async def claim(self, fingerprint: str) -> bool:
        """Atomically record `fingerprint`, returning whether it was new.

        `True` means this caller is the first to present it and may proceed.
        `False` means it has been seen, and the delivery is a replay. Raises
        `ReplayGuardUnavailableError` if the answer cannot be determined —
        never `True`, which would be a guess in the one direction that matters.
        """
        ...

    async def close(self) -> None:
        """Release any connections held. Called once from the app lifespan."""
        ...


# Above this many entries, a claim sweeps the expired ones first. The dict is
# bounded by TTL times the arrival rate, and only an authenticated sender can
# add to it — claims happen after the signature verifies — so this is about a
# busy endpoint's steady state rather than about an attack.
_PURGE_THRESHOLD: Final[int] = 10_000


class InMemoryReplayGuard:
    """Fingerprints in a dict, with monotonic expiry stamps.

    Per-process, which makes it wrong for any deployment running more than one
    worker: a replay landing on a different worker is unseen there and is
    accepted. It exists so the verifier's own behaviour can be tested without a
    Redis server, and for a single-process development run. The factory warns
    when it is selected outside development.

    `time.monotonic` rather than wall-clock time, so an NTP step cannot expire a
    live claim or resurrect a dead one — which matters more here than in most
    caches, because the thing being expired is a security control.
    """

    def __init__(self, *, ttl_seconds: float = 900.0) -> None:
        self._claims: dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._ttl = ttl_seconds

    @property
    def name(self) -> str:
        return "memory"

    @property
    def ttl_seconds(self) -> float:
        return self._ttl

    def _purge_expired(self, now: float) -> None:
        self._claims = {
            fingerprint: expires_at
            for fingerprint, expires_at in self._claims.items()
            if expires_at > now
        }

    async def claim(self, fingerprint: str) -> bool:
        async with self._lock:
            now = time.monotonic()
            expires_at = self._claims.get(fingerprint)
            if expires_at is not None and expires_at > now:
                return False
            if len(self._claims) >= _PURGE_THRESHOLD:
                self._purge_expired(now)
            self._claims[fingerprint] = now + self._ttl
            return True

    async def close(self) -> None:
        """Nothing to close. Present so the lifespan needs no `isinstance`."""

    async def clear(self) -> None:
        """Drop every claim. For tests that reuse a process-wide guard."""
        async with self._lock:
            self._claims.clear()
