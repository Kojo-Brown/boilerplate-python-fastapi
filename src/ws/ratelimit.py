"""Per-connection inbound limits, and the three tempting ways to get them wrong.

The rate limit in `src/limiter.py` is per client address and is applied by
middleware to requests. Neither half of that reaches a WebSocket: the whole
connection is *one* request, so a client that floods a thousand messages down
an established socket has been counted once, and an address is the wrong key
for a protocol whose entire cost model is per connection.

So the budget here is per connection, refilled continuously, and spent by
inbound messages. What matters is not the bucket — that part is textbook — but
what happens when it is empty.

## Do not sleep

    if not bucket.take():
        await asyncio.sleep(bucket.retry_after())   # ← no

It reads like backpressure and is the opposite. The frames are already in the
server's buffers; sleeping in the receive loop does not slow the sender down,
it just stops draining what the sender is still filling. A client that ignores
the limit now costs a suspended task *and* an unbounded read buffer, and the
one lever that was supposed to protect the process is holding it open. Nothing
in this module awaits anything.

## Do not queue the overflow

Buffering rejected messages to replay when the bucket refills turns a rate
limit into a memory limit with extra steps, and delivers a burst of stale
messages at a time nobody asked for. A rejected message is *rejected*: the
client is told, with the number of seconds that would have helped, and it is
the client that decides whether that message is still worth sending.

## Two dimensions, because one is trivially evaded

A limit on messages per second is defeated by one-megabyte messages; a limit
on bytes per second is defeated by a flood of `{}`. Each costs a different
resource — the byte budget covers parsing and fan-out volume, the message
budget covers per-message work like a room lookup and a broadcast — so both
are bounded, and a message has to fit in both to be admitted.

The two are checked together and consumed together, which is why `take` is one
call over a `RateLimiter` rather than two over separate buckets: taking from
the message bucket and then failing the byte check would charge a client for a
message it was not allowed to send, and a client sending steadily over the byte
limit would find its message budget drained too.

## Refusal is not disconnection, until it is

One rejected message is a client that misjudged its own rate; a hundred in a
row is a client that is not reading its errors. `ViolationBudget` counts
*consecutive* rejections — an accepted message resets it — so an application
that occasionally bumps the ceiling is never disconnected for it, and one that
ignores the signal entirely is.
"""

from __future__ import annotations

from dataclasses import dataclass

from src.decorators.base import DEFAULT_CLOCK, Clock


class TokenBucket:
    """A continuously refilling allowance, in whatever unit the caller counts.

    Continuous rather than a fixed window: a window resets on a boundary, so a
    client that learns where the boundary is may spend two full windows back to
    back across it and still be "within the limit". A bucket has no boundary to
    find — the allowance is `rate` per second with a burst of `capacity`,
    always.

    Not thread-safe and not intended to be: one of these belongs to one
    connection, which is handled by one task on one event loop.
    """

    __slots__ = ("_capacity", "_clock", "_rate", "_tokens", "_updated")

    def __init__(
        self,
        *,
        capacity: float,
        rate: float,
        clock: Clock = DEFAULT_CLOCK,
    ) -> None:
        """
        Args:
            capacity: The burst. Also the largest single `take` that can ever
                succeed, which is why a byte bucket's capacity has to be at
                least the maximum message size — otherwise a legal message is
                permanently unaffordable and the client retries it forever.
            rate: Tokens added per second.
            clock: Monotonic seconds. `time.monotonic` by default; pass a
                controllable one in tests rather than sleeping.

        Raises:
            ValueError: `capacity` or `rate` is not positive.
        """
        if capacity <= 0:
            raise ValueError(f"capacity must be positive, got {capacity}.")
        if rate <= 0:
            raise ValueError(f"rate must be positive, got {rate}.")
        self._capacity = capacity
        self._rate = rate
        self._clock = clock
        self._tokens = capacity
        self._updated = clock()

    @property
    def capacity(self) -> float:
        return self._capacity

    @property
    def tokens(self) -> float:
        """Tokens available as of now, refill applied."""
        self._refill()
        return self._tokens

    def _refill(self) -> None:
        now = self._clock()
        # Guarded rather than trusted: `time.monotonic` does not go backwards,
        # but a clock passed in by a caller might, and a negative elapsed would
        # *remove* tokens — a limiter that gets stricter when someone's test
        # double rewinds.
        elapsed = max(0.0, now - self._updated)
        self._updated = now
        if elapsed:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)

    def take(self, cost: float = 1.0) -> bool:
        """Spend `cost` tokens if they are available. Never blocks.

        Returns `False` and spends nothing when the balance is short, including
        when `cost` exceeds `capacity` and no amount of waiting would help —
        `retry_after` is what distinguishes the two.
        """
        self._refill()
        if cost > self._tokens:
            return False
        self._tokens -= cost
        return True

    def retry_after(self, cost: float = 1.0) -> float:
        """Seconds until `take(cost)` would succeed, or `inf` if it never would.

        `inf` rather than an arbitrary large number: "wait 3.4e38 seconds" and
        "this will never fit" are different things to tell a client, and the
        second one means *change the message*, not *wait*.
        """
        if cost > self._capacity:
            return float("inf")
        self._refill()
        if cost <= self._tokens:
            return 0.0
        return (cost - self._tokens) / self._rate


@dataclass(frozen=True, slots=True)
class Decision:
    """The outcome of offering one message to the limiter.

    Args:
        allowed: Whether the message may be processed.
        retry_after: Seconds until it would be affordable. `0.0` when allowed;
            `inf` when the message can never fit the burst.
        disconnect: Whether the consecutive-violation budget is now spent and
            the connection should be closed. Never `True` when `allowed`.
    """

    allowed: bool
    retry_after: float = 0.0
    disconnect: bool = False


class RateLimiter:
    """One connection's inbound budget: messages, bytes, and a patience limit.

    Both buckets are consumed by an admitted message and neither by a rejected
    one, so a client held under the limit is not also being charged for the
    attempts.
    """

    __slots__ = ("_bytes", "_max_violations", "_messages", "_violations")

    def __init__(
        self,
        *,
        messages_per_second: float,
        message_burst: int,
        bytes_per_second: float,
        byte_burst: int,
        max_consecutive_violations: int,
        clock: Clock = DEFAULT_CLOCK,
    ) -> None:
        """
        Args:
            messages_per_second: Sustained inbound message rate.
            message_burst: Messages that may arrive at once after a quiet
                period.
            bytes_per_second: Sustained inbound volume.
            byte_burst: Bytes that may arrive at once. Must be at least
                `WS_MAX_MESSAGE_BYTES`, or a maximum-size message is
                permanently unaffordable — the endpoint asserts this at
                construction rather than discovering it under load.
            max_consecutive_violations: Rejections in a row before the
                connection is closed. Must be at least 1.
            clock: Monotonic seconds, shared by both buckets.

        Raises:
            ValueError: any bound is not positive.
        """
        if max_consecutive_violations < 1:
            raise ValueError(
                "max_consecutive_violations must be at least 1, got "
                f"{max_consecutive_violations}."
            )
        self._messages = TokenBucket(
            capacity=message_burst, rate=messages_per_second, clock=clock
        )
        self._bytes = TokenBucket(
            capacity=byte_burst, rate=bytes_per_second, clock=clock
        )
        self._max_violations = max_consecutive_violations
        self._violations = 0

    @property
    def violations(self) -> int:
        """Consecutive rejections. Reset by any admitted message."""
        return self._violations

    @property
    def byte_capacity(self) -> float:
        """The byte burst, so a caller can check a message could ever fit."""
        return self._bytes.capacity

    def offer(self, size_bytes: int) -> Decision:
        """Decide whether a message of `size_bytes` may be processed.

        The two budgets are checked before either is spent, so a message that
        fails on volume does not consume the message allowance it was about to
        need.
        """
        wait = max(self._messages.retry_after(), self._bytes.retry_after(size_bytes))
        if wait > 0:
            self._violations += 1
            return Decision(
                allowed=False,
                retry_after=wait,
                disconnect=self._violations >= self._max_violations,
            )

        self._messages.take()
        self._bytes.take(size_bytes)
        self._violations = 0
        return Decision(allowed=True)


__all__ = ["Decision", "RateLimiter", "TokenBucket"]
