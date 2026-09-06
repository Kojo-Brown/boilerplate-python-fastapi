"""The consume loop: claim what is stalled, read what is new, acknowledge one at a time.

Nothing here imports `redis`. The transport is a `StreamConsumerGroup`, so this
loop runs against the in-process model where a message's idle time is a number
a test sets, and the same loop then runs against a real server in CI.

## Claim first, read second

Every cycle looks for stalled messages *before* asking for new ones, and the
order is the whole design rather than a preference. A stalled message is one a
consumer took and never acknowledged — its process was killed, its handler hung
until the deployment replaced it, its network went away. Nothing in Redis will
redeliver it: it sits in the group's pending entries list, owned by a name that
may no longer be running, and the only thing that moves it is another consumer
claiming it.

If new messages came first, a busy stream would starve the stalled ones
forever: `read` returns a full batch every time, the claim scan never gets a
turn, and the messages lost to yesterday's crash are still pending next week —
with the group's lag at zero and every dashboard green, because a pending entry
is not lag. Claiming first bounds a message's recovery time at
`min_idle` plus one cycle, however busy the stream is.

The cost is bounded in the other direction by `claim_batch`: a group with ten
thousand stalled messages does not spend an entire cycle on them and stop
consuming what is arriving.

## `min_idle` is a bet about your own handlers

Claiming a message that is *not* actually stalled — one whose owner is alive
and still working on it — gets it handled twice, concurrently. `min_idle` is
what makes that unlikely, and the number to set it from is not the average
handler duration but the worst: comfortably above `handler_timeout`, so a
handler that is merely slow is never mistaken for a consumer that is gone.
Below that, this package's own timeout becomes a duplicate-delivery generator.

Redis makes the race narrow but not impossible: `claim` re-checks the idle time
server-side, so two consumers scanning at the same moment cannot both take a
message. What it cannot know is whether the original owner is still working.
Handlers must be idempotent — the same requirement `src/kafka` has, arrived at
from the other direction.

## Failure isolation is per message, and there is no ladder

A handler that raises leaves its message unacknowledged and everything else
continues. There is no head-of-line blocking to trade against, so there is no
retry-tier ladder here as there is in `src/dlq`: a Redis Streams message
retries by being reclaimed after `min_idle`, in place, without moving between
streams. What that leaves unsolved is the poison message, which is what
`max_deliveries` is for — the PEL counts deliveries, so a message that has been
handed out more times than the cap is moved to the dead-letter stream and
acknowledged, in that order. Publish first, acknowledge second: the opposite
order loses the message if the publish fails.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Final

import structlog

from src.decorators.base import backoff_delay
from src.redis_streams.base import (
    PendingEntry,
    StreamConsumerGroup,
    StreamEntryId,
    StreamError,
    StreamMessage,
)
from src.structured import finalize

logger = structlog.get_logger(__name__)

#: What a runner does with one message. Returning is success — the message is
#: acknowledged; raising is not — it stays pending and is reclaimed later.
StreamHandler = Callable[[StreamMessage], Awaitable[None]]

#: `asyncio.sleep`, or a test double that records the delay without spending it.
Sleeper = Callable[[float], Awaitable[None]]

TASK_NAME_PREFIX: Final[str] = "redis-stream-consumer"

#: Fields added to a dead-lettered copy. Prefixed so they cannot collide with
#: a producer's own field names, and readable as text so that whoever opens
#: the dead-letter stream during an incident can see what happened without a
#: decoder.
DEAD_LETTER_PREFIX: Final[str] = "x-dead-"


@dataclass(frozen=True, slots=True)
class StreamConsumerConfig:
    """The delivery policy: how much, how stale, how many times.

    Every one of these is an application question rather than a connection
    setting, which is why they are separate from what `RedisStreamGroup` is
    built with — and why a test varies them freely.
    """

    #: Messages per cycle, claims included. The claim scan takes from this
    #: budget before the read does, so a cycle never handles more than this
    #: many messages however much is waiting.
    batch_size: int = 50
    #: How long a read waits for a message before coming back empty. This is
    #: the loop's idle cost: the wait happens inside the read, so an idle
    #: consumer makes one blocking call rather than polling. It also bounds
    #: how long a claim scan is delayed by an empty stream, and how long
    #: shutdown waits for the current cycle to end.
    block_timeout: float = 2.0
    #: How long a pending message must have been untouched before another
    #: consumer may take it. Set it well above `handler_timeout`; see the
    #: module docstring.
    min_idle: float = 60.0
    #: Ceiling on the claim scan per cycle, so a large backlog of stalled
    #: messages cannot stop new ones being read.
    claim_batch: int = 20
    #: Deliveries a message may have before it is dead-lettered instead of
    #: handled. The first read is delivery 1, so this is a count of attempts,
    #: not of retries.
    max_deliveries: int = 5
    #: Ceiling on one handler. Below `min_idle` by a wide margin, or a handler
    #: that is merely slow gets its message claimed out from under it.
    handler_timeout: float = 30.0
    retry_base_delay: float = 1.0
    retry_max_delay: float = 60.0
    jitter: bool = True
    #: How long shutdown waits for the group to be left cleanly. Bounded so a
    #: server that has stopped answering cannot hold a SIGTERM open until the
    #: supervisor escalates to SIGKILL.
    shutdown_timeout: float = 10.0
    #: Appended to the stream name for dead letters. An interface rather than
    #: a preference: whoever drains the dead-letter stream derives its name
    #: the same way, and changing this orphans whatever is already in the old
    #: one.
    dead_letter_suffix: str = ".dead"

    def __post_init__(self) -> None:
        if self.batch_size < 1:
            raise ValueError("batch_size must be at least 1.")
        if self.block_timeout <= 0:
            raise ValueError("block_timeout must be positive.")
        if self.min_idle <= 0:
            raise ValueError("min_idle must be positive.")
        if self.claim_batch < 1:
            raise ValueError("claim_batch must be at least 1.")
        if self.max_deliveries < 1:
            raise ValueError("max_deliveries must be at least 1.")
        if self.handler_timeout <= 0:
            raise ValueError("handler_timeout must be positive.")
        if self.retry_base_delay <= 0:
            raise ValueError("retry_base_delay must be positive.")
        if self.retry_max_delay < self.retry_base_delay:
            raise ValueError("retry_max_delay cannot be below retry_base_delay.")
        if self.shutdown_timeout <= 0:
            raise ValueError("shutdown_timeout must be positive.")
        if not self.dead_letter_suffix:
            raise ValueError("dead_letter_suffix must not be empty.")


@dataclass(frozen=True, slots=True)
class ConsumeResult:
    """What one cycle did. Returned so a caller can assert on it.

    `claimed` and `read` are separate numbers because they answer different
    questions during an incident: a rising `claimed` means consumers are dying
    or hanging, where a rising `read` is just traffic.
    """

    read: int = 0
    claimed: int = 0
    delivered: int = 0
    failed: int = 0
    dead_lettered: int = 0
    #: Pending entries that were selected for claiming and could not be taken:
    #: acknowledged in the meantime, re-read by their owner, or gone from the
    #: stream entirely because it was trimmed or the entry deleted. Counted
    #: rather than raised — none of them is this consumer's problem, and a
    #: number that is normally zero is worth watching.
    vanished: int = 0

    @property
    def handled(self) -> int:
        """Messages this cycle took off the stream, however they ended."""
        return self.delivered + self.failed + self.dead_lettered

    @property
    def empty(self) -> bool:
        return self.read == 0 and self.claimed == 0


class StreamConsumerRunner:
    """Runs one consumer group against one handler until cancelled."""

    def __init__(
        self,
        *,
        group: StreamConsumerGroup,
        handler: StreamHandler,
        name: str = "default",
        config: StreamConsumerConfig | None = None,
        sleep: Sleeper = asyncio.sleep,
        rng: random.Random | None = None,
    ) -> None:
        """
        Args:
            group: The transport. Started by `run` and stopped on the way out,
                so a runner owns its membership for exactly as long as it runs.
            handler: What one message means. Raising from it leaves the message
                pending, to be reclaimed once it is idle.
            name: Appears in the task name and every log line, so two runners
                in one process are distinguishable.
            config: Sizes, timings and the delivery cap.
            sleep: How to wait after a failed cycle. Injectable so a test does
                not have to spend the time.
            rng: Source of backoff jitter. Injectable so a test can pin it.
        """
        self._group = group
        self._handler = handler
        self._name = name
        self._config = config if config is not None else StreamConsumerConfig()
        self._sleep = sleep
        self._rng = rng if rng is not None else random.Random()
        self._task: asyncio.Task[None] | None = None

    @property
    def config(self) -> StreamConsumerConfig:
        return self._config

    @property
    def name(self) -> str:
        return self._name

    @property
    def group(self) -> StreamConsumerGroup:
        """The transport this runner owns.

        Exposed for the two callers that need it: a factory's caller checking
        which stream and group a runner was assembled with, and a test driving
        `consume_once` directly, which needs the group started because `run` is
        what would otherwise have started it.
        """
        return self._group

    @property
    def dead_letter_stream(self) -> str:
        return f"{self._group.stream}{self._config.dead_letter_suffix}"

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def consume_once(self) -> ConsumeResult:
        """One cycle: claim the stalled, read the new, handle, acknowledge.

        Public because it is the honest way to drain a stream from a test or a
        one-shot script — everything `run` adds is scheduling and error
        recovery. Assumes the group is started; `run` is what starts it.
        """
        claimed, vanished = await self._claim_stalled()
        budget = self._config.batch_size - len(claimed)
        fresh: tuple[StreamMessage, ...] = ()
        if budget > 0:
            # No blocking wait when there was stalled work: the claim scan
            # already spent time, more of it is probably waiting, and parking
            # for `block_timeout` on an empty stream would delay the next scan
            # for no gain. An idle consumer, having claimed nothing, blocks as
            # usual.
            block = 0.0 if claimed else self._config.block_timeout
            fresh = tuple(await self._group.read(count=budget, block=block))

        delivered = 0
        failed = 0
        dead_lettered = 0
        acked: list[StreamEntryId] = []

        for message in (*claimed, *fresh):
            if message.delivery_count > self._config.max_deliveries:
                if await self._dead_letter(message):
                    dead_lettered += 1
                    acked.append(message.id)
                else:
                    failed += 1
                continue
            if await self._handle(message):
                delivered += 1
                acked.append(message.id)
            else:
                failed += 1

        # One `XACK` for the cycle rather than one per message: the ids are
        # independent, so batching them changes nothing about which messages
        # are acknowledged, and a round trip per message is the cost this
        # package's whole batch shape exists to avoid. A crash before it
        # redelivers the batch, which is the at-least-once guarantee either
        # way.
        await self._ack(acked)

        result = ConsumeResult(
            read=len(fresh),
            claimed=len(claimed),
            delivered=delivered,
            failed=failed,
            dead_lettered=dead_lettered,
            vanished=vanished,
        )
        if not result.empty:
            logger.info(
                "stream.batch_consumed",
                consumer=self._name,
                stream=self._group.stream,
                group=self._group.group,
                read=result.read,
                claimed=result.claimed,
                delivered=result.delivered,
                failed=result.failed,
                dead_lettered=result.dead_lettered,
                vanished=result.vanished,
            )
        return result

    async def _claim_stalled(self) -> tuple[tuple[StreamMessage, ...], int]:
        """Take up to `claim_batch` messages nobody has acknowledged.

        The scan and the claim are two commands, and the gap between them is
        deliberate rather than an inefficiency: the scan reads the group's
        bookkeeping without payloads, so a group with a large backlog costs one
        cheap command per cycle, and the claim re-checks the idle time so
        nothing that stopped being stalled in the gap is taken.
        """
        entries = await self._group.stalled(
            min_idle=self._config.min_idle, count=self._config.claim_batch
        )
        if not entries:
            return (), 0
        claimed = await self._group.claim(entries, min_idle=self._config.min_idle)
        if claimed.missing:
            logger.info(
                "stream.claim_incomplete",
                consumer=self._name,
                stream=self._group.stream,
                group=self._group.group,
                missing=len(claimed.missing),
                detail=(
                    "Pending entries that could not be claimed: acknowledged "
                    "or re-read since the scan, or no longer in the stream."
                ),
            )
        if claimed.messages:
            logger.warning(
                "stream.messages_claimed",
                consumer=self._name,
                stream=self._group.stream,
                group=self._group.group,
                claimed=len(claimed.messages),
                oldest_idle=round(max(entry.idle for entry in entries), 3),
                detail=(
                    "Messages another consumer took and never acknowledged. "
                    "A steady rate here means consumers are dying or hanging."
                ),
            )
        return claimed.messages, len(claimed.missing)

    async def _handle(self, message: StreamMessage) -> bool:
        """Run the handler under its timeout. True if the message is done.

        `Exception` and not `BaseException`: a cancelled runner must not treat
        the message it was carrying as failed. Nothing about it is wrong, it is
        not acknowledged, and it is reclaimed once it has been idle long
        enough — the same outcome as any other interrupted cycle.
        """
        try:
            await asyncio.wait_for(
                self._handler(message), timeout=self._config.handler_timeout
            )
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            logger.warning(
                "stream.handler_timeout",
                consumer=self._name,
                stream=message.stream,
                message_id=str(message.id),
                delivery_count=message.delivery_count,
                timeout=self._config.handler_timeout,
            )
            return False
        except Exception as exc:
            logger.warning(
                "stream.handler_failed",
                consumer=self._name,
                stream=message.stream,
                message_id=str(message.id),
                delivery_count=message.delivery_count,
                error=f"{type(exc).__name__}: {exc}",
            )
            return False
        return True

    async def _dead_letter(self, message: StreamMessage) -> bool:
        """Copy a message past its delivery cap aside. True if it was copied.

        Returning False rather than raising, and the caller then does *not*
        acknowledge: a dead-letter stream that cannot be written to must leave
        the message pending, because the alternative during a Redis incident is
        a stream quietly emptying itself into nowhere. The message stays where
        it is and is tried again — it is already over its cap, so the next
        cycle brings it straight back here.
        """
        fields = dict(message.fields)
        fields[f"{DEAD_LETTER_PREFIX}stream"] = message.stream.encode()
        fields[f"{DEAD_LETTER_PREFIX}id"] = str(message.id).encode()
        fields[f"{DEAD_LETTER_PREFIX}group"] = self._group.group.encode()
        fields[f"{DEAD_LETTER_PREFIX}consumer"] = self._group.consumer.encode()
        fields[f"{DEAD_LETTER_PREFIX}deliveries"] = str(message.delivery_count).encode()
        try:
            dead_id = await self._group.publish(fields, stream=self.dead_letter_stream)
        except StreamError as exc:
            logger.error(
                "stream.dead_letter_failed",
                consumer=self._name,
                stream=message.stream,
                message_id=str(message.id),
                error=f"{type(exc).__name__}: {exc}",
                detail="Left pending rather than dropped; it will be claimed again.",
            )
            return False
        logger.error(
            "stream.message_dead_lettered",
            consumer=self._name,
            stream=message.stream,
            message_id=str(message.id),
            dead_letter_stream=self.dead_letter_stream,
            dead_letter_id=str(dead_id),
            deliveries=message.delivery_count,
            max_deliveries=self._config.max_deliveries,
        )
        return True

    async def _ack(self, ids: list[StreamEntryId]) -> None:
        """Acknowledge what succeeded, and treat a failure as redelivery.

        Not fatal and not retried. An acknowledgement that does not land leaves
        the messages pending, and pending messages are claimed back — by this
        consumer on a later cycle, or by another one. That is the at-least-once
        guarantee doing its job rather than a case to handle specially, and the
        handlers are already required to be idempotent for it.
        """
        if not ids:
            return
        try:
            await self._group.ack(ids)
        except StreamError as exc:
            logger.warning(
                "stream.ack_failed",
                consumer=self._name,
                stream=self._group.stream,
                count=len(ids),
                error=f"{type(exc).__name__}: {exc}",
                detail="Those messages stay pending and will be redelivered.",
            )

    async def run(self) -> None:
        """Start the group, consume until cancelled, then leave it cleanly.

        The group is stopped through `finalize` rather than a bare `await` in
        the `finally`, because that await is the one guaranteed to run on an
        already-cancelled task: shutdown cancels this task, and a plain await
        would be cut at its first suspension.
        """
        logger.info(
            "stream.consumer_run_started",
            consumer=self._name,
            stream=self._group.stream,
            group=self._group.group,
            member=self._group.consumer,
            batch_size=self._config.batch_size,
            min_idle=self._config.min_idle,
        )
        try:
            await self._loop()
        except asyncio.CancelledError:
            logger.info("stream.consumer_run_stopped", consumer=self._name)
            raise
        finally:
            await finalize(
                self._group.stop,
                name=f"{TASK_NAME_PREFIX}-{self._name}-stop",
                timeout=self._config.shutdown_timeout,
            )

    async def _loop(self) -> None:
        """The loop itself, so `run` owns one handler for the whole of it."""
        consecutive_failures = 0
        while True:
            try:
                # Idempotent, and inside the loop rather than before it: a
                # server that is unreachable at start-up becomes a retry with
                # backoff, and a `NOGROUP` — the stream deleted underneath us —
                # is recovered from here rather than retried forever against a
                # group that no longer exists.
                await self._group.start()
                await self.consume_once()
            except Exception as exc:
                consecutive_failures += 1
                delay = backoff_delay(
                    consecutive_failures,
                    base_delay=self._config.retry_base_delay,
                    max_delay=self._config.retry_max_delay,
                    jitter=self._config.jitter,
                    rng=self._rng,
                )
                logger.exception(
                    "stream.cycle_failed",
                    consumer=self._name,
                    stream=self._group.stream,
                    consecutive_failures=consecutive_failures,
                    retry_in=round(delay, 3),
                    error=f"{type(exc).__name__}: {exc}",
                )
                await self._sleep(delay)
                continue
            consecutive_failures = 0

    def start(self) -> None:
        """Run the loop in a background task. Idempotent while it is running.

        Must be called from inside a running event loop, because the task
        belongs to that loop.
        """
        if self.running:
            return
        self._task = asyncio.create_task(
            self.run(), name=f"{TASK_NAME_PREFIX}-{self._name}"
        )

    async def stop(self) -> None:
        """Cancel the loop and wait for it to unwind. Idempotent.

        Waiting is the point rather than politeness: the unwinding is what
        removes this consumer from the group when it owes nothing, and
        returning early would leave a consumer record behind on every restart.
        """
        task = self._task
        if task is None:
            return
        self._task = None
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            # Ours, not the caller's: `stop()` asked for it. Re-raising would
            # cancel whoever is shutting the application down.
            pass


def pending_summary(entries: Sequence[PendingEntry]) -> dict[str, object]:
    """A log-shaped summary of a pending scan, for an operator's benefit.

    Separate from the runner because the interesting time to ask this is from
    outside one — a health check, an admin endpoint, a script run during an
    incident — and the answer is the same either way.
    """
    if not entries:
        return {"pending": 0}
    return {
        "pending": len(entries),
        "oldest_idle": round(max(entry.idle for entry in entries), 3),
        "max_deliveries": max(entry.delivery_count for entry in entries),
        "consumers": sorted({entry.consumer for entry in entries}),
    }


__all__ = [
    "DEAD_LETTER_PREFIX",
    "TASK_NAME_PREFIX",
    "ConsumeResult",
    "Sleeper",
    "StreamConsumerConfig",
    "StreamConsumerRunner",
    "StreamHandler",
    "pending_summary",
]
