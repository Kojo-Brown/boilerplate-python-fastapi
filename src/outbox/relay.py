"""The relay: committed rows in, subscribers out, one transaction at a time.

The relay is a background task, and the two things that matter most about it
are what it does when something goes wrong and what it does when asked to stop.

**A tick that fails must not end the loop.** An unhandled exception inside an
`asyncio.Task` does not crash the process — it is stored on the task and, with
nothing awaiting it, surfaces as a line in the log at garbage-collection time
if at all. A relay that died like that would leave the application publishing
happily into a table nobody drains, and the first symptom would be a report
that emails stopped days ago. So `run` catches everything a tick can raise,
backs off, and carries on; only cancellation gets through.

**Failures are per event, not per batch.** One event that cannot be delivered
reschedules itself and the rest of the batch continues. The alternative —
abandoning the transaction — would make one poison event block every event
behind it forever, which is how an outbox turns into an outage.

**Delivery is at-least-once, and the fan-out makes that sharper than usual.**
`raise_for_failures()` means *any* subscriber failing retries the whole event,
so the subscribers that already succeeded see it again. Nothing here can fix
that: this process cannot commit half a delivery. Subscribers must be
idempotent, and `event_id` is the key they deduplicate on.

**An event with no subscribers is delivered.** `PublishResult` for zero
matching subscriptions is a success, and the row is deleted. That is the bus's
own definition — an event with no observers is the pattern working — but it
makes the shutdown order load-bearing: stop the relay *before* clearing the
bus, or the last batch is "delivered" to nothing at all. `src/main.py` does
that, and says so.

**Backoff comes from the row, not from memory.** The wait after a failure is
computed from the row's `attempts`, so a redeploy in the middle of an incident
does not reset every failing event to its first retry.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Final

import structlog

from src.decorators.base import backoff_delay
from src.events.base import EventDispatchError
from src.outbox.base import BatchScope, EventDispatcher, OutboxBatch, PendingEvent
from src.outbox.codec import OutboxCodec, default_codec

logger = structlog.get_logger(__name__)

#: What `run` sleeps between ticks and what it passes to it. Injectable so the
#: relay's timing can be asserted without a test spending real seconds.
Sleeper = Callable[[float], Awaitable[None]]

#: Name given to the background task, so it is identifiable in
#: `asyncio.all_tasks()` output and in a debugger.
TASK_NAME: Final[str] = "outbox-relay"


def _describe(exc: BaseException) -> str:
    """One line naming what went wrong, for the row's `last_error`.

    The row is what a person reads when an event has been stuck for an hour,
    and `EventDispatchError` alone says only *how many* subscribers failed —
    the useful part, which subscriber and why, is on `.errors`. The log has the
    tracebacks; this has to survive without them, because whoever is looking at
    the row may be doing so long after the log line rolled off.
    """
    described = f"{type(exc).__name__}: {exc}"
    if isinstance(exc, EventDispatchError) and exc.errors:
        causes = "; ".join(f"{type(e).__name__}: {e}" for e in exc.errors)
        return f"{described} [{causes}]"
    return described


@dataclass(frozen=True, slots=True)
class RelayConfig:
    """How hard the relay works and how patient it is.

    `batch_size` and `dispatch_timeout` multiply: a full batch of events that
    each take the timeout is how long one transaction can be held open in the
    worst case, and that transaction is holding a pooled connection and a row
    lock the whole time. Raising the batch size to drain faster is therefore
    also raising the worst-case lock hold, which is the trade to make
    deliberately rather than by leaving the default in place under a load it
    was not chosen for.
    """

    batch_size: int = 100
    #: How long to wait after a tick that found nothing. It is the tail latency
    #: of every notification in the system, so it wants to be short — and it is
    #: also a query per interval per relay against a table that is usually
    #: empty, which is what stops it being zero.
    poll_interval: float = 1.0
    #: Ceiling on one event's dispatch, subscribers included. Without it a
    #: subscriber that never returns holds a transaction and a row lock for as
    #: long as the process lives.
    dispatch_timeout: float = 30.0
    retry_base_delay: float = 1.0
    retry_max_delay: float = 300.0
    jitter: bool = True

    def __post_init__(self) -> None:
        if self.batch_size < 1:
            raise ValueError("batch_size must be at least 1.")
        if self.poll_interval < 0:
            raise ValueError("poll_interval cannot be negative.")
        if self.dispatch_timeout <= 0:
            raise ValueError("dispatch_timeout must be positive.")
        if self.retry_base_delay <= 0:
            raise ValueError("retry_base_delay must be positive.")
        if self.retry_max_delay < self.retry_base_delay:
            raise ValueError("retry_max_delay cannot be below retry_base_delay.")


@dataclass(frozen=True, slots=True)
class DrainResult:
    """What one tick did. Returned so a test — or a caller draining the outbox
    synchronously, as `drain_once` allows — can assert on it instead of
    inspecting the table afterwards."""

    claimed: int
    delivered: int
    failed: int

    @property
    def empty(self) -> bool:
        return self.claimed == 0


class OutboxRelay:
    """Drains committed outbox rows into an event dispatcher."""

    def __init__(
        self,
        *,
        batches: BatchScope,
        dispatcher: EventDispatcher,
        codec: OutboxCodec | None = None,
        config: RelayConfig | None = None,
        sleep: Sleeper = asyncio.sleep,
        rng: random.Random | None = None,
    ) -> None:
        """
        Args:
            batches: Opens one claim transaction. See `src/outbox/store.py`.
            dispatcher: Where delivered events go — the process-wide
                `EventBus` in the application, a bus of its own in a test.
            codec: How rows become events. Defaults to the application
                catalogue.
            config: Sizes and timings.
            sleep: How to wait between ticks. Injectable so a test does not.
            rng: Source of backoff jitter. Injectable so a test can pin it.
        """
        self._batches = batches
        self._dispatcher = dispatcher
        self._codec = codec if codec is not None else default_codec()
        self._config = config if config is not None else RelayConfig()
        self._sleep = sleep
        self._rng = rng if rng is not None else random.Random()
        self._task: asyncio.Task[None] | None = None

    @property
    def config(self) -> RelayConfig:
        return self._config

    @property
    def running(self) -> bool:
        """Whether a background task is currently draining."""
        return self._task is not None and not self._task.done()

    async def drain_once(self) -> DrainResult:
        """Claim one batch, deliver what it holds, and commit the outcome.

        Public because it is the honest way to drain the outbox from a test or
        a one-shot script: everything `run` does beyond this is scheduling.
        """
        async with self._batches() as batch:
            entries = await batch.claim(limit=self._config.batch_size)
            if not entries:
                return DrainResult(claimed=0, delivered=0, failed=0)

            delivered = 0
            failed = 0
            for entry in entries:
                if await self._handle(batch, entry):
                    delivered += 1
                else:
                    failed += 1

        logger.info(
            "outbox.batch_drained",
            claimed=len(entries),
            delivered=delivered,
            failed=failed,
        )
        return DrainResult(claimed=len(entries), delivered=delivered, failed=failed)

    async def _handle(self, batch: OutboxBatch, entry: PendingEvent) -> bool:
        """Deliver one event and record the outcome. True if it was delivered.

        `Exception` and not `BaseException`: a cancelled relay must not mark
        the event it was carrying as failed — nothing about the event is wrong,
        the transaction is about to roll back, and the row is claimable again
        the moment it does.
        """
        try:
            await self._deliver(entry)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            described = _describe(exc)
            retry_in = backoff_delay(
                entry.attempts + 1,
                base_delay=self._config.retry_base_delay,
                max_delay=self._config.retry_max_delay,
                jitter=self._config.jitter,
                rng=self._rng,
            )
            logger.warning(
                "outbox.delivery_failed",
                outbox_id=str(entry.id),
                event_name=entry.event_name,
                event_id=entry.event_id,
                attempts=entry.attempts + 1,
                retry_in=round(retry_in, 3),
                error=described,
            )
            await batch.fail(entry, error=described, retry_in=retry_in)
            return False

        await batch.complete(entry)
        logger.debug(
            "outbox.delivered",
            outbox_id=str(entry.id),
            event_name=entry.event_name,
            event_id=entry.event_id,
        )
        return True

    async def _deliver(self, entry: PendingEvent) -> None:
        """Rebuild the event and publish it, or raise if anything went wrong.

        The timeout wraps the publish rather than each subscriber: the bus
        already offers a per-subscriber timeout, and this one is about the
        transaction being held open, which is a property of the whole dispatch.
        """
        event = self._codec.decode(
            entry.event_name,
            entry.payload,
            event_id=entry.event_id,
            occurred_at=entry.occurred_at,
        )
        result = await asyncio.wait_for(
            self._dispatcher.publish(event), self._config.dispatch_timeout
        )
        # The bus reports subscriber failures rather than raising them, so
        # without this a half-delivered event would have its row deleted.
        result.raise_for_failures()

    async def run(self) -> None:
        """Drain forever. Returns only when cancelled.

        A full batch is followed by another tick immediately — the outbox is
        evidently backed up, and sleeping would be pacing delivery at
        `batch_size` per `poll_interval`. Anything less than a full batch means
        the queue is drained for now.
        """
        logger.info(
            "outbox.relay_started",
            batch_size=self._config.batch_size,
            poll_interval=self._config.poll_interval,
        )
        try:
            await self._loop()
        except asyncio.CancelledError:
            # Around the whole loop rather than around the tick: a relay that
            # is keeping up spends nearly all of its time in `_sleep`, so a
            # handler that only covered `drain_once` would miss the shutdown it
            # was written for and log nothing on the ordinary path.
            logger.info("outbox.relay_stopped")
            raise

    async def _loop(self) -> None:
        """The loop itself, so `run` can own one handler for the whole of it."""
        consecutive_failures = 0
        while True:
            try:
                result = await self.drain_once()
            except Exception as exc:
                consecutive_failures += 1
                delay = backoff_delay(
                    consecutive_failures,
                    base_delay=self._config.retry_base_delay,
                    max_delay=self._config.retry_max_delay,
                    jitter=self._config.jitter,
                    rng=self._rng,
                )
                # A tick fails when the *database* is unreachable, not when an
                # event is bad — that is handled per event inside the batch —
                # so this backoff is about not hammering a server that is
                # already having a hard time.
                logger.exception(
                    "outbox.tick_failed",
                    consecutive_failures=consecutive_failures,
                    retry_in=round(delay, 3),
                    error=f"{type(exc).__name__}: {exc}",
                )
                await self._sleep(delay)
                continue

            consecutive_failures = 0
            if result.claimed >= self._config.batch_size:
                continue
            await self._sleep(self._config.poll_interval)

    def start(self) -> None:
        """Run the loop in a background task. Idempotent while it is running.

        Must be called from inside a running event loop — the FastAPI lifespan
        is one — because the task belongs to that loop.
        """
        if self.running:
            return
        self._task = asyncio.create_task(self.run(), name=TASK_NAME)

    async def stop(self) -> None:
        """Cancel the loop and wait for it to unwind. Idempotent.

        Waiting is the point rather than politeness: cancellation propagates
        into whatever the tick was doing, and the `BatchScope` rolls its
        transaction back on the way out. Returning before that has happened
        would let the process exit with a connection mid-transaction and a
        batch of rows still locked until the server noticed the socket had
        gone.
        """
        task = self._task
        if task is None:
            return
        self._task = None
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            # Ours, not the caller's: `stop()` asked for it. Re-raising here
            # would cancel whoever is shutting the application down.
            pass


__all__ = [
    "TASK_NAME",
    "DrainResult",
    "OutboxRelay",
    "RelayConfig",
    "Sleeper",
]
