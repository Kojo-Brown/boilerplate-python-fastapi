"""The consume loop: poll, handle in order, commit what succeeded.

Everything here is policy, and none of it imports `aiokafka`. The transport is
a `MessageSource`, so this loop is exercised against an in-process broker where
a rebalance, a handler failure and a commit are all deterministic, and the same
loop is then run against a real Kafka in CI.

## The failure rule, and why it is not the outbox's

`OutboxRelay` isolates failures per *event*: a bad row is rescheduled and the
rest of the batch is delivered. Transplanting that here would lose records.
Kafka stores one offset per partition — the position the group reads next — so
"skip record 4, keep record 5" is not expressible. Committing 6 after record 4
failed does not retry 4; it declares it done.

So the rule is per *partition*: a partition stops at its first failing record,
and the other partitions in the batch carry on. Concretely, for a batch holding
records 4, 5, 6 of one partition where 5 fails:

- record 4 is handled and `5` goes into the commit map,
- record 5 fails, so the partition stops there and is seeked back to 5,
- record 6 is not handled at all — it is behind 5 in a partition that is stuck,
- the commit still happens, so the work done on 4 is never repeated.

The seek is what makes the retry *soon*. The consumer's position has already
moved past record 5 in the client's own buffer, so without seeking, the next
poll returns record 6 onwards and record 5 comes back only after a restart or a
rebalance — hours later, in a batch whose other records are unrelated.

## Head-of-line blocking is the honest outcome, not an oversight

A partition that keeps failing stops, and its lag grows. That is what ordering
costs: releasing record 6 while 5 is unresolved means the topic is no longer
ordered, and ordering is the reason to have chosen a key. The escape is the
next item in this phase — a dead-letter queue, where a record that has failed
enough times is moved aside and the partition continues. Until then a poison
record retries at the capped backoff interval, visible in the log and in the
group's lag rather than silently dropped.

## At-least-once, and where the duplicates come from

The commit happens after the handlers, so every crash between the two replays
the batch. That is deliberate — the alternative replays *nothing* and loses it
— and it means handlers must be idempotent. The duplicates arrive from four
places, and it is worth knowing all four: a process killed mid-batch, a
rebalance that reassigns a partition whose commit had not landed, a commit that
failed, and a cancelled shutdown. The runner does not try to shrink that window
with a commit per record: it would pay a round trip per record and would not
close the window anyway, since the crash can land between the handler and the
commit however small the batch is.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Final

import structlog

from src.decorators.base import backoff_delay
from src.kafka.base import ConsumedMessage, ConsumerError, MessageSource, Partition
from src.structured import finalize

logger = structlog.get_logger(__name__)

#: What a runner does with one record. Returning is success; raising is not.
MessageHandler = Callable[[ConsumedMessage], Awaitable[None]]

#: `asyncio.sleep`, or a test double that records the delay without spending it.
Sleeper = Callable[[float], Awaitable[None]]

#: Name given to the background task, so a hung consumer is identifiable in
#: `asyncio.all_tasks()` and in a debugger.
TASK_NAME_PREFIX: Final[str] = "kafka-consumer"


@dataclass(frozen=True, slots=True)
class ConsumerConfig:
    """The delivery policy: how much, how long, how patiently.

    Separate from `ConsumerConnectionConfig`, which is the driver's half. These
    are application questions — how long may one record take, how hard is a
    failing partition retried — and they are the ones a test varies.
    """

    max_records: int = 100
    #: How long one poll waits for records before returning empty. This is the
    #: loop's idle cost: unlike `OutboxRelay`, which sleeps between queries,
    #: the wait happens inside the fetch, so an idle consumer makes no requests
    #: of its own and still notices a record within this long.
    poll_timeout: float = 1.0
    #: Ceiling on one handler. It has to stay well under the connection's
    #: `max_poll_interval_ms`: a batch that takes longer than that in total
    #: makes the broker consider this member gone, and its partitions are
    #: reassigned mid-batch — every record redelivered elsewhere while this
    #: process is still working on them.
    handler_timeout: float = 30.0
    retry_base_delay: float = 1.0
    retry_max_delay: float = 60.0
    jitter: bool = True
    #: How long shutdown waits for the source to leave its group. Bounded
    #: because a broker that has stopped answering must not hold a SIGTERM open
    #: until the supervisor escalates to SIGKILL, which would truncate every
    #: other shutdown step behind it.
    shutdown_timeout: float = 10.0

    def __post_init__(self) -> None:
        if self.max_records < 1:
            raise ValueError("max_records must be at least 1.")
        if self.poll_timeout <= 0:
            raise ValueError("poll_timeout must be positive.")
        if self.handler_timeout <= 0:
            raise ValueError("handler_timeout must be positive.")
        if self.retry_base_delay <= 0:
            raise ValueError("retry_base_delay must be positive.")
        if self.retry_max_delay < self.retry_base_delay:
            raise ValueError("retry_max_delay cannot be below retry_base_delay.")
        if self.shutdown_timeout <= 0:
            raise ValueError("shutdown_timeout must be positive.")


@dataclass(frozen=True, slots=True)
class ConsumeResult:
    """What one poll-handle-commit cycle did.

    Returned so a test, or a caller draining a topic synchronously as
    `consume_once` allows, can assert on the outcome instead of inferring it
    from the broker's state afterwards.
    """

    polled: int
    delivered: int
    failed: int
    #: How long the loop should wait before polling again, from the backoff of
    #: whichever partition has failed most. Zero when nothing failed — a
    #: healthy consumer never sleeps, because the poll already blocks.
    retry_delay: float = 0.0

    @property
    def empty(self) -> bool:
        return self.polled == 0


class ConsumerRunner:
    """Runs one `MessageSource` against one handler until cancelled."""

    def __init__(
        self,
        *,
        source: MessageSource,
        handler: MessageHandler,
        name: str = "default",
        config: ConsumerConfig | None = None,
        sleep: Sleeper = asyncio.sleep,
        rng: random.Random | None = None,
    ) -> None:
        """
        Args:
            source: The transport. Started by `run` and stopped on the way out,
                so a runner owns its membership of the group for exactly as
                long as it is running.
            handler: What one record means. Raising from it fails the record.
            name: Appears in the task name and every log line, so two runners
                in one process are distinguishable.
            config: Sizes and timings.
            sleep: How to wait after a failure. Injectable so a test does not.
            rng: Source of backoff jitter. Injectable so a test can pin it.
        """
        self._source = source
        self._handler = handler
        self._name = name
        self._config = config if config is not None else ConsumerConfig()
        self._sleep = sleep
        self._rng = rng if rng is not None else random.Random()
        self._task: asyncio.Task[None] | None = None
        #: Consecutive failures per partition, which is what the backoff is
        #: computed from. Per partition rather than per loop because one stuck
        #: partition must not pace the healthy ones, and it is reset by the
        #: first record that succeeds there.
        self._failures: dict[Partition, int] = {}

    @property
    def config(self) -> ConsumerConfig:
        return self._config

    @property
    def name(self) -> str:
        return self._name

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def failing_partitions(self) -> frozenset[Partition]:
        """Partitions currently stopped at a failing record."""
        return frozenset(self._failures)

    async def consume_once(self) -> ConsumeResult:
        """Poll once, handle what came back, commit what succeeded.

        Public because it is the honest way to drain a topic from a test or a
        one-shot script: everything `run` adds is scheduling and error
        recovery. Assumes the source is started — `run` is what starts it.
        """
        messages = await self._source.poll(
            max_records=self._config.max_records, timeout=self._config.poll_timeout
        )
        if not messages:
            return ConsumeResult(polled=0, delivered=0, failed=0)

        grouped: dict[Partition, list[ConsumedMessage]] = {}
        for message in messages:
            grouped.setdefault(message.partition, []).append(message)

        commits: dict[Partition, int] = {}
        delivered = 0
        failed = 0
        retry_delay = 0.0

        for partition, records in grouped.items():
            for record in records:
                if await self._handle(record):
                    delivered += 1
                    commits[partition] = record.next_offset
                    self._failures.pop(partition, None)
                    continue

                failed += 1
                retry_delay = max(retry_delay, self._register_failure(record))
                self._seek_back(record)
                # The rest of this partition is behind a record that has not
                # been dealt with. Handling it would be handling records out of
                # order and would leave nowhere to commit.
                break

        await self._commit(commits)
        logger.info(
            "kafka.batch_consumed",
            consumer=self._name,
            polled=len(messages),
            delivered=delivered,
            failed=failed,
            partitions=len(grouped),
        )
        return ConsumeResult(
            polled=len(messages),
            delivered=delivered,
            failed=failed,
            retry_delay=retry_delay,
        )

    async def _handle(self, message: ConsumedMessage) -> bool:
        """Run the handler under its timeout. True if the record is done.

        `Exception` and not `BaseException`: a cancelled runner must not record
        the message it was carrying as failed. Nothing about the record is
        wrong, nothing will be committed, and it is redelivered on the next
        start — which is the same outcome as any other interrupted batch.
        """
        try:
            await asyncio.wait_for(
                self._handler(message), timeout=self._config.handler_timeout
            )
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            logger.warning(
                "kafka.handler_timeout",
                consumer=self._name,
                partition=str(message.partition),
                offset=message.offset,
                timeout=self._config.handler_timeout,
            )
            return False
        except Exception as exc:
            logger.warning(
                "kafka.handler_failed",
                consumer=self._name,
                partition=str(message.partition),
                offset=message.offset,
                key=message.key,
                error=f"{type(exc).__name__}: {exc}",
            )
            return False
        return True

    def _register_failure(self, message: ConsumedMessage) -> float:
        """Count the failure and say how long to wait before trying again."""
        attempts = self._failures.get(message.partition, 0) + 1
        self._failures[message.partition] = attempts
        delay = backoff_delay(
            attempts,
            base_delay=self._config.retry_base_delay,
            max_delay=self._config.retry_max_delay,
            jitter=self._config.jitter,
            rng=self._rng,
        )
        logger.warning(
            "kafka.partition_stalled",
            consumer=self._name,
            partition=str(message.partition),
            offset=message.offset,
            attempts=attempts,
            retry_in=round(delay, 3),
        )
        return delay

    def _seek_back(self, message: ConsumedMessage) -> None:
        """Ask for the failed record again on the next poll.

        A failure here is logged and swallowed on purpose: `seek` fails when
        the partition is no longer assigned, which means a rebalance took it
        while the batch was in flight. Its new owner reads from the last
        committed offset, which is at or before this record, so the record is
        not lost — and raising would turn somebody else's successful takeover
        into this runner's error.
        """
        try:
            self._source.seek(message.partition, message.offset)
        except ConsumerError as exc:
            logger.warning(
                "kafka.seek_failed",
                consumer=self._name,
                partition=str(message.partition),
                offset=message.offset,
                error=f"{type(exc).__name__}: {exc}",
            )

    async def _commit(self, offsets: dict[Partition, int]) -> None:
        """Store what was earned, and treat a failure as redelivery.

        Not fatal, and not retried. A commit fails when this member has been
        removed from the group, and the records it covers already belong to
        whoever took the partitions over. Retrying would either fail again or,
        worse, land after the new owner has committed its own progress.
        """
        if not offsets:
            return
        try:
            await self._source.commit(offsets)
        except ConsumerError as exc:
            logger.warning(
                "kafka.commit_failed",
                consumer=self._name,
                offsets={str(p): o for p, o in sorted(offsets.items())},
                error=f"{type(exc).__name__}: {exc}",
                detail="The batch will be redelivered; handlers must be idempotent.",
            )

    async def run(self) -> None:
        """Start the source, consume until cancelled, then leave the group.

        The source is stopped through `finalize` rather than a bare `await` in
        the `finally`, because that await is the one guaranteed to run on an
        already-cancelled task: shutdown cancels this task, and a plain await
        would be cut at its first suspension. Leaving the group is worth
        insisting on — a member that disappears without saying so is only
        noticed when its session times out, and until then its partitions have
        no owner and their lag grows.
        """
        logger.info(
            "kafka.consumer_run_started",
            consumer=self._name,
            max_records=self._config.max_records,
            poll_timeout=self._config.poll_timeout,
        )
        try:
            await self._loop()
        except asyncio.CancelledError:
            logger.info("kafka.consumer_run_stopped", consumer=self._name)
            raise
        finally:
            await finalize(
                self._source.stop,
                name=f"{TASK_NAME_PREFIX}-{self._name}-stop",
                timeout=self._config.shutdown_timeout,
            )

    async def _loop(self) -> None:
        """The loop itself, so `run` owns one handler for the whole of it."""
        consecutive_failures = 0
        while True:
            try:
                # Idempotent, and inside the loop rather than before it: a
                # broker that is unreachable at start-up then becomes a retry
                # with backoff, where a start outside the loop would raise into
                # a task nobody awaits and leave a consumer that never consumes
                # and never says so.
                await self._source.start()
                result = await self.consume_once()
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
                    "kafka.poll_failed",
                    consumer=self._name,
                    consecutive_failures=consecutive_failures,
                    retry_in=round(delay, 3),
                    error=f"{type(exc).__name__}: {exc}",
                )
                await self._sleep(delay)
                continue

            consecutive_failures = 0
            if result.retry_delay > 0:
                await self._sleep(result.retry_delay)

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
        leaves the consumer group, and returning before it has happened lets
        the process exit with a membership the broker will keep believing in
        until the session times out.
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


__all__ = [
    "TASK_NAME_PREFIX",
    "ConsumeResult",
    "ConsumerConfig",
    "ConsumerRunner",
    "MessageHandler",
    "Sleeper",
]
