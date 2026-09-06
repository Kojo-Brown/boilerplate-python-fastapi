"""The real thing: one consumer group over `redis.asyncio`.

Every method here is one Redis command plus the translation between what the
server speaks and what `base.py` declares. The policy — when to claim, what a
delivery count means, what happens to a message a handler raised on — is in
`consumer.py` and never sees this class.

Four things about these commands are worth knowing before changing anything
here, because each of them fails silently rather than loudly. All four were
measured against Redis 7 rather than taken from the manual; the transcripts are
in `docs/redis-streams.md`.

**`XGROUP DELCONSUMER` discards the consumer's pending entries.** It returns
the number it owed, which reads like a receipt and is a body count: those
entries leave the PEL, no other consumer can claim them, and `XREADGROUP >`
will not return them either because the group's last-delivered-id is already
past them. A tidy-up on shutdown is therefore how you lose exactly the messages
that were still in flight. `stop()` removes this consumer only when it owes
nothing.

**`XCLAIM` and `XAUTOCLAIM` increment the delivery counter; `JUSTID` does
not.** That is the documented behaviour and it is the right way round, but it
makes `JUSTID` a trap for exactly this use: claiming ids without their payloads
to "check on them first" freezes the counter, and a poison message whose
counter never rises never reaches its cap. Nothing in this module uses
`JUSTID`.

**An entry can be in the PEL and gone from the stream.** `XDEL` and `MAXLEN`
trimming delete entries without consulting any group, and the pending entry
survives them. `XCLAIM` drops such an entry from the PEL as it goes and simply
omits it from the reply, which is why `claim` returns what was missing rather
than assuming the reply lines up with the request.

**`BLOCK 0` blocks forever.** Zero is not "do not block" — it is "no timeout",
and a shutdown then waits on a read that can only be interrupted by a message
arriving. `read` refuses to pass it.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Final

import structlog
from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError

from src.immutable import FrozenDict
from src.redis_streams.base import (
    AUTO_ID,
    NEW_MESSAGES,
    ClaimedMessage,
    Fields,
    PendingEntry,
    StreamDecodeError,
    StreamEntryId,
    StreamError,
    StreamGroupError,
    StreamLifecycleError,
    StreamMessage,
    StreamPublishError,
    StreamUnavailableError,
    default_consumer_name,
    validate_fields,
)

logger = structlog.get_logger(__name__)

#: The server's answer to creating a group that is already there. Expected on
#: every start but the first, and the only `ResponseError` this module treats
#: as success.
_BUSYGROUP: Final[str] = "BUSYGROUP"

#: The server's answer when the stream or the group is gone — a `DEL` of the
#: key, or an operator's `XGROUP DESTROY`. Recoverable, and handled by putting
#: this group back into its unstarted state so the next `start()` recreates it.
_NOGROUP: Final[str] = "NOGROUP"

#: Where a group created on an existing stream starts reading. `0` would
#: replay the entire history on first deploy; `$` reads only what arrives after
#: the group exists, which is what "a new consumer group" almost always means
#: and is the opposite default to Kafka's `earliest`. Set deliberately rather
#: than inherited: the two systems disagree here, and a reader of this file
#: should not have to guess which convention applied.
_GROUP_START_ID: Final[str] = "$"


def to_ms(seconds: float) -> int:
    """Seconds to whole milliseconds, never negative.

    Truncating rather than rounding, so a `min_idle` of 0.0009 becomes 0 and
    claims immediately instead of never: every use of this is a *threshold*
    below which nothing is claimed, and erring low costs one wasted claim
    attempt where erring high costs a message that is never reclaimed.
    """
    return max(0, int(seconds * 1000))


def decode_fields(raw: object) -> dict[str, bytes]:
    """Turn the server's `{bytes: bytes}` hash into `{str: bytes}`.

    Names are decoded and values are not, which is the asymmetry in `Fields`:
    a field name is this codebase's own and is text, a value is whatever a
    producer encoded and is nobody's business here.
    """
    if not isinstance(raw, dict):  # pragma: no cover - defensive
        raise StreamDecodeError("A stream entry did not come back as a hash.")
    fields: dict[str, bytes] = {}
    for name, value in raw.items():
        key = name if isinstance(name, bytes) else str(name).encode()
        try:
            fields[key.decode()] = value if isinstance(value, bytes) else bytes(value)
        except UnicodeDecodeError as exc:
            raise StreamDecodeError(
                "A stream entry has a field name that is not UTF-8.",
                details={"field": repr(key)},
            ) from exc
    return fields


class RedisStreamGroup:
    """`StreamConsumerGroup` over a real Redis server.

    The client is injected rather than built here so a caller can hand in a
    pool it already owns; `from_url` is the ordinary case and owns what it
    builds. Only an owned client is closed by `stop()` — closing somebody
    else's pool during this object's shutdown would take out whatever else is
    using it.
    """

    def __init__(
        self,
        client: Redis,
        *,
        stream: str,
        group: str,
        consumer: str | None = None,
        maxlen: int = 0,
        owns_client: bool = False,
    ) -> None:
        """
        Args:
            client: An async Redis client. `decode_responses` must be off —
                message values are bytes, and asking redis-py to decode them
                would corrupt anything that is not text.
            stream: The stream key this group reads.
            group: The group name. It is the identity the PEL and the
                last-delivered-id belong to, so it outlives every process that
                uses it, and renaming it starts again from `$`.
            consumer: This process's name within the group. Defaults to
                `default_consumer_name()`.
            maxlen: Approximate cap on the stream, applied by `publish`. Zero
                leaves the stream unbounded — see the warning in
                `docs/redis-streams.md` about trimming past unacknowledged
                messages.
            owns_client: Whether `stop()` should close the client.
        """
        if not stream:
            raise ValueError("stream must not be empty.")
        if not group:
            raise ValueError("group must not be empty.")
        if maxlen < 0:
            raise ValueError("maxlen cannot be negative.")
        self._client = client
        self._stream = stream
        self._group = group
        self._consumer = consumer if consumer else default_consumer_name()
        self._maxlen = maxlen
        self._owns_client = owns_client
        self._started = False

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        stream: str,
        group: str,
        consumer: str | None = None,
        maxlen: int = 0,
    ) -> RedisStreamGroup:
        """Build a group owning its own connection pool."""
        return cls(
            Redis.from_url(url, decode_responses=False),
            stream=stream,
            group=group,
            consumer=consumer,
            maxlen=maxlen,
            owns_client=True,
        )

    @property
    def stream(self) -> str:
        return self._stream

    @property
    def group(self) -> str:
        return self._group

    @property
    def consumer(self) -> str:
        return self._consumer

    @property
    def started(self) -> bool:
        return self._started

    async def start(self) -> None:
        """Create the stream and the group. Idempotent, and cheap when it is.

        `MKSTREAM` is what makes a consumer that starts before its producer
        legal: without it, a group on a stream that does not exist yet is an
        error, and the deployment order of two services becomes a correctness
        question.
        """
        if self._started:
            return
        try:
            await self._client.xgroup_create(
                self._stream, self._group, id=_GROUP_START_ID, mkstream=True
            )
            logger.info(
                "stream.group_created",
                stream=self._stream,
                group=self._group,
                start_id=_GROUP_START_ID,
            )
        except ResponseError as exc:
            if _BUSYGROUP not in str(exc):
                raise StreamGroupError(
                    "Could not create the consumer group.",
                    details={"stream": self._stream, "group": self._group},
                ) from exc
        except RedisError as exc:
            raise StreamUnavailableError(
                "Could not reach Redis to create the consumer group.",
                details={"stream": self._stream, "group": self._group},
            ) from exc
        self._started = True

    async def stop(self) -> None:
        """Leave the group cleanly, and only if that loses nothing.

        Removing this consumer keeps `XINFO CONSUMERS` from growing by one
        record per process for the lifetime of the group, which on a rolling
        deployment is otherwise permanent. It is skipped — loudly — when this
        consumer still owes messages, because `XGROUP DELCONSUMER` deletes them
        from the PEL along with the consumer and nothing can claim them
        afterwards. Leaving the record behind costs a few dozen bytes; removing
        it there costs the messages.
        """
        if self._started:
            await self._retire_consumer()
        self._started = False
        if self._owns_client:
            try:
                await self._client.aclose()
            except RedisError:  # pragma: no cover - shutdown against a dead server
                logger.warning("stream.close_failed", stream=self._stream)

    async def _retire_consumer(self) -> None:
        """`XGROUP DELCONSUMER`, but only against an empty PEL.

        Failures here are logged and swallowed: this runs during shutdown, and
        a consumer record left in a group is untidy rather than wrong.
        """
        try:
            owed = await self.pending_count(consumer=self._consumer)
            if owed:
                logger.warning(
                    "stream.consumer_retained",
                    stream=self._stream,
                    group=self._group,
                    consumer=self._consumer,
                    pending=owed,
                    detail=(
                        "Not removing a consumer that still owes messages: "
                        "XGROUP DELCONSUMER would delete them from the PEL. "
                        "Another consumer will claim them once they are idle."
                    ),
                )
                return
            await self._client.xgroup_delconsumer(
                self._stream, self._group, self._consumer
            )
            logger.info(
                "stream.consumer_removed",
                stream=self._stream,
                group=self._group,
                consumer=self._consumer,
            )
        except (RedisError, StreamError) as exc:
            logger.warning(
                "stream.consumer_retire_failed",
                stream=self._stream,
                group=self._group,
                consumer=self._consumer,
                error=f"{type(exc).__name__}: {exc}",
            )

    def _require_started(self) -> None:
        if not self._started:
            raise StreamLifecycleError(
                "This consumer group has not been started.",
                details={"stream": self._stream, "group": self._group},
            )

    def _handle_response_error(self, exc: ResponseError, what: str) -> StreamGroupError:
        """Translate a command error, and notice the recoverable one.

        `NOGROUP` means the stream or the group has gone — a `DEL` of the key,
        or an `XGROUP DESTROY`. Unstarting this object is what makes that
        survivable: the runner's loop calls `start()` on every pass, so the
        group is recreated and consuming resumes, where a raised error alone
        would be retried forever against a group that no longer exists.
        """
        if _NOGROUP in str(exc):
            self._started = False
            return StreamGroupError(
                f"The stream or group vanished while trying to {what}.",
                details={"stream": self._stream, "group": self._group},
            )
        return StreamGroupError(
            f"Could not {what}.",
            details={"stream": self._stream, "group": self._group},
        )

    async def publish(
        self, fields: Fields, *, stream: str | None = None
    ) -> StreamEntryId:
        validate_fields(fields)
        target = stream if stream is not None else self._stream
        try:
            # `approximate=True` is the `~` form: the server trims at a node
            # boundary rather than counting to an exact length, which is O(1)
            # amortised instead of O(n) per write. An exact cap is not worth a
            # latency spike on an unlucky `XADD`, and nothing here depends on
            # the stream being a precise length.
            # `dict[Any, Any]` because redis-py declares the hash as a dict over
            # a union of every type it will encode, and `dict` is invariant:
            # a `dict[str, bytes]` is not one of those however correct it is.
            # `validate_fields` above is what actually constrains this.
            payload: dict[Any, Any] = dict(fields)
            raw: Any = await self._client.xadd(
                target,
                payload,
                id=AUTO_ID,
                maxlen=self._maxlen if self._maxlen else None,
                approximate=True,
            )
        except RedisError as exc:
            raise StreamPublishError(
                "Could not append to the stream.", details={"stream": target}
            ) from exc
        return StreamEntryId.parse(raw)

    async def read(self, *, count: int, block: float) -> Sequence[StreamMessage]:
        if count < 1:
            raise ValueError("count must be at least 1.")
        self._require_started()
        # `None` rather than `0`: redis-py sends `BLOCK 0` for a zero, which is
        # "wait indefinitely", and a consumer parked in one cannot be shut
        # down until a message happens to arrive.
        block_ms = to_ms(block) if block > 0 else None
        try:
            raw: Any = await self._client.xreadgroup(
                self._group,
                self._consumer,
                {self._stream: NEW_MESSAGES},
                count=count,
                block=block_ms,
            )
        except ResponseError as exc:
            raise self._handle_response_error(exc, "read from the group") from exc
        except RedisError as exc:
            raise StreamUnavailableError(
                "Could not read from the stream.", details={"stream": self._stream}
            ) from exc

        if not raw:
            return ()
        messages: list[StreamMessage] = []
        for _stream_name, entries in raw:
            for entry_id, fields in entries:
                if fields is None:  # pragma: no cover - only reachable via `0`
                    continue
                messages.append(
                    StreamMessage(
                        stream=self._stream,
                        id=StreamEntryId.parse(entry_id),
                        fields=_frozen(decode_fields(fields)),
                        delivery_count=1,
                    )
                )
        return tuple(messages)

    async def stalled(
        self, *, min_idle: float, count: int, start: StreamEntryId | None = None
    ) -> Sequence[PendingEntry]:
        if count < 1:
            raise ValueError("count must be at least 1.")
        self._require_started()
        try:
            raw: Any = await self._client.xpending_range(
                self._stream,
                self._group,
                min=start.exclusive if start is not None else "-",
                max="+",
                count=count,
                idle=to_ms(min_idle),
            )
        except ResponseError as exc:
            raise self._handle_response_error(exc, "list pending entries") from exc
        except RedisError as exc:
            raise StreamUnavailableError(
                "Could not list pending entries.", details={"stream": self._stream}
            ) from exc

        return tuple(
            PendingEntry(
                id=StreamEntryId.parse(row["message_id"]),
                consumer=_text(row["consumer"]),
                idle=float(row["time_since_delivered"]) / 1000.0,
                delivery_count=int(row["times_delivered"]),
            )
            for row in raw
        )

    async def claim(
        self, entries: Sequence[PendingEntry], *, min_idle: float
    ) -> ClaimedMessage:
        self._require_started()
        if not entries:
            return ClaimedMessage(messages=(), missing=())
        wanted = {entry.id: entry for entry in entries}
        try:
            raw: Any = await self._client.xclaim(
                self._stream,
                self._group,
                self._consumer,
                min_idle_time=to_ms(min_idle),
                message_ids=[str(entry.id) for entry in entries],
            )
        except ResponseError as exc:
            raise self._handle_response_error(exc, "claim pending entries") from exc
        except RedisError as exc:
            raise StreamUnavailableError(
                "Could not claim pending entries.", details={"stream": self._stream}
            ) from exc

        claimed: list[StreamMessage] = []
        for entry_id, fields in raw:
            parsed = StreamEntryId.parse(entry_id)
            entry = wanted.pop(parsed, None)
            if entry is None or fields is None:  # pragma: no cover - defensive
                continue
            claimed.append(
                StreamMessage(
                    stream=self._stream,
                    id=parsed,
                    fields=_frozen(decode_fields(fields)),
                    # The server has just incremented it; `entry` holds the
                    # count as it was before this claim.
                    delivery_count=entry.delivery_count + 1,
                    claimed=True,
                )
            )
        return ClaimedMessage(messages=tuple(claimed), missing=tuple(sorted(wanted)))

    async def ack(self, ids: Sequence[StreamEntryId]) -> int:
        if not ids:
            return 0
        self._require_started()
        try:
            raw: Any = await self._client.xack(
                self._stream, self._group, *[str(entry_id) for entry_id in ids]
            )
        except ResponseError as exc:
            raise self._handle_response_error(exc, "acknowledge messages") from exc
        except RedisError as exc:
            raise StreamUnavailableError(
                "Could not acknowledge messages.", details={"stream": self._stream}
            ) from exc
        return int(raw)

    async def pending_count(self, *, consumer: str | None = None) -> int:
        try:
            raw: Any = await self._client.xpending(self._stream, self._group)
        except ResponseError as exc:
            raise self._handle_response_error(exc, "count pending entries") from exc
        except RedisError as exc:
            raise StreamUnavailableError(
                "Could not count pending entries.", details={"stream": self._stream}
            ) from exc
        if consumer is None:
            return int(raw["pending"])
        for row in raw["consumers"] or ():
            if _text(row["name"]) == consumer:
                return int(row["pending"])
        return 0


def _text(value: object) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _frozen(fields: dict[str, bytes]) -> FrozenDict[str, bytes]:
    return FrozenDict[str, bytes](fields)


__all__ = ["RedisStreamGroup", "decode_fields", "to_ms"]
