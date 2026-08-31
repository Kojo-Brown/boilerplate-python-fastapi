from typing import Final, Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration, read once from the environment and never afterwards.

    `frozen=True` is load-bearing rather than tidy. Several things in this
    codebase already assume settings do not move: `get_strategy` is `@cache`d
    per channel, `StorageFactory` and `PaymentGatewayRegistry` build their
    instances from a `Settings` at first use, and the idempotency and lock
    backends are constructed once in the lifespan. Under a mutable singleton,
    `settings.PAYMENT_GATEWAY = "paypal"` is a line that type-checks, appears
    to work, and takes effect for some subsystems and not others depending on
    what has already been built — the worst failure shape available, because
    every individual piece is behaving exactly as designed.

    A test that needs different configuration constructs its own
    `Settings(...)` and passes it in; every factory here takes one for exactly
    that reason. That is a better seam than mutating the global, since it
    cannot leak into the next test.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    DATABASE_URL: str
    SECRET_KEY: str
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7
    ENVIRONMENT: str = "development"
    LOG_LEVEL: str = "INFO"

    # Argon2 parameters (OWASP recommended minimums)
    ARGON2_TIME_COST: int = 2
    ARGON2_MEMORY_COST: int = 65536  # 64 MiB
    ARGON2_PARALLELISM: int = 2
    ARGON2_HASH_LEN: int = 32
    ARGON2_SALT_LEN: int = 16

    # Redis (Celery broker + backend)
    REDIS_URL: str = "redis://localhost:6379/0"

    # Idempotency (see src/idempotency/factory.py and
    # src/middleware/idempotency.py). The store is separate from Celery's use of
    # Redis only by key namespace; IDEMPOTENCY_REDIS_URL overrides REDIS_URL for
    # deployments that want a different instance or database number.
    #
    # The reservation TTL bounds how long a crashed worker can make retries of
    # its request wait, so it must sit above the slowest request this API
    # serves — expiring under a still-running request lets a retry execute
    # beside it. The record TTL is how long a completed response stays
    # replayable, which is a client-retry-window question, not a server one.
    IDEMPOTENCY_ENABLED: bool = True
    IDEMPOTENCY_BACKEND: Literal["redis", "memory"] = "redis"
    IDEMPOTENCY_REDIS_URL: str = ""
    IDEMPOTENCY_TTL_SECONDS: int = 86400  # 24h
    IDEMPOTENCY_RESERVATION_TTL_SECONDS: int = 60
    IDEMPOTENCY_MAX_BODY_BYTES: int = 1048576  # 1 MiB
    # Serve requests without deduplication when the store is unreachable.
    # Leaving this false trades availability for exactly-once execution, which
    # is the right trade for anything that moves money.
    IDEMPOTENCY_FAIL_OPEN: bool = False

    # Distributed locking (see src/distributed_lock/). Shares the Redis server
    # with Celery and the idempotency store by default, separated by key
    # namespace; DISTRIBUTED_LOCK_REDIS_URL overrides REDIS_URL for deployments
    # that want a different instance or database number.
    #
    # The backend must be "redis" anywhere more than one process runs: "memory"
    # coordinates callers inside a single process and nothing beyond it, and it
    # hands out fencing tokens that only make sense within that process.
    #
    # The TTL bounds how long a crashed holder blocks a name, so it is a
    # trade-off rather than a maximum: too short and a slow section loses its
    # lease mid-flight, too long and a killed worker locks the name for that
    # long. Pass `renew_interval` to DistributedLock for a section whose
    # duration is unpredictable, rather than raising this.
    DISTRIBUTED_LOCK_BACKEND: Literal["redis", "memory"] = "redis"
    DISTRIBUTED_LOCK_REDIS_URL: str = ""
    DISTRIBUTED_LOCK_NAMESPACE: str = "dlock"
    DISTRIBUTED_LOCK_TTL_SECONDS: float = 30.0

    # Parallel execution (see src/parallel/). Two unrelated bounds that happen
    # to live next to each other: how much CPU-bound work may be offloaded to
    # worker processes, and how much outbound IO may be in flight at once.
    #
    # CPU_POOL_MAX_WORKERS is 0 for "derive it", which subtracts one from the
    # CPU allowance this process actually has — not the host's core count, which
    # in a container is a much larger and quite unrelated number. Set it
    # explicitly only when the pod's CPU limit is not what the runtime reports.
    # Remember that uvicorn's own --workers multiplies this: four server
    # workers with four pool workers each is sixteen child processes.
    #
    # The start method must not be "fork" — CpuPool refuses it, because a fork
    # of an async server inherits its open database sockets and any lock held by
    # a thread that did not survive. "forkserver" is cheaper per worker than
    # "spawn" and equally safe; "spawn" is the portable default.
    #
    # QUEUE_DEPTH_PER_WORKER bounds what waits for a busy pool: past
    # workers * (1 + depth), calls are refused with 503 rather than queued.
    # Queued calls hold their pickled arguments in this process's memory, so
    # raising it trades memory and latency for burst tolerance.
    CPU_POOL_MAX_WORKERS: int = 0
    CPU_POOL_MAX_TASKS_PER_CHILD: int = 100
    CPU_POOL_QUEUE_DEPTH_PER_WORKER: int = 4
    CPU_POOL_START_METHOD: Literal["spawn", "forkserver"] = "spawn"

    # The ceiling on concurrent outbound calls for fan-outs that share it.
    # A per-call limit does not compose — `limit=8` inside a handler serving
    # fifty concurrent requests is four hundred sockets — so the real bound has
    # to be process-wide. See src/parallel/factory.py.
    OUTBOUND_CONCURRENCY_LIMIT: int = 20

    # Transactional outbox (see src/outbox/ and docs/outbox.md). There is no
    # switch for *writing* outbox rows: the publisher is wired in
    # src/dependencies.py, and a flag that quietly sent events back through the
    # in-process bus would reintroduce the lost-event window the outbox exists
    # to close, in the deployment least expecting it.
    #
    # RELAY_ENABLED is a different question — whether *this* process drains the
    # table. Turn it off in the API pods and run one or more dedicated relay
    # processes when you want delivery isolated from request traffic; the row
    # locks make any number of relays safe. Turning it off everywhere means
    # nothing is delivered, which the health of the table makes obvious.
    #
    # BATCH_SIZE and DISPATCH_TIMEOUT multiply into the worst-case time one
    # transaction holds a pooled connection and a set of row locks. POLL
    # INTERVAL is the tail latency of every notification when the queue is
    # empty, and one query per interval per relay when it stays empty.
    OUTBOX_RELAY_ENABLED: bool = True
    OUTBOX_BATCH_SIZE: int = 100
    OUTBOX_POLL_INTERVAL_SECONDS: float = 1.0
    OUTBOX_DISPATCH_TIMEOUT_SECONDS: float = 30.0
    OUTBOX_RETRY_BASE_DELAY_SECONDS: float = 1.0
    OUTBOX_RETRY_MAX_DELAY_SECONDS: float = 300.0

    # Streaming exports (see src/streaming/ and docs/streaming.md).
    #
    # CHUNK_BYTES and READAHEAD_CHUNKS multiply into the peak memory one
    # in-flight export holds, and that product times the number of concurrent
    # exports is the figure to size a pod against: 64 KiB x 2 x 20 downloads is
    # 2.5 MiB, which is the whole point of the pair being small numbers rather
    # than a queue depth in rows.
    #
    # BATCH_ROWS is a round-trip knob and not a memory one — the read-ahead
    # bounds memory whatever the cursor fetches — so it trades latency to the
    # first chunk against per-batch overhead.
    #
    # DEADLINE_SECONDS bounds how long one export may hold a pooled connection
    # and a server-side cursor, including time spent waiting on a client that
    # has stopped reading. Past it the stream ends with a `failed` terminal
    # record rather than a truncation the client cannot detect. It has to sit
    # above the slowest legitimate export; when it stops being able to, the
    # answer is an asynchronous export job, not a larger number here.
    EXPORT_CHUNK_BYTES: int = 65536  # 64 KiB
    EXPORT_READAHEAD_CHUNKS: int = 2
    EXPORT_BATCH_ROWS: int = 500
    EXPORT_DEADLINE_SECONDS: float = 300.0

    # Server-sent events (see src/sse/).
    #
    # HEARTBEAT_SECONDS is two limits in one number, and the second is the one
    # that gets forgotten. It has to sit under the shortest idle timeout on the
    # path — 60s for nginx and most load balancers — or connections are closed
    # under a quiet stream. It is *also* the ceiling on how long an abandoned
    # stream holds its subscription, because a failed write is the only way a
    # disconnect is discovered; see the docstring in src/sse/heartbeat.py.
    #
    # RETRY_MS is advice sent to the client for how long to wait before
    # reconnecting. Raising it thins out the reconnect storm after a deploy;
    # lowering it shortens the window in which events are missed, since this
    # application has no replay buffer.
    #
    # CLIENT_BUFFER_EVENTS is how far one stream may fall behind before it is
    # closed with an `overflow` event rather than being allowed to hold events
    # in memory indefinitely. Worst-case memory is this times the connection
    # limit times an event.
    #
    # MAX_STREAM_SECONDS ends a stream cleanly so the client reconnects. An
    # infinite connection accumulates whatever the process cannot reclaim
    # underneath it — a rotated token stays live, a stream stays pinned to a
    # replica a deploy is trying to drain — and a bounded one turns all of that
    # into a recurring, ordinary event.
    SSE_HEARTBEAT_SECONDS: float = 15.0
    SSE_RETRY_MS: int = 3000
    SSE_CLIENT_BUFFER_EVENTS: int = 64
    SSE_MAX_STREAM_SECONDS: float = 3600.0

    # WebSockets (see src/ws/).
    #
    # MAX_MESSAGE_BYTES is *not* the real ceiling on an inbound frame. By the
    # time application code sees a message the ASGI server has already read it,
    # so uvicorn's own `--ws-max-size` (16 MiB by default) is what stops a large
    # frame; this only decides what is then rejected. Lower both, together.
    #
    # MAX_ROOMS_PER_CONNECTION bounds how much of the process's traffic one
    # client can make itself a recipient of. Each membership is an entry in the
    # registry and a share of every broadcast to that room.
    #
    # OUTBOUND_BUFFER_MESSAGES is how far one connection may fall behind before
    # it is closed rather than allowed to hold messages in memory. Worst-case
    # memory is this times the connection count times a message — the same
    # arithmetic as SSE_CLIENT_BUFFER_EVENTS, over a larger message.
    #
    # IDLE_TIMEOUT_SECONDS closes a connection nothing has been received on.
    # It is not a liveness check: WebSocket ping/pong is handled by the ASGI
    # server and has no ASGI message type, so an application cannot observe a
    # pong. Clients that want to stay open through a quiet period send the
    # protocol's own `{"type":"ping"}`.
    #
    # MAX_CONNECTION_SECONDS ends a connection cleanly so the client
    # reconnects, for the reason SSE_MAX_STREAM_SECONDS exists. It is a
    # *ceiling*, not the usual lifetime: a connection also ends when its access
    # token expires, which at the default ACCESS_TOKEN_EXPIRE_MINUTES is far
    # sooner and is the limit most connections actually meet.
    #
    # The four rate-limit values are per connection, not per address: a
    # WebSocket is one request no matter how many messages travel down it, so
    # the middleware limiter in src/limiter.py counts it once and never again.
    # Both a message budget and a byte budget, because either alone is trivial
    # to evade — see src/ws/ratelimit.py. BYTE_BURST must be at least
    # MAX_MESSAGE_BYTES or a maximum-size message could never be afforded;
    # Connection refuses to be constructed otherwise.
    #
    # MAX_RATE_VIOLATIONS is *consecutive* rejections before the connection is
    # closed. An accepted message resets it, so an application that
    # occasionally bumps the ceiling is never disconnected for it.
    WS_MAX_MESSAGE_BYTES: int = 65536  # 64 KiB
    WS_MAX_ROOMS_PER_CONNECTION: int = 16
    WS_OUTBOUND_BUFFER_MESSAGES: int = 128
    WS_IDLE_TIMEOUT_SECONDS: float = 120.0
    WS_MAX_CONNECTION_SECONDS: float = 3600.0
    WS_MESSAGES_PER_SECOND: float = 10.0
    WS_MESSAGE_BURST: int = 30
    WS_BYTES_PER_SECOND: float = 262144.0  # 256 KiB/s
    WS_BYTE_BURST: int = 1048576  # 1 MiB
    WS_MAX_RATE_VIOLATIONS: int = 20

    # Google OAuth 2.0
    GOOGLE_CLIENT_ID: str = ""
    GOOGLE_CLIENT_SECRET: str = ""
    GOOGLE_REDIRECT_URI: str = "http://localhost:8000/auth/google/callback"

    # AWS S3
    AWS_ACCESS_KEY_ID: str = ""
    AWS_SECRET_ACCESS_KEY: str = ""
    AWS_REGION: str = "us-east-1"
    AWS_S3_BUCKET: str = ""
    AWS_S3_PRESIGNED_URL_EXPIRY: int = 3600

    # Object storage backend (see src/storage/factory.py). "local" and "memory"
    # exist for development and tests; "memory" holds objects in the worker
    # process, so it is never correct for a multi-worker deployment.
    STORAGE_BACKEND: Literal["s3", "local", "memory"] = "s3"
    STORAGE_LOCAL_ROOT: str = "./var/storage"

    # Notifications (see src/notifications/registry.py). The default is only a
    # fallback for users whose preference is unset — a user who has chosen a
    # channel is routed there regardless of this value.
    NOTIFICATION_DEFAULT_CHANNEL: Literal["email", "webhook", "none"] = "email"
    # Shared with webhook receivers so they can authenticate deliveries. Empty
    # means unsigned: fine for a local receiver, never for a third party.
    NOTIFICATION_WEBHOOK_SECRET: str = ""
    NOTIFICATION_WEBHOOK_TIMEOUT_SECONDS: float = 10.0
    NOTIFICATION_WEBHOOK_MAX_ATTEMPTS: int = 3
    NOTIFICATION_WEBHOOK_BACKOFF_SECONDS: float = 0.5
    # Lets webhooks target loopback and RFC 1918 addresses. Development only —
    # switching it on in production turns a user-supplied URL into SSRF.
    NOTIFICATION_WEBHOOK_ALLOW_PRIVATE_HOSTS: bool = False

    # Payments (see src/payments/registry.py). Only the selected provider's
    # credentials need to be present; the registry raises
    # PaymentConfigurationError when they are missing, rather than failing at a
    # customer's checkout. The base URLs are overridable so tests and staging
    # can point at a sandbox or a local stub.
    PAYMENT_GATEWAY: Literal["stripe", "paypal"] = "stripe"
    PAYMENT_TIMEOUT_SECONDS: float = 15.0
    STRIPE_SECRET_KEY: str = ""
    STRIPE_API_BASE_URL: str = "https://api.stripe.com"
    PAYPAL_CLIENT_ID: str = ""
    PAYPAL_CLIENT_SECRET: str = ""
    # Sandbox by default: a wrong value here charges real cards, so production
    # has to say so explicitly (https://api-m.paypal.com).
    PAYPAL_API_BASE_URL: str = "https://api-m.sandbox.paypal.com"


settings: Final[Settings] = Settings()
