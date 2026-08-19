from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
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


settings = Settings()
