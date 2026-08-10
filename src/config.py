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
