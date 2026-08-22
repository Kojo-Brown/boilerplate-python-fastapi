"""Background email tasks using asyncio + FastAPI BackgroundTasks.

Usage in a router:
    from fastapi import BackgroundTasks
    from src.tasks import send_welcome_email

    @router.post("/register")
    async def register(
        data: RegisterRequest,
        background_tasks: BackgroundTasks,
        db: AsyncSession = Depends(get_db),
    ) -> UserResponse:
        user = await service.register(data)
        background_tasks.add_task(send_welcome_email, user.email)
        return user
"""

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass

import structlog

from src.immutable import EMPTY_MAPPING, freeze_mapping

logger = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class EmailMessage:
    """One email, as a value object.

    Frozen because `send_email_with_retry` may deliver this object up to three
    times: a message edited between attempts means attempt two sends something
    the caller never asked for, and the log line naming the failure refers to a
    subject that no longer exists. Retry is only safe over an argument that
    cannot change under it.
    """

    to: str
    subject: str
    body: str
    html_body: str | None = None
    #: Frozen on construction — see `__post_init__`.
    headers: Mapping[str, str] = EMPTY_MAPPING

    def __post_init__(self) -> None:
        # `frozen=True` refuses `message.headers = {...}` and permits
        # `message.headers["Bcc"] = ...`, which is the mutation that matters
        # for a mail header.
        object.__setattr__(self, "headers", freeze_mapping(self.headers))


async def _deliver_email(message: EmailMessage) -> None:
    """Stub delivery: replace with SMTP, SES, SendGrid, etc.

    The ``await asyncio.sleep(0)`` yields to the event loop so this function
    is genuinely async and safe to call from background tasks without blocking.
    """
    await asyncio.sleep(0)
    logger.info(
        "email.delivered",
        to=message.to,
        subject=message.subject,
    )


async def send_email_with_retry(
    message: EmailMessage,
    *,
    max_attempts: int = 3,
    backoff: float = 1.0,
    timeout: float = 30.0,
) -> None:
    """Send *message* with exponential back-off retry and per-attempt timeout.

    Args:
        message: The :class:`EmailMessage` to send.
        max_attempts: Maximum delivery attempts before raising.
        backoff: Base delay in seconds; doubles on each retry.
        timeout: Per-attempt timeout in seconds.

    Raises:
        Exception: Re-raises the last delivery error after *max_attempts*.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            await asyncio.wait_for(_deliver_email(message), timeout=timeout)
            return
        except Exception as exc:
            if attempt == max_attempts:
                logger.error(
                    "email.failed",
                    to=message.to,
                    subject=message.subject,
                    attempts=attempt,
                    error=str(exc),
                )
                raise
            delay = backoff * (2 ** (attempt - 1))
            logger.warning(
                "email.retrying",
                to=message.to,
                attempt=attempt,
                next_attempt_in=delay,
                error=str(exc),
            )
            await asyncio.sleep(delay)


async def send_welcome_email(to: str, *, username: str | None = None) -> None:
    """Send a welcome email after a user registers.

    Args:
        to: Recipient email address.
        username: Optional display name; falls back to the email address.
    """
    name = username or to
    message = EmailMessage(
        to=to,
        subject="Welcome to the platform",
        body=f"Hi {name},\n\nWelcome! Your account is ready.",
        html_body=(
            f"<p>Hi <strong>{name}</strong>,</p><p>Welcome! Your account is ready.</p>"
        ),
    )
    await send_email_with_retry(message)


async def send_password_reset_email(to: str, *, reset_token: str) -> None:
    """Send a password-reset email.

    Args:
        to: Recipient email address.
        reset_token: The one-time reset token embedded in the email link.
    """
    message = EmailMessage(
        to=to,
        subject="Reset your password",
        body=(
            f"Use the following token to reset your password:\n\n{reset_token}\n\n"
            "This token expires in 30 minutes."
        ),
        html_body=(
            "<p>Use the following token to reset your password:</p>"
            f"<p><code>{reset_token}</code></p>"
            "<p>This token expires in 30 minutes.</p>"
        ),
    )
    await send_email_with_retry(message)
