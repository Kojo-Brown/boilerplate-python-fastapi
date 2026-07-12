"""Celery email tasks.

These tasks run in a Celery worker process and use Redis as the message broker.
They are the preferred production alternative to FastAPI BackgroundTasks for
work that should survive server restarts and be distributed across workers.

Usage:
    from src.tasks.celery_email import send_welcome_email_task

    # Fire-and-forget
    send_welcome_email_task.delay(to="user@example.com")

    # With options (countdown = delay in seconds, expires = task TTL)
    send_welcome_email_task.apply_async(
        args=["user@example.com"],
        kwargs={"username": "Alice"},
        countdown=5,
        expires=3600,
    )
"""

import structlog
from celery import Task

from src.tasks.email import EmailMessage
from src.worker import celery_app

logger = structlog.get_logger(__name__)


def _deliver_email_sync(message: EmailMessage) -> None:
    """Synchronous delivery stub. Replace with SMTP / SES / SendGrid in production."""
    logger.info("email.delivered", to=message.to, subject=message.subject)


@celery_app.task(
    bind=True,
    name="tasks.send_welcome_email",
    max_retries=3,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=False,
)
def send_welcome_email_task(
    self: Task,
    to: str,
    username: str | None = None,
) -> dict[str, str]:
    """Send a welcome email via the Celery task queue.

    Args:
        to: Recipient email address.
        username: Optional display name; falls back to the email address.

    Returns:
        Mapping with ``status`` and ``to`` keys.
    """
    name = username or to
    _deliver_email_sync(
        EmailMessage(
            to=to,
            subject="Welcome to the platform",
            body=f"Hi {name},\n\nWelcome! Your account is ready.",
            html_body=(
                f"<p>Hi <strong>{name}</strong>,</p>"
                "<p>Welcome! Your account is ready.</p>"
            ),
        )
    )
    logger.info("celery.task.send_welcome_email.success", to=to)
    return {"status": "sent", "to": to}


@celery_app.task(
    bind=True,
    name="tasks.send_password_reset_email",
    max_retries=3,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=300,
    retry_jitter=False,
)
def send_password_reset_email_task(
    self: Task,
    to: str,
    reset_token: str,
) -> dict[str, str]:
    """Send a password-reset email via the Celery task queue.

    Args:
        to: Recipient email address.
        reset_token: One-time reset token embedded in the email link.

    Returns:
        Mapping with ``status`` and ``to`` keys.
    """
    _deliver_email_sync(
        EmailMessage(
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
    )
    logger.info("celery.task.send_password_reset_email.success", to=to)
    return {"status": "sent", "to": to}
