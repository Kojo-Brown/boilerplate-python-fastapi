from src.tasks.celery_email import (
    send_password_reset_email_task,
    send_welcome_email_task,
)
from src.tasks.email import (
    EmailMessage,
    send_email_with_retry,
    send_password_reset_email,
    send_welcome_email,
)

__all__ = [
    # Async (FastAPI BackgroundTasks)
    "EmailMessage",
    "send_email_with_retry",
    "send_password_reset_email",
    "send_welcome_email",
    # Celery tasks (distributed task queue)
    "send_welcome_email_task",
    "send_password_reset_email_task",
]
