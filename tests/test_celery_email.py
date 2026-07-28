import os
from unittest.mock import patch

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")

import pytest
from celery.exceptions import Retry

from src.tasks.celery_email import (
    _deliver_email_sync,
    send_password_reset_email_task,
    send_welcome_email_task,
)
from src.tasks.email import EmailMessage

# ---------------------------------------------------------------------------
# _deliver_email_sync
# ---------------------------------------------------------------------------


def test_deliver_email_sync_logs_without_raising() -> None:
    msg = EmailMessage(to="a@b.com", subject="Hi", body="Hello")
    _deliver_email_sync(msg)  # must not raise


# ---------------------------------------------------------------------------
# send_welcome_email_task
# ---------------------------------------------------------------------------


def test_send_welcome_email_task_returns_sent() -> None:
    with patch("src.tasks.celery_email._deliver_email_sync"):
        result = send_welcome_email_task.apply(args=["user@example.com"])
    assert result.get() == {"status": "sent", "to": "user@example.com"}


def test_send_welcome_email_task_uses_email_as_name_when_no_username() -> None:
    captured: list[EmailMessage] = []

    def fake_deliver(msg: EmailMessage) -> None:
        captured.append(msg)

    with patch("src.tasks.celery_email._deliver_email_sync", side_effect=fake_deliver):
        send_welcome_email_task.apply(args=["user@example.com"])

    assert len(captured) == 1
    msg = captured[0]
    assert msg.to == "user@example.com"
    assert "user@example.com" in msg.body
    assert "Welcome" in msg.subject


def test_send_welcome_email_task_uses_provided_username() -> None:
    captured: list[EmailMessage] = []

    def fake_deliver(msg: EmailMessage) -> None:
        captured.append(msg)

    with patch("src.tasks.celery_email._deliver_email_sync", side_effect=fake_deliver):
        send_welcome_email_task.apply(
            args=["user@example.com"], kwargs={"username": "Alice"}
        )

    msg = captured[0]
    assert "Alice" in msg.body
    assert msg.html_body is not None
    assert "Alice" in msg.html_body


def test_send_welcome_email_task_html_body_not_none() -> None:
    captured: list[EmailMessage] = []

    def fake_deliver(msg: EmailMessage) -> None:
        captured.append(msg)

    with patch("src.tasks.celery_email._deliver_email_sync", side_effect=fake_deliver):
        send_welcome_email_task.apply(args=["user@example.com"])

    assert captured[0].html_body is not None


# ---------------------------------------------------------------------------
# send_password_reset_email_task
# ---------------------------------------------------------------------------


def test_send_password_reset_email_task_returns_sent() -> None:
    with patch("src.tasks.celery_email._deliver_email_sync"):
        result = send_password_reset_email_task.apply(
            args=["user@example.com", "tok-abc123"]
        )
    assert result.get() == {"status": "sent", "to": "user@example.com"}


def test_send_password_reset_email_task_includes_token() -> None:
    captured: list[EmailMessage] = []

    def fake_deliver(msg: EmailMessage) -> None:
        captured.append(msg)

    with patch("src.tasks.celery_email._deliver_email_sync", side_effect=fake_deliver):
        send_password_reset_email_task.apply(args=["user@example.com", "tok-abc123"])

    msg = captured[0]
    assert msg.to == "user@example.com"
    assert "tok-abc123" in msg.body
    assert msg.html_body is not None
    assert "tok-abc123" in msg.html_body
    assert "Reset" in msg.subject


def test_send_password_reset_email_task_html_body_not_none() -> None:
    captured: list[EmailMessage] = []

    def fake_deliver(msg: EmailMessage) -> None:
        captured.append(msg)

    with patch("src.tasks.celery_email._deliver_email_sync", side_effect=fake_deliver):
        send_password_reset_email_task.apply(args=["user@example.com", "tok-abc123"])

    assert captured[0].html_body is not None


# ---------------------------------------------------------------------------
# Retry behaviour
#
# Both tasks declare autoretry_for=(Exception,), so a delivery failure is turned
# into a retry request rather than propagating the original exception. Celery
# signals that by raising Retry with the triggering error attached as `.exc`.
# ---------------------------------------------------------------------------


def test_send_welcome_email_task_retries_on_delivery_error() -> None:
    with patch(
        "src.tasks.celery_email._deliver_email_sync",
        side_effect=RuntimeError("smtp down"),
    ):
        with pytest.raises(Retry) as exc_info:
            send_welcome_email_task.apply(args=["user@example.com"])

    cause = exc_info.value.exc
    assert isinstance(cause, RuntimeError)
    assert str(cause) == "smtp down"


def test_send_password_reset_email_task_retries_on_delivery_error() -> None:
    with patch(
        "src.tasks.celery_email._deliver_email_sync",
        side_effect=RuntimeError("smtp down"),
    ):
        with pytest.raises(Retry) as exc_info:
            send_password_reset_email_task.apply(args=["user@example.com", "tok-x"])

    cause = exc_info.value.exc
    assert isinstance(cause, RuntimeError)
    assert str(cause) == "smtp down"


def test_email_tasks_are_configured_to_retry() -> None:
    for task in (send_welcome_email_task, send_password_reset_email_task):
        assert task.max_retries == 3
        assert task.retry_backoff is True


# ---------------------------------------------------------------------------
# Worker module — celery_app is importable and configured
# ---------------------------------------------------------------------------


def test_celery_app_has_email_task_routes() -> None:
    from src.worker import celery_app

    routes = celery_app.conf.task_routes
    assert routes.get("tasks.send_welcome_email") == {"queue": "email"}
    assert routes.get("tasks.send_password_reset_email") == {"queue": "email"}


def test_celery_app_registered_tasks() -> None:
    from src.worker import celery_app

    registered = set(celery_app.tasks.keys())
    assert "tasks.send_welcome_email" in registered
    assert "tasks.send_password_reset_email" in registered
