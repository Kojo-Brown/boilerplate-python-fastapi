import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, call, patch

os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://test:test@localhost/test")
os.environ.setdefault("SECRET_KEY", "test-secret-key-minimum-32-characters-long!")

import pytest

from src.tasks.email import (
    EmailMessage,
    send_email_with_retry,
    send_password_reset_email,
    send_welcome_email,
)


# ---------------------------------------------------------------------------
# EmailMessage
# ---------------------------------------------------------------------------


def test_email_message_defaults() -> None:
    msg = EmailMessage(to="a@b.com", subject="Hi", body="Hello")
    assert msg.html_body is None
    assert msg.headers == {}


def test_email_message_with_html() -> None:
    msg = EmailMessage(
        to="a@b.com",
        subject="Hi",
        body="Hello",
        html_body="<p>Hello</p>",
    )
    assert msg.html_body == "<p>Hello</p>"


# ---------------------------------------------------------------------------
# send_email_with_retry — happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_email_with_retry_success_on_first_attempt() -> None:
    msg = EmailMessage(to="x@y.com", subject="Test", body="Body")
    deliver_mock = AsyncMock()

    with patch("src.tasks.email._deliver_email", deliver_mock):
        await send_email_with_retry(msg)

    deliver_mock.assert_awaited_once_with(msg)


# ---------------------------------------------------------------------------
# send_email_with_retry — retry behaviour
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_email_with_retry_retries_then_succeeds() -> None:
    msg = EmailMessage(to="x@y.com", subject="Test", body="Body")
    deliver_mock = AsyncMock(side_effect=[RuntimeError("transient"), None])

    with (
        patch("src.tasks.email._deliver_email", deliver_mock),
        patch("asyncio.sleep", new_callable=AsyncMock) as sleep_mock,
    ):
        await send_email_with_retry(msg, max_attempts=3, backoff=0.0)

    assert deliver_mock.await_count == 2
    sleep_mock.assert_awaited_once_with(0.0)


@pytest.mark.asyncio
async def test_send_email_with_retry_raises_after_max_attempts() -> None:
    msg = EmailMessage(to="x@y.com", subject="Test", body="Body")
    deliver_mock = AsyncMock(side_effect=RuntimeError("permanent"))

    with (
        patch("src.tasks.email._deliver_email", deliver_mock),
        patch("asyncio.sleep", new_callable=AsyncMock),
    ):
        with pytest.raises(RuntimeError, match="permanent"):
            await send_email_with_retry(msg, max_attempts=3, backoff=0.0)

    assert deliver_mock.await_count == 3


@pytest.mark.asyncio
async def test_send_email_with_retry_exponential_backoff() -> None:
    msg = EmailMessage(to="x@y.com", subject="Test", body="Body")
    deliver_mock = AsyncMock(
        side_effect=[RuntimeError("err"), RuntimeError("err"), None]
    )

    with (
        patch("src.tasks.email._deliver_email", deliver_mock),
        patch("asyncio.sleep", new_callable=AsyncMock) as sleep_mock,
    ):
        await send_email_with_retry(msg, max_attempts=3, backoff=1.0)

    assert sleep_mock.await_args_list == [call(1.0), call(2.0)]


@pytest.mark.asyncio
async def test_send_email_with_retry_timeout_propagates() -> None:
    msg = EmailMessage(to="x@y.com", subject="Test", body="Body")

    async def slow_deliver(_: EmailMessage) -> None:
        await asyncio.sleep(10)

    with patch("src.tasks.email._deliver_email", slow_deliver):
        with pytest.raises(asyncio.TimeoutError):
            await send_email_with_retry(msg, max_attempts=1, timeout=0.001)


# ---------------------------------------------------------------------------
# send_welcome_email
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_welcome_email_uses_email_as_name_when_no_username() -> None:
    captured: list[EmailMessage] = []

    async def mock_retry(msg: EmailMessage, **kwargs: object) -> None:
        captured.append(msg)

    with patch("src.tasks.email.send_email_with_retry", mock_retry):
        await send_welcome_email("user@example.com")

    assert len(captured) == 1
    msg = captured[0]
    assert msg.to == "user@example.com"
    assert "user@example.com" in msg.body
    assert "Welcome" in msg.subject


@pytest.mark.asyncio
async def test_send_welcome_email_uses_provided_username() -> None:
    captured: list[EmailMessage] = []

    async def mock_retry(msg: EmailMessage, **kwargs: object) -> None:
        captured.append(msg)

    with patch("src.tasks.email.send_email_with_retry", mock_retry):
        await send_welcome_email("user@example.com", username="Alice")

    msg = captured[0]
    assert "Alice" in msg.body
    assert msg.html_body is not None
    assert "Alice" in msg.html_body


# ---------------------------------------------------------------------------
# send_password_reset_email
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_password_reset_email_includes_token() -> None:
    captured: list[EmailMessage] = []

    async def mock_retry(msg: EmailMessage, **kwargs: object) -> None:
        captured.append(msg)

    with patch("src.tasks.email.send_email_with_retry", mock_retry):
        await send_password_reset_email("user@example.com", reset_token="abc123")

    msg = captured[0]
    assert msg.to == "user@example.com"
    assert "abc123" in msg.body
    assert msg.html_body is not None
    assert "abc123" in msg.html_body
    assert "Reset" in msg.subject


# ---------------------------------------------------------------------------
# FastAPI BackgroundTasks integration — register endpoint
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_endpoint_schedules_welcome_email(
    async_client: object,
) -> None:
    """The register endpoint should schedule send_welcome_email as a background task."""
    import uuid
    from unittest.mock import patch as _patch

    from src.auth.schemas import UserResponse
    from src.auth.service import AuthService

    fake_user = UserResponse(
        id=uuid.uuid4(),
        email="new@example.com",
        role="user",
        is_active=True,
        is_verified=False,
    )

    with (
        _patch.object(AuthService, "register", new=AsyncMock(return_value=fake_user)),
        _patch(
            "src.tasks.email.send_email_with_retry", new=AsyncMock()
        ) as mock_send,
    ):
        from httpx import AsyncClient

        client: AsyncClient = async_client  # type: ignore[assignment]
        response = await client.post(
            "/api/v1/auth/register",
            json={"email": "new@example.com", "password": "StrongPass123!"},
        )

    assert response.status_code == 201
    assert response.json()["email"] == "new@example.com"
    # BackgroundTasks runs inline in HTTPX test transport
    mock_send.assert_awaited_once()
    delivered_msg: EmailMessage = mock_send.await_args[0][0]
    assert delivered_msg.to == "new@example.com"
