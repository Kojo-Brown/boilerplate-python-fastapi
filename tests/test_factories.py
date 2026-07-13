"""Tests demonstrating factory-boy + faker + pytest-factoryboy fixture patterns.

Shows:
- Direct factory builds (no DB required)
- Batch creation
- Trait-based overrides
- Pytest fixture injection via pytest-factoryboy register()
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

from src.models.refresh_token import RefreshToken
from src.models.user import User
from tests.factories import AdminUserFactory, RefreshTokenFactory, UserFactory


# ---------------------------------------------------------------------------
# Direct factory build
# ---------------------------------------------------------------------------


def test_user_factory_builds_valid_user() -> None:
    user = UserFactory.build()

    assert isinstance(user, User)
    assert isinstance(user.id, uuid.UUID)
    assert "@" in user.email
    assert user.is_active is True
    assert user.is_verified is True
    assert user.role == "user"
    assert isinstance(user.created_at, datetime)
    assert user.oauth_provider is None


def test_user_factory_emails_are_unique() -> None:
    users = UserFactory.build_batch(10)
    emails = [u.email for u in users]
    assert len(emails) == len(set(emails)), "Each built user should have a unique email"


def test_user_factory_override() -> None:
    user = UserFactory.build(email="custom@test.com", role="editor")

    assert user.email == "custom@test.com"
    assert user.role == "editor"


# ---------------------------------------------------------------------------
# Admin factory
# ---------------------------------------------------------------------------


def test_admin_factory_sets_role() -> None:
    admin = AdminUserFactory.build()

    assert admin.role == "admin"
    assert "admin" in admin.email


def test_admin_factory_is_subclass_of_user_factory() -> None:
    admin = AdminUserFactory.build()
    assert isinstance(admin, User)


# ---------------------------------------------------------------------------
# Trait tests
# ---------------------------------------------------------------------------


def test_inactive_trait() -> None:
    user = UserFactory.build(inactive=True)
    assert user.is_active is False


def test_unverified_trait() -> None:
    user = UserFactory.build(unverified=True)
    assert user.is_verified is False


def test_oauth_trait() -> None:
    user = UserFactory.build(oauth=True)
    assert user.oauth_provider == "google"
    assert user.oauth_sub is not None
    assert user.hashed_password is None


# ---------------------------------------------------------------------------
# RefreshToken factory
# ---------------------------------------------------------------------------


def test_refresh_token_factory_builds_valid_token() -> None:
    token = RefreshTokenFactory.build()

    assert isinstance(token, RefreshToken)
    assert isinstance(token.id, uuid.UUID)
    assert len(token.token) > 0
    assert token.revoked is False
    assert token.expires_at > datetime.now(UTC)


def test_expired_trait() -> None:
    token = RefreshTokenFactory.build(expired=True)
    assert token.expires_at < datetime.now(UTC)


def test_revoked_token_trait() -> None:
    token = RefreshTokenFactory.build(revoked_token=True)
    assert token.revoked is True


def test_refresh_token_linked_to_user_id() -> None:
    user = UserFactory.build()
    token = RefreshTokenFactory.build(user_id=user.id)
    assert token.user_id == user.id


# ---------------------------------------------------------------------------
# Batch helpers
# ---------------------------------------------------------------------------


def test_build_batch_returns_correct_count() -> None:
    users = UserFactory.build_batch(5)
    assert len(users) == 5
    assert all(isinstance(u, User) for u in users)


def test_build_batch_refresh_tokens() -> None:
    tokens = RefreshTokenFactory.build_batch(3)
    token_values = [t.token for t in tokens]
    assert len(token_values) == len(set(token_values)), "Tokens should be unique"


# ---------------------------------------------------------------------------
# pytest-factoryboy injected fixtures
# Fixtures auto-created by register():
#   UserFactory        → user_factory, user
#   AdminUserFactory   → admin_user_factory, admin_user
#   RefreshTokenFactory→ refresh_token_factory, refresh_token
# ---------------------------------------------------------------------------


def test_user_fixture_is_user_instance(user: User) -> None:
    assert isinstance(user, User)
    assert user.role == "user"
    assert user.is_active is True


def test_admin_user_fixture_has_admin_role(admin_user: User) -> None:
    assert isinstance(admin_user, User)
    assert admin_user.role == "admin"


def test_refresh_token_fixture(refresh_token: RefreshToken) -> None:
    assert isinstance(refresh_token, RefreshToken)
    assert refresh_token.revoked is False


def test_user_factory_fixture_builds_on_demand(user_factory: type[UserFactory]) -> None:
    built = user_factory.build(role="moderator")
    assert built.role == "moderator"


def test_multiple_fixture_users_are_distinct(user: User, admin_user: User) -> None:
    assert user.id != admin_user.id
    assert user.email != admin_user.email
    assert user.role != admin_user.role


@pytest.mark.parametrize("role", ["user", "editor", "viewer"])
def test_factory_parametrized_roles(role: str) -> None:
    u = UserFactory.build(role=role)
    assert u.role == role
