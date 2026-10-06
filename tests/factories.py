"""Factory-boy factories for User and RefreshToken models.

Used by pytest-factoryboy to auto-register pytest fixtures and by tests
that need repeatable, randomised model instances without a live database.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import factory
from faker import Faker

from src.models.refresh_token import RefreshToken
from src.models.tenant import DEFAULT_TENANT_ID
from src.models.user import User

_fake = Faker()


class UserFactory(factory.Factory):
    class Meta:
        model = User

    id = factory.LazyFunction(uuid.uuid4)
    # Set explicitly rather than left to the column default, which is a
    # *server* default: a factory-built user never reaches an INSERT in most
    # of these tests, so `tenant_id` would be `None` on an object the code
    # under test reads it from — `AuthService.login` puts it in the token.
    # The bootstrap tenant, which is the one `tests/conftest.py` scopes the
    # whole suite to, so a factory user and a persisted one agree.
    tenant_id = DEFAULT_TENANT_ID
    email = factory.Sequence(lambda n: f"user{n}@example.com")
    hashed_password = factory.LazyFunction(lambda: _fake.password(length=60))
    is_active = True
    is_verified = True
    role = "user"
    created_at = factory.LazyFunction(lambda: datetime.now(UTC))
    updated_at = factory.LazyFunction(lambda: datetime.now(UTC))
    oauth_provider = None
    oauth_sub = None
    notification_channel = "email"
    notification_webhook_url = None
    # A row that came from the database always has one: SQLAlchemy populates
    # the version counter on INSERT, and `apply_column_defaults` cannot infer
    # it, because the ORM sets it rather than any column default. Leaving it
    # `None` would give every factory-built user an ETag of "<id>.None".
    version = 1

    class Params:
        inactive = factory.Trait(is_active=False)
        unverified = factory.Trait(is_verified=False)
        oauth = factory.Trait(
            hashed_password=None,
            oauth_provider="google",
            oauth_sub=factory.LazyFunction(lambda: _fake.uuid4()),
        )
        webhook_notifications = factory.Trait(
            notification_channel="webhook",
            notification_webhook_url=factory.Sequence(
                lambda n: f"https://hooks.example.com/u/{n}"
            ),
        )
        opted_out = factory.Trait(notification_channel="none")


class AdminUserFactory(UserFactory):
    email = factory.Sequence(lambda n: f"admin{n}@example.com")
    role = "admin"


class RefreshTokenFactory(factory.Factory):
    class Meta:
        model = RefreshToken

    id = factory.LazyFunction(uuid.uuid4)
    tenant_id = DEFAULT_TENANT_ID
    token = factory.LazyFunction(lambda: _fake.sha256())
    user_id = factory.LazyFunction(uuid.uuid4)
    # A fresh family per built token, which is what a login produces. A test
    # that needs two tokens in the *same* family — the shape rotation leaves
    # behind, and the only shape reuse detection has anything to say about —
    # passes `family_id=` explicitly, so that relationship is never accidental.
    family_id = factory.LazyFunction(uuid.uuid4)
    expires_at = factory.LazyFunction(lambda: datetime.now(UTC) + timedelta(days=7))
    revoked = False
    created_at = factory.LazyFunction(lambda: datetime.now(UTC))

    class Params:
        expired = factory.Trait(
            expires_at=factory.LazyFunction(
                lambda: datetime.now(UTC) - timedelta(hours=1)
            )
        )
        revoked_token = factory.Trait(revoked=True)
