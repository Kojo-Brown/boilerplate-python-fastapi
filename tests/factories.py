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
from src.models.user import User

_fake = Faker()


class UserFactory(factory.Factory):
    class Meta:
        model = User

    id = factory.LazyFunction(uuid.uuid4)
    email = factory.Sequence(lambda n: f"user{n}@example.com")
    hashed_password = factory.LazyFunction(lambda: _fake.password(length=60))
    is_active = True
    is_verified = True
    role = "user"
    created_at = factory.LazyFunction(lambda: datetime.now(UTC))
    updated_at = factory.LazyFunction(lambda: datetime.now(UTC))
    oauth_provider = None
    oauth_sub = None

    class Params:
        inactive = factory.Trait(is_active=False)
        unverified = factory.Trait(is_verified=False)
        oauth = factory.Trait(
            hashed_password=None,
            oauth_provider="google",
            oauth_sub=factory.LazyFunction(lambda: _fake.uuid4()),
        )


class AdminUserFactory(UserFactory):
    email = factory.Sequence(lambda n: f"admin{n}@example.com")
    role = "admin"


class RefreshTokenFactory(factory.Factory):
    class Meta:
        model = RefreshToken

    id = factory.LazyFunction(uuid.uuid4)
    token = factory.LazyFunction(lambda: _fake.sha256())
    user_id = factory.LazyFunction(uuid.uuid4)
    expires_at = factory.LazyFunction(
        lambda: datetime.now(UTC) + timedelta(days=7)
    )
    revoked = False
    created_at = factory.LazyFunction(lambda: datetime.now(UTC))

    class Params:
        expired = factory.Trait(
            expires_at=factory.LazyFunction(
                lambda: datetime.now(UTC) - timedelta(hours=1)
            )
        )
        revoked_token = factory.Trait(revoked=True)
