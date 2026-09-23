"""The start-up check, and the warning it emits about a published key."""

from __future__ import annotations

import base64

import pytest
from structlog.testing import capture_logs

from src.config import Settings
from src.encryption.errors import KeyRingConfigurationError
from src.encryption.startup import validate_encryption_configuration

REAL_LOOKING = base64.b64encode(b"0" * 32).decode("ascii")
PUBLISHED = base64.b64encode(b"insecure-development-key-notreal").decode("ascii")


def settings_with(**overrides: object) -> Settings:
    return Settings(
        DATABASE_URL="postgresql+asyncpg://fake:fake@localhost/fake",
        SECRET_KEY="mock-secret-key-not-a-real-one",
        **overrides,  # type: ignore[arg-type]
    )


class TestValidateEncryptionConfiguration:
    def test_returns_the_ring_it_built(self) -> None:
        ring = validate_encryption_configuration(
            settings_with(
                ENCRYPTION_KEYS=f"live:{REAL_LOOKING}",
                ENCRYPTION_ACTIVE_KEY_ID="live",
            )
        )
        assert ring.active_key_id == "live"

    def test_a_broken_configuration_raises_here_rather_than_at_first_write(
        self,
    ) -> None:
        """The reason this function exists at all.

        Without it, `ENCRYPTION_ACTIVE_KEY_ID` pointing at a key that is not in
        the ring is a healthy-looking process that 500s on the first request
        touching an encrypted column.
        """
        with pytest.raises(KeyRingConfigurationError):
            validate_encryption_configuration(
                settings_with(
                    ENCRYPTION_KEYS=f"live:{REAL_LOOKING}",
                    ENCRYPTION_ACTIVE_KEY_ID="typo",
                )
            )

    def test_warns_when_the_active_key_is_the_published_one(self) -> None:
        with capture_logs() as logs:
            validate_encryption_configuration(
                settings_with(
                    ENVIRONMENT="development",
                    ENCRYPTION_KEYS=f"dev:{PUBLISHED}",
                    ENCRYPTION_ACTIVE_KEY_ID="dev",
                )
            )
        events = [
            entry for entry in logs if entry["event"] == "encryption.key.published"
        ]
        assert len(events) == 1
        assert events[0]["log_level"] == "warning"
        assert events[0]["key_id"] == "dev"

    def test_says_nothing_when_the_key_is_not_published(self) -> None:
        with capture_logs() as logs:
            validate_encryption_configuration(
                settings_with(
                    ENCRYPTION_KEYS=f"live:{REAL_LOOKING}",
                    ENCRYPTION_ACTIVE_KEY_ID="live",
                )
            )
        assert [
            entry for entry in logs if entry["event"].startswith("encryption")
        ] == []

    def test_production_with_the_published_key_fails_rather_than_warns(self) -> None:
        with pytest.raises(KeyRingConfigurationError, match="published"):
            validate_encryption_configuration(
                settings_with(
                    ENVIRONMENT="production",
                    ENCRYPTION_KEYS=f"dev:{PUBLISHED}",
                    ENCRYPTION_ACTIVE_KEY_ID="dev",
                )
            )
