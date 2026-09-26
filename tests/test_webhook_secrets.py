"""The secret set: parsing, what it refuses, and what it keeps out of logs."""

from __future__ import annotations

import pytest

from src.config import Settings
from src.webhooks.errors import WebhookConfigurationError
from src.webhooks.secrets import (
    MIN_SECRET_LENGTH,
    WELL_KNOWN_DEVELOPMENT_SECRET,
    SigningSecret,
    SigningSecretSet,
    build_signing_secrets,
    parse_signing_secrets,
)

LONG_ENOUGH = "x" * MIN_SECRET_LENGTH
OTHER = "y" * MIN_SECRET_LENGTH


def a_settings(**overrides: object) -> Settings:
    """A `Settings` for this module, built rather than mutated.

    `Settings` is frozen, and every factory in this codebase takes one for
    exactly this reason — a test that needs different configuration constructs
    its own instead of reaching into the global.
    """
    values: dict[str, object] = {
        "DATABASE_URL": "postgresql+asyncpg://test:test@localhost/test",
        "SECRET_KEY": "test-secret-key",
        "WEBHOOK_SIGNING_SECRETS": f"dev:{LONG_ENOUGH}",
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


class TestParsing:
    async def test_one_entry(self) -> None:
        secrets = parse_signing_secrets(f"2026-09:{LONG_ENOUGH}")

        assert [s.key_id for s in secrets.secrets] == ["2026-09"]
        assert secrets.secrets[0].secret == LONG_ENOUGH

    async def test_several_entries_keep_their_order(self) -> None:
        """Order decides which id a delivery is attributed to when two match.

        Only a reporting detail, but a stable one: the attribution must not
        depend on which worker handled the delivery.
        """
        secrets = parse_signing_secrets(f"old:{LONG_ENOUGH},new:{OTHER}")

        assert [s.key_id for s in secrets.secrets] == ["old", "new"]

    async def test_whitespace_and_line_breaks_around_entries(self) -> None:
        """A long ring gets written across several lines in a secret manager."""
        secrets = parse_signing_secrets(f"  old:{LONG_ENOUGH} ,\n new:{OTHER}  ")

        assert [s.key_id for s in secrets.secrets] == ["old", "new"]

    async def test_a_secret_may_contain_a_colon(self) -> None:
        """Only the first `:` splits, so a URL-shaped secret survives."""
        secrets = parse_signing_secrets(f"id:{LONG_ENOUGH}:tail")

        assert secrets.secrets[0].secret == f"{LONG_ENOUGH}:tail"

    async def test_an_empty_setting_is_refused(self) -> None:
        """Empty must not mean "accept anything".

        It is the difference between an endpoint that is unconfigured and one
        that is unauthenticated, and only one of those fails visibly.
        """
        with pytest.raises(WebhookConfigurationError, match="empty"):
            parse_signing_secrets("")

    async def test_a_setting_of_only_separators_is_refused(self) -> None:
        with pytest.raises(WebhookConfigurationError, match="empty"):
            parse_signing_secrets(" , , ")

    async def test_an_entry_with_no_colon_is_refused(self) -> None:
        with pytest.raises(WebhookConfigurationError, match="no ':'"):
            parse_signing_secrets(LONG_ENOUGH)

    async def test_the_error_for_a_bare_entry_does_not_quote_it(self) -> None:
        """A malformed entry is still probably a secret.

        An error message is a place a secret gets logged once and then lives
        wherever logs are aggregated.
        """
        with pytest.raises(WebhookConfigurationError) as caught:
            parse_signing_secrets("hunter2-but-long-enough-to-pass-the-floor")

        assert "hunter2" not in str(caught.value)

    async def test_a_short_secret_is_refused(self) -> None:
        """Nothing rate-limits guessing it: the attacker holds a whole delivery."""
        with pytest.raises(WebhookConfigurationError, match="shorter than"):
            parse_signing_secrets("id:short")

    async def test_an_entry_with_an_empty_id_is_refused(self) -> None:
        with pytest.raises(WebhookConfigurationError, match="must have an id"):
            parse_signing_secrets(f":{LONG_ENOUGH}")

    async def test_duplicate_ids_are_refused(self) -> None:
        """Two rows claiming one name make the `key_id` in a log line a lie."""
        with pytest.raises(WebhookConfigurationError, match="Duplicate"):
            parse_signing_secrets(f"same:{LONG_ENOUGH},same:{OTHER}")

    async def test_an_empty_set_is_refused(self) -> None:
        with pytest.raises(WebhookConfigurationError, match="at least one"):
            SigningSecretSet(secrets=())


class TestKeepingMaterialOutOfLogs:
    async def test_the_repr_redacts_the_secret(self) -> None:
        """The generated `__repr__` would put material in every traceback."""
        secret = SigningSecret(key_id="dev", secret=LONG_ENOUGH)

        assert LONG_ENOUGH not in repr(secret)
        assert "<redacted>" in repr(secret)
        assert "dev" in repr(secret)

    async def test_the_set_repr_redacts_every_member(self) -> None:
        """The dataclass `__repr__` of the set delegates to each member's."""
        rendered = repr(parse_signing_secrets(f"a:{LONG_ENOUGH},b:{OTHER}"))

        assert LONG_ENOUGH not in rendered
        assert OTHER not in rendered


class TestTheWellKnownDevelopmentSecret:
    async def test_it_is_recognised(self) -> None:
        secret = SigningSecret(key_id="dev", secret=WELL_KNOWN_DEVELOPMENT_SECRET)
        assert secret.is_well_known is True

    async def test_a_real_secret_is_not(self) -> None:
        assert SigningSecret(key_id="dev", secret=LONG_ENOUGH).is_well_known is False

    async def test_it_is_long_enough_to_be_accepted_at_all(self) -> None:
        """Otherwise `cp .env.example .env` would not produce a working app."""
        assert len(WELL_KNOWN_DEVELOPMENT_SECRET) >= MIN_SECRET_LENGTH

    async def test_it_says_what_it_is(self) -> None:
        """Somebody finding it in a config file should not have to ask."""
        assert "insecure" in WELL_KNOWN_DEVELOPMENT_SECRET
        assert "notreal" in WELL_KNOWN_DEVELOPMENT_SECRET


class TestBuildingFromSettings:
    async def test_it_reads_the_setting(self) -> None:
        secrets = build_signing_secrets(
            a_settings(WEBHOOK_SIGNING_SECRETS=f"prod:{LONG_ENOUGH}")
        )

        assert [s.key_id for s in secrets.secrets] == ["prod"]

    async def test_production_refuses_the_published_secret(self) -> None:
        with pytest.raises(WebhookConfigurationError, match="published"):
            build_signing_secrets(
                a_settings(
                    ENVIRONMENT="production",
                    WEBHOOK_SIGNING_SECRETS=f"dev:{WELL_KNOWN_DEVELOPMENT_SECRET}",
                )
            )

    async def test_production_refuses_it_even_alongside_a_real_one(self) -> None:
        """Every entry, not a notional active one.

        Any secret in the set will authenticate a delivery, so one readable off
        GitHub means the endpoint is open to whoever finds it.
        """
        with pytest.raises(WebhookConfigurationError, match="published"):
            build_signing_secrets(
                a_settings(
                    ENVIRONMENT="production",
                    WEBHOOK_SIGNING_SECRETS=(
                        f"real:{LONG_ENOUGH},dev:{WELL_KNOWN_DEVELOPMENT_SECRET}"
                    ),
                )
            )

    async def test_the_error_names_the_generator_script(self) -> None:
        """Somebody hitting this at deploy time needs the next step, not a rule."""
        with pytest.raises(WebhookConfigurationError) as caught:
            build_signing_secrets(
                a_settings(
                    ENVIRONMENT="production",
                    WEBHOOK_SIGNING_SECRETS=f"dev:{WELL_KNOWN_DEVELOPMENT_SECRET}",
                )
            )

        assert "scripts/generate_webhook_secret.py" in str(caught.value)

    async def test_development_accepts_the_published_secret(self) -> None:
        secrets = build_signing_secrets(
            a_settings(WEBHOOK_SIGNING_SECRETS=f"dev:{WELL_KNOWN_DEVELOPMENT_SECRET}")
        )

        assert secrets.secrets[0].is_well_known is True

    async def test_it_is_cached_per_settings_object(self) -> None:
        config = a_settings()

        assert build_signing_secrets(config) is build_signing_secrets(config)
