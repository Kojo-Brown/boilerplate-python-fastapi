"""The set of shared secrets one endpoint will accept a delivery under.

A set rather than a single value, for the same reason `ENCRYPTION_KEYS` is a
ring: rotating a shared secret needs both halves live at once. The sender starts
signing under the new secret while the receiver still accepts the old, or the
sender signs under both (the scheme allows several `v1` elements) — either way
there is a window in which two secrets are correct, and a receiver that holds
one cannot open it.

## One set is one sender

Key ids here name *versions of one counterparty's secret*, not different
counterparties. Two senders get two `WebhookVerifier`s with two namespaces, and
`delivery_fingerprint` explains what goes wrong if they share one: two senders
who post identical bytes in the same second would collide in the replay guard,
and the second would be turned away as a replay of the first.
"""

from __future__ import annotations

import hmac
from dataclasses import dataclass
from functools import cache
from typing import Final

from src.config import Settings
from src.webhooks.errors import WebhookConfigurationError

# Below this, a shared secret is worth brute-forcing offline against a single
# captured delivery: the attacker holds the body, the timestamp and the digest,
# so nothing rate-limits the guessing. 32 characters is not a strong opinion
# about entropy, it is a floor that catches a hand-typed password.
MIN_SECRET_LENGTH: Final[int] = 32

# Published here on purpose, and refused outright when ENVIRONMENT is
# "production". The alternative — no default at all — is a boilerplate whose
# webhook receiver cannot be exercised until somebody invents a secret, and the
# usual response to that is a short one. It says what it is in ASCII.
WELL_KNOWN_DEVELOPMENT_SECRET: Final[str] = (
    "insecure-development-webhook-secret-notreal"
)


@dataclass(frozen=True, slots=True)
class SigningSecret:
    """One shared secret and the id that names it.

    The id is not secret: it exists so a log line can say which version of the
    secret a delivery arrived under, which is the only way to tell whether a
    rotation has finished. The secret itself never reaches a log or an error —
    hence the hand-written `__repr__`, since the generated one would put the
    material into every traceback that happened to hold this object.
    """

    key_id: str
    secret: str

    def __post_init__(self) -> None:
        if not self.key_id:
            raise WebhookConfigurationError("A signing secret must have an id.")
        if len(self.secret) < MIN_SECRET_LENGTH:
            raise WebhookConfigurationError(
                f"Signing secret {self.key_id!r} is shorter than "
                f"{MIN_SECRET_LENGTH} characters."
            )

    def __repr__(self) -> str:
        return f"SigningSecret(key_id={self.key_id!r}, secret=<redacted>)"

    @property
    def is_well_known(self) -> bool:
        """Whether this is the secret published in this repository.

        `compare_digest` is not needed on its merits — the other operand is a
        constant printed in this file, and anybody curious whether a deployment
        uses it can simply sign a delivery with it, which is cheaper than any
        measurement. It is here so that the gate in
        `tests/test_webhook_gates.py` needs no exemption table: one primitive
        for every comparison of secret-derived material is a rule that survives
        review, where "this particular one is fine" has to be re-established by
        whoever reads it next.

        Encoded first because `compare_digest` raises `TypeError` on a `str`
        outside ASCII, and nothing stops an operator choosing a secret with an
        accent in it.
        """
        return hmac.compare_digest(
            self.secret.encode(), WELL_KNOWN_DEVELOPMENT_SECRET.encode()
        )


@dataclass(frozen=True, slots=True)
class SigningSecretSet:
    """Every secret a verifier will try, in configured order.

    Order is only a performance detail — a delivery matches whichever secret
    signed it — but it is a stable one, so the key id a delivery is attributed
    to does not depend on which worker handled it.
    """

    secrets: tuple[SigningSecret, ...]

    def __post_init__(self) -> None:
        if not self.secrets:
            # Empty cannot be allowed to mean "accept anything". It is the
            # difference between an endpoint that is unconfigured and one that
            # is unauthenticated, and only one of those fails visibly.
            raise WebhookConfigurationError(
                "A verifier needs at least one signing secret."
            )
        ids = [secret.key_id for secret in self.secrets]
        duplicates = sorted({key_id for key_id in ids if ids.count(key_id) > 1})
        if duplicates:
            raise WebhookConfigurationError(
                f"Duplicate signing secret ids: {', '.join(duplicates)}."
            )


def parse_signing_secrets(raw: str) -> SigningSecretSet:
    """Build a set from `id:secret` entries separated by commas.

    Whitespace around an entry is ignored so that a long list can be written
    across several lines in a secret manager. Only the first `:` splits, so a
    secret may contain one; a secret may **not** contain a comma, which is why
    `scripts/generate_webhook_secret.py` emits URL-safe base64.
    """
    entries = [entry.strip() for entry in raw.split(",") if entry.strip()]
    if not entries:
        raise WebhookConfigurationError(
            "WEBHOOK_SIGNING_SECRETS is empty. Expected 'id:secret' entries "
            "separated by commas; see docs/webhook-signatures.md."
        )

    parsed: list[SigningSecret] = []
    for entry in entries:
        key_id, separator, secret = entry.partition(":")
        if not separator:
            # The entry is probably a bare secret, so nothing of it is quoted
            # back: an error message is a place a secret gets logged once and
            # then lives wherever logs are aggregated.
            raise WebhookConfigurationError(
                "A WEBHOOK_SIGNING_SECRETS entry has no ':'. Expected 'id:secret'."
            )
        parsed.append(SigningSecret(key_id=key_id.strip(), secret=secret.strip()))

    return SigningSecretSet(secrets=tuple(parsed))


@cache
def build_signing_secrets(settings: Settings) -> SigningSecretSet:
    """The secret set for a configuration, built once per `Settings`.

    Cached on the settings object, which is frozen and therefore hashable, so a
    test builds a different set by constructing its own `Settings(...)` — the
    seam every factory in this codebase uses.

    Unlike the encryption key ring, this is *not* validated during start-up.
    Nothing in the default application receives webhooks, so a deployment that
    has not configured a secret is an ordinary deployment rather than a broken
    one; the check happens when something first asks for a verifier. Mount a
    receiving route and that becomes the first request to it, which is loud
    enough — a 500 on a route that has never worked, rather than a silent
    acceptance.
    """
    secrets = parse_signing_secrets(settings.WEBHOOK_SIGNING_SECRETS)
    if settings.ENVIRONMENT == "production":
        published = sorted(
            secret.key_id for secret in secrets.secrets if secret.is_well_known
        )
        if published:
            # Every entry, not just a notional active one: any secret in this
            # set will authenticate a delivery, so one that is readable off
            # GitHub means the endpoint is open to anybody who finds it.
            raise WebhookConfigurationError(
                "WEBHOOK_SIGNING_SECRETS contains the secret published in this "
                f"repository ({', '.join(published)}). Generate one with "
                "`python scripts/generate_webhook_secret.py` and load it from "
                "your secret store."
            )
    return secrets
