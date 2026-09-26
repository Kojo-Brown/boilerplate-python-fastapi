"""Receiving webhooks: HMAC signatures, constant-time comparison, replay windows.

This package is the **inbound** half. `src/notifications/webhook.py` is the
outbound half — it POSTs signed deliveries to a URL a user registered — and the
two share `src/webhooks/signature.py`, which is the only place the wire format
is written down.

Start at `docs/webhook-signatures.md`. The short version:

* `verifier.py` — the four checks, in the order that makes them safe.
* `signature.py` — the format, and why the timestamp is inside the signed bytes.
* `replay.py` — why a tolerance window is not replay protection on its own, and
  why the guard's TTL is twice the tolerance rather than equal to it.
* `secrets.py` — a set rather than one value, because rotating a shared secret
  needs both halves live at once.
* `dependencies.py` — the route parameter, and why it does not take a model.
"""

from src.webhooks.dependencies import (
    VerifiedWebhookDep,
    read_signed_body,
    verify_webhook_request,
)
from src.webhooks.errors import (
    ReplayGuardUnavailableError,
    SignatureHeaderMalformedError,
    SignatureHeaderMissingError,
    SignatureMismatchError,
    SignatureTimestampOutsideWindowError,
    WebhookConfigurationError,
    WebhookPayloadTooLargeError,
    WebhookReplayedError,
    WebhookVerificationError,
)
from src.webhooks.factory import (
    close_replay_guard,
    create_replay_guard,
    create_verifier,
    get_replay_guard,
    get_webhook_verifier,
)
from src.webhooks.redis_guard import RedisReplayGuard
from src.webhooks.replay import InMemoryReplayGuard, ReplayGuard
from src.webhooks.secrets import (
    MIN_SECRET_LENGTH,
    SigningSecret,
    SigningSecretSet,
    build_signing_secrets,
    parse_signing_secrets,
)
from src.webhooks.signature import (
    DEFAULT_SIGNATURE_HEADER,
    SIGNATURE_VERSION,
    ParsedSignature,
    compute_digest,
    delivery_fingerprint,
    digest_matches,
    parse_signature_header,
    sign,
    signed_material,
)
from src.webhooks.verifier import VerifiedDelivery, WebhookVerifier

__all__ = [
    "DEFAULT_SIGNATURE_HEADER",
    "MIN_SECRET_LENGTH",
    "SIGNATURE_VERSION",
    "InMemoryReplayGuard",
    "ParsedSignature",
    "RedisReplayGuard",
    "ReplayGuard",
    "ReplayGuardUnavailableError",
    "SignatureHeaderMalformedError",
    "SignatureHeaderMissingError",
    "SignatureMismatchError",
    "SignatureTimestampOutsideWindowError",
    "SigningSecret",
    "SigningSecretSet",
    "VerifiedDelivery",
    "VerifiedWebhookDep",
    "WebhookConfigurationError",
    "WebhookPayloadTooLargeError",
    "WebhookReplayedError",
    "WebhookVerificationError",
    "WebhookVerifier",
    "build_signing_secrets",
    "close_replay_guard",
    "compute_digest",
    "create_replay_guard",
    "create_verifier",
    "delivery_fingerprint",
    "digest_matches",
    "get_replay_guard",
    "get_webhook_verifier",
    "parse_signature_header",
    "parse_signing_secrets",
    "read_signed_body",
    "sign",
    "signed_material",
    "verify_webhook_request",
]
