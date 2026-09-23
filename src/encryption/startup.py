"""Pull key-ring resolution forward to start-up.

Every other module here resolves the cipher lazily, which is what lets a model
be imported and a migration be run on a deployment that has no keys yet. The
cost of that is a misconfiguration surfacing as a 500 on whichever request
first touched an encrypted column — possibly minutes after the rollout, in one
replica, after the deployment was marked successful.

So the lifespan calls this. A bad `ENCRYPTION_KEYS` then fails the process
before it binds a socket, which is the same treatment `HSTSPolicy` gives an
impossible preload and `get_message_publisher().start()` gives an unreachable
broker.
"""

from __future__ import annotations

import structlog

from src.config import Settings
from src.encryption.cipher import build_field_cipher
from src.encryption.keys import KeyRing

logger = structlog.get_logger(__name__)

_SELF_TEST_CONTEXT = "src.encryption.startup.self-test"


def validate_encryption_configuration(config: Settings) -> KeyRing:
    """Build the key ring and prove it can seal and open a value.

    The round trip is worth the microsecond it costs, but be clear about what
    it proves: that this process holds *a* working AES-256 key, not that it
    holds the *right* one. Nothing local can tell the difference — that is
    what an encryption key is — and the first read of an existing row is where
    a wrong key shows up, as a `DecryptionError`.

    Returns the ring so a caller that wants to log or assert on its contents
    does not have to build it again; it is cached either way.
    """
    cipher = build_field_cipher(config)
    probe = b"encryption self-test"
    sealed = cipher.encrypt(probe, context=_SELF_TEST_CONTEXT)
    if cipher.decrypt(sealed, context=_SELF_TEST_CONTEXT) != probe:
        raise AssertionError(  # pragma: no cover - AES-GCM does not do this
            "AES-256-GCM round trip did not return the plaintext."
        )

    ring = cipher.key_ring
    if ring.active.is_well_known:
        # Not an error outside production — `.env.example` ships this key on
        # purpose — but it should never be a surprise, so it is said once per
        # start-up rather than left to be discovered.
        logger.warning(
            "encryption.key.published",
            key_id=ring.active_key_id,
            environment=config.ENVIRONMENT,
            detail=(
                "The active field-encryption key is the one published in this "
                "repository. Anything written with it is readable by anyone "
                "with the source."
            ),
        )
    return ring
