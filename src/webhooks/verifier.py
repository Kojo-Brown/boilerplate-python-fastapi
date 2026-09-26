"""Deciding whether an inbound delivery is authentic.

Four questions, in this order, and the order is the design:

1. Is there a signature header, and can it be read? (`SignatureHeaderMissing`,
   `SignatureHeaderMalformed`)
2. Is the signed timestamp inside the tolerance window?
   (`SignatureTimestampOutsideWindow`)
3. Does a configured secret reproduce one of the offered digests, compared in
   constant time? (`SignatureMismatch`)
4. Has this delivery been accepted before? (`WebhookReplayed`)

**The replay guard is asked last, after the signature verifies.** The other
arrangement — remember first, then authenticate — lets any unauthenticated
caller write a record per request into a store shared by every replica, which is
a way to fill Redis from the internet. Ordering it after authentication means
only a party holding the shared secret can cause a write at all.

**The window is checked before the HMAC** for a smaller version of the same
reason: it is an integer comparison against work that is proportional to the
body, and there is no need to do the expensive thing for a delivery that is
already too old to accept.

## What is not consulted

Any timestamp header sent alongside the signature. `src/notifications/webhook.py`
emits one, third parties emit one, and reading it is the mistake this scheme is
built to make impossible: the timestamp that is *inside* the signed material is
the one the sender committed to, and the one in a separate header is a value
anybody who captured the delivery can rewrite. A receiver that takes the window
from the unsigned header and the digest from the signed one accepts a year-old
capture as current, and passes every test that does not try it. See
`tests/test_webhook_verifier.py::TestTheUnsignedTimestampHeader`.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

import structlog

from src.webhooks.errors import (
    ReplayGuardUnavailableError,
    SignatureHeaderMissingError,
    SignatureMismatchError,
    SignatureTimestampOutsideWindowError,
    WebhookConfigurationError,
    WebhookReplayedError,
)
from src.webhooks.replay import ReplayGuard
from src.webhooks.secrets import SigningSecretSet
from src.webhooks.signature import (
    DEFAULT_SIGNATURE_HEADER,
    compute_digest,
    delivery_fingerprint,
    digest_matches,
    parse_signature_header,
)

logger = structlog.get_logger(__name__)

Clock = Callable[[], float]

#: A claim has to outlive the window its delivery is acceptable in. See
#: `src/webhooks/replay.py` — the factor is two, because the window extends
#: `tolerance` either side of the signed timestamp.
REPLAY_TTL_MULTIPLE: Final[int] = 2


@dataclass(frozen=True, slots=True)
class VerifiedDelivery:
    """An authenticated delivery, with what verification learned about it.

    `body` is carried so a handler never re-reads the request: the bytes that
    were signed are the bytes the handler should parse, and a second read is an
    opportunity for them to differ.

    `key_id` is the point of the whole set. It is the only way to see a rotation
    finish — deliveries arriving under the new id and none under the old — and it
    belongs in a log line, never in a response.
    """

    key_id: str
    timestamp: int
    fingerprint: str
    body: bytes


class WebhookVerifier:
    """Verifies inbound deliveries against one sender's secrets.

    One instance is one counterparty: its secrets are that sender's rotation
    ring and its `namespace` separates its replay records from another sender's.
    Two senders sharing one verifier would collide in the guard — see
    `delivery_fingerprint`.

    `clock` is injectable because every interesting property of the tolerance
    window is otherwise only observable by waiting, and a test that sleeps for
    the width of a real window is a test nobody runs.
    """

    def __init__(
        self,
        *,
        secrets: SigningSecretSet,
        tolerance_seconds: int = 300,
        replay_guard: ReplayGuard | None = None,
        signature_header: str = DEFAULT_SIGNATURE_HEADER,
        namespace: str = "inbound",
        fail_open: bool = False,
        clock: Clock | None = None,
    ) -> None:
        if tolerance_seconds < 1:
            raise WebhookConfigurationError("tolerance_seconds must be at least 1.")

        required_ttl = tolerance_seconds * REPLAY_TTL_MULTIPLE
        if replay_guard is not None and replay_guard.ttl_seconds < required_ttl:
            # Checked here because this is the last moment it is cheap. A guard
            # that forgets too early still passes every test that claims twice
            # in a row; the case it fails is a replay arriving near the end of
            # the window, which is the case an attacker picks.
            raise WebhookConfigurationError(
                f"A replay guard holding claims for {replay_guard.ttl_seconds}s "
                f"cannot cover a {tolerance_seconds}s tolerance window: a "
                f"delivery stays acceptable for {required_ttl}s after it is "
                "first seen. Raise WEBHOOK_REPLAY_TTL_SECONDS or lower "
                "WEBHOOK_TOLERANCE_SECONDS."
            )

        self._secrets = secrets
        self._tolerance = tolerance_seconds
        self._guard = replay_guard
        self._namespace = namespace
        self._fail_open = fail_open
        self._clock: Clock = clock if clock is not None else time.time
        self.signature_header = signature_header

    @property
    def tolerance_seconds(self) -> int:
        return self._tolerance

    @property
    def guard_name(self) -> str:
        """Which guard is in use, or `"none"`. For logs and tests."""
        return self._guard.name if self._guard is not None else "none"

    async def verify(self, *, signature: str | None, body: bytes) -> VerifiedDelivery:
        """Authenticate one delivery, or raise.

        `signature` is the raw header value — this object never touches a
        request, so it is equally usable from a queue consumer replaying a
        captured delivery or a test with no ASGI app in sight.
        """
        if not signature:
            raise SignatureHeaderMissingError(
                f"Missing {self.signature_header} header.",
                details={"header": self.signature_header},
            )

        parsed = parse_signature_header(signature)
        self._check_window(parsed.timestamp)

        for secret in self._secrets.secrets:
            expected = compute_digest(secret.secret, parsed.timestamp, body)
            if digest_matches(expected, parsed.digests):
                fingerprint = delivery_fingerprint(parsed.timestamp, body)
                await self._claim(fingerprint)
                logger.info(
                    "webhook.verified",
                    key_id=secret.key_id,
                    guard=self.guard_name,
                    body_bytes=len(body),
                )
                return VerifiedDelivery(
                    key_id=secret.key_id,
                    timestamp=parsed.timestamp,
                    fingerprint=fingerprint,
                    body=body,
                )

        logger.warning(
            "webhook.signature_mismatch",
            offered_digests=len(parsed.digests),
            configured_secrets=len(self._secrets.secrets),
            body_bytes=len(body),
        )
        raise SignatureMismatchError(
            "Webhook signature does not match any configured secret."
        )

    def _check_window(self, timestamp: int) -> None:
        """Reject a signed timestamp too far from now, in either direction.

        Bounded on the future side as well, and not as a formality. A sender
        whose clock runs an hour fast produces deliveries that stay acceptable
        for an hour beyond the window anybody reasoned about — the capture is
        replayable until *our* clock catches up with its timestamp. Refusing it
        turns a misconfigured sender into an error somebody fixes, rather than
        into a quietly widened window.
        """
        skew = self._clock() - timestamp
        if abs(skew) <= self._tolerance:
            return

        logger.warning(
            "webhook.timestamp_outside_window",
            skew_seconds=int(skew),
            tolerance_seconds=self._tolerance,
        )
        raise SignatureTimestampOutsideWindowError(
            "Webhook timestamp is outside the accepted window.",
            details={
                # The skew is what a partner needs to see to find a drifting
                # clock, and it reveals only this server's time, which every
                # HTTP `Date` header already does.
                "skew_seconds": int(skew),
                "tolerance_seconds": self._tolerance,
            },
        )

    async def _claim(self, fingerprint: str) -> None:
        """Record the delivery, or raise if it is already recorded."""
        if self._guard is None:
            return

        try:
            claimed = await self._guard.claim(f"{self._namespace}:{fingerprint}")
        except ReplayGuardUnavailableError:
            if not self._fail_open:
                raise
            # Deliberately a warning and not a debug line: this is the interval
            # in which replays are accepted, and it needs to be visible after
            # the fact rather than only while somebody is watching.
            logger.warning(
                "webhook.replay_guard_unavailable_accepting",
                guard=self.guard_name,
                detail=(
                    "WEBHOOK_REPLAY_FAIL_OPEN is on: the delivery is accepted "
                    "without a replay check."
                ),
            )
            return

        if not claimed:
            logger.warning("webhook.replayed", guard=self.guard_name)
            raise WebhookReplayedError("This webhook delivery was already accepted.")
