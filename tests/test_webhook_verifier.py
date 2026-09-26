"""The four checks, their order, and the ways a verifier is usually wrong.

Time is injected throughout. Every interesting property of a tolerance window is
otherwise only observable by waiting, and a test that sleeps out a real window is
a test nobody runs.
"""

from __future__ import annotations

import asyncio

import pytest

from src.webhooks import verifier as verifier_module
from src.webhooks.errors import (
    ReplayGuardUnavailableError,
    SignatureHeaderMalformedError,
    SignatureHeaderMissingError,
    SignatureMismatchError,
    SignatureTimestampOutsideWindowError,
    WebhookConfigurationError,
    WebhookReplayedError,
)
from src.webhooks.replay import InMemoryReplayGuard
from src.webhooks.secrets import parse_signing_secrets
from src.webhooks.signature import compute_digest, delivery_fingerprint, sign
from src.webhooks.verifier import VerifiedDelivery, WebhookVerifier
from tests.conftest import LogCapturer

SECRET = "the-current-secret-thirty-two-plus!!"
RETIRING = "the-retiring-secret-thirty-two-plus!"
BODY = b'{"event":"order.paid","id":"evt_1"}'
NOW = 1_700_000_000.0
TOLERANCE = 300


def a_verifier(
    *,
    secrets: str = f"current:{SECRET}",
    tolerance_seconds: int = TOLERANCE,
    guard: InMemoryReplayGuard | None = None,
    now: float = NOW,
    fail_open: bool = False,
) -> WebhookVerifier:
    return WebhookVerifier(
        secrets=parse_signing_secrets(secrets),
        tolerance_seconds=tolerance_seconds,
        replay_guard=guard,
        fail_open=fail_open,
        clock=lambda: now,
    )


def a_guard(ttl_seconds: float = TOLERANCE * 2) -> InMemoryReplayGuard:
    return InMemoryReplayGuard(ttl_seconds=ttl_seconds)


class TestAGenuineDelivery:
    async def test_it_verifies(self) -> None:
        verified = await a_verifier().verify(
            signature=sign(SECRET, int(NOW), BODY), body=BODY
        )

        assert isinstance(verified, VerifiedDelivery)

    async def test_it_reports_which_secret_matched(self) -> None:
        """The only way to see a rotation finish: deliveries under the new id
        and none under the old."""
        verified = await a_verifier(
            secrets=f"retiring:{RETIRING},current:{SECRET}"
        ).verify(signature=sign(SECRET, int(NOW), BODY), body=BODY)

        assert verified.key_id == "current"

    async def test_it_carries_the_body_that_was_signed(self) -> None:
        """So a handler never re-reads the request.

        The bytes that were signed are the bytes that should be parsed, and a
        second read is an invitation for the two to differ.
        """
        verified = await a_verifier().verify(
            signature=sign(SECRET, int(NOW), BODY), body=BODY
        )

        assert verified.body == BODY

    async def test_it_carries_the_signed_timestamp(self) -> None:
        verified = await a_verifier().verify(
            signature=sign(SECRET, int(NOW), BODY), body=BODY
        )

        assert verified.timestamp == int(NOW)

    async def test_an_empty_body_is_a_delivery_like_any_other(self) -> None:
        """A signed empty body is authentic; only an unsigned one is not."""
        verified = await a_verifier().verify(
            signature=sign(SECRET, int(NOW), b""), body=b""
        )

        assert verified.body == b""

    async def test_it_logs_the_key_id_and_not_the_digest(
        self, capture_module_logs: LogCapturer
    ) -> None:
        """A digest in a log line is an oracle for whoever reads the logs."""
        logs = capture_module_logs(verifier_module)
        signature = sign(SECRET, int(NOW), BODY)

        await a_verifier().verify(signature=signature, body=BODY)

        entry = next(log for log in logs if log["event"] == "webhook.verified")
        assert entry["key_id"] == "current"
        assert compute_digest(SECRET, int(NOW), BODY) not in str(entry)


class TestTheSignatureHeader:
    async def test_a_missing_header_is_refused(self) -> None:
        with pytest.raises(SignatureHeaderMissingError):
            await a_verifier().verify(signature=None, body=BODY)

    async def test_an_empty_header_is_refused(self) -> None:
        with pytest.raises(SignatureHeaderMissingError):
            await a_verifier().verify(signature="", body=BODY)

    async def test_the_error_names_the_header_to_send(self) -> None:
        """A sender that omitted it needs to know what to add."""
        with pytest.raises(SignatureHeaderMissingError) as caught:
            await a_verifier().verify(signature=None, body=BODY)

        assert "X-Webhook-Signature" in str(caught.value)

    async def test_a_malformed_header_is_a_400_not_a_401(self) -> None:
        """Nothing was refused — the value could not be read.

        A sender seeing this has a bug in how it builds the header, not a secret
        that disagrees with ours, and the two are fixed by different people.
        """
        with pytest.raises(SignatureHeaderMalformedError) as caught:
            await a_verifier().verify(signature="nonsense", body=BODY)

        assert caught.value.status_code == 400


class TestTheTimestampWindow:
    async def test_a_delivery_inside_the_window_is_accepted(self) -> None:
        verified = await a_verifier().verify(
            signature=sign(SECRET, int(NOW) - TOLERANCE + 1, BODY), body=BODY
        )

        assert verified.timestamp == int(NOW) - TOLERANCE + 1

    async def test_the_boundary_is_inclusive(self) -> None:
        verified = await a_verifier().verify(
            signature=sign(SECRET, int(NOW) - TOLERANCE, BODY), body=BODY
        )

        assert verified.timestamp == int(NOW) - TOLERANCE

    async def test_a_delivery_older_than_the_window_is_refused(self) -> None:
        with pytest.raises(SignatureTimestampOutsideWindowError):
            await a_verifier().verify(
                signature=sign(SECRET, int(NOW) - TOLERANCE - 1, BODY), body=BODY
            )

    async def test_a_delivery_from_the_future_is_refused(self) -> None:
        """Bounded on the future side too, and not as a formality.

        A sender whose clock runs an hour fast produces deliveries that stay
        acceptable for an hour beyond the window anybody reasoned about — the
        capture is replayable until *our* clock catches up with its timestamp.
        Refusing it turns a misconfigured sender into something somebody fixes.
        """
        with pytest.raises(SignatureTimestampOutsideWindowError):
            await a_verifier().verify(
                signature=sign(SECRET, int(NOW) + TOLERANCE + 1, BODY), body=BODY
            )

    async def test_the_future_boundary_is_inclusive_too(self) -> None:
        """Ordinary clock skew between two healthy servers, not an attack."""
        verified = await a_verifier().verify(
            signature=sign(SECRET, int(NOW) + TOLERANCE, BODY), body=BODY
        )

        assert verified.timestamp == int(NOW) + TOLERANCE

    async def test_the_error_reports_the_skew(self) -> None:
        """What a partner needs to find a drifting clock.

        It reveals this server's time, which every HTTP `Date` header already
        does.
        """
        with pytest.raises(SignatureTimestampOutsideWindowError) as caught:
            await a_verifier().verify(
                signature=sign(SECRET, int(NOW) - 900, BODY), body=BODY
            )

        assert caught.value.details == {
            "skew_seconds": 900,
            "tolerance_seconds": TOLERANCE,
        }

    async def test_the_window_is_checked_before_the_hmac(self) -> None:
        """An integer comparison ahead of work proportional to the body.

        Observable through the error: a delivery that is both too old *and*
        signed with the wrong secret reports the timestamp, because the cheap
        check ran first.
        """
        with pytest.raises(SignatureTimestampOutsideWindowError):
            await a_verifier().verify(
                signature=sign("a-completely-different-secret-value!", 1, BODY),
                body=BODY,
            )


class TestTheUnsignedTimestampHeader:
    """The mistake the scheme is built to make impossible.

    `src/notifications/webhook.py` sends `X-Notification-Timestamp` next to the
    signature, third parties send their own, and reading it is the bug: the
    timestamp *inside* the signed material is the one the sender committed to,
    and the one in a separate header is a value anybody who captured the
    delivery can rewrite. A receiver that takes its window from the unsigned
    header and its digest from the signed one accepts a year-old capture as
    current, and passes every test that does not try it.
    """

    async def test_the_window_comes_from_the_signed_timestamp(self) -> None:
        """`verify` takes no timestamp argument at all.

        Stated as a test because the fix is structural: there is no parameter
        through which an unsigned timestamp could be supplied, so no caller can
        pass one by mistake. A year-old capture with a perfectly valid digest is
        refused on age.
        """
        a_year_ago = int(NOW) - 365 * 24 * 3600
        captured = sign(SECRET, a_year_ago, BODY)

        with pytest.raises(SignatureTimestampOutsideWindowError):
            await a_verifier().verify(signature=captured, body=BODY)

    async def test_rewriting_the_timestamp_in_the_header_invalidates_it(self) -> None:
        """The other half: the attacker cannot move `t` either.

        Forging a current timestamp onto a captured digest gives a header that
        parses and then fails on the digest, because `t` is inside the material.
        """
        a_year_ago = int(NOW) - 365 * 24 * 3600
        digest = compute_digest(SECRET, a_year_ago, BODY)
        forged = f"t={int(NOW)},v1={digest}"

        with pytest.raises(SignatureMismatchError):
            await a_verifier().verify(signature=forged, body=BODY)


class TestTheDigest:
    async def test_a_wrong_secret_is_refused(self) -> None:
        with pytest.raises(SignatureMismatchError):
            await a_verifier().verify(
                signature=sign("not-the-configured-secret-at-all!!!", int(NOW), BODY),
                body=BODY,
            )

    async def test_a_modified_body_is_refused(self) -> None:
        """The delivery an attacker actually wants to send."""
        signature = sign(SECRET, int(NOW), BODY)

        with pytest.raises(SignatureMismatchError):
            await a_verifier().verify(
                signature=signature, body=b'{"event":"order.paid","id":"evt_2"}'
            )

    async def test_a_body_that_differs_only_in_whitespace_is_refused(self) -> None:
        """Why a receiver must not parse and re-serialise before verifying."""
        signature = sign(SECRET, int(NOW), b'{"a":1}')

        with pytest.raises(SignatureMismatchError):
            await a_verifier().verify(signature=signature, body=b'{"a": 1}')

    async def test_the_error_reveals_no_digest(self) -> None:
        """A digest in an error body turns one request into the answer."""
        with pytest.raises(SignatureMismatchError) as caught:
            await a_verifier().verify(
                signature=sign("not-the-configured-secret-at-all!!!", int(NOW), BODY),
                body=BODY,
            )

        expected = compute_digest(SECRET, int(NOW), BODY)
        assert expected not in str(caught.value)
        assert caught.value.details is None


class TestRotation:
    async def test_either_live_secret_verifies(self) -> None:
        """What a set is for: a rotation has a window with two right answers."""
        verifier = a_verifier(secrets=f"retiring:{RETIRING},current:{SECRET}")

        under_old = await verifier.verify(
            signature=sign(RETIRING, int(NOW), BODY), body=BODY
        )
        under_new = await verifier.verify(
            signature=sign(SECRET, int(NOW), BODY), body=BODY
        )

        assert (under_old.key_id, under_new.key_id) == ("retiring", "current")

    async def test_a_delivery_signed_under_both_verifies(self) -> None:
        """The other way a sender crosses the window: two `v1` elements."""
        timestamp = int(NOW)
        both = (
            f"t={timestamp}"
            f",v1={compute_digest(RETIRING, timestamp, BODY)}"
            f",v1={compute_digest(SECRET, timestamp, BODY)}"
        )

        verified = await a_verifier(secrets=f"current:{SECRET}").verify(
            signature=both, body=BODY
        )

        assert verified.key_id == "current"

    async def test_a_retired_secret_stops_verifying(self) -> None:
        """Rotation finishes by dropping the entry, and that has to take effect."""
        with pytest.raises(SignatureMismatchError):
            await a_verifier(secrets=f"current:{SECRET}").verify(
                signature=sign(RETIRING, int(NOW), BODY), body=BODY
            )

    async def test_the_replay_guard_survives_a_rotation(self) -> None:
        """Why the guard keys on the delivery and not on the signature.

        A capture taken while both secrets were live must stay a replay after
        the retiring one is dropped. Keyed on the signature it would be renamed
        by the rotation, which is one free replay per rotation.
        """
        guard = a_guard()
        during = a_verifier(
            secrets=f"retiring:{RETIRING},current:{SECRET}", guard=guard
        )
        signature = sign(SECRET, int(NOW), BODY)
        await during.verify(signature=signature, body=BODY)

        after = a_verifier(secrets=f"current:{SECRET}", guard=guard)

        with pytest.raises(WebhookReplayedError):
            await after.verify(signature=signature, body=BODY)


class TestReplay:
    async def test_the_same_delivery_twice_is_refused(self) -> None:
        """What the tolerance window does not do on its own.

        Inside the window a capture is byte-for-byte a delivery this server
        accepts, and it would accept it as many times as it was sent.
        """
        verifier = a_verifier(guard=a_guard())
        signature = sign(SECRET, int(NOW), BODY)
        await verifier.verify(signature=signature, body=BODY)

        with pytest.raises(WebhookReplayedError):
            await verifier.verify(signature=signature, body=BODY)

    async def test_a_replay_is_a_409_not_a_401(self) -> None:
        """The signature was good.

        A sender can tell "you rejected me" from "you already have this", which
        are different things to page somebody about.
        """
        verifier = a_verifier(guard=a_guard())
        signature = sign(SECRET, int(NOW), BODY)
        await verifier.verify(signature=signature, body=BODY)

        with pytest.raises(WebhookReplayedError) as caught:
            await verifier.verify(signature=signature, body=BODY)

        assert caught.value.status_code == 409

    async def test_a_re_signed_retry_is_not_a_replay(self) -> None:
        """A sender retrying after a 500 signs afresh, and must get through."""
        verifier = a_verifier(guard=a_guard())
        await verifier.verify(signature=sign(SECRET, int(NOW), BODY), body=BODY)

        verified = await verifier.verify(
            signature=sign(SECRET, int(NOW) + 1, BODY), body=BODY
        )

        assert verified.timestamp == int(NOW) + 1

    async def test_a_different_delivery_is_not_a_replay(self) -> None:
        verifier = a_verifier(guard=a_guard())
        await verifier.verify(signature=sign(SECRET, int(NOW), BODY), body=BODY)

        other = b'{"event":"order.paid","id":"evt_2"}'
        verified = await verifier.verify(
            signature=sign(SECRET, int(NOW), other), body=other
        )

        assert verified.body == other

    async def test_two_concurrent_copies_yield_one_acceptance(self) -> None:
        """A double delivery arriving together is the case a `get`/`set` misses."""
        verifier = a_verifier(guard=a_guard())
        signature = sign(SECRET, int(NOW), BODY)

        results = await asyncio.gather(
            *(verifier.verify(signature=signature, body=BODY) for _ in range(10)),
            return_exceptions=True,
        )

        accepted = [r for r in results if isinstance(r, VerifiedDelivery)]
        replays = [r for r in results if isinstance(r, WebhookReplayedError)]
        assert (len(accepted), len(replays)) == (1, 9)

    async def test_the_guard_is_asked_after_the_signature_verifies(self) -> None:
        """The ordering that keeps the store out of reach of the internet.

        Remember-then-authenticate lets any unauthenticated caller write a
        record per request into a store shared by every replica. Observable
        here: a forged delivery leaves the guard empty, so the *genuine*
        delivery of the same bytes is still accepted afterwards.
        """
        guard = a_guard()
        verifier = a_verifier(guard=guard)
        timestamp = int(NOW)

        with pytest.raises(SignatureMismatchError):
            await verifier.verify(
                signature=sign("a-forged-secret-of-adequate-length!!", timestamp, BODY),
                body=BODY,
            )

        verified = await verifier.verify(
            signature=sign(SECRET, timestamp, BODY), body=BODY
        )
        assert verified.timestamp == timestamp

    async def test_a_delivery_outside_the_window_never_reaches_the_guard(self) -> None:
        """Same property for the cheap check: nothing is remembered."""
        guard = a_guard()
        verifier = a_verifier(guard=guard)

        with pytest.raises(SignatureTimestampOutsideWindowError):
            await verifier.verify(
                signature=sign(SECRET, int(NOW) - 10_000, BODY), body=BODY
            )

        assert await guard.claim(f"inbound:{delivery_fingerprint(int(NOW), BODY)}")

    async def test_the_fingerprint_is_reported(self) -> None:
        """So a handler can log or store what it accepted under the same name."""
        verified = await a_verifier(guard=a_guard()).verify(
            signature=sign(SECRET, int(NOW), BODY), body=BODY
        )

        assert verified.fingerprint == delivery_fingerprint(int(NOW), BODY)

    async def test_the_namespace_separates_verifiers_sharing_a_guard(self) -> None:
        """One verifier is one counterparty.

        Two senders posting identical bytes in the same second must not be read
        as a replay of one another.
        """
        guard = a_guard()
        first = WebhookVerifier(
            secrets=parse_signing_secrets(f"current:{SECRET}"),
            replay_guard=guard,
            namespace="sender-a",
            clock=lambda: NOW,
        )
        second = WebhookVerifier(
            secrets=parse_signing_secrets(f"current:{SECRET}"),
            replay_guard=guard,
            namespace="sender-b",
            clock=lambda: NOW,
        )
        signature = sign(SECRET, int(NOW), BODY)

        await first.verify(signature=signature, body=BODY)
        verified = await second.verify(signature=signature, body=BODY)

        assert verified.key_id == "current"


class TestNoGuard:
    async def test_verification_still_works(self) -> None:
        """Signature checking is worth having without Redis."""
        verified = await a_verifier(guard=None).verify(
            signature=sign(SECRET, int(NOW), BODY), body=BODY
        )

        assert verified.key_id == "current"

    async def test_a_replay_is_accepted_and_that_is_the_documented_cost(self) -> None:
        """Asserted rather than left as prose.

        `WEBHOOK_REPLAY_BACKEND=none` keeps the signature check and gives up
        knowing whether a delivery has been seen. This is the behaviour that
        setting buys, and it should fail this test if it ever changes silently.
        """
        verifier = a_verifier(guard=None)
        signature = sign(SECRET, int(NOW), BODY)
        await verifier.verify(signature=signature, body=BODY)

        verified = await verifier.verify(signature=signature, body=BODY)

        assert verified.key_id == "current"

    async def test_the_guard_name_says_none(self) -> None:
        assert a_verifier(guard=None).guard_name == "none"


class TestAnUnavailableGuard:
    class FailingGuard:
        """A guard that cannot reach its store. `ttl_seconds` satisfies the check."""

        name = "failing"
        ttl_seconds = float(TOLERANCE * 2)

        async def claim(self, fingerprint: str) -> bool:
            raise ReplayGuardUnavailableError("Could not reach the guard.")

        async def close(self) -> None:
            return None

    def _verifier(self, *, fail_open: bool) -> WebhookVerifier:
        return WebhookVerifier(
            secrets=parse_signing_secrets(f"current:{SECRET}"),
            tolerance_seconds=TOLERANCE,
            replay_guard=self.FailingGuard(),
            fail_open=fail_open,
            clock=lambda: NOW,
        )

    async def test_it_is_a_503_by_default(self) -> None:
        """Accepting anyway means accepting replays for the width of the window,
        and the sender's own retry will bring the delivery back."""
        with pytest.raises(ReplayGuardUnavailableError) as caught:
            await self._verifier(fail_open=False).verify(
                signature=sign(SECRET, int(NOW), BODY), body=BODY
            )

        assert caught.value.status_code == 503

    async def test_fail_open_accepts_the_delivery(self) -> None:
        verified = await self._verifier(fail_open=True).verify(
            signature=sign(SECRET, int(NOW), BODY), body=BODY
        )

        assert verified.key_id == "current"

    async def test_fail_open_says_so_at_warning_level(
        self, capture_module_logs: LogCapturer
    ) -> None:
        """This is the interval in which replays are accepted.

        It has to be visible after the fact rather than only while somebody is
        watching, which is why it is not a debug line.
        """
        logs = capture_module_logs(verifier_module)

        await self._verifier(fail_open=True).verify(
            signature=sign(SECRET, int(NOW), BODY), body=BODY
        )

        entry = next(
            log
            for log in logs
            if log["event"] == "webhook.replay_guard_unavailable_accepting"
        )
        assert entry["log_level"] == "warning"


class TestConstruction:
    async def test_a_guard_that_forgets_before_the_window_closes_is_refused(
        self,
    ) -> None:
        """The factor of two, enforced where it is still cheap to catch.

        A delivery stamped T is acceptable from T-tolerance to T+tolerance, so a
        claim made the moment it becomes acceptable must survive the whole span.
        A guard that forgets sooner passes every test that claims twice in a row;
        the case it fails is a replay arriving near the end of the window, which
        is the case an attacker picks.
        """
        with pytest.raises(WebhookConfigurationError, match="cannot cover"):
            a_verifier(tolerance_seconds=300, guard=a_guard(ttl_seconds=300.0))

    async def test_exactly_twice_the_tolerance_is_enough(self) -> None:
        verifier = a_verifier(tolerance_seconds=300, guard=a_guard(ttl_seconds=600.0))

        assert verifier.tolerance_seconds == 300

    async def test_the_error_names_both_settings(self) -> None:
        """Either can be moved, and which one depends on the deployment."""
        with pytest.raises(WebhookConfigurationError) as caught:
            a_verifier(tolerance_seconds=300, guard=a_guard(ttl_seconds=59.0))

        message = str(caught.value)
        assert "WEBHOOK_REPLAY_TTL_SECONDS" in message
        assert "WEBHOOK_TOLERANCE_SECONDS" in message

    async def test_no_guard_skips_the_ttl_check(self) -> None:
        """There is no TTL to compare against, and that is the documented mode."""
        assert a_verifier(guard=None).guard_name == "none"

    async def test_a_zero_tolerance_is_refused(self) -> None:
        """It would refuse every delivery whose second had already ticked over."""
        with pytest.raises(WebhookConfigurationError, match="at least 1"):
            a_verifier(tolerance_seconds=0)

    async def test_a_negative_tolerance_is_refused(self) -> None:
        with pytest.raises(WebhookConfigurationError, match="at least 1"):
            a_verifier(tolerance_seconds=-1)

    async def test_the_signature_header_is_configurable(self) -> None:
        """A third party names it whatever it likes; the name is not signed."""
        verifier = WebhookVerifier(
            secrets=parse_signing_secrets(f"current:{SECRET}"),
            signature_header="Stripe-Signature",
            clock=lambda: NOW,
        )

        assert verifier.signature_header == "Stripe-Signature"

    async def test_the_default_clock_is_wall_time(self) -> None:
        """The injected clock is a test seam, not the production path.

        Built with no clock and handed a delivery stamped now, it verifies —
        which it could not do if the default were anything but the real time.
        """
        import time

        verifier = WebhookVerifier(secrets=parse_signing_secrets(f"current:{SECRET}"))
        timestamp = int(time.time())

        verified = await verifier.verify(
            signature=sign(SECRET, timestamp, BODY), body=BODY
        )

        assert verified.timestamp == timestamp
