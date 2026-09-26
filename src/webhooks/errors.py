"""What a rejected delivery raises.

Every HTTP-shaped error here is an `AppException` subclass, so a route that
depends on `verify_webhook_request` produces the same JSON envelope as the rest
of the API without the handler catching anything.

## On how much a rejection says

These carry distinct `error_code`s, and the timestamp rejection reports the skew
it measured. That is deliberate: the counterparty is a partner holding a shared
secret, and the two failures they will actually hit — a clock that has drifted
and a secret that was rotated on one side only — are indistinguishable from
"your signature is wrong" unless the error says which happened. Neither code
tells an attacker anything they could not establish by other means: the skew
reveals this server's clock, which every HTTP `Date` header already does, and
learning that a captured delivery was *replayed* rather than *forged* requires
having captured a valid delivery in the first place.

What no message here contains is secret material, a computed digest, or which
key id matched. A digest in an error body is an oracle: it turns one request
into the answer the attacker was trying to guess.
"""

from __future__ import annotations

from src.exceptions import (
    AppException,
    BadRequestError,
    ConflictError,
    UnauthorizedError,
)


class WebhookVerificationError(UnauthorizedError):
    """Base for a delivery this server will not accept as authentic.

    401 rather than 403: the delivery failed to prove who sent it, which is
    exactly what the two statuses distinguish.
    """

    error_code = "WEBHOOK_VERIFICATION_FAILED"


class SignatureHeaderMissingError(WebhookVerificationError):
    """No signature header on the request at all."""

    error_code = "WEBHOOK_SIGNATURE_MISSING"


class SignatureHeaderMalformedError(BadRequestError):
    """The header is present but is not in the scheme's format.

    400 and not 401, because nothing was authenticated or refused — the value
    could not be read. A sender seeing this has a bug in how it builds the
    header, not a secret that disagrees with ours.
    """

    error_code = "WEBHOOK_SIGNATURE_MALFORMED"


class SignatureMismatchError(WebhookVerificationError):
    """No configured secret produces any of the digests offered."""

    error_code = "WEBHOOK_SIGNATURE_MISMATCH"


class SignatureTimestampOutsideWindowError(WebhookVerificationError):
    """The signed timestamp is too old, or too far in the future."""

    error_code = "WEBHOOK_TIMESTAMP_OUTSIDE_WINDOW"


class WebhookReplayedError(ConflictError):
    """This exact delivery has been accepted before.

    409 rather than a second 401: the signature was good. Saying so lets a
    sender tell "you rejected me" from "you already have this", which are
    different things to page somebody about.
    """

    error_code = "WEBHOOK_REPLAYED"


class WebhookPayloadTooLargeError(AppException):
    """The body is larger than this endpoint will read.

    Checked before the HMAC, because the point of a cap is to bound the work an
    unauthenticated caller can ask for.
    """

    status_code = 413
    error_code = "WEBHOOK_PAYLOAD_TOO_LARGE"


class ReplayGuardUnavailableError(AppException):
    """The replay guard could not be reached.

    503 by default: accepting the delivery anyway means accepting replays for
    the width of the tolerance window, and the sender's own retry will bring it
    back. `WEBHOOK_REPLAY_FAIL_OPEN` is for deployments that would rather take
    that risk than drop deliveries, and it is off for the same reason
    `IDEMPOTENCY_FAIL_OPEN` is.
    """

    status_code = 503
    error_code = "WEBHOOK_REPLAY_GUARD_UNAVAILABLE"
    headers = {"Retry-After": "1"}

    def __init__(
        self,
        message: str = "Webhook replay guard unavailable",
        details: object = None,
    ) -> None:
        super().__init__(message, details)


class WebhookConfigurationError(ValueError):
    """The verifier or the guard was built with settings that cannot be safe.

    A `ValueError` and not an `AppException` because there is no response to
    shape: every case it covers is reachable only while building the verifier,
    which happens before the object exists that could answer a request. A
    deployment that trips one of these fails at start-up or on the first
    delivery, which is louder — and therefore kinder — than verifying nothing.
    """
