# Notifications

Application code says *what* to notify about. The recipient's stored preference
says *how*. No call site names a channel.

```python
from src.notifications import Notification, notify, recipient_from_user

await notify(
    recipient_from_user(user),
    Notification(subject="Your export is ready", body="Available for 24 hours."),
)
```

That is the whole API for the common case. `notify` reads `user.notification_channel`,
resolves the strategy, and sends. Adding a channel later does not touch this code.

## The contract

Every strategy implements `NotificationStrategy`:

| Member | Behaviour |
|---|---|
| `name` | `"email"`, `"webhook"` or `"none"` — matches the stored preference. |
| `supports(recipient)` | `True` if this strategy could reach the recipient. Never raises. |
| `send(recipient, notification)` | Delivers, or raises. Returns `NotificationResult`. |

Two rules hold on every channel:

- **`delivered=False` is success, not failure.** It is how a strategy reports
  that it correctly decided *not* to send. Only the opt-out strategy does this.
  Real failures raise.
- **A missing address raises `RecipientNotReachableError` (422).** It never
  comes back as `delivered=False`, because that would make "this user has no
  email address" indistinguishable from "this user opted out".

Failures are translated at the boundary, so a caller never catches
`httpx.ConnectTimeout` or an SMTP library's exception:

| Exception | Status | When |
|---|---|---|
| `RecipientNotReachableError` | 422 | Recipient has no address for their channel. |
| `NotificationDeliveryError` | 502 | Channel was usable, delivery failed, retries exhausted. |
| `UnknownNotificationChannelError` | 500 | Stored preference has no registered strategy. |

## The value objects

`Notification` and `Recipient` are frozen dataclasses. `Recipient` is
deliberately **not** the `User` model — a strategy that took the ORM row would
drag a database session into every delivery path, and a webhook body assembled
from a wider object is one refactor away from shipping a password hash to a
third-party URL. `recipient_from_user` is the only code that knows both types.

`html_body` and `metadata` are optional enrichments, not requirements. A
strategy that cannot render HTML still has to *accept* a notification carrying
it; otherwise the caller would need to know the channel, which is the coupling
the pattern removes.

## The channels

### `email`

Renders the notification as an `EmailMessage` and hands it to
`src.tasks.email.send_email_with_retry`. Retry, back-off and per-attempt
timeout live there, not here — two retry loops for one channel is how they
drift apart. `category` and `metadata` become `X-Notification-*` headers.

`build_message` is public so a caller can queue the same rendering through
Celery instead of sending inline.

### `webhook`

POSTs a signed JSON document to a URL the user registered.

The body is serialised with sorted keys and no whitespace, because **the bytes
signed are the bytes sent** — a receiver that re-serialises before verifying
would compute a different digest.

```
X-Notification-Signature: t=1754400000,v1=<hex>
X-Notification-Timestamp: 1754400000
X-Notification-Category: exports
```

The digest is `HMAC-SHA256(secret, f"{timestamp}." + body)`. The timestamp is
*inside* the signed material, not merely alongside it, so a captured delivery
cannot be replayed later by rewriting the header. To verify:

```python
import hashlib, hmac

expected = hmac.new(
    SECRET.encode(), f"{timestamp}.".encode() + raw_body, hashlib.sha256
).hexdigest()
assert hmac.compare_digest(expected, received_v1)
assert abs(now - timestamp) < tolerance
```

With `NOTIFICATION_WEBHOOK_SECRET` empty, no signature header is sent at all —
better an absent header than a digest keyed on the empty string, which looks
authentic to a receiver doing a naive comparison.

Retry policy: 5xx and 429 are retried with doubling back-off; every other 4xx
fails immediately, because a rejected request stays rejected. Redirects are not
followed — a 302 would let a vetted URL hand the delivery to an unvetted one.

### `none`

The Null Object. A user who turned notifications off is a *supported*
preference, not a missing one, so it gets a strategy like any other channel
instead of a `None` every call site would have to remember to check. It logs
and returns `delivered=False`; suppression is recorded rather than silent,
because "the user never got it" and "the code never tried" look identical in an
incident otherwise.

## SSRF: what is and is not guarded

A webhook URL is attacker-influenced data, and this process can reach things
the user cannot. `validate_webhook_url` rejects, before any socket is opened:

- non-HTTPS schemes
- credentials embedded in the URL
- loopback hostnames
- private, link-local, multicast and otherwise reserved **address literals** —
  including `169.254.169.254`, the cloud metadata endpoint

**Known gap, stated rather than papered over:** a hostname that *resolves* to a
private address passes. Re-resolving here would not close it either, because
the socket does its own lookup afterwards (DNS rebinding). Blocking that is
egress policy — an allow-list proxy or a network rule — not a string check.
`tests/test_notification_base.py` asserts this gap explicitly so it cannot be
mistaken for a guarantee.

`NOTIFICATION_WEBHOOK_ALLOW_PRIVATE_HOSTS` exists so development can point at a
local listener. It also permits `http://`. Keep it `false` in production.

## Configuration

| Setting | Default | Purpose |
|---|---|---|
| `NOTIFICATION_DEFAULT_CHANNEL` | `email` | Fallback for users with no stored preference. |
| `NOTIFICATION_WEBHOOK_SECRET` | `""` | HMAC key. Empty means unsigned. |
| `NOTIFICATION_WEBHOOK_TIMEOUT_SECONDS` | `10.0` | Per-request timeout. |
| `NOTIFICATION_WEBHOOK_MAX_ATTEMPTS` | `3` | Total attempts, not retries. |
| `NOTIFICATION_WEBHOOK_BACKOFF_SECONDS` | `0.5` | Base delay; doubles each retry. |
| `NOTIFICATION_WEBHOOK_ALLOW_PRIVATE_HOSTS` | `false` | Development only. |

The default channel is only a fallback. A user who has chosen a channel is
routed there regardless of this value.

## Per-user preference

Two columns on `users` (migration `0003`):

- `notification_channel` — `NOT NULL`, defaults to `email`
- `notification_webhook_url` — nullable

`notification_channel` is a plain string rather than a native Postgres enum on
purpose: adding a channel should be a `register` call, not a migration plus a
deploy-order problem. An unrecognised value raises
`UnknownNotificationChannelError` rather than being silently defaulted —
delivering over a channel the user did not pick is worse than failing loudly.

An **empty** preference is different from an unrecognised one, and falls back to
the configured default: rows predating the column are an ordinary case.

No route exposes preference updates yet — the columns and the routing exist;
the settings endpoint does not.

## Adding a channel

Write a class satisfying the protocol, then register it. Nothing else changes:

```python
from src.notifications import NotificationStrategyRegistry

class SmsNotificationStrategy:
    @property
    def name(self) -> str:
        return "sms"

    def supports(self, recipient) -> bool:
        return bool(recipient.phone)

    async def send(self, recipient, notification):
        ...

NotificationStrategyRegistry.register("sms", lambda config: SmsNotificationStrategy())
```

`for_recipient` starts routing `"sms"` immediately. Note that a new address
field means a new `Recipient` field and a new column — the value object is the
one place the channels share.

In tests, call `NotificationStrategyRegistry.reset()` and
`get_strategy.cache_clear()` in teardown; `tests/test_notification_registry.py`
has an autouse fixture that does both.

## Testing

`tests/test_notification_contract.py` runs one suite against all three
strategies, parametrised — a factory is only worth having if what it returns is
interchangeable, so the shared behaviour is asserted once rather than written
three times with three sets of assumptions.

The webhook strategy participates through `httpx.MockTransport`, which
exercises the real request-building and response-handling code without a
socket. Its `sleep` and `clock` are injectable, so the retry schedule and the
signature are asserted exactly and no test waits out a back-off.
