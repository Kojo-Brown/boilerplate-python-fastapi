# Signed webhooks

A webhook endpoint is a URL on the public internet that makes things happen —
marks an invoice paid, provisions an account, ships an order — on the word of
whoever posts to it. Verifying that word is the entire security model, and the
two halves of doing it live in different directions:

- **Outbound**, signing what this application sends:
  `src/notifications/webhook.py`
- **Inbound**, verifying what it receives: `src/webhooks/`
- **The format both use**, written down once: `src/webhooks/signature.py`

## The wire format

```http
POST /api/v1/webhooks/orders HTTP/1.1
Content-Type: application/json
X-Webhook-Signature: t=1700000000,v1=8f2a...c41d

{"event":"order.paid","id":"evt_1"}
```

`t` is unix seconds. `v1` is `HMAC-SHA256(secret, b"<t>." + body)`, hex. The
header may carry several `v1` values — that is how a sender crosses a rotation.

Two properties of the signed material carry the whole scheme:

**The timestamp is inside it.** A timestamp sent merely *alongside* the
signature is a value the sender never committed to, so anyone who captured one
delivery could resend it forever with that header rewritten to now. With `t` in
the signed bytes, changing it invalidates the digest.

**It is the raw body.** A receiver that parses JSON and re-serialises it before
verifying computes its digest over different bytes whenever key order,
whitespace or float formatting differ — and the failure looks exactly like a
wrong secret. `VerifiedDelivery.body` hands the handler the bytes that were
signed so there is no second read to disagree with the first.

## Receiving a webhook

```python
import json

from fastapi import APIRouter

from src.webhooks import VerifiedWebhookDep

router = APIRouter()


@router.post("/webhooks/orders")
async def receive_order_event(delivery: VerifiedWebhookDep) -> dict[str, str]:
    event = json.loads(delivery.body)
    return {"status": "accepted", "id": event["id"]}
```

The handler cannot see an unverified byte: the dependency resolves first, and a
delivery that fails any check raises before the body runs.

**Do not add a pydantic model parameter.** FastAPI parses and validates a
declared body *before* dependencies run, so the 422 for a malformed payload
would be answered ahead of the 401 for a forged one — letting an unauthenticated
caller map out the schema. Take `VerifiedWebhookDep` and parse `delivery.body`
inside the handler.

## What is checked, in order

| Check | Failure |
| --- | --- |
| Body within `WEBHOOK_MAX_BODY_BYTES` | `413 WEBHOOK_PAYLOAD_TOO_LARGE` |
| Signature header present | `401 WEBHOOK_SIGNATURE_MISSING` |
| Header is readable as `t=…,v1=…` | `400 WEBHOOK_SIGNATURE_MALFORMED` |
| Signed timestamp within tolerance | `401 WEBHOOK_TIMESTAMP_OUTSIDE_WINDOW` |
| A configured secret reproduces a digest | `401 WEBHOOK_SIGNATURE_MISMATCH` |
| Delivery not seen before | `409 WEBHOOK_REPLAYED` |
| Replay guard reachable | `503 WEBHOOK_REPLAY_GUARD_UNAVAILABLE` |

The order is not incidental.

**The replay guard is asked last, after the signature verifies.** The other
arrangement — remember first, then authenticate — lets any unauthenticated
caller write a record per request into a store shared by every replica, which is
a way to fill Redis from the internet. After authentication, only a party
holding the shared secret can cause a write at all.

**The window is checked before the HMAC**: an integer comparison ahead of work
proportional to the body.

**A malformed header is a 400 and a bad digest is a 401.** Nothing was refused
in the first case — the value could not be read. The sender has a bug in how it
builds the header, not a secret that disagrees with ours, and those are fixed by
different people.

## Constant-time comparison

`hmac.compare_digest`, never `==`. String equality returns as soon as two bytes
differ, and how long it took is a measurement of how many leading bytes were
right; repeat that while adjusting one byte at a time and the comparison hands
over the answer it was supposed to be checking. Whether the difference is
measurable over a real network is beside the point — it costs one function call
to not have to argue about it.

Both sides are known to be lower-case hex before they reach the comparison,
which also keeps `compare_digest` from raising `TypeError` on a non-ASCII `str`.
Without that shape check, a header of 64 accented characters would be a 500
rather than a rejected delivery.

## The replay window

A tolerance window bounds how long a captured delivery stays replayable. **It
does not stop the replay.** Inside the window the capture is byte-for-byte a
delivery this server accepts, and it will accept it as many times as it is sent.
Narrowing the window trades that against every sender whose clock is off and
every retry queued behind an outage, and no width makes the problem go away.

What closes it is remembering. `ReplayGuard.claim()` asks and answers in one
atomic operation — a `get`-then-`set` pair lets two copies of one delivery
arriving together both find nothing and both proceed, which is the case the
guard exists for.

### The TTL is twice the tolerance, not equal to it

A delivery stamped `T` is acceptable while the clock reads within `tolerance` of
`T`. At the earliest it can arrive at `T - tolerance`; at the latest it is still
good at `T + tolerance`. A claim made the moment it becomes acceptable must
therefore survive `2 × tolerance`.

Get this wrong and the guard forgets a delivery that can still be used —
**silently**, because nothing distinguishes "never seen" from "seen and
expired". A guard that forgets too early passes every test that claims twice in
a row; the case it fails is a replay arriving near the end of the window, which
is the case an attacker picks. `WebhookVerifier` refuses to be constructed when
`WEBHOOK_REPLAY_TTL_SECONDS` cannot cover the window.

### What the guard remembers

`sha256(b"<t>." + body)` — the delivery, not the signature. A signature names
the delivery *and* the secret that signed it, so with two secrets live during a
rotation one capture has two names, and dropping the retiring secret renames it:
one free replay per rotation.

That the fingerprint is a value an attacker could also compute costs nothing,
because reaching the guard requires a signature that verified. A guard keyed on
something forgeable but written only after authentication cannot be poisoned;
one keyed on an unforgeable value but written *before* authentication can be, by
anyone, to suppress a delivery that has not arrived yet.

### The bound it does not give you

A claim is recorded before the handler runs, and the two are not in one
transaction. So a delivery whose handler crashes after the claim is not
redelivered *if the sender resends the identical bytes* — its fingerprint is
already spent. Senders that re-sign each attempt (Stripe and most others) get a
new fingerprint and come back fine. For work that must not be lost either way,
pair this with the outbox (`docs/outbox.md`): claim, enqueue, commit, then do
the work from the queue.

One more false positive worth knowing about: two genuinely distinct events with
byte-identical bodies in the same second are one delivery as far as the
fingerprint is concerned. Every sender worth integrating with puts a unique
event id in the payload, which makes the bodies differ.

## Secrets and rotation

`WEBHOOK_SIGNING_SECRETS` is `id:secret` entries separated by commas. A set
rather than one value, because rotating a shared secret needs both halves live
at once. The ids are not secret — they name a secret, and the `key_id` on the
`webhook.verified` log line is the only way to watch a rotation finish.

```
WEBHOOK_SIGNING_SECRETS=2026-06:oldsecret…,2026-09:newsecret…
```

To rotate:

1. Generate one: `uv run python scripts/generate_webhook_secret.py`
2. Add it alongside the current entry and deploy. Both now verify.
3. Have the sender switch to it — or sign under both, which the scheme allows.
4. Watch `key_id` on `webhook.verified`. When nothing has arrived under the old
   id for longer than the sender's retry horizon, drop that entry.

Empty is refused rather than treated as "accept anything": an endpoint that is
unconfigured and one that is unauthenticated must not look alike. A secret
shorter than 32 characters is refused too — an attacker holding one captured
delivery can brute-force offline, with nothing rate-limiting the guessing.

`.env.example` ships a development secret that is published in this repository
and says so in ASCII, so `cp .env.example .env` gives a working receiver.
`build_signing_secrets` refuses it outright when `ENVIRONMENT=production` —
every entry, not just a notional active one, because any secret in the set will
authenticate a delivery.

No secret, digest, or key material reaches a log line or an error body.
`SigningSecret.__repr__` is hand-written for that reason: the generated one
would put material into every traceback holding the object.

## Configuration

| Setting | Default | Notes |
| --- | --- | --- |
| `WEBHOOK_SIGNING_SECRETS` | *(empty)* | `id:secret,…`. Empty means no verifier can be built. |
| `WEBHOOK_SIGNATURE_HEADER` | `X-Webhook-Signature` | A third party names it whatever it likes; the name is not signed. |
| `WEBHOOK_TOLERANCE_SECONDS` | `300` | Either side of the signed timestamp. |
| `WEBHOOK_REPLAY_BACKEND` | `redis` | `redis`, `memory`, or `none`. |
| `WEBHOOK_REPLAY_REDIS_URL` | *(shares `REDIS_URL`)* | Separated by key namespace. |
| `WEBHOOK_REPLAY_TTL_SECONDS` | `900` | Must be ≥ `2 × WEBHOOK_TOLERANCE_SECONDS`. |
| `WEBHOOK_REPLAY_FAIL_OPEN` | `false` | Accept without a replay check when the guard is down. |
| `WEBHOOK_MAX_BODY_BYTES` | `1048576` | Bounds what an unauthenticated caller can make this process buffer and HMAC. |

`WEBHOOK_REPLAY_BACKEND=none` keeps signature verification and gives up knowing
whether a delivery has been seen before. It is a supported answer — a deployment
without Redis should get signature checking rather than nothing — and it is not
a quiet one: the factory logs `webhook.replay_protection_disabled` at warning
level with the width of the window it leaves open. `memory` is per-process, so a
replay delivered to a sibling worker is not recognised.

`WEBHOOK_REPLAY_FAIL_OPEN` is off for the reason `IDEMPOTENCY_FAIL_OPEN` is off:
the sender's own retry brings the delivery back, and a replay window nobody
opened on purpose is worse than a delivery that arrives late. When it is on, the
interval is logged at warning level rather than debug, because it needs to be
visible after the fact rather than only while somebody is watching.

The future side of the window is bounded as well, and not as a formality. A
sender whose clock runs an hour fast produces deliveries that stay acceptable
for an hour beyond the window anybody reasoned about — the capture is replayable
until *our* clock catches up with its timestamp.

## Why the verifier never reads a timestamp header

`src/notifications/webhook.py` sends `X-Notification-Timestamp` beside the
signature, and so do plenty of third parties. It is there for a human reading a
request log. Taking the replay window from it is the mistake this scheme exists
to prevent: that header is unsigned, so an attacker who captured a delivery
rewrites it to now, the digest still verifies over the original `t=`, and a
year-old capture is accepted as current.

The two values are equal on every genuine delivery, which is exactly why the bug
survives testing. `WebhookVerifier.verify()` takes no timestamp argument at all
— the fix is structural, not a rule to remember — and
`tests/test_webhook_roundtrip.py::TestTheUnsignedTimestampHeaderIsReallyUnsigned`
demonstrates the attack from both ends.

## Sending a signed webhook

The outbound side is `WebhookNotificationStrategy` (`docs/notifications.md`).
Both directions now go through `src/webhooks/signature.py`, and
`tests/test_webhook_roundtrip.py` asserts that a delivery this application sends
is one it accepts — the only test that fails if the two drift apart.

One asymmetry is deliberate: an empty secret on the *outbound* side means
"unsigned", while on the inbound side it is refused. An unsigned delivery is a
choice a sender makes about a local receiver; an unsigned acceptance is an open
endpoint.

## The two claims a test cannot make

`tests/test_webhook_gates.py` is a pair of source-level fitness functions, the
`test_immutability_gate.py` idiom, because two properties in this document cannot
be observed by watching the code work:

**`hmac.compare_digest(a, b)` and `a == b` return the same answer for every
input.** Swapping one for the other passes the whole suite, reads as a
simplification in review, and removes the only defence against a timing oracle.
The gate parses `digest_matches` and every module in the package and fails on an
`==` applied to secret-derived material. `SigningSecret.is_well_known` uses the
same primitive even though it compares against a published constant, so that the
gate needs no exemption table — one rule for the whole package survives review in
a way "this particular one is fine" does not.

**Nothing may read an unsigned timestamp header.** Since that header agrees with
the signed timestamp on every genuine delivery, a receiver that trusted it would
pass every test written from the sender's side. The gate fails on any reference
to one as a value, asserts `verify()` has no timestamp parameter, and asserts the
dependency reads exactly one header.

Both were checked against the mutations they name: replacing `compare_digest`
with `==`, pointing the dependency at `X-Notification-Timestamp`, and adding a
`timestamp` parameter to `verify()` each failed this file and nothing else.

## Not done, and written down

- **No provider adapters.** Stripe's `Stripe-Signature` is this format with a
  different header name and works by setting `WEBHOOK_SIGNATURE_HEADER`;
  GitHub's `X-Hub-Signature-256` is `sha256=<hex>` over the body with *no*
  timestamp, so it has no replay window to speak of and would need its own
  parser. PayPal signs with a certificate chain fetched from its own CDN and has
  nothing to do with this module. See the note on `PaymentGateway` in
  `src/payments/base.py`.
- **No per-sender route.** The verifier is a dependency, not a mounted
  endpoint: this application has no webhook of its own to receive yet, and one
  invented for the sake of an example would be a route with no handler worth
  running. `tests/test_webhook_dependency.py` wires the documented pattern into
  a real ASGI app and exercises it over HTTP.
- **No signature on this application's own replies.** A receiver's `2xx` is
  unauthenticated, which is the sender's problem to care about and not one the
  scheme addresses in either direction.
- **The body cap cannot bound a chunked request's memory.** By the time the
  length is known the bytes are in this process; Starlette buffers the whole
  body to serve `await request.body()`. The real defence there is the ASGI
  server's own limit or the proxy in front of it. This cap bounds the HMAC and
  keeps an accidental 900 MiB POST out of a handler.
