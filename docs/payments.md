# Payments

Application code depends on the `PaymentGateway` protocol, never on Stripe or
PayPal. Configuration picks the provider; `PaymentGatewayRegistry` builds it.

```python
from src.payments import ChargeRequest, Money, PaymentGateway, get_payment_gateway

async def checkout(gateway: PaymentGateway, order_id: str) -> None:
    payment = await gateway.charge(
        ChargeRequest(
            amount=Money(amount_minor=2500, currency="USD"),  # $25.00
            payment_method_token=token_from_the_browser,
            reference=order_id,
            description="One widget",
        )
    )
    if payment.is_settled:
        ...
```

In a route, take it as a dependency so a test can swap it:

```python
from fastapi import Depends
from src.payments import PaymentGateway, get_payment_gateway

@router.post("/checkout")
async def checkout(gateway: PaymentGateway = Depends(get_payment_gateway)) -> None:
    ...
```

## The contract

| Method | Behaviour |
|---|---|
| `charge(request)` | Takes payment. Idempotent on `request.reference`. Returns `Payment`. |
| `refund(provider_payment_id, amount=None)` | Refunds fully, or partially when `amount` is given. Returns `Refund`. |
| `get_payment(provider_payment_id)` | Current state of a payment. Returns `Payment`. |
| `name` | `"stripe"` or `"paypal"` — for logs and error messages. |

### Why this is an adapter and not a strategy

`NotificationStrategy` has three implementations of one idea this codebase
invented. `PaymentGateway` has two implementations of an idea two other
companies invented, differently, and neither will change to suit us. Each
adapter's whole job is translation, and the table below is the work:

| | Stripe | PayPal |
|---|---|---|
| Auth | static secret key, every request | OAuth2 token from `/v1/oauth2/token`, cached until expiry |
| Body | `application/x-www-form-urlencoded`, `metadata[key]` | JSON |
| Amount | integer minor units (`2500`) | decimal string in the major unit (`"25.00"`) |
| Idempotency | `Idempotency-Key` header | `PayPal-Request-Id` header |
| Charge endpoint | `POST /v1/payment_intents` (`confirm=true`) | `POST /v2/checkout/orders` (`intent=CAPTURE`) |
| Refundable handle | the PaymentIntent id | the **capture** id, dug out of `purchase_units[].payments.captures[]` |
| Decline | HTTP 402, `error.type == "card_error"` | HTTP 422, `details[].issue == "INSTRUMENT_DECLINED"` |

Everything in that table stops at the adapter's edge. Above it there is one
`Payment` type, one `PaymentDeclinedError`, and one 503 for "try again".

## Money

`Money` is integer minor units plus an ISO 4217 code — `Money(2500, "USD")` is
$25.00, `Money(1000, "JPY")` is ¥1000. Never a float, and never a bare
`Decimal`: floats acquire rounding error that reconciles against nothing, and a
`Decimal` on its own still leaves "is `10` ten dollars or ten cents?" to
whoever reads the field name.

The exponent table in `CURRENCY_EXPONENTS` is the only place in the codebase
that knows how many decimals a currency settles in, and both adapters depend on
it in opposite directions. A currency that is not in it raises
`UnsupportedCurrencyError` rather than defaulting to two decimals, because the
default is wrong in both directions at once: the same ¥1000 becomes ¥10 at
Stripe and ¥100,000 at PayPal. Adding a currency is a line in that table plus a
check that both providers actually settle it.

`Money.from_decimal_string` refuses to round. A provider reporting `"10.005"`
on a two-decimal currency has said something this code does not understand, and
silently absorbing it loses a fraction of a cent per transaction in whichever
direction the rounding mode happens to fall.

## Statuses

`status` is one of `succeeded` / `pending` / `failed`; `provider_status` keeps
the provider's own string for support tickets. Branch on the first, search the
dashboard with the second.

`pending` is load-bearing. It covers a Stripe `requires_action` (3-D Secure not
yet completed), a PayPal order in `PAYER_ACTION_REQUIRED`, and a PayPal capture
in `PENDING` review. None of those is a failure and none of them is money in
the bank, which is why `Payment.is_settled` is `status == "succeeded"` and
nothing else. A status neither adapter recognises also degrades to `pending`:
calling an unknown status `failed` would be a lie in the expensive direction,
since the money may well have moved.

For PayPal specifically, the capture's status wins over the order's. An order
reports `COMPLETED` the moment it is captured, but the capture underneath it
can still be `PENDING` — and that difference is whether the money exists.

## Errors

| Exception | Status | Means |
|---|---|---|
| `PaymentDeclinedError` | 402 | The issuer said no. Show the shopper a message; `decline_code` is the provider's own reason. |
| `PaymentGatewayUnavailableError` | 503 | Transport failure, 429, or 5xx. The same charge may be retried. |
| `PaymentNotFoundError` | 404 | No such payment at this provider. |
| `PaymentConfigurationError` | 500 | Credentials missing. Raised when the gateway is built, not at a customer's checkout. |
| `PaymentError` | 502 | The provider answered something unusable. |

The distinction that matters is the first two. A decline is the one payment
failure a caller can act on, so it must never be indistinguishable from an
outage — and the reverse mistake is worse: telling a shopper their card was
declined because this integration sent a malformed body sends them to their
bank over our bug. Both adapters are tested against exactly that confusion
(`test_invalid_request_error_is_a_payment_error`,
`test_other_422_issues_are_not_declines`).

## Idempotency

`ChargeRequest.reference` is required, not optional, and becomes the provider's
idempotency key. An optional one is an idempotency key nobody passes, and the
first time that matters is a network timeout that charged a customer twice.

That is also what makes `PaymentGatewayUnavailableError` safe to retry: if the
provider did process the original request, the retry returns the original
payment instead of creating a second one. Refunds key on
`refund:{payment_id}:{full|amount}` for the same reason.

## What the protocol deliberately leaves out

**Customer vaulting, subscriptions, and webhook verification.** Each provider
models these differently enough that a shared signature would be a shape only
one of them could honour — Stripe verifies webhooks with an HMAC over the raw
request body, PayPal with a signed certificate chain fetched from its own CDN.
They stay on the concrete adapters, the same way presigned URLs stay on
`S3Storage` rather than joining `StorageBackend`.

**Retries and circuit breaking.** One attempt per call. Both adapters send an
idempotency key precisely so a retry layer can be added above them safely, and
that layer is Phase 9's `circuit breaker + retry with jitter on outbound httpx
calls` — building half of it here would mean two retry policies to reconcile
later.

**A route.** There is no `/api/v1/payments` endpoint yet. Wiring one up is the
next SPEC item (dependency inversion via `Depends` + protocol-typed providers),
and `get_payment_gateway` is already written to be used directly as a FastAPI
dependency.

## Configuration

```
PAYMENT_GATEWAY=stripe          # stripe | paypal
PAYMENT_TIMEOUT_SECONDS=15.0
STRIPE_SECRET_KEY=
STRIPE_API_BASE_URL=https://api.stripe.com
PAYPAL_CLIENT_ID=
PAYPAL_CLIENT_SECRET=
PAYPAL_API_BASE_URL=https://api-m.sandbox.paypal.com
```

Only the selected provider's credentials need to be present; the registry
raises `PaymentConfigurationError` naming the missing variable when they are
not. `PAYPAL_API_BASE_URL` defaults to the **sandbox** — production is
`https://api-m.paypal.com` and has to be set deliberately, because the wrong
value here charges real cards.

## Adding a provider

A third provider is a new module plus one registration call. Nothing that
already charges a card changes:

```python
from src.payments import PaymentGatewayRegistry

PaymentGatewayRegistry.register("adyen", lambda config: AdyenGateway(...))
```

Then add it to `PAYMENT_GATEWAY`'s `Literal` in `src/config.py`, and run the
contract suite against it — `tests/test_payment_contract.py` is parametrised
over the adapters, so a new entry there is what proves the new provider is
actually interchangeable rather than merely type-compatible.

## Testing

Both adapters take an injectable `httpx.AsyncClient`, so tests drive them with
`httpx.MockTransport` and assert the exact request the provider would receive —
`tests/test_payment_stripe.py` and `tests/test_payment_paypal.py`. `PayPalGateway`
also takes an injectable clock, so token expiry and re-auth are asserted
without a test that waits nine hours.

`tests/test_payment_contract.py` runs one suite against both adapters, each
behind a fake that speaks its own provider's wire format. An adapter that
produced the right `Payment` from the wrong request would pass the contract
suite and fail against the real API, which is why the per-provider files assert
the encoding directly.
