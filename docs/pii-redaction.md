# PII redaction in the log pipeline

`src/redaction/` is a structlog processor that strips personal data and
credentials out of every log event this service emits, on the way out and
regardless of who wrote the call site.

The premise is that the call site is the wrong place to rely on. A log line is
written once and kept for years, which is what turns a field that was fine to
write into a subject-access request and a `to=` that was fine into a breach
notification. Reviewing every `logger.info` forever is not a control; a
processor the events cannot go round is.

```python
logger.info("request.started", query="email=ada@example.com&access_token=abc")
# {"query": "email=[redacted]&access_token=[redacted]", "event": "request.started", ...}
```

## Where it sits, and why that is the decision

`configure_logging` builds one processor chain, and there are **two sinks on
it**: the JSON renderer that writes to stdout, and `log_forwarder`, which
mirrors every event into the OpenTelemetry logs pipeline
(`src/observability/logs.py`). Three orders are available and only one is
correct:

| Order | stdout | OTLP |
| --- | --- | --- |
| redactor → forwarder → renderer | clean | clean |
| forwarder → redactor → renderer | clean | **original** |
| forwarder → renderer → redactor | — | — |

The middle row is the one to be afraid of, because it passes every test anyone
naturally writes. You read stdout, the marker is there, the feature is done —
and the unredacted copy is in the log backend, which is the system that keeps
things for years and the one nobody greps while checking their own work.
`tests/test_redaction_processor.py::TestTheChain` asserts both the index and
the exported record, because the index alone can be satisfied by a chain that
does not actually export.

The processor also runs *after* `structlog.contextvars.merge_contextvars`.
`src/middleware/request_id.py` binds `query=`, `path=`, `method=` and `client=`
there for the life of a request, and a raw query string is the most reliably
sensitive value this service logs — `?email=…&token=…` on a link somebody
pasted into a ticket. A processor upstream of the merge would never see it.

## Two passes, because either alone fails

### By field name

Matched on contiguous **word runs**, not on equality and not on substrings.

Equality misses every field that exists: nobody logs `password`, they log
`password_hash` and `user_email` and `stripe_api_key`. Substring over-matches,
and over-matching is not the safe direction — `secret` eats `secretary_id`,
`token` eats `tokenizer`, `key` eats every `idempotency_key` and storage `key`
this codebase already logs. A log where a third of the fields read
`[redacted]` for no reason is a log nobody can read during an incident, and
what gets changed at 3am is the redactor.

So a name is split into words (on underscores, on camelCase humps, on the
digit seam in `addr1`) and a phrase matches when its words appear as a
contiguous run. `user_email` and `emailAddress` both match `email`;
`passengers` does not match `pass`.

Multi-word phrases are what make the list usable here. `key` is **not**
sensitive — this service logs storage keys, idempotency keys and lock names
under it — while `api key`, `secret key`, `private key`, `signing key` and
`encryption key` are.

Four names are deliberately **off** the list:

- `name` — `event_name`, provider and backend names, consumer and stream
  names. The personal senses are listed instead: `full name`, `first name`,
  `surname`, `maiden name`.
- `address` — `ip_address` is logged on purpose, and an email address is
  already caught by `email`. `street address`, `billing address`,
  `postal address` and `home address` are listed instead.
- `id` — the join key of every line this service writes. Redacting it does not
  protect a user, it removes the ability to follow one request.
- `signature` — what you need in front of you when a webhook is being
  rejected, and a keyed digest does not disclose the key.

### By value shape

The key pass covers fields somebody chose to log. It cannot cover the ones
nobody chose, and those are how secrets actually arrive:

```python
logger.warning("payment.rejected", error=str(exc))
```

where `exc` is an upstream 401 quoting the `Authorization` header it refused.
`error` must never become a sensitive key — it is the field the incident is
read through — so the string under it is examined instead.

**Every detector validates before it redacts.** A JWT's first segment is
base64url-decoded and has to be a JSON object naming an `alg`; a card number
has to pass Luhn; an IBAN has to pass the ISO 13616 mod-97 check. That is what
keeps a sixteen-digit order id, an epoch-millis timestamp and a `eyJ`-prefixed
pagination cursor out of the marker.

Detectors with nothing to check — "looks like a phone number", "looks like a
name" — are deliberately **absent**. Their false positives land on exactly the
numbers an incident is being read for, and a marker appearing where nothing
was sensitive teaches people that `[redacted]` means "ignore this", which is
the belief that makes the real ones invisible. A field *named* `phone` is on
the key list, because a name is a statement of intent where a shape is a
guess.

Matches are replaced in place, not swallowed:

```
upstream rejected token <a JWT> for ada@example.com
upstream rejected token [redacted] for [redacted]
```

One detector is not about shape at all: `name=value` pairs are scanned with
the *key* policy, because a query string is a sequence of named fields that
happens to be spelled as a single value. It is also what catches a secret with
no shape at all — `otp=1234` is redacted for its name, since four digits look
like nothing.

## How far in it goes

The realistic leak is not `logger.info("x", password=secret)`, which review
catches. It is `logger.error("x", error=exc, request=payload)`, where the
value is three levels down inside something nobody wrote out by hand. So the
walk descends — and every other rule is chosen so that **a line with nothing
sensitive in it comes out byte-identical to the line this service writes
today**:

- Mappings, sequences and sets are rebuilt with redacted members. A set comes
  back as a list, because redaction collapses distinct members onto the same
  marker and a set would then swallow the duplicates — turning "three tokens
  were logged here" into "one was".
- Numbers, booleans and `None` are untouched. There is no shape to check on a
  number and no key-independent way to tell a user id from an account number,
  so numbers are covered by their field name or not at all. A card detector
  that ran on integers would be redacting the `size`, `offset` and
  `status_code` fields this service is read through.
- Anything else — a `UUID`, a `datetime`, an exception, a connection pool — is
  returned **as the object it was**, unless the string it renders to trips a
  detector, in which case the redacted string replaces it. The renderer calls
  `str()` on it a few processors later regardless, so this costs nothing that
  was not already going to be paid, and it closes the hole that `error=exc`
  would otherwise leave wide open.

Three bounds stop a pathological value becoming an incident of its own:
`MAX_DEPTH` ends the descent at `[max-depth]`; a cycle is detected against the
*ancestors* of the current value rather than everything seen so far, so a list
appearing twice under different keys still renders both times instead of the
second being falsely called `[circular]`; and `str()` on a foreign object is
wrapped, because `__str__` is other people's code running inside a log call.

## Failing closed

If redaction raises, the event is replaced by its structural keys plus
`redaction_failed=<ExceptionType>`, and every caller-supplied field is
dropped.

Passing the original through on error would mean the redactor stops working on
precisely the input strange enough to be worth hiding, and the line would look
completely ordinary while doing it. Dropping is loud: `redaction_failed` is
worth an alert, because it means log lines are being lost *and* that the
redactor has met something it cannot read.

## Configuration

```bash
LOG_REDACTION_EXTRA_KEYS=employeeNumber,badge_id
```

Entries are split into words the same way a real field name is, so
`employeeNumber` and `employee_number` are one entry.

It can only **widen**. There is deliberately no setting that removes a name and
none that turns redaction off. That switch is one hurried incident away from
being set by somebody who will not be the person who discovers, a month later,
that it was never set back — and the value of a control that can be turned off
under pressure is roughly the value of not having it.

## What this does not cover

- **Exception messages and tracebacks.** The forwarder hands the exception
  *object* to the OTLP SDK, which formats it downstream of every processor —
  out of reach from here. (On stdout the question does not arise: the JSON
  renderer emits `"exc_info": true` and no traceback at all.) The rule that
  follows is: do not interpolate user data into an exception message.
- **Numbers.** See above. An account number logged as an `int` under a name
  the policy does not know is invisible to both passes.
- **Free text.** A passport number typed into a `note` field passes straight
  through, because nothing about it is checkable.
- **The uvicorn access log**, which is `logging`-based and does not go through
  structlog at all (`configure_logging` uses `PrintLoggerFactory`). It
  interpolates no user data beyond the request path.

Redaction is the last line, not a licence. It exists because the call site
cannot be trusted forever — not so that PII can be logged deliberately and
cleaned up on the way out.

## Where the code is

| File | What it holds |
| --- | --- |
| `src/redaction/keys.py` | The word-run matcher and the deny list, with the reason for each absence |
| `src/redaction/values.py` | The validated shape detectors |
| `src/redaction/walk.py` | How far in the two passes are applied, and the bounds |
| `src/redaction/processor.py` | The structlog processor and the fail-closed path |
| `src/logging_config.py` | Where it sits in the chain |
