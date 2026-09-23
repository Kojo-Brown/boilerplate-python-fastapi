# Field-level encryption at rest

Postgres can already encrypt at rest. A managed instance writes its data files
to an encrypted volume, and so do its backups. What that protects against is
precisely one thing: somebody walking off with the disk. It is transparent to
everything else — which is to say that a leaked read replica, a `pg_dump` in a
CI artifact, a support engineer with `SELECT`, a SQL injection, and a restored
snapshot in a staging account all see plaintext.

Field-level encryption moves the boundary. The value is sealed by the
application, with a key the database has never held, and the column holds bytes
that are useless without it.

It is not free, and the cost is paid in queries: an encrypted column cannot be
filtered, sorted, joined or uniquely constrained. So this is a per-column
decision made against a threat model, not a setting to turn on for a table.

## What is encrypted here, and why that column

`users.notification_webhook_url`.

A webhook URL supplied by a user is a credential in practice. Slack, Discord,
Teams and most incident tools put the shared secret directly in the path, so
possession of the URL *is* the authorisation to post into somebody's channel.
It is also never filtered or sorted on: `src/notifications/recipients.py` loads
the row and reads the attribute, which is the access pattern this costs nothing
on.

The columns next to it were considered and left alone:

| column | why not |
|---|---|
| `email` | unique, indexed, and looked up by equality on every login |
| `oauth_sub` | indexed; the OAuth callback finds the user by it |
| `hashed_password` | already a one-way hash; encrypting a hash adds key management and no secrecy |
| `role`, `notification_channel` | filtered on, and low-cardinality enough that ciphertext would leak the value through its frequency anyway |

The general rule this reflects: encrypt what is *read* and never *searched*.

## How a value is stored

```
+---------+------------+------------------+-----------+---------------------+
| version | id length  | key id           | nonce     | ciphertext ‖ tag    |
| 1 byte  | 1 byte     | 1..64 bytes      | 12 bytes  | len(plaintext) + 16 |
+---------+------------+------------------+-----------+---------------------+
```

AES-256-GCM. The header is in the clear — a reader has to know which key and
which nonce before it can authenticate anything — and every byte of it is
passed to GCM as additional authenticated data, together with the column's
**context label**. Overhead is `2 + len(key_id) + 12 + 16` bytes, which for a
seven-character key id is 37.

The context label is the column's durable name, `users.notification_webhook_url`,
declared in the model:

```python
notification_webhook_url: Mapped[str | None] = mapped_column(
    EncryptedString("users.notification_webhook_url"), nullable=True
)
```

Binding it means a ciphertext lifted out of one column and written into another
fails to authenticate. Without it, every encrypted column using the same key is
interchangeable, and an attacker who cannot *read* a value can still *move* one.

The label is written as a literal rather than derived from `table.column`
because deriving it would make a column rename silently re-key every row in it,
with the first symptom arriving whenever somebody next read an old row.
`tests/test_encryption_types.py` asserts that every encrypted column's label
matches where it lives today, with an exemption table for a column that has
since been renamed — so the label is correct when it is written and deliberate
whenever it diverges.

## What this does and does not defend against

| | |
|---|---|
| Stolen disk, backup, or snapshot | **covered** — the key is not in it |
| Leaked read replica, `pg_dump`, `SELECT` access | **covered** |
| SQL injection that reads the column | **covered** |
| An attacker with `UPDATE` editing a value | **caught** — the tag fails and the read raises |
| An attacker with `UPDATE` moving a value to another *column* | **caught** — the context label differs |
| An attacker with `UPDATE` moving a value to another *row* | **not caught** — see below |
| Anyone who can read the application's memory or environment | **not covered** — the key is there |
| Knowing *whether* a row has a value | **not covered** — `NULL` is still `NULL` |

The row-move gap is real and worth stating plainly, because it is the one
people assume is closed. A `TypeDecorator` sees one value and has no idea whose
row it belongs to — the primary key is not available at bind time — so the
additional data binds a value to its column and not to its row. An attacker
with `UPDATE` on `users` can therefore copy one user's sealed webhook URL onto
another user, and it will decrypt. They still cannot read it and cannot forge
one, but they can redirect notifications to an endpoint that already belongs to
somebody.

Closing it would mean sealing where the row id is known — a mapper-level event
rather than a column type — and would make it impossible to insert a row and
its encrypted values in one statement, since the id is not settled until the
INSERT. `tests/test_encryption_db.py::test_moving_a_value_between_rows_is_not_caught`
asserts the current behaviour, so the limit is a measured fact rather than an
assumption.

## Queries

Randomised encryption means two sealings of the same plaintext differ. So a
`WHERE` clause on an encrypted column matches nothing — it does not error, it
simply returns no rows, which is indistinguishable from the record not
existing.

That is too quiet a failure to leave to documentation, so the column's
comparator raises instead:

```python
select(User).where(User.notification_webhook_url == url)
# EncryptedColumnComparisonError: == is not supported on an encrypted column…
```

`==`, `!=`, the four ordering operators, `in_`, `not_in`, `like` and `ilike`
are all refused. `IS NULL` and `IS NOT NULL` are allowed, because absence is
not encrypted and those clauses mean exactly what they say.

If a column genuinely has to be searchable, the answer is a blind index — a
second column holding a keyed MAC of a normalised plaintext, which supports
equality and leaks equality by construction. That is a different feature with a
different threat model and is not in this package.

## Keys

Two settings:

```bash
ENCRYPTION_KEYS=2026-09:<base64>,2026-08:<base64>
ENCRYPTION_ACTIVE_KEY_ID=2026-09
```

Each key is 32 bytes, standard base64. Generate one with:

```bash
uv run python scripts/generate_encryption_key.py --key-id 2026-09
```

Key ids are not secret — they travel in the clear inside every stored value and
appear in logs. They name a secret; they are not one. A date or a KMS alias is
the right shape.

The ring is a list rather than one key because rotation needs two live at once,
and `src/encryption/keys.py` refuses a ring whose active id is missing, whose
ids collide, or whose material is not 32 bytes. `DataKey.__repr__` never prints
material, and neither does any error message in the package.

`.env.example` and the CI workflow both ship a key that is published in this
repository; the base64 decodes to ASCII saying so. It exists because a
boilerplate where `cp .env.example .env` produces an application that will not
boot teaches the wrong lesson. `build_key_ring` refuses it outright — not just
as the active key, but anywhere in the ring — when `ENVIRONMENT=production`.

`validate_encryption_configuration` runs in the application lifespan, so a
broken configuration is a failed start-up rather than a 500 on whichever
request first touches an encrypted column.

## Rotating a key

Three steps, and the order is what makes it safe. Doing it in one is the way to
produce rows nothing can read.

**1. Publish the new key, keep the old one active.**

```bash
ENCRYPTION_KEYS=2026-08:<old>,2026-09:<new>
ENCRYPTION_ACTIVE_KEY_ID=2026-08     # unchanged
```

Roll this out everywhere. Nothing changes yet; every replica can now *read* the
new key's output, which matters because the next step is not atomic across
replicas.

**2. Move the active id.**

```bash
ENCRYPTION_ACTIVE_KEY_ID=2026-09
```

New writes seal under `2026-09`. Old rows still carry `2026-08` in their header
and still read.

**3. Re-encrypt, then drop the old key.**

Read each row and write it straight back; the column type seals it under
whatever is active. Only when no row references `2026-08` — every header can be
read without a key, so this is checkable in SQL — remove it from
`ENCRYPTION_KEYS`.

Dropping it earlier is the one irreversible mistake available here. Those rows
become unreadable and the error is `UnknownKeyError`, which names the key it
wants; putting the key back is the whole remedy, so keep it somewhere retrieval
is possible.

## The migration

`alembic/versions/0007_encrypt_notification_webhook_url.py` converts the
existing column: add a `bytea` column, encrypt every non-null value into it,
drop the `varchar`, rename. The conversion is not expressible in SQL, because
it needs a key the database does not have.

The add, the drop and the rename each take a brief `ACCESS EXCLUSIVE` lock and
are catalog-only; the backfill between them takes no table-level lock. On a
table large enough for its row locks to matter, split it: deploy a release that
writes both columns, run the backfill as a resumable job, then deploy the
release that reads the new column and drops the old one.

`downgrade()` decrypts back to `varchar`, and needs every key any surviving row
was sealed under. Run after a key has been retired, it fails with
`UnknownKeyError` before it has dropped anything — which is the right outcome,
since the alternative is a `users` table with a `NULL` where a webhook URL used
to be.

## Nonces, and why keys have a lifetime

The nonce is 96 random bits per value. GCM fails badly on nonce reuse under one
key: two messages sharing a nonce leak their XOR, and — worse — the
authentication subkey, which forgives forgeries from then on.

With random 96-bit nonces the birthday bound puts the collision probability at
roughly `2^-32` after about `2^32` values sealed under one key (NIST SP 800-38D,
Appendix B.2). That is billions of writes, not thousands, so it is not a
pressing operational concern — but it does mean a key is a budget rather than a
permanent fixture, and it is the reason rotation is built in rather than
bolted on later.

## Files

| | |
|---|---|
| `src/encryption/keys.py` | the ring: parsing, validation, the production guard |
| `src/encryption/envelope.py` | the byte format and the AES-GCM calls |
| `src/encryption/cipher.py` | write with the active key, read with whichever key a value names |
| `src/encryption/types.py` | `EncryptedString`, `EncryptedBytes`, and the comparator that refuses |
| `src/encryption/startup.py` | the lifespan check |
| `scripts/generate_encryption_key.py` | key generation |
