# Optimistic concurrency: `version_id_col`, ETags and `If-Match`

## The failure this prevents

Two support agents open the same user's profile. One switches the notification
channel to `webhook`; the other, working from the copy they loaded a minute
earlier, switches it to `none`. Both requests succeed. The first change is
gone, nobody was told, and the row now says something neither of them decided.

That is a lost update. It is not a database fault — both writes were valid, in
order, and committed — and no amount of transaction isolation fixes it, because
the two writes never overlap in time. The read that informed the second write
is the stale part, and only the application knows that read happened.

Optimistic concurrency turns the second write into an error. It is *optimistic*
in that nothing is locked and nothing waits: conflicts are assumed rare, and
paid for only when they happen. The cost is that clients must be able to handle
a rejected write, which is why the API is explicit about it rather than
silently retrying.

## The two halves

### 1. A version counter on the row

`User` declares one:

```python
version: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")

__mapper_args__ = {"version_id_col": version}
```

From then on SQLAlchemy sets `version` to 1 on INSERT and emits every update as

```sql
UPDATE users SET notification_channel = $1, version = 2
 WHERE users.id = $3 AND users.version = 1
```

If that matches no rows, someone else has written since this transaction read
the row, and SQLAlchemy raises `StaleDataError` instead of reporting success.
The check costs nothing — it is a predicate on an index lookup that was
happening anyway — and it holds no locks.

Two properties worth knowing:

- **An update that changes nothing does not bump the counter.** SQLAlchemy
  emits no UPDATE for an assignment that leaves the value as it was, so a
  representation that did not change keeps its ETag.
- **DELETE is versioned too.** Removing a row someone else has modified raises
  the same error.

### 2. The counter, exposed as an entity tag

`GET /api/v1/users/me` answers with

```
ETag: "6f1c…-…-…-…-9ab2.1"
Cache-Control: private, no-store
```

and an unsafe request has to echo that tag back:

```
PATCH /api/v1/users/me
If-Match: "6f1c…-…-…-…-9ab2.1"
```

| Situation | Status |
| --- | --- |
| Tag matches the current row | `200`, with the new `ETag` |
| Tag is stale | `412`, with the current `ETag` |
| `If-Match` absent | `428` |
| `If-Match` unparseable | `400` |

A client that gets a `412` re-reads, reapplies its change to the new state, and
retries. The `ETag` on the `412` is the current one, so the re-read is a
courtesy rather than a requirement.

## Why the check happens twice

`ProfileService.update` compares the client's tag against the row it just
loaded, *and* the UPDATE carries the version in its WHERE clause. Both are
load-bearing:

- The application-level comparison is the one that fires in practice, when a
  client is simply out of date. It knows the current tag and can return it, and
  it costs no write.
- The database-level check is the one that is *sound*. Between the comparison
  and the UPDATE there is a window; under concurrency, something eventually
  lands in it. Only the database can adjudicate that, because only the database
  serialises the writes.

Keep the first and drop the second and the endpoint has a lost-update race that
no test issuing one request at a time will ever reveal.
`tests/test_optimistic_concurrency_db.py` exercises exactly that window against
a real Postgres: the precondition passes, and the write is still refused.

## Details that are easy to get wrong

**`If-Match` uses strong comparison.** RFC 9110 §13.1.1. `W/"…"` never
satisfies it. A weak tag claims two representations are semantically
equivalent, which is a reasonable thing to say about a cached copy and a
useless thing to say about a row you are about to overwrite.

**A comma can appear inside an entity tag.** `etagc` admits `%x2C`, so
`If-Match: "a,b"` is one tag, and `header.split(",")` is wrong. The parser in
`src/concurrency/etag.py` scans the grammar instead.

**A malformed `If-Match` is a 400, not a shrug.** Ignoring a header that failed
to parse switches the protection off at the exact moment the client believed it
was on.

**The tag includes the row id, not just the version.** Every row is at version
1 the moment it is created, and `/users/me` is one URI naming a different
resource per bearer token — so a bare `"1"` obtained by one client would
compare equal to a different user's row. Those responses are also marked
`Cache-Control: private, no-store`, since a shared cache has no business
holding either the representation or its tag.

**412 is not 409.** A `409` says the request conflicts with the resource's
rules and repeating it will fail the same way. A `412` says the request was
fine but out of date: re-read, reapply, retry, and it works.

## Using it on another resource

1. Add the counter to the model and a migration for it:
   ```python
   version: Mapped[int] = mapped_column(Integer, nullable=False, server_default="1")

   __mapper_args__ = {"version_id_col": version}
   ```
   `server_default` matters for rows that already exist and for INSERTs that
   bypass the ORM; the ORM never reaches it.
2. Stamp reads with `resource_version_tag(row.id, row.version).serialize()`.
3. Take the precondition as a dependency — `precondition: IfMatchDep` — and
   call `precondition.require_match(current_tag)` before mutating.
4. Wrap the commit and translate `StaleDataError` into
   `PreconditionFailedError`.

Steps 3 and 4 are eight lines in `src/users/service.py`; they are deliberately
not hidden behind a decorator, because the interesting question — *which* tag
is current for this representation — is different for every resource, and a
decorator that guesses it wrongly fails open.

## What this is not

**Not a lock.** Two writers still both do their work; one of them is told to
redo it. Where waiting is cheaper than redoing — a counter, a balance, a queue
claim — pessimistic locking (`SELECT … FOR UPDATE`) is the better trade, and it
is the next item in `SPEC.md`.

**Not idempotency.** `docs/idempotency.md` stops the *same* request from being
executed twice. This stops *different* requests from silently overwriting each
other. A `PATCH` retried after a network timeout wants both.

**Not conditional GET.** `If-None-Match` and `304 Not Modified` share the ETag
machinery and answer a caching question rather than a correctness one; they are
not implemented here. The `ETag` this API serves is fit for that purpose if you
add it — it changes whenever the representation does.
