# The N+1, and choosing between `selectinload` and `joinedload`

Every number in this document is asserted in `tests/test_n_plus_one.py` against
a real Postgres, and the detector those tests use is `tests/querycount.py`. A
tuning guide is the kind of document that is right on the day it is written and
quietly wrong two SQLAlchemy releases later, so the claims here are executable
rather than remembered. Where this contradicts something you have read
elsewhere — the `LIMIT` section does — the test is the reason.

## The failure

An N+1 is one query for a list and then one more per item in it. Ten users cost
eleven queries, a thousand users cost a thousand and one, and each is a network
round trip that the previous one had to finish before it could start. It is not
a slow query: every statement involved is fast, the plan is fine, and nothing in
a slow-query log will ever show it. What it is, is *latency times cardinality* —
which is why it looks fine on a development machine with a local database and
twelve rows of seed data, and falls over on production data behind a 2ms link.

The name is worth taking literally, because it is also the test. Not "too many
queries" — a query count that **grows with the number of rows already fetched**.
That distinction is why the tests here run the same block at two row counts and
compare, instead of asserting a literal number:

```python
small, large = await _statements_at_each_size(sessions, seed, block)
assert small == large == 2
```

A test that asserts `len(log) == 3` pins an implementation detail and fails the
next time somebody adds a legitimate query. A test that asserts the count did
not move when the data did pins the property the name refers to.

## What async already prevents, and what it does not

On the ordinary `await session.execute(...)` path, this codebase cannot have the
textbook N+1. Touching an unloaded relationship needs I/O, an attribute access
is not a place an `await` can go, and SQLAlchemy raises `MissingGreenlet`
instead of quietly issuing the query:

```python
users = (await session.execute(select(User))).scalars().all()
for user in users:
    user.refresh_tokens   # MissingGreenlet, on the first one
```

That is a real safety property and it is worth knowing you have it. It is also
thinner than it looks, in two directions.

**`run_sync` and `awaitable_attrs` hand it straight back.** Both exist to let
synchronous ORM code run under asyncio, and inside them the lazy load works
exactly as it always did — one query per parent, no error, nothing in the log
but a lot of small fast statements. `tests/test_n_plus_one.py` measures six
users producing six extra statements through `run_sync`.

**The loop is untouched by any of it.** This is the version no loader strategy
can fix and no ORM feature protects you from:

```python
for user in users:
    await session.execute(select(RefreshToken).where(RefreshToken.user_id == user.id))
```

Every iteration is an ordinary top-level query. The ORM has no opinion about it,
there is no relationship load to report, and the fix is not a loader option but
a single statement with `IN`:

```python
await session.execute(select(RefreshToken).where(RefreshToken.user_id.in_(ids)))
```

This is why the detector has two independent checks rather than one. See
"Detecting it" below.

## The three strategies

Measured with 2 and 6 users, 3 tokens each.

| | statements | rows on the wire | notes |
|---|---|---|---|
| lazy (default) | 1 + N | N parents + their children | `MissingGreenlet` under asyncio; a real N+1 inside `run_sync` |
| `selectinload` | 2, at any row count | parents, then children once | second statement is `WHERE user_id IN (...)` |
| `joinedload` | 1, at any row count | parents **×** children | caller must `.unique()` the result |
| `contains_eager` | 1 | as your own join returns | the collection is whatever the join matched, not what the table holds |

### `selectinload` — the default for a collection

One query for the parents, a second `WHERE parent_id IN (...)` for all of their
children at once. Two statements whatever the row count, and the parent columns
cross the wire once.

Use it unless you have a specific reason not to. The extra round trip is the
price of not multiplying rows, and one extra round trip does not grow with the
data — which is the only property that matters at scale.

### `joinedload` — for many-to-one, and for collections you have measured

A `LEFT OUTER JOIN`, so there is no second statement at all. For a many-to-one
(`RefreshToken.user`) this is nearly always right: joining one parent row
multiplies nothing, and it saves a round trip for free.

For a collection it is a trade, and the cost is invisible from the call site.
The ORM de-duplicates the parents in memory, so the caller gets the right
objects and nothing looks wrong; the wire does not de-duplicate. Two users with
three tokens each is six rows, every user column repeated in each one. With a
wide parent row and a large collection that is the dominant cost of the query,
and it grows as the product of both.

Two practical notes:

- **The result must be `.unique()`d by hand.** Without it, SQLAlchemy raises
  `InvalidRequestError` rather than handing back duplicate parents. Swapping
  `selectinload` for `joinedload` therefore breaks the call site — loudly, which
  is the good outcome.
- **`LIMIT` is *not* broken by it, contrary to the folklore.** In hand-written
  SQL, `LIMIT 2` over a join counts joined rows and returns one user with two of
  their tokens. SQLAlchemy applies the limit to a subquery of the parent table
  and joins the children to that, so the limit means what you meant. This is
  pinned by a test, because it is the kind of behaviour that would silently
  return a wrong page rather than fail if it ever changed.

### `contains_eager` — a read model, never something you are about to mutate

You write the join yourself and tell the ORM the rows it produced are the
relationship. One statement, and you can filter the children:

```python
select(User)
  .join(User.refresh_tokens)
  .options(contains_eager(User.refresh_tokens))
  .where(RefreshToken.revoked.is_(False))
```

That is the point of it, and it is also the trap: `user.refresh_tokens` now
contains only the unrevoked tokens, the object says the user has one token when
the table says three, and nothing about the object marks it as a partial view.
Anything that iterates that collection and writes back is working from a lie —
`cascade="all, delete-orphan"` on a filtered collection is the worst version of
this. Use it for a response you are about to serialise; do not put it in a path
that mutates.

### A note on the mapper

All of this belongs on the *query*. Setting `lazy="joined"` on the relationship
itself joins the child table into every query for that parent — including the
ones that never look at the collection, including a count — and multiplies the
rows returned by all of them. A query that wants the children can ask; a query
that does not cannot easily opt out. `tests/test_n_plus_one.py` gates this: no
collection relationship in `src/models/` may be configured `joined`, `subquery`
or `immediate`. Many-to-one is deliberately not gated, since joining a single
parent row multiplies nothing.

## Detecting it

`tests/querycount.py` records two independent signals while a block runs, and
neither subsumes the other.

**Statements**, from the engine's `before_cursor_execute` — everything that
reaches the driver, whoever built it. One SQL *shape* sent N times is how the
hand-written loop gives itself away, and it is the only signal that catches it,
because the ORM never hears about it.

**Relationship loads**, from the session's `do_orm_execute`. A statement
carrying `is_relationship_load` was emitted to populate a relationship;
`lazy_loaded_from` then separates the two ways that happens. Non-`None` means a
lazy load fired for one already-loaded parent — the N+1 in its textbook form.
`None` means an eager strategy issued its one extra statement for the whole
batch, which is `selectinload` doing its job. Counting relationship loads
without that flag would condemn the fix along with the bug.

What it buys is a failure message that names the attribute:

```
NPlusOneDetected: lazy relationship load(s): User.refresh_tokens (6x).
Load it eagerly at the query — selectinload() for a collection,
joinedload() for a many-to-one; see docs/n-plus-one.md.
```

A statement count tells you there were seven queries. This tells you where the
loader option goes.

### Using it

```python
from tests.querycount import assert_no_n_plus_one, capture_queries

async with sessions() as session:
    with capture_queries(session) as log:
        await do_the_thing(session)

assert_no_n_plus_one(log)
```

`assert_no_lazy_loads` and `assert_no_repeated_statements(limit=...)` are
available separately; `assert_no_n_plus_one` runs both, lazy loads first,
because that is the message with the answer in it.

Two things to know before you trust a number it gives you:

- The listener attaches to the **engine** behind the session, so it sees every
  statement that engine sends while the block is open, including another
  session's. Resolving a statement back to its session from inside
  `before_cursor_execute` means asking the session for a connection, and doing
  I/O there is the exact greenlet violation this module helps find — so the
  tests give each case its own engine instead.
- **Statements and parameter sets are counted separately**, and the difference
  matters. SQLAlchemy's unit of work collapses N single-row `UPDATE`s into one
  `executemany`: `RefreshTokenRepository.revoke_all_for_user` loads the tokens
  and sets an attribute on each, and reads like an N+1 in the source, but it is
  two round trips whether there are three tokens or nine. Reporting the
  parameter sets as statements would flag a bug that is not there; reporting
  only the statements would hide how much was really written.

## The state of this codebase

Nothing in `src/` traverses a relationship today, so there is no N+1 here to
fix — which is a fact with a shelf life, and the reason the repository methods
are gated rather than merely inspected. `list_active`, `stream_export` and
`revoke_all_for_user` are each measured at two row counts on every pull request;
the first method that adds a traversal fails there rather than in production.

`stream_export` is the one worth calling out: it pages with `yield_per`, which
is a server-side cursor, so the batches are fetches against one statement rather
than a query each. A "streaming" export that re-queried per batch would be an
N+1 in its paging, and the test runs it with `batch_size=1` to prove this one is
not.
