# TDD kata: conditional GET for `If-None-Match`

A worked record of one feature built red→green→refactor, one commit per step.
The feature is real and shipped — `If-None-Match` in
`src/concurrency/conditional.py`, wired into `GET /api/v1/users/me` — so this
is an account of how the code in the repository was actually arrived at, rather
than a demonstration written backwards from a finished answer.

## Why this feature

A kata needs a problem with enough real structure to punish guessing and small
enough that each cycle is one idea. `If-None-Match` qualifies on both counts,
and it had the rarer property of being a gap the codebase could *name*: the
profile route's own docstring said "a client cannot make a conditional request
without one" about the `ETag` it served, and the only conditional request it
accepted was a write. The server minted a validator on every response and then
refused it on a read.

It also had somewhere honest to land. `/api/v1/users/me` already had the
machinery — a version column, `resource_version_tag`, `If-Match` on the
`PATCH` — so the feature is the other half of a protocol that was half built,
not a new subsystem. And the two preconditions use *different* comparison
functions, which is a distinction no amount of planning makes memorable and
one failing test makes permanent.

## The rules followed

1. One step per commit, and the commit message states what was run and what it
   printed.
2. A red commit fails its **tests**, not the toolchain. The first one ships a
   module of typed stubs that raise rather than no module at all — in Python a
   missing module is a collection error, and a commit whose failure is an
   `ImportError` tells nobody bisecting anything about the feature. Every red
   commit here passes mypy, ruff and `ruff format --check`.
3. Green is the least code that passes. Anything not demanded by a case waits
   for the case that demands it.
4. Refactor changes no behaviour, and the proof is that no test file is touched
   by the refactor commit.
5. No test is weakened to reach green. Three expectations changed; all three
   are recorded below rather than buried, and two of them were my own bugs.

## The cycles

| Step | Commit | What it did |
| --- | --- | --- |
| 1 | `6171804` | red — parsing and the weak comparison function, 25 failing |
| 2 | `e89b3a5` | green — weak comparison, and `IfNoneMatch` over the shared scanner |
| 3 | `c6419f3` | red — a match is 304 on a safe method, 412 on any other, 19 failing |
| 4 | `0468b54` | green — `_SAFE_METHODS` and the three branches of `evaluate` |
| 5 | `f90e861` | red — the 400 must name the header the client sent, 2 failing |
| 6 | `660517f` | green — the scanner takes the field name to blame |
| 7 | `4b57153` | **refactor** — the entity-tag grammar moves to `tags.py` |
| 8 | `26f0a87` | red — the route, and the `no-store` contradiction, 9 failing |
| 9 | `467cb9c` | green — both routes honour the field; `no-store` → `no-cache` |

Step 10 is the commit that added this document and
[`conditional-get.md`](./conditional-get.md).

48 unit cases on the parser and the evaluation rule, 13 through the route,
3 on precondition precedence. `src/concurrency/conditional.py`,
`src/concurrency/tags.py`, `src/concurrency/etag.py`, `src/api/v1/users.py` and
`src/users/service.py` are each at 100% line coverage; the suite is 4188 cases.

## What the cycles found that planning had not

**A header that contradicted the feature being added (step 8).** The route
sent `Cache-Control: private, no-store`, and writing the integration case is
what made the conflict unavoidable: `no-store` forbids a client to keep the
representation at all (RFC 9111 §5.2.2.5), so there is nothing an
`If-None-Match` could revalidate and the `ETag` the route had always served was
decoration on a read. For one commit the suite asserted both `no-store` and
`no-cache` and was therefore impossible to satisfy — which is the honest state
of a red step that has found a real problem, rather than one that is merely
waiting for code. The resolution is in
[`conditional-get.md`](./conditional-get.md#no-cache-and-why-it-is-not-no-store),
including what it costs: `no-cache` lets the representation sit in a private
cache that `no-store` kept it out of. Worth saying plainly that the kata did
not discover the HTTP rule — it discovered that this route's existing header
and this route's existing `ETag` had never been read against each other.

**An error message that blamed the wrong header (step 5).** Step 2 reused
`_parse_tag_list` rather than copying a scanner, which was the right trade and
borrowed every error message with it — all of which said `If-Match`. A client
debugging its `If-None-Match` would have been told that a header it never sent
was invalid, which sends the investigation into the wrong half of its own code.
Nothing about this was visible while writing the parser; it surfaced from
asking what the 400 says. Two of the four cases in that step are guards rather
than failures: one that `If-Match` keeps naming itself, and one that naming the
field does not cost the offending value — fixing a message is exactly the
change that quietly drops the rest of it.

**A refactor that was supposed to be cosmetic (step 7).** Promoting the scanner
to a public name would have been enough to stop `conditional.py` reaching for a
private one. Moving the grammar out of `etag.py` entirely made a smaller thing
visible: `etag.py` had a module docstring about `If-Match` and owned the
`etagc` regex, `EntityTag` and both comparison functions, so whichever
precondition module held the grammar would describe its own field while serving
the other's. `tags.py` now owns the grammar and the comparisons; `etag.py` and
`conditional.py` own one precondition each. The `field` argument also loses the
default it was given in step 6 — it existed so that the fix and the tidy-up
were separable, and a default header name is precisely how one field's name
ends up on the other's errors a second time.

**A docstring I wrote, checked, and had to retract (step 9).** The handler
reads `request.method` rather than a literal `"GET"`, and I justified that by
writing that Starlette answers `HEAD` from the same handler. That is true of
Starlette's plain `Route` and not of FastAPI's `APIRoute`: a `HEAD` of this URI
is a 405 naming `Allow: GET`. I found it by probing the claim rather than by a
failing case, which is worth recording — nine steps of tests had no opinion
about it. The comment is now accurate and
`test_head_is_not_routed_here` pins the 405, so the next person to read it is
reading a fact rather than a plausible sentence.

**A branch that would have been dead code (step 8).** `evaluate` grew its 412
branch in step 4 because §13.1.2 says so, and the route planned in step 8 was
`GET` only — which would have shipped a tested, unreachable branch. §13.2.2's
precedence rule is what made it live: step 3 of that list evaluates
`If-None-Match` *after* a successful `If-Match`, so a `PATCH` carrying both is
a 412, and `ProfileService.update` now evaluates both in order. The scope grew
by three cases and one line, and the alternative was a module whose most
interesting branch no caller used.

**Two of my own tests that were wrong (step 9).**
`test_a_stale_tag_gets_the_full_representation` built its "stale" tag at
version 1, which is the fixture's current version — it asserted the opposite of
its name and would have passed against a route that answered 200 to
everything. `test_a_malformed_field_is_400_rather_than_ignored` read
`error_code` from the error envelope, where the field is `error`, so it was
asserting a `KeyError` rather than a status code. Both were corrected rather
than deleted. A test that passes for the wrong reason is the shape that holds
forever while asserting nothing, and the first of these was the more dangerous
of the two: it was *failing*, which is why it got looked at. The version of
that bug which chooses a stale-looking version that happens to be current, in a
test that then passes, is invisible.

## Decisions the tests pinned down

- **Which comparison function, per field.** Strong for `If-Match`, weak for
  `If-None-Match` (§8.8.3.2). `EntityTag` offers both and each has exactly one
  caller.
- **A malformed field is a 400, not a shrug.** Consistent with `If-Match`.
  Ignoring it answers 200 to a client that believed it had asked conditionally.
- **An empty field is malformed too.** A field listing zero tags cannot be
  satisfied and is far more likely a client that joined a list badly. Same
  answer `If-Match` already gave.
- **Neither header may be split on commas.** A comma is a legal `etagc`.
- **A 304 carries the validator and the caching policy** (§15.4.5), and no
  body — Starlette omits `Content-Length` for a 304, which the test asserts
  rather than assumes.
- **A match on an unsafe method is 412, never 304.** A 304 there says the write
  succeeded and changed nothing.
- **`If-Match` is decided before `If-None-Match`** (§13.2.2), so a request both
  fields refuse gets the error naming `If-Match`.
- **Authentication is decided before any precondition.** A 304 to an
  unauthenticated caller would confirm that a guessed tag describes somebody's
  current profile.
- **One account's tag cannot confirm another's copy.** The tag carries the row
  id, so a cross-account `If-None-Match` is a 200.
- **The method token is case-sensitive** (§9.1), so the module reads `get` as
  the unsafe branch rather than lowercasing a routing question it does not own.

## What this item did not do

`If-Modified-Since` and `Last-Modified` are not implemented, and the reason is
specific rather than a matter of time. The row has an `updated_at`, but it is
`onupdate=func.now()` — the database writes it, so the value the ORM holds
after a write is stale until something refreshes it, and serving an accurate
`Last-Modified` would mean a round trip to keep a header honest. On top of that
an HTTP-date has one-second resolution, which makes it a *weak* validator that
cannot distinguish two changes inside the same second, while the `ETag` is a
strong validator derived from the version column and already distinguishes
them. Adding the date would mean a second, weaker answer to a question already
answered, plus the §13.2.2 step-4 ordering to keep it from being consulted when
`If-None-Match` is present. It is its own item, not a line in this one.

`If-Unmodified-Since` and `If-Range` are not implemented either; the first is
the date-based form of `If-Match` and carries the same `updated_at` problem,
and the second needs range requests, which this API does not serve.

Only `/api/v1/users/me` is conditional. Nothing else in the service has a
strong validator to offer: the NDJSON export at `/api/v1/exports/users` is the
one large representation here and it is a stream with no stable tag — its size
is not known until the last row is read, which is why it carries no
`Content-Length` either. `Vary` and `HEAD` are both unaddressed and both are
written down in
[`conditional-get.md`](./conditional-get.md#using-it-on-another-route) rather
than left for the next route to rediscover.
