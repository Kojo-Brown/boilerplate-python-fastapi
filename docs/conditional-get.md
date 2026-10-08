# Conditional GET: `If-None-Match` and the 304

`GET /api/v1/users/me` has always returned an `ETag`. Until this change the
only request that could use it was the `PATCH` — `If-Match`, the write
precondition in [`optimistic-concurrency.md`](./optimistic-concurrency.md). A
client holding a tag had no way to ask whether its copy was still current; it
could only fetch the representation again and compare, which is the work the
tag exists to avoid.

`If-None-Match` closes that. A client sends back the tag it holds, and a server
that recognises it answers `304 Not Modified` with no body.

```
GET /api/v1/users/me
If-None-Match: "550e8400-e29b-41d4-a716-446655440000.7"

304 Not Modified
ETag: "550e8400-e29b-41d4-a716-446655440000.7"
Cache-Control: private, no-cache
```

## The two comparison functions

RFC 9110 §8.8.3.2 defines two, and which one a field uses is a property of the
field, not of the tag:

| Field | Comparison | Why |
| --- | --- | --- |
| `If-Match` | strong | Guards a write. `W/"7"` claims the representations are *semantically* equivalent, which is not the same as "unchanged since I read it". |
| `If-None-Match` | weak | Guards a read. Semantic equivalence is exactly what a cache is asking about: may I keep what I have? |

So `W/"7"` and `"7"` name the same representation to this field and different
ones to `If-Match`. Reusing one comparison for both is the mistake this code is
arranged to make impossible: `EntityTag` has `strongly_matches` and
`weakly_matches`, each with one caller, and neither precondition type can reach
the other's.

Nothing in this API currently *emits* a weak tag — `resource_version_tag`
builds strong ones from the row's version column — so the weak comparison
matters only for tags a client weakens on the way back. It is still the
required behaviour, and a server that compared strongly would stop answering
304 to such a client without anything reporting it.

## What a match means depends on the method

§13.1.2: a satisfied `If-None-Match` is a 304 for `GET` and `HEAD`, and a 412
for anything else. On an unsafe method the field means "only if it does not
already exist", so a match is a refusal — and answering 304 there would tell a
client its write had succeeded and changed nothing, when the write never ran.

`ConditionalOutcome` carries the first case and `PreconditionFailedError` the
second, which is why `evaluate` both returns and raises.

## Precedence when both fields arrive

§13.2.2 fixes the order, and `ProfileService.update` follows it: `If-Match` at
step 1, `If-None-Match` at step 3, then the write. The ordering is observable.
A `PATCH` whose `If-Match` is stale *and* whose `If-None-Match` matches gets
the 412 that names `If-Match`, because that is the precondition the client got
wrong.

Both are evaluated before anything is written, so a 412 from either never
leaves a partial update behind.

## `no-cache`, and why it is not `no-store`

The route sent `Cache-Control: private, no-store` before this change, and that
directive and a conditional GET cannot both be meant. `no-store` (RFC 9111
§5.2.2.5) forbids the client to keep the representation at all — so there is
nothing for an `If-None-Match` to revalidate, and the `ETag` on a read is
decoration.

`no-cache` (§5.2.2.4) is the directive that was wanted: store it, and never
reuse it without asking the origin first. Both halves matter here.

- `private` is unchanged, and it is the directive carrying the real hazard:
  this URI names a different resource for every bearer token, so no shared
  cache may hold the representation or its tag. The tag folds in the row id as
  well, so one account's tag cannot confirm another's copy — a cross-account
  `If-None-Match` gets a 200, which
  `test_another_accounts_tag_at_the_same_version_is_not_304` pins.
- `no-cache` means a stale `role` or `is_active` is never served out of a cache
  without the origin agreeing, which is the correctness property `no-store` was
  really being relied on for.

**What the change costs.** `no-cache` permits the representation to sit in a
private cache, on disk, where `no-store` kept it out of one. That is a real
loss, and it is the right side of the trade for this resource: the body is the
caller's own profile and carries no credential or token. A deployment whose PII
policy says otherwise should keep `no-store` and stop honouring the field —
both live on `_CACHE_CONTROL` in `src/api/v1/users.py`, and dropping the
conditional branch with it is the whole revert.

The 304 repeats the header rather than omitting it (§15.4.5). A revalidation
that dropped it would leave the client's stored copy governed by whatever it
remembered from the response it was revalidating.

## Using it on another route

`IfNoneMatch.evaluate` takes the representation's *current* tag, which
presupposes that the representation exists. That is the only shape this API has
— `/me` is the authenticated caller's own row — but it means the type cannot
express `If-None-Match: *` against a resource that is absent, where the correct
answer is to proceed rather than to answer 304. A route with an optional
resource needs to decide the existence question before it gets here.

Two smaller things a second route will need and this one does not:

- **`Vary`.** Not set. This representation varies by `Authorization`, and
  `private` plus a tag that carries the row id is what keeps that safe here: a
  revalidation always reaches the origin, and a tag from another account never
  matches. A route that becomes cacheable by anything shared needs `Vary`
  before a cache sees it.
- **`HEAD`.** Not routed. FastAPI's `APIRoute` does not add `HEAD` to a `GET`
  route the way Starlette's plain `Route` does, so a `HEAD` of this URI is a
  405 naming `Allow: GET`. The handler reads the method off the request rather
  than hardcoding `"GET"`, so adding `HEAD` needs no change here.

## Malformed fields

A field that does not parse is a 400 carrying `MALFORMED_PRECONDITION`, not a
shrug — the same policy as `If-Match`, for the same reason. Ignoring it would
answer 200 to a client that believed it had asked conditionally, which is a
silent waste rather than a reported bug.

The 400 names the field the client actually sent. Both headers share one
scanner (`parse_entity_tag_list` in `src/concurrency/tags.py`, whose `field`
argument is required precisely so that neither module's name can end up on the
other's errors), because a comma is a legal `etagc`: `If-None-Match: "a,b"` is
one tag, and splitting either header on commas is wrong.
