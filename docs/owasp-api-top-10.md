# OWASP API Security Top 10 (2023)

A checklist is only as honest as its weakest line, so every line here names the
test that defends it. The tests are in
[`tests/test_owasp_api_top_10.py`](../tests/test_owasp_api_top_10.py), one per
mitigation, and the route-table helpers they share are in `tests/owasp.py`.

**What this document claims.** For each category: the mitigations this
application has, the test that fails if one stops holding, and — in the **Not
covered** paragraph — what the category asks for that is *not* done here. It does
not claim the application is secure. A category with three ticks against it has
three specific mitigations, not a clean bill of health, and the gaps are the part
worth reading.

**Why the tests exist at all.** Prose about security decays in a particular way:
it stays true of the version it was written against and goes on *reading* as
though it were still true. Three of the mitigations below were added by the item
that wrote this file, because writing the checklist is what found them missing —
see [What this item changed](#what-this-item-changed). A checklist assembled
without running anything would have ticked two of the three.

---

## API1:2023 — Broken Object Level Authorization

Authorisation per object, not merely per caller.

- [x] **No route takes an object id from the client.** Every per-object route
      resolves its object from the credential: `/users/me` reads the row the
      bearer token names. There is no `/users/{id}`, so there is no path
      parameter on which a check could be forgotten — a structural absence rather
      than a mitigation, and gated so the first route to break it has to argue
      for itself. → `test_no_route_takes_an_object_id_from_the_client`
- [x] **Object keys carry their owner.** `owner_key_prefix(owner_id)` puts every
      stored object under `users/<id>/`, and `require_key_owned_by` refuses a key
      outside a given account's prefix. That prefix is the *only* record of
      ownership — there is no uploads table — so it is what an authorisation
      check has to go on. → `test_presigned_upload_lands_in_the_callers_namespace`,
      `test_minting_a_key_requires_naming_an_owner`
- [x] **A presigned download is refused for another account's key.** A presigned
      GET is a bearer capability for one object: S3 honours it without consulting
      this application again, so the decision has to be complete before the URL
      is returned. → `test_presigned_download_refuses_another_accounts_key`,
      `test_presigned_download_allows_the_callers_own_key`
- [x] **Validation runs before the prefix comparison.** `users/<me>/../<victim>/x`
      starts with the caller's prefix and resolves elsewhere, so a `startswith`
      test over an unvalidated key admits it. The order of the two steps is the
      mitigation. → `test_a_traversal_key_naming_the_callers_prefix_is_refused`
- [x] **The prefix is `/`-terminated.** Without the slash, `users/<a>` prefixes
      `users/<ab>…`. Ids here are UUIDs, where that cannot arise — which is
      exactly why it is a test: dropping the slash passes every scenario written
      with real ids. → `test_the_owner_prefix_is_slash_terminated`

**Not covered.** There is no per-object ACL and no sharing model: ownership is
sole and is expressed by the key prefix, so "user A may read user B's object"
cannot be represented at all. Nor is there an admin override — a support flow
that needs to read a customer's object would need its own audited route, and
adding one to the existing route by relaxing the check would remove the property
this section is about. Objects are not enumerable through the API (`list_keys` is
not routed), but the prefix is derived from a user id rather than being a secret,
so it must not be mistaken for one.

## API2:2023 — Broken Authentication

- [x] **Login reaches the password hasher on every branch.** `or` short-circuits,
      so the natural spelling of the credential check never calls argon2 for an
      address nobody registered — measured here at ~75ms against ~0ms, three
      orders of magnitude, readable from a single request and a working "is this
      address registered" oracle. The unknown-address and OAuth-only branches
      verify against `decoy_hash()` instead. The mitigation is *wasted work*, so
      the only direct evidence is a duration and a wall-clock assertion in CI
      would be a flake generator; what is pinned is that both branches reach the
      hasher. → `test_login_hashes_even_when_the_address_is_unknown`,
      `test_login_hashes_for_an_oauth_only_account`
- [x] **The decoy is derived from the live hasher, not committed as a constant.**
      argon2 reads its cost parameters from the hash string it verifies, so a
      literal pinned in source would keep verifying at whatever `ARGON2_TIME_COST`
      produced it — and raising the setting would silently reopen the gap. It
      hashes a random value, so no password can match it.
      → `test_the_decoy_is_derived_from_the_live_hasher`, `test_the_decoy_matches_no_password`
- [x] **Both login refusals are byte-identical.** Timing is one channel and the
      message is the other. → `test_both_login_refusals_are_byte_identical`
- [x] **Token verification pins one algorithm.** `algorithms=[...]` is the whole
      defence against algorithm confusion: a verifier that took the algorithm
      from the token's own header would accept `alg: none`, and — with an
      asymmetric key configured — a token HMAC-signed with the public key.
      → `test_token_verification_pins_one_algorithm`, `test_an_unsigned_token_is_refused`
- [x] **A refresh token does not authenticate a request.** Both are signed with
      the same key, so only the `type` claim separates them; without the check, a
      deliberately long-lived credential works as the short-lived one.
      → `test_a_refresh_token_is_not_an_access_token`
- [x] **Passwords are argon2id with per-password salts**, and rehash-on-login is
      available through `needs_rehash`. Covered by `tests/test_auth.py`.
- [x] **Refresh tokens rotate, and a replay revokes the family.** The full
      design, including why the commit precedes the refusal, is in
      [`docs/refresh-token-reuse.md`](refresh-token-reuse.md); tests in
      `tests/test_refresh_token_reuse.py`.
- [x] **Refusals carry a challenge.** Both shapes — a missing `Authorization`
      header, refused by `HTTPBearer` before the dependency runs, and an invalid
      one, refused by `get_current_user` after it — answer 401.
      → `test_an_unauthenticated_request_is_refused_with_a_challenge`

**Not covered.** No MFA, and no account lockout or progressive backoff after
repeated failures — the per-IP rate limit is the only brake, and it is per IP, so
a distributed attempt against one account is not slowed by it. No password
breach-list check. No revocation list for *access* tokens: one stays valid until
`exp`, so deactivating an account takes effect on the next token refresh rather
than immediately, bounded by `ACCESS_TOKEN_EXPIRE_MINUTES`. The decoy verify
equalises the branches but does not make login *constant*-time; argon2's own
duration varies slightly with input.

## API3:2023 — Broken Object Property Level Authorization

- [x] **The profile update schema forbids unknown fields.** `ProfileService.update`
      does `setattr(user, key, value)` over `model_dump(exclude_unset=True)` —
      the classic mass-assignment shape — and it is safe for exactly one reason:
      the schema is closed, so only declared keys can reach it. Loosen `extra` and
      the same loop writes `role`. → `test_the_profile_update_schema_forbids_unknown_fields`
- [x] **Every field the schema declares is on an allow-list.** `extra="forbid"`
      stops an *undeclared* `role` arriving. It does nothing about somebody adding
      `role: str | None = None` to the model, which the loop would then apply.
      → `test_the_profile_update_schema_declares_only_editable_fields`
- [x] **No response model serialises a credential.** These models are built off
      the ORM row via `from_attributes=True`, so a field added is a column
      published — `hashed_password`, `oauth_sub`, `oauth_provider` and `version`
      are gated out of every one of them.
      → `test_response_models_never_serialise_a_credential`
- [x] **Registration cannot assign a role.** `AuthService.register` names `email`
      and `hashed_password` explicitly rather than splatting the request model, so
      a `role` in the body is ignored. Gated on the *call* as well as the
      response, because a future `create(**data.model_dump())` would pass a
      response-only test — the row would then say `admin` and `UserResponse` would
      faithfully report it. → `test_register_cannot_assign_a_role`,
      `test_register_does_not_splat_the_request_model`

**Not covered.** There is no field-level read policy that varies by *role*: the
response models are fixed, so an admin reading a profile sees the same fields the
owner does. Nothing here would stop a route from returning a model that is
correct for one caller and over-broad for another, because no route does that yet.

## API4:2023 — Unrestricted Resource Consumption

- [x] **Every publicly reachable route carries an explicit rate limit.** The
      200/minute default is a backstop, not a decision; a route that lost its
      decorator is absent from slowapi's registry, which no behavioural test
      would notice because the default still applies.
      → `test_every_public_route_carries_an_explicit_rate_limit`
- [x] **The credential routes are limited harder than the default** (5/minute).
      The numbers are asserted, not just their presence: a limit quietly widened
      to the default would otherwise read as "still limited".
      → `test_the_credential_routes_are_limited_harder_than_the_default`
- [x] **The login password is bounded.** This is a cost of the API2 fix, paid
      deliberately: before it an unknown address short-circuited and the field's
      length did not matter, and now every login reaches argon2, which hashes what
      it is given. Bounded at `RegisterRequest`'s limit, because a longer password
      is one no account can have. → `test_the_login_password_is_bounded`,
      `test_an_oversized_login_password_never_reaches_the_hasher`
- [x] **Pagination has a ceiling** (`le=100`), asserted one over the boundary so
      that widening it fails. → `test_pagination_cannot_request_an_unbounded_page`
- [x] **The export is bounded in rows, bytes and time.** The deadline is the one
      that matters most: without it a slow consumer holds a database cursor open
      indefinitely. → `test_the_export_stream_is_bounded_in_time_and_memory`
- [x] **WebSocket frames, room counts, idle time and total lifetime are capped.**
      A connection is a held resource in every one of those dimensions.
      → `test_websocket_frames_and_lifetimes_are_bounded`
- [x] **Upload size and content type travel inside the signed policy.** A
      presigned POST goes to S3 without touching this process, so a cap enforced
      in a handler would not apply at all.
      → `test_upload_size_and_type_are_bounded_in_the_signed_policy`
- [x] **Request bodies are capped** on the idempotency path
      (`IDEMPOTENCY_MAX_BODY_BYTES`), and readiness probes run under a per-check
      timeout with one run shared by concurrent callers — see
      [`docs/health.md`](health.md).

**Not covered.** The rate limiter's default storage is **in-process**, so limits
are per replica: N replicas mean N times the configured rate, and a restart
forgets the window. Moving slowapi to the Redis backend is the fix and is not
done here. There is no global request body cap — only the idempotency path has
one — so an unkeyed POST to an unlimited-body route is bounded by the proxy
rather than by this application. No query-cost or complexity budget, and no
connection-pool-exhaustion guard beyond SQLAlchemy's own pool limits.

## API5:2023 — Broken Function Level Authorization

- [x] **Every route either authenticates or is listed as public with a reason.**
      BFLA is mostly a bookkeeping failure — somebody adds a handler, forgets the
      dependency, and nothing complains because the route works. The route table
      is compared against an explicit list, so forgetting is a red build rather
      than a silent exposure. → `test_every_route_either_authenticates_or_is_listed_as_public`
- [x] **The exemption lists cannot outlive their routes**, which is how a stale
      allow-list comes to hide the next omission.
      → `test_the_public_list_does_not_name_routes_that_no_longer_exist`,
      `test_the_non_api_list_does_not_name_routes_that_no_longer_exist`
- [x] **The bulk export is admin-only.** Reading every account is a different
      function from reading one, and an ordinary user is allowed the latter —
      exactly the case this category describes.
      → `test_the_export_refuses_a_non_admin`
- [x] **It refuses before the response starts.** `require_role` resolves during
      dependency injection, so the refusal is an ordinary 403 envelope; a check
      inside the generator would arrive after the 200 and the content type, making
      the failure a truncated NDJSON stream clients would have to parse to notice.
      → `test_the_export_refuses_before_the_response_starts`,
      `test_the_role_guard_is_a_dependency_not_a_handler_check`

**Not covered.** Authorisation is a flat role string (`user`, `admin`) checked by
`require_role`; there are no permissions, no scopes and no resource-relative roles,
so "admin of this tenant" cannot be expressed. The `role` claim in the access
token is not what is trusted — the row is, loaded per request — but there is also
no re-check of role *during* a long-lived WebSocket connection, only at the
handshake.

## API6:2023 — Unrestricted Access to Sensitive Business Flows

- [x] **Registration is throttled per client**, driven end to end rather than read
      off the decorator: a `@limiter.limit` on a route whose app has no
      `state.limiter` raises at request time instead of limiting, so the wiring is
      part of the claim. Unbounded, this is free bulk account creation, and each
      one enqueues a welcome email — so the cost lands on a mail reputation as
      well as on a table. → `test_registration_is_rate_limited_per_client`
- [x] **A throttled response says when to retry.** A 429 with no `Retry-After`
      produces a client that retries immediately.
      → `test_a_throttled_response_tells_the_client_when_to_retry`
- [x] **Idempotency keys** stop a retried request from running a flow twice — see
      [`docs/idempotency.md`](idempotency.md).

**Not covered.** This is the weakest category here, and the reason is structural:
the only mitigation is a per-IP rate limit, which is precisely what this category
says is insufficient. There is no device fingerprinting, no proof-of-work, no
CAPTCHA, no disposable-email-domain check and no behavioural detection, so a
distributed client with one request per IP is not slowed at all. Email addresses
are not verified before an account is usable (`is_verified` defaults false but no
route requires it), so registration is not rate-limited by access to an inbox
either. Anything relying on scarcity of accounts should not rely on this.

## API7:2023 — Server Side Request Forgery

- [x] **HTTPS only, and no embedded credentials.** A scheme check is not redundant
      with a host check: `file:///etc/passwd` has no host to classify, so an
      address-based check alone passes it. → `test_non_https_schemes_are_refused`,
      `test_embedded_credentials_are_refused`
- [x] **Private, loopback, link-local, multicast and reserved literals are
      refused.** 169.254.169.254 is the one that matters most — on EC2 it hands
      role credentials to anything that can reach it, and this process can. The
      test cases are spelled `https` throughout on purpose: over `http` the scheme
      check refuses them first, so an `http` spelling would pass with the address
      check deleted. → `test_private_and_reserved_targets_are_refused`
- [x] **The permissive switch defaults to off.** A default of `True` would make
      every deployment that never set the flag SSRF-able.
      → `test_private_hosts_are_refused_by_default`
- [x] **The outbound client does not follow redirects.** A 302 to
      169.254.169.254 is a second request to an address nothing checked, which is
      how a guard that inspects only the first URL is walked around.
      → `test_the_webhook_client_does_not_follow_redirects`
- [x] **The authoritative check is at delivery, not at write time.** A hostname
      that is public when it is saved can point at a private address by the time
      anything is sent to it, so validating only on `PATCH` would be a check with a
      shelf life. The schema's own validation is about shape only.
      → `test_the_profile_route_defers_the_host_check_to_delivery`

**Not covered, and stated in the code as well.** A hostname that *resolves* to a
private address passes: re-resolving in the validator would not close it either,
because the socket does its own lookup afterwards (DNS rebinding). The mitigation
is egress policy — an allow-list proxy or a network rule — and it is not
implemented here. → `test_the_resolving_hostname_gap_is_not_silently_closed`
pins the behaviour so a later reader finds a test rather than an assumption.
There is also no allow-list of permitted webhook hosts, and no separate egress
identity, so this process's own IAM role is what any successful SSRF would reach.

## API8:2023 — Security Misconfiguration

- [x] **Security headers reach responses no handler produced.** The middleware is
      added last and therefore runs outermost, so a 404, a 429 and an idempotency
      refusal are all stamped. → `test_security_headers_reach_a_response_no_handler_produced`
- [x] **The 500 envelope leaks nothing and is stamped anyway.** Starlette installs
      the handler for bare `Exception` outside the user middleware stack, so this
      one response is stamped by `unhandled_exception_handler` itself — and it is
      also the response most likely to be reached with a malformed request.
      → `test_the_500_envelope_leaks_nothing_and_is_still_stamped`
- [x] **No CORS.** Nothing is mounted, so no cross-origin page can read a
      response; a deployment that needs one has to say so, rather than inheriting
      `allow_origins=["*"]` with `allow_credentials=True`. Asserted both on the
      middleware stack and on a real response, because a gate on the stack alone
      would pass if CORS were configured some other way.
      → `test_no_wildcard_cors_is_configured`,
      `test_a_cross_origin_response_carries_no_access_control_headers`
- [x] **The content security policy denies by default** (`default-src 'none'`),
      with the documentation pages served through nonced routes rather than
      `'unsafe-inline'` — see [`docs/security-headers.md`](security-headers.md).
      → `test_the_content_security_policy_denies_by_default`
- [x] **Debug mode is off**, so FastAPI renders the envelope rather than a
      traceback page. → `test_the_debug_flag_is_off`
- [x] **Configuration is validated at import**, so a missing `SECRET_KEY` is a
      failed start-up rather than a runtime surprise, and published development
      keys are refused outright when `ENVIRONMENT=production` — see
      [`docs/field-encryption.md`](field-encryption.md).

**Not covered.** `Settings` holds secrets as plain `str`, so `repr(settings)`
renders `SECRET_KEY` in full and any traceback or debugger frame holding the
settings object exposes it. `src/redaction` scrubs *log* lines (see
[`docs/pii-redaction.md`](pii-redaction.md)) and does nothing about a repr
elsewhere; moving these fields to `SecretStr` is the fix, touches every call site
that reads one, and is deliberately not bundled into this item. The
documentation pages and `/openapi.json` are also served unauthenticated, which is
a schema of every route available to anyone who can reach the service.

## API9:2023 — Improper Inventory Management

- [x] **Every API route is under `/api/v1/`.** An unversioned route cannot be
      retired without breaking a client, which is how a "temporary" endpoint
      becomes a permanent one nobody owns. Probes, the scrape endpoint and the
      documentation pages are infrastructure and are listed out explicitly.
      → `test_every_api_route_is_under_a_version_prefix`
- [x] **Every HTTP route appears in the OpenAPI document.** `include_in_schema=False`
      is the switch that hides one, and a hidden route is one that does not appear
      in a review of what this service exposes. `/metrics` is the single
      deliberate exception, named rather than assumed.
      → `test_every_http_route_appears_in_the_openapi_document`
- [x] **The documented responses include the refusals**, because a client cannot
      handle a 403 it was never told about.
      → `test_the_documented_error_shapes_include_the_refusals`

**Not covered.** There is one version and therefore no deprecation policy,
`Sunset` header, or machine-readable notice of retirement — none of which can be
tested until there are two versions to move between. Nothing here distinguishes
environments: there is no inventory of which hosts run this image, which is the
half of this category that lives outside the repository.

## API10:2023 — Unsafe Consumption of APIs

- [x] **Every outbound HTTP client is built with a timeout.** httpx defaults to
      five seconds, but `timeout=None` is legal and means *no* limit — so a third
      party that accepts a connection and never answers holds a worker and, behind
      it, a database session. Gated across `src/` rather than on one module,
      because the next module to make an outbound call is the one that will forget.
      → `test_every_outbound_client_is_built_with_a_timeout`
- [x] **An inbound webhook needs a verified signature**, inside a replay window,
      remembered after verification — three separate refusals, because they are
      three different bugs. The design, including why the four checks are in that
      order, is in [`docs/webhook-signatures.md`](webhook-signatures.md).
      → `test_an_inbound_webhook_needs_a_verified_signature`
- [x] **A provider's bad response is a 400, not a 500.** An unparseable OAuth
      callback is not an outage, and the distinction is what keeps it from paging
      somebody — and keeps the provider's error text out of a 500 envelope.
      → `test_an_oauth_callback_failure_is_a_400_not_a_500`
- [x] **Third-party calls go through circuit breakers, bulkheads and a bounded
      retry budget** — see [`docs/resilience.md`](resilience.md).
- [x] **Provider responses are parsed into declared models** (`OAuthUserInfo`),
      so a field arriving with an unexpected type is a validation failure rather
      than a value flowing onward.

**Not covered.** There is no certificate pinning and no allow-list of hosts this
service may call, so `httpx` trusts the system CA store. No response size cap on
an outbound fetch: a provider returning gigabytes is bounded by the timeout
rather than by a byte limit. And the signature scheme has no provider adapters —
GitHub's `X-Hub-Signature-256` carries no timestamp and needs its own parser,
PayPal uses a certificate chain.

---

## What this item changed

Writing this file found three mitigations missing. Each is small, each is in the
subject matter of the category it sits under, and none could be ticked above
without being implemented — a test for an absent mitigation is either vacuous or
a lie.

1. **API1 — `/api/v1/uploads/presigned-download` had no object-level check at
   all.** It required *a* user and signed whatever key the request named, so every
   object in the bucket was readable by every authenticated account. Keys are now
   owner-scoped (`users/<id>/…`) and the signer refuses a key outside the caller's
   prefix. It also did not validate the key, so one carrying `..` or a control
   character was signed as readily as a well-formed one. **This changes the key
   layout**: keys minted before this change are outside every account's prefix and
   are no longer downloadable through this route.
2. **API2 — login skipped argon2 for unknown addresses.** ~75ms against ~0ms,
   which is a user-enumeration oracle readable from one timed request. Both
   password-less branches now verify against a decoy hash.
3. **API4 — `LoginRequest.password` was unbounded.** Harmless while unknown
   addresses short-circuited; a CPU multiplier on an unauthenticated request once
   fix 2 made every login reach the hasher. Now bounded at `RegisterRequest`'s
   limit.

The three fixes were mutation-checked rather than assumed: restoring the
short-circuiting login, dropping the trailing slash from the owner prefix,
comparing the prefix before validating the key, and removing the ownership check
from the signer each fail the suite.

## Adding a route

The structural gates are the part of this file that acts on code nobody has
written yet. A new route fails one of them unless it either carries a guard or is
written down, which is the intended friction: the omission becomes a decision.

- Needs a credential → take `CurrentUserDep`, or `require_role(...)` for an
  administrative function. Do the check as a **dependency**, not as the handler's
  first statement, so it resolves before the response class does.
- Public by design → add the path and its reason to `INTENTIONALLY_PUBLIC` in
  `tests/test_owasp_api_top_10.py`. The reason is the point of the entry.
- Serves an API → put it under `/api/v1/`. Infrastructure goes in `NON_API_PATHS`
  in `tests/owasp.py`.
- Addresses an object → resolve it from the credential. If it genuinely must take
  an id, `test_no_route_takes_an_object_id_from_the_client` will fail, and the fix
  is an ownership check on that route rather than an exemption.
- Reaches a URL a user supplied → `validate_webhook_url` at the point of the
  call, not at the point of storage.
