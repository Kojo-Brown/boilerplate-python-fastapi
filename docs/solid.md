# SOLID audit

An audit of `src/` against the five SOLID principles, with the refactors it
produced. It is a record of specific defects and what was done about them, not a
tutorial: every "before" below is code that was on `main`, and every "after" is
code in the tree now.

Two findings are reported but deliberately **not** fixed here, because a later
`SPEC.md` item owns the fix and doing it now would smear one change across two
items. Those are marked *Deferred* and name the item that owns them.

## Summary

| # | Principle | Where | Status |
|---|-----------|-------|--------|
| 1 | SRP, OCP | `AuthService` signalled every rejection as `ValueError`; each router re-derived a status code | Fixed |
| 2 | LSP | `except ValueError` caught unrelated subtypes and reported internal failures as client errors | Fixed (same refactor) |
| 3 | SRP | `get_current_user` assembled its own `select(User)`, duplicating `UserRepository` | Fixed |
| 4 | DIP | `AuthService` constructs its own repositories | Deferred — Phase 6, "Dependency inversion via FastAPI `Depends` + protocol-typed providers" |
| 5 | OCP | `src/storage/s3.py` is hardwired to boto3; a second backend means editing it | Deferred — Phase 6, "Factory pattern: `StorageFactory`" |
| 6 | ISP | `AuthService` takes a whole `AsyncSession` to call `commit()` and `flush()` | Deferred — follows finding 4 |

Findings 1–3 changed the HTTP contract. The changes are listed in
[API changes](#api-changes) at the end.

---

## 1. SRP + OCP — the service made HTTP decisions by omission

**Before.** `AuthService` raised a bare `ValueError` for every rejection, so it
carried no information about *what kind* of rejection it was. Each router then
guessed, one `except` block per endpoint:

```python
# src/auth/service.py
if not user.is_active:
    raise ValueError("Account is inactive")

# src/auth/router.py — /auth/login
try:
    return await service.login(data.email, data.password)
except ValueError as exc:
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=str(exc),
        headers={"WWW-Authenticate": "Bearer"},
    ) from exc
```

Two responsibilities live in that block: deciding *whether* to reject (the
service's job) and deciding *how to say so over HTTP* (the edge's job). Because
the second one was resolved per route rather than per failure, the same domain
condition got different answers depending on the door the caller used:

| Condition | `/auth/login` | `/auth/google/callback` | `get_current_user` |
|---|---|---|---|
| Account is inactive | **401** | 403 | 403 |

The repository's own tests asserted both readings — `test_login_inactive_user_returns_401`
and `test_get_current_user_inactive_raises_403` — and neither noticed the other.

It also failed OCP in the ordinary way: a new failure mode meant editing every
`except` block that might see it, and every block that *didn't* get edited
quietly relabelled the new failure as whatever that route already returned.

**After.** The service raises the domain exception the condition actually is.
Each one already knows its status code and error code, and the global
`app_exception_handler` renders it:

```python
# src/auth/service.py
if not user.is_active:
    raise ForbiddenError("Account is inactive")

# src/auth/router.py — /auth/login
service = AuthService(db)
return await service.login(data.email, data.password)
```

`AppException` grew one field to make this complete. A 401 must name its
challenge scheme (RFC 9110 §11.6.1), and `headers={"WWW-Authenticate": "Bearer"}`
was hand-copied at all five sites that raised one. All five did get it right;
nothing made them, and a sixth would have been one forgotten line away from a
non-conforming 401. Declaring it on the class states it once:

```python
class UnauthorizedError(AppException):
    status_code = status.HTTP_401_UNAUTHORIZED
    error_code = "UNAUTHORIZED"
    headers = {"WWW-Authenticate": "Bearer"}
```

The base class already owned `status_code`, so headers that a status code
mandates belong at the same level. The purer alternative — keeping the exception
transport-free and mapping to headers in a table at the edge — was rejected
because it splits one fact across two files without removing the coupling.

Adding a failure mode is now a new `raise` in the service and nothing else.

**Verified by** `tests/test_auth_error_contract.py::test_inactive_account_is_forbidden_not_unauthorized`
and `::test_auth_failures_report_their_own_error_code`.
`::test_unauthorized_responses_carry_the_bearer_challenge` pins the challenge
header, which was correct before and is now structural rather than copied.

## 2. LSP — the catch was wider than the contract

Liskov is usually read as a statement about subclasses, but it constrains
exception contracts just as directly: a caller that catches type `T` accepts
every subtype of `T`, so it must be prepared to handle all of them
interchangeably. `except ValueError` was not.

**Before.** The router wrapped the entire service call:

```python
try:
    user = await service.register(data)
except ValueError as exc:
    raise HTTPException(status_code=400, detail=str(exc)) from exc
```

`pydantic.ValidationError` is a subclass of `ValueError`. So is anything
`uuid.UUID()`, `int()`, or a third-party library raises on bad input. The block
treats all of them as "the client sent something invalid".

This was not hypothetical. `AuthService.register` ends with
`UserResponse.model_validate(user)` — serialising *our own* response. In
`tests/test_rate_limit.py` the stub session never resolved column defaults, so
that call raised `ValidationError` on every request, and the router turned it
into `400 "4 validation errors for UserResponse …"`. The test asserted
`status_code in {200, 201, 400, 422}` and passed, green, for as long as the file
existed — reporting a server-side serialisation bug as the client's fault, with
no traceback logged anywhere.

**After.** The service raises types that mean exactly one thing, and nothing
catches a supertype of them. `ValueError` survives at four sites, each wrapping a
single expression whose failure contract is known — `decode_token` in
`service.py` and `dependencies.py`, `uuid.UUID` in `dependencies.py`, and
Pydantic validation of a third party's payload in `router.py`:

```python
# src/auth/service.py — decode_token's documented failure signal
try:
    payload = decode_token(refresh_token)
except ValueError as exc:
    raise UnauthorizedError("Invalid refresh token") from exc

# src/auth/router.py — pydantic validation of a third party's payload
try:
    user_info = OAuthUserInfo.model_validate(user_info_data)
except ValueError as exc:
    raise BadRequestError("Invalid user info from Google") from exc
```

Anything else reaches `unhandled_exception_handler`: a 500, a generic message,
and `logger.exception` with the stack. The rate-limit stub was fixed to resolve
column defaults the way a real flush does, and its assertions tightened from a
set of four acceptable statuses to the one correct status.

**Verified by** `tests/test_auth_error_contract.py::test_incidental_value_error_is_not_laundered_into_a_401`,
which drives a `ValueError` out of the service and asserts the client sees a 500
with a generic message rather than an authentication failure.

## 3. SRP — an authentication dependency doing data access

**Before.** `get_current_user` built its own query against the `User` table:

```python
user_id: object = payload.get("sub")
result = await db.execute(select(User).where(User.id == user_id))
user = result.scalar_one_or_none()
```

`UserRepository` exists and is the only other place that reads `users`. Two
responsibilities in one function — verifying a token and knowing how users are
stored — and the repository convention broken in the one module most likely to
be copied.

The `object` annotation is the tell. `sub` arrives as a string, `User.id` is a
`UUID` column, and the comparison went to the database untyped. A signed token
carrying a non-UUID subject — one issued by an older service, or by any other
system sharing the key — pushed the problem down to the driver. Run against
Postgres 16, that query raises `asyncpg.exceptions.DataError: invalid input for
query argument`; nothing in `get_current_user` caught it, so it reached
`unhandled_exception_handler` as a 500. An unusable credential is a 401.

**After.** The lookup goes through the repository, and the claim is parsed into
the type it is supposed to be before anything touches the database:

```python
def _subject_to_uuid(raw: object) -> uuid.UUID:
    if not isinstance(raw, str):
        raise UnauthorizedError("Token is missing a subject")
    try:
        return uuid.UUID(raw)
    except ValueError as exc:
        raise UnauthorizedError("Token subject is not a valid user id") from exc


user = await UserRepository(db).get(_subject_to_uuid(payload.get("sub")))
```

`_subject_to_uuid` is a `str` → `UUID` conversion with a 401 on failure, and the
`object` annotation is gone.

**Verified by** `tests/test_dependencies.py::test_get_current_user_non_uuid_subject_raises_401`,
which asserts the database is never consulted, and `::test_get_current_user_valid_token`,
which asserts the load is a primary-key `get` with a real `UUID`.

---

## Deferred findings

Real, and left alone on purpose. Each has a `SPEC.md` item whose whole subject it
is; fixing it here would leave that item with nothing to do and this change with
a diff nobody can review as one idea.

### 4. DIP — `AuthService` constructs what it depends on

```python
def __init__(self, db: AsyncSession) -> None:
    self.users = UserRepository(db)
    self.tokens = RefreshTokenRepository(db)
```

Policy naming its own concrete collaborators. Nothing can substitute an
in-memory user store, so every service test needs a session stub faithful enough
to survive SQLAlchemy — which is exactly the stub that was silently wrong in
finding 2. `CLAUDE.md` already asks for the opposite ("typed against Protocols so
tests can override").

Owned by Phase 6, *"Dependency inversion via FastAPI `Depends` + protocol-typed
providers, overridable in tests"*.

### 5. OCP — storage is boto3, structurally

`src/storage/s3.py` exposes module-level functions over an `lru_cache`d boto3
client. Adding a local-disk or in-memory backend means editing this module and
every caller: closed to extension in the specific way OCP names.

Owned by Phase 6, *"Factory pattern: `StorageFactory` returning S3/local/memory
backends via `Protocol`"*.

### 6. ISP — a whole session for two methods

`AuthService` accepts an `AsyncSession` and uses `commit()` and `flush()`. It
depends on the entire session surface — `execute`, `scalars`, `merge`, the
connection — to reach two of them, which is what forces every test to construct
one. The narrow interface is a unit-of-work protocol.

Follows finding 4; it is the same seam and should be cut once, not twice.

---

## Checked and clean

- **LSP across the exception hierarchy.** Every `AppException` subclass widens
  `__init__` by adding a default and narrows nothing. `StorageError` (in
  `src/storage/s3.py`) is substitutable for `AppException` everywhere the
  handler expects one.
- **SRP in `src/health.py`.** Liveness and readiness are separate endpoints for a
  documented reason, and the split is right.
- **DIP in `src/repositories/base.py`.** `BaseRepository[ModelT: Base]` is
  parameterised over the model rather than hardcoding one, and the two concrete
  repositories add queries without overriding inherited behaviour.
- **OCP in `src/exception_handlers.py`.** A new error type needs a new
  `AppException` subclass and no handler change.

## API changes

Both are behaviour changes to a public contract, made deliberately.

| Endpoint | Condition | Before | After |
|---|---|---|---|
| `POST /api/v1/auth/register` | Email already registered | `400 BAD_REQUEST` | `409 CONFLICT` |
| `POST /api/v1/auth/login` | Credentials valid, account inactive | `401 UNAUTHORIZED` | `403 FORBIDDEN` |

Registering an address that already exists conflicts with the current state of
the resource; the request itself is well-formed, which is what 400 asserts. And
an inactive account has already passed authentication — 401 invites the client to
retry with credentials, which can never succeed, while 403 says the server
understood and refuses. 403 is also what `get_current_user` and the OAuth
callback returned for that condition all along.

The response envelope is unchanged — `{error, message, status}`, with `details`
when present. Auth failures now report a specific `error` code
(`CONFLICT`, `UNAUTHORIZED`, `FORBIDDEN`) where they previously reported
`HTTP_ERROR`, because they no longer travel as `HTTPException`.
