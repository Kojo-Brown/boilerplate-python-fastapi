# Dependency injection

FastAPI's `Depends` is a dependency *injector*. On its own it does not invert
anything: a handler that asks for an `AsyncSession` and builds a
`UserRepository` from it has had its session injected and still names every
concrete class it uses. That was this codebase until now, and it is findings 4
and 6 of [solid.md](./solid.md).

Inversion is the second half: the thing being injected is described by a
protocol the *caller* owns, and the concrete class is named in one place that
nothing imports back.

## The shape

```
src/repositories/protocols.py   UserStore, RefreshTokenStore   — the ports
src/unit_of_work.py             UnitOfWork
src/events/base.py              EventPublisher
src/dependencies.py             providers + Annotated aliases  — the composition root
tests/fakes.py                  in-memory implementations
```

`AuthService` imports the first three and nothing else about persistence. It
does not import `sqlalchemy`, `src.database`, or either repository module —
`test_dependency_inversion.py` parses its imports and fails if one appears.

## Writing a handler

Ask for the alias:

```python
from src.dependencies import AuthServiceDep

@router.post("/login", response_model=TokenResponse)
async def login(data: LoginRequest, service: AuthServiceDep) -> TokenResponse:
    return await service.login(data.email, data.password)
```

`AuthServiceDep` is `Annotated[AuthService, Depends(get_auth_service)]`. Prefer
the annotated form over `service: AuthService = Depends(get_auth_service)`: a
parameter with a default cannot precede one without, so the older style makes
every handler order its parameters around FastAPI rather than around what it
takes.

The aliases available are `UserStoreDep`, `RefreshTokenStoreDep`,
`UnitOfWorkDep`, `EventPublisherDep`, `AuthServiceDep`, `StorageDep`,
`PaymentGatewayDep` and `DbSession`. Reach for `DbSession` only when you
genuinely need the session — a handler that wants two rows wants a store.

## Writing a provider

A provider is a plain function annotated with a protocol, returning an
implementation:

```python
def get_user_store(db: DbSession) -> UserStore:
    return UserRepository(db)
```

mypy is what makes this load-bearing. The annotation is the protocol, the return
is the class, so the day `UserRepository` stops satisfying `UserStore` the build
fails here rather than at a call site three modules away.

Two rules for anything added to `src/dependencies.py`:

**Take the session through `get_db`.** `get_user_store`,
`get_refresh_token_store` and `get_unit_of_work` all declare `Depends(get_db)`
and all three receive the *same* session, because FastAPI caches a dependency's
result per request, keyed on the callable. That is not an optimisation — three
separate sessions would mean the unit of work committing a transaction the
repositories never wrote to, and a registration answering `201` while persisting
nothing. `test_one_request_gets_one_session` pins it.

**Cache what owns a connection pool, per process, not per request.**
`get_storage` and `get_payment_gateway` are `lru_cache`d beside their factories:
both adapters hold an `httpx.AsyncClient`, and rebuilding one per request throws
away the pool that made it worth having — and, for PayPal, the cached OAuth
token with it, spending an extra round trip on every payment. `get_event_publisher`
returns the process-wide bus for a different reason: subscribers are registered
once from the lifespan, so a bus built per request would have none of them.

## Overriding in tests

`app.dependency_overrides` is keyed on the **callable inside `Depends`**, not on
the alias:

```python
app.dependency_overrides[get_user_store] = lambda: InMemoryUserStore()
```

Override the narrowest provider that removes what you are trying to avoid.

| Override | Keeps | Removes |
|---|---|---|
| `get_auth_service` | the route, its validation and rate limit | the service's own wiring |
| `get_user_store` etc. | the real `AuthService` | only the database |
| `get_db` | every real provider | only the engine |

Use a lambda, not the class. FastAPI inspects whatever it is handed as a
dependency callable, so `dependency_overrides[get_user_store] = InMemoryUserStore`
turns that class's `__init__` parameters into request parameters — the seeding
argument becomes a query field of type `list[User] | None`, and the app fails to
build its response model rather than failing the assertion you wrote.

`tests/conftest.py` provides the common wiring: `user_store`, `token_store`,
`uow`, `publisher`, an `auth_service` composed from all four, and
`fake_backed_client`, whose auth routes run the real service over in-memory
stores with no database in the resolved tree at all.

## What the fakes are and are not

`tests/fakes.py` holds `InMemoryUserStore`, `InMemoryRefreshTokenStore`,
`RecordingUnitOfWork` and `CollectingPublisher`. They are lists with methods.
That is the point — the stub they replaced had to answer `session.execute()` with
an object whose `scalar_one_or_none()` returned the next row in a `side_effect`
list, so adding a lookup to the service shifted every later answer onto the
wrong question, silently.

They do not emulate a database: no unique constraint, no cascade, no isolation,
and `RecordingUnitOfWork` cannot roll back. A test that turns on any of those is
an integration test and belongs against the Postgres service in CI. `flush()`
being a no-op is honest rather than lazy — what it buys against a real session is
read-your-writes, and an in-memory store already has it.

## What this does not do

**No DI container, and no service locator.** The composition root is a module of
functions. A container would buy lifecycle management this app does not need —
FastAPI already scopes per request and `lru_cache` handles per process — at the
cost of a registry nothing type-checks.

**The protocols still name the SQLAlchemy models.** `UserStore.get` returns
`User | None`, the ORM class. Those classes are this codebase's domain entities
as well as its rows, and splitting them is a much larger change that would not
buy what this seam is for: the cost of a concrete model is that a fake must
construct one, which needs no session; the cost of a concrete *repository* was
that a fake had to be one.

**`AuthService` is the only service inverted.** It is the only one that had this
problem — storage, notifications and payments arrived with their protocols
already. A second service should follow the same shape: a protocol per
collaborator, a provider per protocol, both in the two files above.
