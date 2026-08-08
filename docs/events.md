# Domain events: an async bus with typed subscribers

`AuthService.register` creates a user and publishes `UserRegistered`. It does
not know that a welcome email follows, and nothing in `src/auth` imports the
code that sends one. That is the whole trade: the cause and its consequences
stop being written in the same place, so a consequence can be added, removed or
replaced without touching the cause.

```python
from src.events import UserRegistered, event_bus

@event_bus.on(UserRegistered)
async def warm_the_dashboard_cache(event: UserRegistered) -> None:
    ...
```

Everything lives in `src/events`: `base.py` (the event contract), `bus.py`
(dispatch), `catalog.py` (what this application publishes) and
`subscribers.py` (what it does about it).

## Defining an event

```python
@dataclass(frozen=True, kw_only=True)
class InvoicePaid(DomainEvent):
    event_name: ClassVar[str] = "invoice.paid"   # optional; defaults to the class name

    invoice_id: str
    amount_cents: int
```

| Field | Where it comes from |
|---|---|
| `event_id` | uuid4, assigned at construction. Overridable in tests. |
| `occurred_at` | `datetime.now(UTC)` at construction. Overridable in tests. |
| `event_name` | `ClassVar`, defaults to the class name. Pin it once the value is written somewhere durable. |

Events are frozen and keyword-only. Frozen because subscribers run
concurrently and a mutable event would let the first one to be scheduled
rewrite what the others see; keyword-only because the base carries defaulted
fields and every subclass adds required ones, which is a `TypeError` at
class-definition time without it.

Carry facts, not handles. By the time subscribers run the transaction has
committed and the session is closed, so an event holding an ORM row hands every
subscriber a detached object. A handler that needs the full row should load it
in its own session and accept that the row may have moved on.

## Subscribing

```python
subscription = event_bus.subscribe(UserRegistered, send_welcome, timeout=5.0)
subscription.unsubscribe()
```

| Argument | Meaning |
|---|---|
| `event_type` | The class to observe. **Subclasses match too.** |
| `handler` | `async def` taking that event. A sync handler is refused at registration. |
| `name` | Label in logs and outcomes. Defaults to the handler's `module.qualname`. |
| `timeout` | Seconds before the handler is cancelled and recorded as failed. |

The type checker ties the first two together — a handler annotated
`UserLoggedIn` cannot be registered for `UserRegistered`, and inside the
handler the event is the concrete type, not `DomainEvent`.

Subclass dispatch is what makes cross-cutting subscribers ordinary:
`subscribe(UserEvent, audit)` sees every user event including ones added later,
and `subscribe(DomainEvent, audit)` sees everything.

Registration happens in the FastAPI lifespan (`src/main.py` →
`register_default_subscribers()`), never at import. Importing a module should
not quietly start sending mail, and a unit test gets an empty bus unless it
asks for one.

## Publishing

```python
result = await event_bus.publish(UserRegistered(user_id=..., email=...))
```

**Publish after the commit.** A subscriber that fires inside the transaction is
reacting to state the database may still roll back. Every publish in
`AuthService` sits after `await self.db.commit()` for that reason.

**`publish` awaits its subscribers.** It returns once all of them have
finished, so the request that published pays for its observers. Firing them
into `asyncio.create_task` instead would drop exceptions nothing is holding,
allow the task to be collected mid-flight, and outlive the request scope whose
context it borrowed. Work that should not be paid for inline belongs in a
subscriber that *enqueues* — `send_welcome_email_on_registration` calls a
Celery task and returns.

**Subscribers run concurrently and fail independently.** One that raises
neither cancels its siblings nor propagates to the publisher; the exception is
logged as `events.subscriber_failed` and recorded in the result. A caller who
wants the failure to matter asks:

```python
result = await event_bus.publish(event)
result.raise_for_failures()      # EventDispatchError, chained to the first failure
```

| `PublishResult` | |
|---|---|
| `outcomes` | One `SubscriberOutcome` per subscriber: `subscriber`, `ok`, `duration_ms`, `error`. |
| `failures` | The ones that raised. |
| `delivered` | How many completed. |
| `ok` | No failures. |
| `raise_for_failures()` | Raise `EventDispatchError`, or return quietly. |

There is no "run these in order" mode. If B must observe the world A left
behind, that is a sequencing requirement the observer pattern does not express,
and the honest encoding is one subscriber that calls both.

Cancellation is not a subscriber failure: if the publishing task is cancelled,
the handlers are cancelled with it and `CancelledError` propagates, because
nothing is waiting for the answer any more. A handler that merely overran its
`timeout` is a different thing and is recorded as a failure.

## Nesting

A subscriber may publish — that is how a domain reacts to itself. What it may
not do is form a cycle: each hop is an `await` inside the previous one, so a
ring shows up as a request that never returns. Publishes are counted in a
`ContextVar` and the eighth nested one raises `EventCycleError` naming the
event that closed the ring. `EventBus(max_depth=...)` changes the cap.

## What this bus is not

It is in-process and it forgets. Nothing is persisted, so a crash between the
commit and the subscribers loses the reaction, and a second application
instance never sees the event at all. The durable answer is the transactional
outbox — write an event row in the same transaction as the state change, relay
it afterwards — which is a separate item with a separate set of tradeoffs.
Until then: anything that must not be lost should be enqueued to Celery by its
subscriber, where the broker owns the durability.

## Testing

Build a bus per test rather than registering against the process-wide one:

```python
bus = EventBus()
seen: list[DomainEvent] = []

async def record(event: DomainEvent) -> None:
    seen.append(event)

bus.subscribe(DomainEvent, record)
service = AuthService(db, events=bus)
```

`AuthService` takes the bus as an argument for exactly this reason.
`EventBus(timer=...)` injects the clock behind `duration_ms`, so assertions
about durations do not need a `sleep`.

## Events this application publishes

| Event | When | Fields |
|---|---|---|
| `UserRegistered` | An account exists and is committed | `user_id`, `email`, `via` (`"password"` / `"oauth"`) |
| `UserLoggedIn` | Credentials accepted, tokens issued | `user_id`, `email`, `method` (`"password"` / `"oauth"`) |

A first OAuth sign-in publishes both. A token refresh publishes neither — a
rotation is the same session continuing, and counting it as a login would make
"last seen" mean "last polled".

## Subscribers this application ships

| Subscriber | Observes | Does |
|---|---|---|
| `audit.user_activity` | `UserEvent` | One structured log line per user event; no address, since logs outlive accounts |
| `email.welcome` | `UserRegistered` | Enqueues the Celery welcome-email task, with a 5s timeout on the broker call |

Add one by adding a `SubscriberSpec` to `DEFAULT_SUBSCRIBERS` in
`src/events/subscribers.py`. No publisher changes.
