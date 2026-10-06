# Multi-tenancy

One database, one schema, a `tenant_id` on every tenant-scoped table, and a
PostgreSQL row-level security policy that compares it against a setting the
application binds per transaction.

The alternative designs are a schema per tenant and a database per tenant.
Both isolate harder; both make a migration an operation whose cost grows with
the number of customers, and both make "count active users across the estate"
a loop. This one keeps a single migration and a single connection pool, and
pays for it by making the boundary a thing that has to be *enforced* rather
than a thing that is structurally impossible. The rest of this document is
about getting that enforcement right, because almost every way of getting it
wrong is silent.

## The mechanism in four lines

```sql
-- what the policy reads
CREATE FUNCTION app_current_tenant_id() RETURNS uuid LANGUAGE sql STABLE AS $$
    SELECT NULLIF(current_setting('app.tenant_id', true), '')::uuid $$;

-- what the application sends, first statement of every transaction
SELECT set_config('app.tenant_id', $1, true);

-- what the database does with it
CREATE POLICY users_tenant_isolation ON users
    USING (tenant_id = app_current_tenant_id())
    WITH CHECK (tenant_id = app_current_tenant_id());
```

`src/tenancy/sql.py` holds all of it, `alembic/versions/0008_*` installs it,
and `src/tenancy/binding.py` sends the middle line.

Application code writes no tenant filters. That is the entire argument for
this approach over a `WHERE tenant_id = …` convention: a filter written four
hundred times is a filter forgotten once, and the one that is forgotten is
not a failed query, it is another customer's data in a response.

## Request to row

1. `TenantContextMiddleware` resolves the tenant — the `tid` claim of the
   access token if there is one, otherwise the `X-Tenant-ID` header — and
   enters `tenant_scope` for the request.
2. Anything that opens a transaction on any engine in the process fires the
   `begin` listener in `src/tenancy/binding.py`, which sends `set_config`.
3. Every statement in that transaction is filtered by the policies.
4. `COMMIT` ends the setting, because `set_config`'s third argument is
   `is_local => true`. The next transaction re-binds.

### Why the token is not the only source

`POST /api/v1/auth/login` finds a user by email, and that read is itself
tenant-scoped: with no tenant bound it returns nothing and every login fails.
Authentication cannot be what establishes the tenant when authentication is a
tenant-scoped read, so an unauthenticated request names its tenant in a
header.

That header is **routing information, not a credential**. Naming a tenant
gets you exactly what an anonymous caller may do in it, which is attempt a
login. Where the tenant is derived some other way — a per-tenant hostname
terminated at a proxy that sets the header on an internal hop — set
`TENANCY_TRUST_HEADER=false` and the fallback is gone.

A token and a header that disagree are a 403, never a silent preference for
the token: a client sending the wrong tenant is either broken or probing, and
answering it successfully with the right tenant's data hides the first and
rewards the second.

## Roles — the part that is silently wrong

Row-level security has one failure mode with no symptom: it is configured
perfectly and applies to nobody. Three ways in, all of them ordinary.

| Situation | Effect | Fix |
| --- | --- | --- |
| The application connects as a **superuser** | Every policy bypassed | Connect as a dedicated role |
| The role has **`BYPASSRLS`** | Every policy bypassed | `ALTER ROLE app NOBYPASSRLS` |
| The role **owns the tables** and they are not `FORCE`d | Every policy bypassed for that role | Migration 0008 sets `FORCE`; keep it |

The third is the trap, because it is what you get by running the migrations
and the application as the same user — which is what almost every deployment
starts out doing. Nothing errors. Queries return rows. Every test that
asserts a tenant can see its own data passes.

The layout that works:

```sql
-- owns the schema, runs migrations, exempt from policies on purpose
CREATE ROLE app_migrate LOGIN PASSWORD '…' BYPASSRLS;

-- what the application connects as: no ownership, no bypass
CREATE ROLE app LOGIN PASSWORD '…';
GRANT USAGE ON SCHEMA public TO app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO app;
ALTER DEFAULT PRIVILEGES FOR ROLE app_migrate IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app;
```

Check it, in the environment you are asking about, with the application's own
`DATABASE_URL`:

```
uv run python scripts/check_tenant_isolation.py
```

It reads the catalog, writes nothing, prints every problem it finds, and
exits non-zero when the boundary does not hold — so it belongs in a
deployment pipeline rather than in somebody's notes. `tests/test_tenancy_db.py`
runs the same check against CI's database, and connects as a purpose-made
unprivileged role so that its assertions mean something even though CI's own
`DATABASE_URL` is a superuser.

## Writing migrations after 0008

`FORCE ROW LEVEL SECURITY` applies to the migration too. A later migration
that touches rows in `tenants`, `users` or `refresh_tokens` will find it can
see none of them. Three ways out, best first:

1. **Run migrations as a `BYPASSRLS` role** (`app_migrate` above). Nothing in
   the migration has to know about tenancy. This is the recommended setup.
2. **Bind a tenant and loop.** Right when the change really is per-tenant:
   ```python
   for (tenant_id,) in op.get_bind().execute(sa.text("SELECT id FROM tenants")):
       op.get_bind().execute(
           sa.text("SELECT set_config('app.tenant_id', CAST(:t AS text), true)"),
           {"t": str(tenant_id)},
       )
       ...
   ```
3. **Lift and restore `FORCE`** around the data change. Works, and leaves a
   window in which an application connection owned by the same role is
   exempt. Use it only when 1 and 2 are both unavailable.

0008 itself does all its writing *before* it turns the policies on, which is
the same constraint seen from the other side.

## What is not tenant-scoped, and why

**`outbox_events`.** The relay (`src/outbox/relay.py`) drains it from a
background task that belongs to no request and therefore to no tenant. Under
a policy it would bind nothing, see nothing, and report an empty queue
forever. Running one relay per tenant is the alternative, and it turns one
background loop into one per customer. What the outbox carries instead is the
tenant inside the event payload; a subscriber that needs to touch
tenant-scoped data re-enters `tenant_scope` explicitly:

```python
with tenant_scope(event.payload["tenant_id"]):
    ...
```

**`alembic_version`.** Schema state, not customer data.

## Consequences you will meet

**An unbound query returns nothing.** `tenant_id = NULL` is never true, so a
request that resolved no tenant reads an empty table rather than the whole
one. This is the failure direction worth having: a feature that stops working
is reported in minutes, and one that quietly returns another customer's rows
is not.

**An unbound insert fails.** `users.tenant_id` is `NOT NULL` with a default
of `app_current_tenant_id()`, so an insert with no tenant bound is a not-null
violation rather than an orphan row. Every fixture in `tests/` runs inside
`tenant_scope` for this reason; see the `default_tenant` fixture in
`tests/conftest.py`.

**Email is unique per tenant, not globally.** `uq_users_tenant_email`
replaced the global unique index on `email`. A global one would let the first
customer to register `ops@acme.example` make that address unusable for every
other customer — and would say so, failing with a unique violation naming a
row the caller cannot see, which is an oracle for "does this person have an
account with one of your other customers".

**A cross-tenant read is a 404, not a 403.** The row is not visible, so there
is nothing to refuse. Answering 403 would make the boundary enumerable one
request at a time.

**One extra round trip per transaction**, and only for work that has a tenant
in scope — the listener returns immediately when there is none, so health
probes, the relay and migrations pay nothing. It cannot be pipelined with the
first real statement, because it has to be inside the same transaction. A
deployment where that matters wants fewer, longer transactions rather than a
different mechanism: binding at connection checkout instead and trusting
nothing to commit mid-request is the pooled-connection leak this design
exists to remove.

## Provisioning a tenant

Creating a tenant is not a tenant-scoped operation, and under the policy on
`tenants` a bound connection cannot even check whether a slug is taken — it
cannot see the rows that would take it. Provisioning therefore runs outside
the request path, as the migration role or another `BYPASSRLS` role:

```sql
INSERT INTO tenants (id, slug, name) VALUES (gen_random_uuid(), 'acme', 'Acme Inc');
```

The first user of a new tenant is created the same way, or by a signup flow
that is explicitly granted the ability to step outside a tenant. Deleting a
tenant cascades to its users and their refresh tokens.

## The bootstrap tenant

`00000000-0000-0000-0000-000000000001`, slug `default`. Migration 0008
creates it and backfills every pre-existing row into it, so an upgrade of a
single-tenant deployment keeps working with `X-Tenant-ID` set to that value.
It is an ordinary tenant in every other respect — nothing special-cases it,
no policy exempts it, and a deployment that never had single-tenant data can
delete it.

## Settings

| Setting | Default | What it does |
| --- | --- | --- |
| `TENANCY_HEADER` | `X-Tenant-ID` | The header an unauthenticated request names its tenant in |
| `TENANCY_TRUST_HEADER` | `true` | Whether that header is honoured at all |

There is deliberately no setting that turns tenancy off. The boundary is in
the database, not in a branch in this process; a flag here could not disable
it, only stop binding the tenant — under which every query returns nothing. A
single-tenant deployment runs with one tenant row, not with the feature
switched off.

## Where things are

| File | What |
| --- | --- |
| `src/tenancy/context.py` | The `ContextVar` and `tenant_scope` |
| `src/tenancy/binding.py` | The `begin` listener that sends `set_config` |
| `src/tenancy/sql.py` | The function, the setting name, the statements |
| `src/tenancy/resolver.py` | Token vs header precedence |
| `src/tenancy/middleware.py` | Per-request scope, and the refusals |
| `src/tenancy/dependencies.py` | `CurrentTenantDep` for routes that need to name it |
| `src/tenancy/isolation.py` | Does this database enforce it, for this role |
| `scripts/check_tenant_isolation.py` | The same, as a deployment gate |
| `alembic/versions/0008_*` | The schema, the backfill and the policies |
| `tests/test_tenancy_db.py` | The claims above, measured as an unprivileged role |
