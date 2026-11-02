"""Bring the suite's own Postgres and Redis up, when nothing else has.

Fourteen modules in this package talk to a real server rather than a fake —
`tests/test_tenancy_db.py` asks Postgres to enforce a row-level security policy,
`tests/test_idempotency_contract.py` asks Redis whether `SET NX` is atomic — and
none of those claims can be made against an emulator. Every one of them skips
when there is nothing listening, and CI supplies service containers, so the
claims are measured on every pull request and nowhere else.

That gap is not hypothetical. The Phase 0 note in `SPEC.md` records
`tests/test_distributed_lock_redis.py` being red locally for weeks while CI was
green, because the gate `CLAUDE.md` tells an agent to run was quietly executing
a smaller suite than the gate that blocks a merge. Measured on this branch
before the change: **195 of 316 tests in those fourteen modules skipped** on a
machine with Docker and no servers running.

So this module starts them. A developer with a Docker socket gets the same 316
tests CI runs, having provisioned nothing.

Three backends, chosen by `INTEGRATION_BACKEND`:

``external``
    Use `DATABASE_URL` and `REDIS_URL` exactly as given and never touch Docker.
    This is what the `test` CI job sets, so the service containers it declares
    are what the suite talks to and this module is a no-op there. Today's
    behaviour, unchanged.

``testcontainers``
    Always start containers, ignoring anything already listening, and **fail**
    rather than skip when there is no Docker. This is what the `integration` CI
    job sets. The hard failure is the point of that job: a container path that
    silently skipped would rot exactly the way the Redis suite above did.

``auto`` (the default)
    Reuse a reachable server if there is one, start containers if there is not
    and Docker works, and otherwise leave the environment alone so the existing
    per-test skips fire. A developer gets the full suite without choosing
    anything, and a machine with neither servers nor Docker behaves as it
    always has.

Readiness is probed with `asyncpg` and `redis.asyncio` — the drivers the
application itself uses — rather than with a TCP connect. A listening socket is
not a Postgres that will accept a connection, and the window between the two is
most of a container's start-up. See `docs/integration-testing.md`.
"""

from __future__ import annotations

import asyncio
import atexit
import os
import pathlib
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Coroutine
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Final
from urllib.parse import urlparse

if TYPE_CHECKING:
    from testcontainers.core.container import DockerContainer

#: Images the containers are built from. Both match the services the `test` CI
#: job declares, which matters more than the size of the download: `postgres:17`
#: rather than `postgres:17-alpine` because alpine is musl, and musl's `strcoll`
#: orders text differently from glibc's — a suite that asserts on `ORDER BY`
#: would then disagree with CI for a reason no one would look for. Overridable
#: for the same reason anything here is: so a bisect against another version
#: needs no edit.
POSTGRES_IMAGE: Final = os.environ.get("INTEGRATION_POSTGRES_IMAGE", "postgres:17")
REDIS_IMAGE: Final = os.environ.get("INTEGRATION_REDIS_IMAGE", "redis:7-alpine")

#: Credentials for a container that exists for the length of one pytest run and
#: is reachable only from this machine on an ephemeral port. Obviously fake, and
#: deliberately not the `postgres/postgres` pair CI uses, so that a URL leaking
#: into a log is recognisably this and not a real environment's.
_PG_USER: Final = "integration"
_PG_PASSWORD: Final = "mock-integration-password"
_PG_DB: Final = "boilerplate_integration"

_PG_PORT: Final = 5432
_REDIS_PORT: Final = 6379

#: How long a freshly started container gets to accept a driver connection.
#: Postgres runs its first-boot `initdb` before it listens, which is seconds on
#: a warm page cache and considerably more on a cold CI runner.
_READY_TIMEOUT_SECONDS: Final = 90.0
_READY_POLL_SECONDS: Final = 0.25

#: How long `alembic upgrade head` gets. Generous: it is nine migrations
#: against an empty database, which is about a second here, but it runs on cold
#: CI runners too and a false timeout would be a flake in every job.
_MIGRATE_TIMEOUT_SECONDS: Final = 300.0

#: The project root — the parent of `tests/`. Resolved from this file rather
#: than from the working directory, which pytest does not promise.
_PROJECT_ROOT: Final = pathlib.Path(__file__).resolve().parent.parent


class Backend(Enum):
    """How `provision` should obtain its servers."""

    AUTO = "auto"
    EXTERNAL = "external"
    TESTCONTAINERS = "testcontainers"


class Action(Enum):
    """What `decide` concluded for one service."""

    #: Talk to whatever the environment already points at.
    REUSE = "reuse"
    #: Start a container and point the suite at it.
    START = "start"
    #: Change nothing; the per-test reachability guards will skip.
    SKIP = "skip"
    #: Refuse to run: containers were demanded and Docker cannot supply them.
    FAIL = "fail"


class IntegrationBackendUnavailableError(RuntimeError):
    """`INTEGRATION_BACKEND=testcontainers` was set and Docker is not usable."""


@dataclass(frozen=True)
class Provisioned:
    """What `provision` did, per service, for the session report."""

    actions: dict[str, Action]
    database_url: str | None
    redis_url: str | None

    @property
    def started(self) -> tuple[str, ...]:
        return tuple(
            name for name, action in self.actions.items() if action is Action.START
        )

    @property
    def environment(self) -> dict[str, str]:
        """The variables the caller should publish into `os.environ`.

        Returned rather than written, so `provision` has no global side effect
        and `tests/conftest.py` applies it with one visible statement. That also
        happens to be what keeps the file lint-clean: ruff's E402 permits an
        `os.environ` mutation before a module's imports — which is the whole
        shape of that conftest — and nothing else, so a bare call there would
        make every import below it an error.

        Only names a URL for a service we started. A `REUSE` needs no
        publishing, since the value came from the environment to begin with, and
        a `SKIP` deliberately leaves the placeholder defaults to stand.
        """
        published: dict[str, str] = {}
        if self.actions.get("postgres") is Action.START and self.database_url:
            published["DATABASE_URL"] = self.database_url
        if self.actions.get("redis") is Action.START and self.redis_url:
            published["REDIS_URL"] = self.redis_url
        return published


def parse_backend(raw: str | None) -> Backend:
    """Read `INTEGRATION_BACKEND`, defaulting to `auto`.

    An unrecognised value raises rather than falling back. The whole purpose of
    the `testcontainers` setting is to turn a silent skip into a failure, and a
    typo that quietly selected `auto` would hand that back.
    """
    if raw is None or raw == "":
        return Backend.AUTO
    try:
        return Backend(raw.strip().lower())
    except ValueError:
        permitted = ", ".join(repr(b.value) for b in Backend)
        raise ValueError(
            f"INTEGRATION_BACKEND={raw!r} is not one of {permitted}"
        ) from None


def decide(backend: Backend, *, listening: bool, docker_available: bool) -> Action:
    """The whole policy, as a pure function of three facts.

    Separated from the IO so the table above is testable without a Docker
    socket, which is the only way the `no Docker` rows can be checked at all on
    a machine that has one. `tests/test_containers.py` covers all twelve
    combinations.
    """
    if backend is Backend.EXTERNAL:
        return Action.REUSE
    if backend is Backend.TESTCONTAINERS:
        return Action.START if docker_available else Action.FAIL
    # AUTO
    if listening:
        return Action.REUSE
    return Action.START if docker_available else Action.SKIP


def port_is_listening(url: str, default_port: int, timeout: float = 1.0) -> bool:
    """Whether something holds the port `url` names.

    A TCP connect, not a driver handshake, and that asymmetry with
    `_wait_until_ready` is deliberate. Here the question is "has a human
    already provided a server?", where a listener is the honest answer and a
    handshake would make `auto` depend on credentials. Once we have started a
    container ourselves the question is "can the driver connect *yet*?", which
    only the driver can answer.
    """
    parsed = urlparse(url)
    try:
        with socket.create_connection(
            (parsed.hostname or "localhost", parsed.port or default_port),
            timeout=timeout,
        ):
            return True
    except OSError:
        return False


def running_under_mutmut() -> bool:
    """Whether a mutation run is what imported us.

    The mutation gate selects seven test files that touch no Postgres, Redis or
    Kafka — that is the whole reason the `mutation` CI job declares no services
    and finishes in minutes. Starting a database for it would add a container
    and an `alembic upgrade head` to a run that cannot reach either, and would
    make a gate that needs no Docker refuse to start without one.

    `sys.modules` rather than an environment variable because mutmut calls
    `pytest.main()` *in this process* — `mutmut_src_layout.py` documents the
    same fact for its own reasons — so by the time a conftest is imported,
    mutmut is unambiguously already loaded. It also means a developer running
    `uv run mutmut run` locally gets the same treatment as CI without setting
    anything, which is the point: the `mutation` job also sets
    `INTEGRATION_BACKEND=external` explicitly, and this is what makes the two
    agree when nobody remembers to.

    Only consulted for `auto`. An explicit `testcontainers` is still honoured,
    so there is a way to run the mutation selection against real servers if a
    future `only_mutate` entry ever needs one.
    """
    return "mutmut" in sys.modules


def docker_is_available() -> bool:
    """Whether a Docker daemon will answer.

    `docker.from_env()` succeeding is not enough — it builds a client against
    `DOCKER_HOST` without dialling it — so this pings. Broad `except` because
    the failure modes are a socket that is absent, present and dead, or present
    and refusing authorisation, and they surface as `DockerException`,
    `OSError`, and `urllib3` errors respectively.
    """
    try:
        import docker

        client = docker.from_env()
    except Exception:
        return False
    try:
        client.ping()
        return True
    except Exception:
        return False
    finally:
        # Best-effort: the answer above is what the caller wanted, and a
        # socket that will not close cleanly does not change it.
        with suppress(Exception):
            client.close()


# --- the IO ----------------------------------------------------------------

#: Containers this process started, newest first, so `atexit` tears them down in
#: reverse order of creation.
_started: list[DockerContainer] = []

#: What the last `provision()` concluded. Kept here rather than assigned in
#: `tests/conftest.py` for the E402 reason in `Provisioned.environment`: that
#: file cannot hold a module-level assignment above its imports.
_last: Provisioned | None = None


def last_provisioned() -> Provisioned:
    """What `provision()` decided, for the session report header."""
    if _last is None:
        raise RuntimeError("provision() has not been called in this process")
    return _last


def _register_teardown() -> None:
    atexit.register(_stop_all)


def _stop_all() -> None:
    while _started:
        container = _started.pop()
        # Best-effort by necessity: this runs from `atexit`, where raising
        # would replace the suite's own exit status with a teardown error.
        # Ryuk is the backstop for anything this misses.
        with suppress(Exception):
            container.stop()


async def _postgres_accepts(dsn: str) -> bool:
    import asyncpg

    try:
        conn = await asyncpg.connect(dsn, timeout=5)
    except Exception:
        return False
    try:
        # `bool(...)` rather than returning the comparison directly: asyncpg
        # ships no `py.typed`, so `fetchval` is `Any` and the `==` with it.
        return bool(await conn.fetchval("SELECT 1") == 1)
    finally:
        await conn.close()


async def _redis_accepts(url: str) -> bool:
    import redis.asyncio as redis_asyncio

    client = redis_asyncio.Redis.from_url(url)
    try:
        return bool(await client.ping())
    except Exception:
        return False
    finally:
        await client.aclose()


#: A readiness probe: given a URL, whether the real driver can round-trip a
#: trivial statement against it.
Probe = Callable[[str], Coroutine[Any, Any, bool]]


def _wait_until_ready(service: str, probe: Probe, target: str) -> None:
    """Poll until the driver connects, or give up loudly.

    A timeout here is a failure, never a skip: we started the container, so
    nothing about the environment excuses it not coming up.
    """
    deadline = time.monotonic() + _READY_TIMEOUT_SECONDS
    while True:
        if asyncio.run(probe(target)):
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"the {service} container did not accept a driver connection "
                f"within {_READY_TIMEOUT_SECONDS:.0f}s"
            )
        time.sleep(_READY_POLL_SECONDS)


def _start_postgres() -> str:
    from testcontainers.core.container import DockerContainer

    container = (
        DockerContainer(POSTGRES_IMAGE)
        .with_env("POSTGRES_USER", _PG_USER)
        .with_env("POSTGRES_PASSWORD", _PG_PASSWORD)
        .with_env("POSTGRES_DB", _PG_DB)
        # `initdb` runs fsync per file by default, which is most of a cold
        # start. The data directory dies with the container, so durability
        # buys nothing here.
        .with_env("POSTGRES_INITDB_ARGS", "--nosync")
        .with_exposed_ports(_PG_PORT)
    )
    container.start()
    _started.append(container)

    host = container.get_container_host_ip()
    port = container.get_exposed_port(_PG_PORT)
    credentials = f"{_PG_USER}:{_PG_PASSWORD}@{host}:{port}/{_PG_DB}"

    # asyncpg's own DSN form, which carries no SQLAlchemy dialect suffix.
    _wait_until_ready("postgres", _postgres_accepts, f"postgresql://{credentials}")
    return f"postgresql+asyncpg://{credentials}"


def _start_redis() -> str:
    from testcontainers.core.container import DockerContainer

    container = DockerContainer(REDIS_IMAGE).with_exposed_ports(_REDIS_PORT)
    container.start()
    _started.append(container)

    host = container.get_container_host_ip()
    port = container.get_exposed_port(_REDIS_PORT)
    url = f"redis://{host}:{port}/0"
    _wait_until_ready("redis", _redis_accepts, url)
    return url


def _migrate(database_url: str) -> None:
    """Run `alembic upgrade head` against a container we just started.

    A subprocess rather than `alembic.command.upgrade`, for two reasons that
    both bite in-process. `alembic/env.py` reads `settings.DATABASE_URL` from
    the already-imported singleton, and it calls `fileConfig(...)`, which
    reconfigures the root logger — `tests/conftest.py` spends a 40-line fixture
    working around exactly that kind of clobbering. A subprocess inherits the
    environment and leaves this one's logging alone. It is also the same command
    the `test` CI job runs, so there is one migration path rather than two.

    `create_all` is not an alternative. `tests/test_tenancy_db.py` asserts on
    `FORCE ROW LEVEL SECURITY` and the policies, which exist only in migration
    `0008`, and it fails loudly rather than skipping when they are absent —
    correctly, since a database without them turns its every assertion into a
    tautology.
    """
    try:
        completed = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            env={**os.environ, "DATABASE_URL": database_url},
            # `alembic.ini` sets `script_location = alembic` and
            # `prepend_sys_path = .`, both relative, so the command has to run
            # from the project root rather than from wherever pytest was
            # invoked.
            cwd=_PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=False,
            # Output is captured, so a migration that hangs would otherwise
            # hang collection with nothing on screen to explain it.
            timeout=_MIGRATE_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as expired:
        raise RuntimeError(
            "`alembic upgrade head` did not finish within "
            f"{_MIGRATE_TIMEOUT_SECONDS:.0f}s against the Postgres container."
        ) from expired

    if completed.returncode != 0:
        raise RuntimeError(
            "`alembic upgrade head` failed against the Postgres container.\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )


def provision() -> Provisioned:
    """Decide, then act, for both services. Called once from `tests/conftest.py`.

    Returns the URLs it started in `Provisioned.environment` for the caller to
    publish; it writes nothing to `os.environ` itself. The caller has to publish
    them before importing anything under `src.*`, because `src.config` builds a
    settings singleton at import time and the fourteen modules read the URL off
    that singleton rather than out of the environment — set any later and it is
    read after it was frozen. `tests/conftest.py` has the full ordering note.
    """
    backend = parse_backend(os.environ.get("INTEGRATION_BACKEND"))

    if backend is Backend.AUTO and running_under_mutmut():
        backend = Backend.EXTERNAL

    if backend is Backend.EXTERNAL:
        return _record(
            Provisioned(
                actions={"postgres": Action.REUSE, "redis": Action.REUSE},
                database_url=os.environ.get("DATABASE_URL"),
                redis_url=os.environ.get("REDIS_URL"),
            )
        )

    # One ping for both services rather than one each.
    docker_available = docker_is_available()

    configured_pg = os.environ.get("DATABASE_URL")
    configured_redis = os.environ.get("REDIS_URL")

    def configured_and_listening(url: str | None, default_port: int) -> bool:
        """An unset variable is nothing to reuse, so it is not listening.

        `tests/conftest.py` keeps its two URL placeholders *below* the call to
        `provision`, so an absent variable here means genuinely unconfigured
        rather than defaulted — which is the distinction that lets `auto` start
        a container instead of probing `localhost` for a placeholder.
        """
        return url is not None and port_is_listening(url, default_port)

    pg_action = decide(
        backend,
        listening=configured_and_listening(configured_pg, _PG_PORT),
        docker_available=docker_available,
    )
    redis_action = decide(
        backend,
        listening=configured_and_listening(configured_redis, _REDIS_PORT),
        docker_available=docker_available,
    )

    if Action.FAIL in (pg_action, redis_action):
        raise IntegrationBackendUnavailableError(
            "INTEGRATION_BACKEND=testcontainers requires a reachable Docker "
            "daemon and none answered. Start Docker, or set "
            "INTEGRATION_BACKEND=external to use DATABASE_URL and REDIS_URL "
            "as given. Refusing to skip: this backend exists so that a missing "
            "server fails the build instead of quietly shrinking the suite."
        )

    if Action.START in (pg_action, redis_action):
        _register_teardown()

    if pg_action is Action.START:
        configured_pg = _start_postgres()
        # The URL is passed to the migration rather than read back out of
        # `os.environ`, so the subprocess cannot disagree with the container we
        # just started no matter when the caller publishes it.
        _migrate(configured_pg)

    if redis_action is Action.START:
        configured_redis = _start_redis()

    return _record(
        Provisioned(
            actions={"postgres": pg_action, "redis": redis_action},
            database_url=configured_pg,
            redis_url=configured_redis,
        )
    )


def _record(provisioned: Provisioned) -> Provisioned:
    global _last
    _last = provisioned
    return provisioned
