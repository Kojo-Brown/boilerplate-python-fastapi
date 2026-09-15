"""Which checks a given configuration produces, and how critical each one is.

This is where the interesting decisions live: an in-process backend produces no
check at all, four subsystems sharing one Redis produce one check rather than
four, and `IDEMPOTENCY_FAIL_OPEN` decides on its own whether an unreachable
store is a 503 or a footnote.

Every test builds its own `Settings` rather than mutating the global one, for
the reason spelled out in `src/config.py`.
"""

from __future__ import annotations

import pytest

from src.config import Settings
from src.health.wiring import (
    _redis_client_factory,
    build_redis_checks,
    build_registry,
    get_health_registry,
    redis_uses,
)
from tests.fakes import FakeEngine

# Obviously fake, and never connected to by these tests: nothing here opens a
# socket, because a check builds its client on the first probe.
FAKE_DSN = "postgresql+asyncpg://fake:fake@localhost:5432/fake"


def make_settings(**overrides: object) -> Settings:
    """A `Settings` pinned to the repository defaults, plus the overrides.

    Every field these tests read is named explicitly rather than left to the
    environment: `tests/conftest.py` exports `IDEMPOTENCY_BACKEND=memory` for
    the app under test, and a helper that inherited it would quietly assert
    against a configuration no deployment has.
    """
    fields: dict[str, object] = {
        "DATABASE_URL": FAKE_DSN,
        "SECRET_KEY": "test-secret-key-not-a-real-one",
        "REDIS_URL": "redis://localhost:6379/0",
        "IDEMPOTENCY_ENABLED": True,
        "IDEMPOTENCY_BACKEND": "redis",
        "IDEMPOTENCY_REDIS_URL": "",
        "IDEMPOTENCY_FAIL_OPEN": False,
        "DISTRIBUTED_LOCK_BACKEND": "redis",
        "DISTRIBUTED_LOCK_REDIS_URL": "",
        "REDIS_STREAMS_BACKEND": "memory",
        "REDIS_STREAMS_URL": "",
    }
    fields.update(overrides)
    return Settings(**fields)  # type: ignore[arg-type]


# ── Which Redis dependencies exist ────────────────────────────────────────────


def test_celery_is_always_a_redis_dependency() -> None:
    """No switch guards it: `src/worker.py` builds the app from REDIS_URL.

    And `src/auth/router.py` enqueues through it inside the register handler,
    which is what makes it required rather than background.
    """
    uses = redis_uses(make_settings())

    celery = next(use for use in uses if use.subsystem == "celery")
    assert celery.criticality == "required"
    assert celery.url == "redis://localhost:6379/0"


def test_an_in_process_backend_contributes_no_dependency() -> None:
    """There is nothing to round-trip, and a check that cannot fail is noise."""
    uses = redis_uses(
        make_settings(
            IDEMPOTENCY_BACKEND="memory",
            DISTRIBUTED_LOCK_BACKEND="memory",
            REDIS_STREAMS_BACKEND="memory",
        )
    )

    assert [use.subsystem for use in uses] == ["celery"]


def test_a_disabled_idempotency_middleware_contributes_no_dependency() -> None:
    uses = redis_uses(
        make_settings(IDEMPOTENCY_ENABLED=False, IDEMPOTENCY_BACKEND="redis")
    )

    assert "idempotency" not in {use.subsystem for use in uses}


def test_fail_open_decides_whether_the_idempotency_store_is_required() -> None:
    """The clearest case for reading criticality out of configuration.

    Fail-open off means every request carrying an `Idempotency-Key` is refused
    while the store is down — a required dependency. Fail-open on means those
    requests are served without deduplication, which is a degradation the
    operator chose and must not cost the process its place in the load
    balancer.
    """
    strict = redis_uses(make_settings(IDEMPOTENCY_FAIL_OPEN=False))
    permissive = redis_uses(make_settings(IDEMPOTENCY_FAIL_OPEN=True))

    assert next(u for u in strict if u.subsystem == "idempotency").criticality == (
        "required"
    )
    assert next(u for u in permissive if u.subsystem == "idempotency").criticality == (
        "optional"
    )


def test_the_lock_backend_is_required_and_streams_are_not() -> None:
    """A handler that cannot take a lock fails; a stream consumer only lags."""
    uses = {
        use.subsystem: use
        for use in redis_uses(
            make_settings(
                DISTRIBUTED_LOCK_BACKEND="redis", REDIS_STREAMS_BACKEND="redis"
            )
        )
    }

    assert uses["distributed-lock"].criticality == "required"
    assert uses["redis-streams"].criticality == "optional"


def test_a_subsystems_own_url_override_is_honoured() -> None:
    uses = {
        use.subsystem: use
        for use in redis_uses(
            make_settings(IDEMPOTENCY_REDIS_URL="redis://idempotency:6379/1")
        )
    }

    assert uses["idempotency"].url == "redis://idempotency:6379/1"
    assert uses["celery"].url == "redis://localhost:6379/0"


# ── Turning dependencies into checks ──────────────────────────────────────────


def test_subsystems_sharing_one_server_produce_one_check() -> None:
    """Probing the same server four times per poll would report it four times.

    And multiply this endpoint's load on it by four, which is the opposite of
    what a probe against a struggling dependency should do.
    """
    checks = build_redis_checks(
        make_settings(DISTRIBUTED_LOCK_BACKEND="redis", REDIS_STREAMS_BACKEND="redis")
    )

    assert [check.name for check in checks] == ["redis"]
    assert checks[0].description == (
        "redis, used by celery, distributed-lock, idempotency, redis-streams"
    )


def test_the_shared_check_takes_the_strongest_criticality_of_its_users() -> None:
    """One required user makes the server required, whatever shares it.

    Streams alone would be optional; Celery on the same server is not, and the
    process cannot serve traffic without it.
    """
    checks = build_redis_checks(
        make_settings(IDEMPOTENCY_BACKEND="memory", REDIS_STREAMS_BACKEND="redis")
    )

    assert [check.criticality for check in checks] == ["required"]


def test_separate_servers_produce_separate_checks_named_after_their_users() -> None:
    """A name is the only stable way to tell two servers apart in the body.

    The URL is not available for it: the response is unauthenticated, and a
    hostname is the least of what a Redis URL carries.
    """
    checks = build_redis_checks(
        make_settings(
            REDIS_STREAMS_BACKEND="redis",
            REDIS_STREAMS_URL="redis://streams:6379/0",
        )
    )

    assert sorted(check.name for check in checks) == [
        "redis:celery+distributed-lock+idempotency",
        "redis:redis-streams",
    ]


def test_a_split_out_optional_server_is_optional_on_its_own() -> None:
    """The point of splitting: streams going down stops being a 503."""
    checks = {
        check.name: check
        for check in build_redis_checks(
            make_settings(
                REDIS_STREAMS_BACKEND="redis",
                REDIS_STREAMS_URL="redis://streams:6379/0",
            )
        )
    }

    assert checks["redis:redis-streams"].criticality == "optional"
    assert checks["redis:celery+distributed-lock+idempotency"].criticality == (
        "required"
    )


def test_no_check_description_contains_a_url() -> None:
    """The one assertion that has to hold for every configuration."""
    checks = build_redis_checks(
        make_settings(
            REDIS_URL="redis://user:hunter2@cache:6379/0",
            REDIS_STREAMS_BACKEND="redis",
            REDIS_STREAMS_URL="redis://user:hunter2@streams:6379/0",
        )
    )

    for check in checks:
        assert "://" not in check.description
        assert "hunter2" not in check.description


# ── The probe's own Redis client ──────────────────────────────────────────────


def test_the_probe_client_is_capped_and_timeout_bounded() -> None:
    """The driver's own timeouts, not just `asyncio.timeout`.

    An event loop busy enough to delay a cancellation is exactly the state a
    probe has to answer from, so the socket timeouts are what actually bound
    it then. The single connection is because a probe is one command at a time
    and a pool that grows under a slow server hides the slowness.

    `from_url` opens nothing: redis-py connects on the first command, which is
    why building this during registry construction costs no socket.
    """
    client = _redis_client_factory("redis://localhost:6379/0", 1.5)

    assert client.connection_pool.max_connections == 1
    kwargs = client.connection_pool.connection_kwargs
    assert kwargs["socket_connect_timeout"] == 1.5
    assert kwargs["socket_timeout"] == 1.5


def test_the_probe_client_takes_its_timeout_from_the_settings() -> None:
    """The one link between the setting and the socket, and it is invisible.

    `HEALTH_CHECK_TIMEOUT_SECONDS` is bound into a `partial` at build time, so
    nothing public on the check reports it and a hardcoded default here would
    look identical from outside — until a deployment raised the setting and the
    driver kept timing out at 2s. Reaching for the factory is the price of
    asserting it; calling it is exactly what the first probe does.
    """
    check = build_redis_checks(make_settings(HEALTH_CHECK_TIMEOUT_SECONDS=0.75))[0]

    client = check._client_factory()  # type: ignore[attr-defined]

    assert client.connection_pool.connection_kwargs["socket_timeout"] == 0.75


# ── The registry a configuration produces ─────────────────────────────────────


def test_the_registry_always_has_a_database_check_first() -> None:
    registry = build_registry(config=make_settings(), database=FakeEngine())

    assert [check.name for check in registry.checks] == ["database", "redis"]


def test_the_registry_carries_the_configured_timeout_and_ttl() -> None:
    registry = build_registry(
        config=make_settings(
            HEALTH_CHECK_TIMEOUT_SECONDS=0.25, HEALTH_CACHE_TTL_SECONDS=0.0
        ),
        database=FakeEngine(),
    )

    assert registry.timeout_seconds == 0.25
    assert registry.cache_ttl_seconds == 0.0


@pytest.mark.asyncio
async def test_a_registry_with_only_in_process_backends_still_probes_postgres() -> None:
    """Every deployment has at least one real dependency, and this is it."""
    registry = build_registry(
        config=make_settings(
            IDEMPOTENCY_BACKEND="memory",
            DISTRIBUTED_LOCK_BACKEND="memory",
            REDIS_STREAMS_BACKEND="memory",
        ),
        database=FakeEngine(),
    )

    # Celery keeps its Redis whatever the other three are set to, so the
    # registry is database + celery's server rather than database alone.
    assert [check.name for check in registry.checks] == ["database", "redis"]

    report = await registry.report()
    assert report.outcomes[0].name == "database"
    assert report.outcomes[0].status == "ok"


def test_the_process_registry_is_built_once() -> None:
    """A registry per request would build a client per probe and coalesce nothing."""
    get_health_registry.cache_clear()
    try:
        assert get_health_registry() is get_health_registry()
    finally:
        get_health_registry.cache_clear()
