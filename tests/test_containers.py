"""Tests for the provisioning policy in `tests/containers.py`.

The module they cover is test infrastructure rather than application code, which
is the argument for testing it rather than against it: it runs before any
assertion in the suite and decides *which* servers every DB- and Redis-backed
test addresses. A wrong answer there does not fail — it silently moves 195 tests
between "measured" and "skipped", which is the exact failure the module exists
to end.

The policy is a pure function of three facts precisely so that it can be checked
here. All twelve combinations are enumerated below, including the six that
require no Docker — unreachable as live behaviour on a machine that has a daemon,
which is every machine where this matters.
"""

from __future__ import annotations

import socket
import sys
from collections.abc import Iterator

import pytest

from tests import containers
from tests.containers import (
    Action,
    Backend,
    IntegrationBackendUnavailableError,
    Provisioned,
    decide,
    docker_is_available,
    last_provisioned,
    parse_backend,
    port_is_listening,
    provision,
    running_under_mutmut,
)


class TestParseBackend:
    @pytest.mark.parametrize("raw", [None, ""])
    def test_absent_means_auto(self, raw: str | None) -> None:
        assert parse_backend(raw) is Backend.AUTO

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("auto", Backend.AUTO),
            ("external", Backend.EXTERNAL),
            ("testcontainers", Backend.TESTCONTAINERS),
        ],
    )
    def test_each_name(self, raw: str, expected: Backend) -> None:
        assert parse_backend(raw) is expected

    @pytest.mark.parametrize("raw", ["TESTCONTAINERS", " External ", "Auto"])
    def test_case_and_surrounding_space_are_forgiven(self, raw: str) -> None:
        assert parse_backend(raw) in set(Backend)

    @pytest.mark.parametrize("raw", ["testcontainer", "docker", "yes", "1"])
    def test_an_unknown_value_raises_rather_than_defaulting(self, raw: str) -> None:
        """The one behaviour here that is load-bearing.

        `testcontainers` exists to turn a silent skip into a failure. If a typo
        fell back to `auto` it would hand that back: the `integration` CI job
        would quietly become a second, slower copy of `test`, green either way.
        """
        with pytest.raises(ValueError, match="INTEGRATION_BACKEND"):
            parse_backend(raw)

    def test_the_error_names_the_permitted_values(self) -> None:
        with pytest.raises(ValueError) as caught:
            parse_backend("nope")
        message = str(caught.value)
        for backend in Backend:
            assert repr(backend.value) in message


class TestDecide:
    """The policy table, in full.

    Three backends by two reachability states by two Docker states. Written out
    rather than generated, because the table *is* the specification and a
    generator that reproduced the implementation's own logic would assert
    nothing.
    """

    @pytest.mark.parametrize("listening", [True, False])
    @pytest.mark.parametrize("docker_available", [True, False])
    def test_external_always_reuses(
        self, listening: bool, docker_available: bool
    ) -> None:
        """`external` never consults Docker and never probes.

        This is what the `test` CI job sets, where the services are declared by
        the workflow. Holding it constant across all four combinations is the
        statement that adding this module changed nothing about that job.
        """
        assert (
            decide(
                Backend.EXTERNAL,
                listening=listening,
                docker_available=docker_available,
            )
            is Action.REUSE
        )

    @pytest.mark.parametrize("listening", [True, False])
    def test_testcontainers_starts_when_docker_answers(self, listening: bool) -> None:
        """Even with a server already up: the job asked for containers."""
        assert (
            decide(Backend.TESTCONTAINERS, listening=listening, docker_available=True)
            is Action.START
        )

    @pytest.mark.parametrize("listening", [True, False])
    def test_testcontainers_fails_without_docker(self, listening: bool) -> None:
        """Never `SKIP`. A skip is what this backend exists to prevent."""
        assert (
            decide(Backend.TESTCONTAINERS, listening=listening, docker_available=False)
            is Action.FAIL
        )

    @pytest.mark.parametrize("docker_available", [True, False])
    def test_auto_reuses_a_reachable_server(self, docker_available: bool) -> None:
        """A server someone else provided wins over starting one.

        Including when Docker is available: a developer running against their
        own Postgres gets their own Postgres, and pays nothing for this module.
        """
        assert (
            decide(Backend.AUTO, listening=True, docker_available=docker_available)
            is Action.REUSE
        )

    def test_auto_starts_when_nothing_listens_and_docker_answers(self) -> None:
        assert (
            decide(Backend.AUTO, listening=False, docker_available=True) is Action.START
        )

    def test_auto_skips_when_there_is_neither(self) -> None:
        """Today's behaviour, preserved exactly.

        A machine with no servers and no Docker must behave as it did before
        this module existed — the per-test reachability guards skip — rather
        than failing a suite that used to run.
        """
        assert (
            decide(Backend.AUTO, listening=False, docker_available=False) is Action.SKIP
        )

    def test_every_combination_is_covered(self) -> None:
        """Guards the table above against a backend added without a row.

        A fourth `Backend` member would make this fail rather than quietly
        inheriting whatever `decide`'s final branch happens to return.
        """
        seen = {
            (backend, listening, docker)
            for backend in Backend
            for listening in (True, False)
            for docker in (True, False)
        }
        assert len(seen) == 12
        for backend, listening, docker in seen:
            assert isinstance(
                decide(backend, listening=listening, docker_available=docker), Action
            )


class TestPortIsListening:
    def test_true_for_a_socket_this_test_holds_open(self) -> None:
        """A real listener on a real ephemeral port, not a patched probe."""
        with socket.socket() as server:
            server.bind(("127.0.0.1", 0))
            server.listen(1)
            port = server.getsockname()[1]
            assert port_is_listening(f"postgresql://u:p@127.0.0.1:{port}/db", 5432)

    def test_false_once_the_socket_is_closed(self) -> None:
        with socket.socket() as server:
            server.bind(("127.0.0.1", 0))
            server.listen(1)
            port = server.getsockname()[1]
        assert not port_is_listening(
            f"postgresql://u:p@127.0.0.1:{port}/db", 5432, timeout=0.25
        )

    def test_the_default_port_is_used_when_the_url_omits_one(self) -> None:
        """`redis://host/0` has to be probed on 6379, not on nothing."""
        with socket.socket() as server:
            server.bind(("127.0.0.1", 0))
            server.listen(1)
            port = server.getsockname()[1]
            # The URL carries no port, so the default decides, and here the
            # default is the port actually held open.
            assert port_is_listening("redis://127.0.0.1/0", port)

    def test_an_unresolvable_host_is_false_rather_than_an_exception(self) -> None:
        """`auto` must degrade to a skip, never crash collection."""
        assert not port_is_listening(
            "postgresql://u:p@no-such-host.invalid:5432/db", 5432, timeout=0.25
        )


class TestRunningUnderMutmut:
    def test_true_when_mutmut_is_imported(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(sys.modules, "mutmut", object())
        assert running_under_mutmut() is True

    def test_false_when_it_is_not(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delitem(sys.modules, "mutmut", raising=False)
        assert running_under_mutmut() is False

    def test_a_plain_pytest_run_is_not_a_mutation_run(self) -> None:
        """Unpatched, in this very process.

        The suite's own run must not be mistaken for a mutation run, or `auto`
        would downgrade itself to `external` and every integration module would
        skip — green, and measuring nothing.
        """
        assert running_under_mutmut() is False


class TestProvisioned:
    def test_started_names_only_the_started_services(self) -> None:
        provisioned = Provisioned(
            actions={"postgres": Action.START, "redis": Action.REUSE},
            database_url="postgresql+asyncpg://u:p@127.0.0.1:1/db",
            redis_url="redis://127.0.0.1:2/0",
        )
        assert provisioned.started == ("postgres",)

    def test_started_is_empty_when_nothing_was(self) -> None:
        provisioned = Provisioned(
            actions={"postgres": Action.SKIP, "redis": Action.REUSE},
            database_url=None,
            redis_url=None,
        )
        assert provisioned.started == ()

    def test_environment_publishes_only_what_was_started(self) -> None:
        """A `REUSE` needs no publishing: the value came from the environment."""
        provisioned = Provisioned(
            actions={"postgres": Action.START, "redis": Action.REUSE},
            database_url="postgresql+asyncpg://u:p@127.0.0.1:1/db",
            redis_url="redis://127.0.0.1:2/0",
        )
        assert provisioned.environment == {
            "DATABASE_URL": "postgresql+asyncpg://u:p@127.0.0.1:1/db"
        }

    def test_environment_publishes_both_when_both_started(self) -> None:
        provisioned = Provisioned(
            actions={"postgres": Action.START, "redis": Action.START},
            database_url="postgresql+asyncpg://u:p@127.0.0.1:1/db",
            redis_url="redis://127.0.0.1:2/0",
        )
        assert provisioned.environment == {
            "DATABASE_URL": "postgresql+asyncpg://u:p@127.0.0.1:1/db",
            "REDIS_URL": "redis://127.0.0.1:2/0",
        }

    def test_environment_is_empty_for_a_skip(self) -> None:
        """So the placeholder defaults in `tests/conftest.py` stand."""
        provisioned = Provisioned(
            actions={"postgres": Action.SKIP, "redis": Action.SKIP},
            database_url=None,
            redis_url=None,
        )
        assert provisioned.environment == {}


class TestProvision:
    """`provision` itself, on the paths that start nothing.

    The `START` path is not mocked here. It is exercised for real by every one
    of the 316 tests in the fourteen integration modules, which is a far stronger
    statement than a test asserting that a patched `DockerContainer` was
    constructed — and `tests/containers.py` is imported by `tests/conftest.py`,
    so this module's own collection already depended on it having worked.
    """

    @pytest.fixture(autouse=True)
    def isolate_environment(self, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
        """Keep these tests from disturbing the URLs the rest of the run uses.

        `provision` reads both URLs to decide what to do, so the two below are
        set explicitly: a developer's own exports would otherwise change the
        outcome. `monkeypatch` restores whatever was there.
        """
        monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@127.0.0.1:1/db")
        monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:2/0")
        yield

    def test_external_reuses_both_and_reports_the_configured_urls(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("INTEGRATION_BACKEND", "external")
        result = provision()
        assert result.actions == {
            "postgres": Action.REUSE,
            "redis": Action.REUSE,
        }
        assert result.database_url == "postgresql+asyncpg://u:p@127.0.0.1:1/db"
        assert result.redis_url == "redis://127.0.0.1:2/0"
        assert result.started == ()

    def test_external_does_not_touch_docker(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The `test` CI job's path must cost nothing and need no daemon."""

        def explode() -> bool:
            raise AssertionError("external must not consult Docker")

        monkeypatch.setattr(containers, "docker_is_available", explode)
        monkeypatch.setenv("INTEGRATION_BACKEND", "external")
        assert provision().started == ()

    def test_external_publishes_nothing_and_touches_nothing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Nothing to publish, and `os.environ` left exactly as it was.

        `provision` is deliberately free of global side effects — it returns
        what should be set and the caller sets it — so both halves are worth
        stating: an empty mapping, and an unchanged environment.
        """
        import os

        monkeypatch.setenv("INTEGRATION_BACKEND", "external")
        assert provision().environment == {}
        assert os.environ["DATABASE_URL"] == "postgresql+asyncpg://u:p@127.0.0.1:1/db"
        assert os.environ["REDIS_URL"] == "redis://127.0.0.1:2/0"

    def test_testcontainers_without_docker_raises_and_names_the_way_out(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The `integration` job's guard against going green on an empty suite."""
        monkeypatch.setattr(containers, "docker_is_available", lambda: False)
        monkeypatch.setenv("INTEGRATION_BACKEND", "testcontainers")
        with pytest.raises(IntegrationBackendUnavailableError) as caught:
            provision()
        message = str(caught.value)
        assert "INTEGRATION_BACKEND=external" in message
        assert "Refusing to skip" in message

    def test_auto_skips_when_docker_is_absent_and_nothing_listens(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The pre-existing environment: no servers, no Docker, no failure."""
        monkeypatch.setattr(containers, "docker_is_available", lambda: False)
        monkeypatch.setenv("INTEGRATION_BACKEND", "auto")
        result = provision()
        assert result.actions == {"postgres": Action.SKIP, "redis": Action.SKIP}
        assert result.started == ()

    def test_auto_reuses_servers_that_are_already_listening(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two real listeners, so the probe is the real one."""
        with socket.socket() as pg, socket.socket() as redis_sock:
            pg.bind(("127.0.0.1", 0))
            pg.listen(1)
            redis_sock.bind(("127.0.0.1", 0))
            redis_sock.listen(1)
            monkeypatch.setenv(
                "DATABASE_URL",
                f"postgresql+asyncpg://u:p@127.0.0.1:{pg.getsockname()[1]}/db",
            )
            monkeypatch.setenv(
                "REDIS_URL", f"redis://127.0.0.1:{redis_sock.getsockname()[1]}/0"
            )

            def explode() -> bool:
                raise AssertionError("a reachable server must not start a container")

            monkeypatch.setattr(containers, "_start_postgres", explode)
            monkeypatch.setattr(containers, "_start_redis", explode)
            monkeypatch.setenv("INTEGRATION_BACKEND", "auto")

            result = provision()

        assert result.actions == {"postgres": Action.REUSE, "redis": Action.REUSE}
        assert result.started == ()

    def test_auto_defers_to_mutmut(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A mutation run gets no containers and needs no daemon.

        Asserted through `provision` rather than `running_under_mutmut` alone,
        because the wiring is the part that can be wrong: `decide` never sees
        the mutmut signal, so a missing branch in `provision` would leave the
        mutation gate starting a Postgres per run.
        """
        monkeypatch.setitem(sys.modules, "mutmut", object())

        def explode() -> bool:
            raise AssertionError("a mutation run must not consult Docker")

        monkeypatch.setattr(containers, "docker_is_available", explode)
        monkeypatch.delenv("INTEGRATION_BACKEND", raising=False)

        result = provision()

        assert result.actions == {"postgres": Action.REUSE, "redis": Action.REUSE}
        assert result.started == ()

    def test_an_explicit_backend_still_overrides_the_mutmut_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """`auto` defers to mutmut; `testcontainers` does not.

        Checked via the no-Docker failure, which is reachable without starting
        anything: if the mutmut branch had swallowed the explicit setting this
        would return `REUSE` instead of raising.
        """
        monkeypatch.setitem(sys.modules, "mutmut", object())
        monkeypatch.setattr(containers, "docker_is_available", lambda: False)
        monkeypatch.setenv("INTEGRATION_BACKEND", "testcontainers")
        with pytest.raises(IntegrationBackendUnavailableError):
            provision()

    def test_an_unknown_backend_raises_from_provision_too(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("INTEGRATION_BACKEND", "sometimes")
        with pytest.raises(ValueError, match="INTEGRATION_BACKEND"):
            provision()


class TestLastProvisioned:
    def test_a_result_is_recorded_and_well_formed(self) -> None:
        """Something is always recorded by the time any test runs.

        `tests/conftest.py` calls `provision()` to collect this very module, so
        `last_provisioned()` can never be the "not called" error here — and the
        shape it returns is what `pytest_report_header` indexes into. Not an
        assertion about *which* call was last: `TestProvision` above runs first
        and overwrites it, which is why this checks the shape rather than the
        values.
        """
        provisioned = last_provisioned()
        assert set(provisioned.actions) == {"postgres", "redis"}
        assert all(
            isinstance(action, Action) for action in provisioned.actions.values()
        )

    def test_it_reflects_the_most_recent_call(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("INTEGRATION_BACKEND", "external")
        monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@127.0.0.1:9/db")
        returned = provision()
        assert last_provisioned() is returned


class TestDockerIsAvailable:
    def test_it_answers_a_bool_without_raising(self) -> None:
        """Whichever way it lands here, it must never propagate.

        It is called during conftest import, where an exception is a collection
        error rather than a test failure — so the contract is the return type,
        not the value, which legitimately differs between a developer's machine
        and a runner without a daemon.
        """
        assert isinstance(docker_is_available(), bool)

    def test_a_dead_docker_host_is_false_rather_than_an_exception(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A socket path that does not exist is the common broken install."""
        monkeypatch.setenv("DOCKER_HOST", "unix:///nonexistent/docker.sock")
        assert docker_is_available() is False
