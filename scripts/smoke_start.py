#!/usr/bin/env python
"""Start the real ASGI server and prove it can serve traffic from a real Postgres.

``python -c "from src.main import app"`` only proves the module tree imports. It
never runs the lifespan, never opens a connection pool, and never binds a socket,
so it stays green against a database that does not exist. This script closes that
gap by doing what a deployment does:

1. spawn ``uvicorn src.main:app`` as a subprocess, exactly as the Dockerfile does;
2. poll ``/health`` until the socket accepts and the app answers;
3. assert ``/health/ready`` returns 200 with ``database: ok`` — a real ``SELECT 1``
   round-trip through the async engine against the configured Postgres;
4. send SIGTERM and require a clean shutdown, so a lifespan that hangs or raises
   on the way down fails the build too.

Any failure prints the server's captured stdout/stderr, because a start-up crash
is otherwise invisible from the client side.

Usage::

    uv run python scripts/smoke_start.py            # defaults below
    uv run python scripts/smoke_start.py --port 8123 --timeout 45
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, NamedTuple

REPO_ROOT = Path(__file__).resolve().parent.parent

# Long enough to cover a cold interpreter start and the first connection to a
# freshly booted Postgres service container, short enough that a genuinely broken
# app fails the job quickly rather than sitting until the runner's global timeout.
DEFAULT_TIMEOUT_SECONDS = 60.0
DEFAULT_POLL_INTERVAL_SECONDS = 0.5
SHUTDOWN_GRACE_SECONDS = 15.0


class ProbeResult(NamedTuple):
    status_code: int
    body: Any


class SmokeTestError(RuntimeError):
    """Raised when the app fails to start, serve, or shut down cleanly."""


def _probe(url: str, timeout: float = 5.0) -> ProbeResult:
    """GET ``url`` and decode the JSON body, treating any HTTP status as a result.

    urllib raises on 4xx/5xx, but a 503 from the readiness probe is a meaningful
    answer rather than a transport failure, so it is unwrapped back into a result.
    """
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            return ProbeResult(int(response.status), json.loads(raw))
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            body: Any = json.loads(raw)
        except json.JSONDecodeError:
            body = raw.decode("utf-8", "replace")
        return ProbeResult(int(exc.code), body)


@contextmanager
def serve(
    port: int, *, output: int | IO[str] = subprocess.PIPE
) -> Iterator[subprocess.Popen[str]]:
    """Run uvicorn for the duration of the block, always reaping the process.

    Public, like `wait_for_liveness` below, because `scripts/run_load_test.py`
    needs exactly this: the real server as the Dockerfile starts it, held up
    for the duration of a block and reaped however the block ends. Two copies
    of process management is one too many, and the second copy is the one that
    leaks a uvicorn on a failure path.

    ``output`` is where the server's combined stdout/stderr goes, and a caller
    that is about to send more than a handful of requests must pass a file
    rather than take the default. The default is a pipe, which suits this
    module — a smoke test makes three requests and `_drain` reads the pipe on
    the failure path — but uvicorn writes an access-log line per request, and a
    pipe nobody is reading holds about 64 KiB before the *writer* blocks. The
    server then stops answering and never shuts down, with no error anywhere
    and a process that ignores SIGTERM. That is not hypothetical: it is what
    `scripts/run_load_test.py` did on its first run, after about 700 requests.
    """
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "src.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            # Single worker: the smoke test asserts on one process's lifecycle, and
            # a worker pool would mask a child that died on start-up.
            "--workers",
            "1",
        ],
        cwd=REPO_ROOT,
        stdout=output,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        yield process
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=SHUTDOWN_GRACE_SECONDS)


def _drain(process: subprocess.Popen[str]) -> str:
    """Read the server's captured output, stopping it first if it is still up.

    ``stdout.read()`` blocks until EOF, and a live uvicorn never closes its pipe —
    so draining a running server would hang the job instead of reporting the
    failure. This is only ever called on a failure path, where the server is about
    to be torn down anyway, so killing it first is both safe and necessary.
    """
    if process.poll() is None:
        process.kill()
        try:
            process.wait(timeout=SHUTDOWN_GRACE_SECONDS)
        except subprocess.TimeoutExpired:  # pragma: no cover - kill(2) not reaped
            return "<server would not die; output unavailable>"

    # Reachable whenever `serve` was given somewhere other than a pipe to write
    # to; the caller that did that knows where the output went and says so.
    if process.stdout is None:
        return "<no output captured; the server's log went where the caller sent it>"
    try:
        return process.stdout.read() or "<no output>"
    except OSError:  # pragma: no cover - only on an already-closed pipe
        return "<output unavailable>"


def wait_for_liveness(
    process: subprocess.Popen[str],
    base_url: str,
    timeout: float,
    poll_interval: float,
) -> None:
    """Block until ``/health`` answers 200, the server dies, or the deadline passes.

    Public for the same reason as `serve`: `scripts/run_load_test.py` must not
    start sending load at a socket that is accepting but whose lifespan has not
    finished — the first few requests would pay for a cold connection pool and
    land in the percentiles the budget is read from.
    """
    deadline = time.monotonic() + timeout
    last_error = "no response yet"

    while time.monotonic() < deadline:
        exit_code = process.poll()
        if exit_code is not None:
            raise SmokeTestError(
                f"server exited with code {exit_code} before it became live\n"
                f"--- server output ---\n{_drain(process)}"
            )

        try:
            result = _probe(f"{base_url}/health")
        except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        else:
            if result.status_code == 200:
                if result.body != {"status": "ok"}:
                    raise SmokeTestError(
                        f"/health answered 200 with an unexpected body: {result.body!r}"
                    )
                return
            last_error = f"HTTP {result.status_code}: {result.body!r}"

        time.sleep(poll_interval)

    raise SmokeTestError(
        f"/health did not answer within {timeout:.0f}s (last: {last_error})\n"
        f"--- server output ---\n{_drain(process)}"
    )


def _assert_ready(process: subprocess.Popen[str], base_url: str) -> None:
    """Require the readiness probe to confirm a live round-trip to every dependency.

    The probe reports one entry per dependency this configuration actually has
    (`src/health/wiring.py`), so this asserts on the shape rather than on a
    fixed body: every check must have passed, and `database` must be among them
    — a regression that stopped probing Postgres altogether would otherwise
    produce a cheerful `ready` with nothing behind it.
    """
    try:
        # Generous next to the 5s default: a readiness check that has to wait out
        # a TCP connect to a wedged database is slow, but its answer is still the
        # signal we want rather than a transport error.
        result = _probe(f"{base_url}/health/ready", timeout=30.0)
    except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
        raise SmokeTestError(
            f"/health/ready never answered ({type(exc).__name__}: {exc}); "
            "the request most likely blocked on a database connection\n"
            f"--- server output ---\n{_drain(process)}"
        ) from exc

    if result.status_code != 200:
        raise SmokeTestError(
            f"/health/ready answered HTTP {result.status_code}: {result.body!r}\n"
            "The app started but could not reach a required dependency — the "
            "failing check is named in the body above.\n"
            f"--- server output ---\n{_drain(process)}"
        )

    body = result.body
    if not isinstance(body, dict):
        raise SmokeTestError(f"/health/ready answered with a non-object: {body!r}")

    checks = body.get("checks")
    if body.get("status") != "ready" or not isinstance(checks, dict):
        raise SmokeTestError(
            f"/health/ready answered 200 with an unexpected body: {body!r}"
        )

    if "database" not in checks:
        raise SmokeTestError(
            "/health/ready reported ready without probing the database at all: "
            f"{sorted(checks)}"
        )

    failed = sorted(
        name
        for name, report in checks.items()
        if not isinstance(report, dict) or report.get("status") != "ok"
    )
    if failed:
        # Reachable on a `degraded` 200: an optional dependency is down, which
        # is a correct answer from the probe and still a broken environment for
        # a build gate that provisions every service it configures.
        raise SmokeTestError(
            f"/health/ready is serving traffic with failed checks: {failed}\n"
            f"body: {body!r}"
        )


def _assert_clean_shutdown(process: subprocess.Popen[str]) -> None:
    """SIGTERM the server and require it to stop promptly without erroring."""
    process.terminate()
    try:
        exit_code = process.wait(timeout=SHUTDOWN_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=SHUTDOWN_GRACE_SECONDS)
        raise SmokeTestError(
            f"server ignored SIGTERM for {SHUTDOWN_GRACE_SECONDS:.0f}s; "
            "a lifespan shutdown hook is probably hanging"
        ) from None

    # uvicorn exits 0 on SIGTERM; on POSIX a signal-terminated child reports the
    # negated signal number, which is also an orderly stop rather than a crash.
    if exit_code not in (0, -15):
        raise SmokeTestError(
            f"server exited with code {exit_code} on shutdown\n"
            f"--- server output ---\n{_drain(process)}"
        )


def run_smoke_test(port: int, timeout: float, poll_interval: float) -> None:
    base_url = f"http://127.0.0.1:{port}"

    with serve(port) as process:
        wait_for_liveness(process, base_url, timeout, poll_interval)
        print(f"✓ app started and /health answered on {base_url}")

        _assert_ready(process, base_url)
        print("✓ /health/ready round-tripped every configured dependency")

        _assert_clean_shutdown(process)
        print("✓ app shut down cleanly on SIGTERM")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument(
        "--poll-interval", type=float, default=DEFAULT_POLL_INTERVAL_SECONDS
    )
    args = parser.parse_args()

    try:
        run_smoke_test(args.port, args.timeout, args.poll_interval)
    except SmokeTestError as exc:
        print(f"✗ start-up smoke test failed: {exc}", file=sys.stderr)
        return 1

    print("start-up smoke test passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
