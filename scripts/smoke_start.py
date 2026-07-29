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
from typing import Any, NamedTuple

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
def _server(port: int) -> Iterator[subprocess.Popen[str]]:
    """Run uvicorn for the duration of the block, always reaping the process."""
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
        stdout=subprocess.PIPE,
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

    if process.stdout is None:  # pragma: no cover - stdout is always a pipe here
        return "<no output captured>"
    try:
        return process.stdout.read() or "<no output>"
    except OSError:  # pragma: no cover - only on an already-closed pipe
        return "<output unavailable>"


def _wait_for_liveness(
    process: subprocess.Popen[str],
    base_url: str,
    timeout: float,
    poll_interval: float,
) -> None:
    """Block until ``/health`` answers 200, the server dies, or the deadline passes."""
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
    """Require the readiness probe to confirm a live database round-trip."""
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
            "The app started but could not reach Postgres — check DATABASE_URL "
            "and that migrations ran.\n"
            f"--- server output ---\n{_drain(process)}"
        )

    if result.body != {"status": "ready", "database": "ok"}:
        raise SmokeTestError(
            f"/health/ready answered 200 with an unexpected body: {result.body!r}"
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

    with _server(port) as process:
        _wait_for_liveness(process, base_url, timeout, poll_interval)
        print(f"✓ app started and /health answered on {base_url}")

        _assert_ready(process, base_url)
        print("✓ /health/ready confirmed a SELECT 1 round-trip to Postgres")

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
