#!/usr/bin/env python
"""Boot the app, drive `loadtest/locustfile.py` at it, write the stats CSV.

This is the `load` CI job in one command, and that is the point of it existing:
the gate a contributor runs locally has to be the same gate that blocks a merge,
or the two drift and the difference reads as a pass. `docs/load-testing.md`
records what happened the last time this repository had a gate that only CI
could run.

What it does, in order:

1. starts ``uvicorn src.main:app`` as a subprocess, reusing
   `scripts.smoke_start.serve` so there is one copy of that code;
2. waits for ``/health`` — a socket that accepts before the lifespan has
   finished would charge the first requests for a cold connection pool, and
   those requests land in the percentiles the budget is read from;
3. registers and logs in one obviously-fake account, **once**. A login costs an
   argon2 verification by design and `/auth/login` is rate-limited to five a
   minute, so this cannot be part of the load profile; the access token and the
   profile's entity tag are handed to locust in its environment;
4. runs locust headless for the configured duration, writing
   ``<prefix>_stats.csv`` and its siblings;
5. SIGTERMs the server, whatever happened.

It does **not** check the budgets. `scripts/check_load_budget.py` does that,
separately and on the CSV, because locust exits 0 at any latency — the same
split as `mutmut run` and `scripts/check_mutation_score.py`, for the same
reason. Run the two together::

    uv run python scripts/run_load_test.py
    uv run python scripts/check_load_budget.py

Both need the `loadtest` dependency group: ``uv sync --group loadtest``.

Usage::

    uv run python scripts/run_load_test.py
    uv run python scripts/run_load_test.py --users 20 --run-time 60
    uv run python scripts/run_load_test.py --base-url https://staging.example.com
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from pathlib import Path
from typing import Final

from scripts.smoke_start import SmokeTestError, serve, wait_for_liveness

REPO_ROOT: Final[Path] = Path(__file__).resolve().parent.parent
LOCUSTFILE: Final[Path] = REPO_ROOT / "loadtest" / "locustfile.py"

#: The default load. Twenty paced users against one uvicorn worker: enough
#: concurrency that queueing would show, far enough below saturation that the
#: numbers are response times rather than queue depths — measured at ~155 req/s
#: answered in single-digit milliseconds. See docs/load-testing.md § The
#: profile.
DEFAULT_USERS: Final[int] = 20
DEFAULT_SPAWN_RATE: Final[float] = 20.0
#: Sixty seconds. Thirty was enough for the per-name request-count floors in
#: `scripts/check_load_budget.py`, but not enough for the p95 of the two
#: lightest-weighted names to stop moving between runs.
DEFAULT_RUN_TIME_SECONDS: Final[int] = 60
DEFAULT_PORT: Final[int] = 8002
DEFAULT_CSV_PREFIX: Final[Path] = Path("build/load/load")

#: How long the app gets to honour SIGTERM once the run is over.
SHUTDOWN_GRACE_SECONDS: Final[float] = 30.0

#: The account the profile reads. Created by this script through the public API
#: and never anything but a load-test fixture: the address is in a reserved
#: domain (RFC 2606) and the password says what it is. Nothing here is a
#: credential — it is minted against whatever database this run points at.
LOADTEST_EMAIL: Final[str] = "loadtest@example.com"
LOADTEST_PASSWORD: Final[str] = "loadtest-not-a-real-password"

#: `src.models.tenant.DEFAULT_TENANT_ID`, spelled out rather than imported:
#: this script must work against a `--base-url` on another machine, where
#: importing the application's models would mean importing its settings and
#: validating a `DATABASE_URL` this process has no use for.
DEFAULT_TENANT_ID: Final[str] = "00000000-0000-0000-0000-000000000001"


class LoadTestError(RuntimeError):
    """The app would not serve, or the bootstrap could not mint a token."""


def _post(
    url: str, payload: dict[str, str], headers: dict[str, str]
) -> tuple[int, str]:
    """POST JSON and return the status and body, treating 4xx/5xx as answers."""
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", **headers},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30.0) as response:
            return int(response.status), response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return int(exc.code), exc.read().decode("utf-8", "replace")
    except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
        raise LoadTestError(f"POST {url} never answered: {exc}") from exc


def _get_etag(url: str, headers: dict[str, str]) -> str:
    """GET the profile and return the entity tag the response stamps on it."""
    request = urllib.request.Request(url, headers=headers, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=30.0) as response:
            # `http.client.HTTPMessage.get` is typed as returning Any, and
            # the value is read back into an f-string and an HTTP header, so
            # it is narrowed here rather than at the point of use.
            raw_etag = response.headers.get("ETag")
            etag = str(raw_etag) if raw_etag else ""
            status = int(response.status)
    except urllib.error.HTTPError as exc:
        raise LoadTestError(
            f"GET {url} answered HTTP {exc.code} during bootstrap: "
            f"{exc.read().decode('utf-8', 'replace')[:200]}"
        ) from exc
    except (urllib.error.URLError, ConnectionError, TimeoutError) as exc:
        raise LoadTestError(f"GET {url} never answered: {exc}") from exc

    if status != 200 or not etag:
        raise LoadTestError(
            f"GET {url} answered HTTP {status} with ETag {etag!r}; the "
            "conditional task has no validator to send"
        )
    return etag


def bootstrap(base_url: str, tenant_id: str) -> dict[str, str]:
    """Mint the credentials the locustfile needs, and nothing more.

    A 409 from `/auth/register` is success: this account outlives one run, and a
    database that already has it is the normal case on a developer's machine. A
    401 from the login that follows is *not* — it means the address exists with
    some other password, which is a fixture collision somebody has to resolve
    rather than something to work around.
    """
    api = f"{base_url.rstrip('/')}/api/v1"
    tenant_headers = {"Accept": "application/json", "X-Tenant-ID": tenant_id}

    status, body = _post(
        f"{api}/auth/register",
        {"email": LOADTEST_EMAIL, "password": LOADTEST_PASSWORD},
        tenant_headers,
    )
    if status not in (201, 409):
        raise LoadTestError(
            f"POST /auth/register answered HTTP {status}: {body[:300]}\n"
            "The load profile needs one account; see docs/load-testing.md."
        )

    status, body = _post(
        f"{api}/auth/login",
        {"email": LOADTEST_EMAIL, "password": LOADTEST_PASSWORD},
        tenant_headers,
    )
    if status == 401:
        raise LoadTestError(
            f"POST /auth/login refused {LOADTEST_EMAIL}. The address exists in "
            "this database with a different password — delete that row, or "
            "point this run at a database that does not have it."
        )
    if status == 429:
        raise LoadTestError(
            "POST /auth/login answered HTTP 429. The route admits five a "
            "minute per address and this run shares one; wait a minute."
        )
    if status != 200:
        raise LoadTestError(f"POST /auth/login answered HTTP {status}: {body[:300]}")

    try:
        token = str(json.loads(body)["access_token"])
    except (json.JSONDecodeError, KeyError, TypeError) as exc:
        raise LoadTestError(
            f"POST /auth/login answered 200 without an access_token: {body[:200]}"
        ) from exc

    authorized = {**tenant_headers, "Authorization": f"Bearer {token}"}
    etag = _get_etag(f"{api}/users/me", authorized)

    return {
        "LOADTEST_ACCESS_TOKEN": token,
        "LOADTEST_ETAG": etag,
        "LOADTEST_TENANT_ID": tenant_id,
    }


def run_locust(
    base_url: str,
    credentials: dict[str, str],
    csv_prefix: Path,
    users: int,
    spawn_rate: float,
    run_time: int,
) -> int:
    """Run locust headless against `base_url`, returning its exit status.

    `--exit-code-on-error 1` is locust's default and is stated anyway, because
    it is the half of the gate that lives in locust: a run where requests
    failed must not reach `scripts/check_load_budget.py` looking like a run that
    simply had a good p95. The budget script refuses a failed run too — the two
    overlap on purpose, since this one gives the better message and that one
    cannot be skipped.
    """
    csv_prefix.parent.mkdir(parents=True, exist_ok=True)
    return subprocess.call(
        [
            sys.executable,
            "-m",
            "locust",
            "--locustfile",
            str(LOCUSTFILE),
            "--host",
            base_url,
            "--headless",
            "--users",
            str(users),
            "--spawn-rate",
            str(spawn_rate),
            "--run-time",
            f"{run_time}s",
            "--csv",
            str(csv_prefix),
            "--exit-code-on-error",
            "1",
            # Suppresses the per-ten-second table on stdout, not the
            # `_stats_history.csv` beside the stats — that one is written
            # regardless and the CI job uploads it. In a job's log the periodic
            # table is six screens of numbers nobody reads, and the final
            # summary is the one that matters.
            "--only-summary",
        ],
        cwd=REPO_ROOT,
        env={**os.environ, **credentials},
    )


@contextmanager
def _app(port: int, log_path: Path) -> Iterator[str]:
    """Serve the app for the duration of the block and yield its base URL.

    The server's log goes to `log_path` and **must not** go to a pipe. uvicorn
    writes an access-log line per request; a pipe nobody reads blocks the writer
    at about 64 KiB, which stops the server answering partway through the run
    and leaves a process that ignores SIGTERM. See `serve`.

    The file is kept rather than discarded: it is the only account of what the
    server did during the run, and the `load` CI job uploads it next to the CSV.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log:
        with serve(port, output=log) as process:
            base_url = f"http://127.0.0.1:{port}"
            try:
                wait_for_liveness(process, base_url, timeout=60.0, poll_interval=0.5)
            except SmokeTestError as exc:
                raise SmokeTestError(f"{exc}\nserver log: {log_path}") from exc
            yield base_url
            # SIGTERM and a bounded wait, so a lifespan that hangs on the way
            # down fails this script rather than being killed and forgotten;
            # `serve` kills whatever is left on the way out of the block.
            process.terminate()
            try:
                process.wait(timeout=SHUTDOWN_GRACE_SECONDS)
            except subprocess.TimeoutExpired as exc:
                raise LoadTestError(
                    f"the server ignored SIGTERM for "
                    f"{SHUTDOWN_GRACE_SECONDS:.0f}s after the run; its log is "
                    f"at {log_path}"
                ) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__ or None)
    parser.add_argument(
        "--base-url",
        default=None,
        help=(
            "load an app that is already running instead of starting one, e.g. "
            "a staging deployment. Budgets are measured on this repository's "
            "own CI runner, so check the result with --headroom"
        ),
    )
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--users", type=int, default=DEFAULT_USERS)
    parser.add_argument("--spawn-rate", type=float, default=DEFAULT_SPAWN_RATE)
    parser.add_argument("--run-time", type=int, default=DEFAULT_RUN_TIME_SECONDS)
    parser.add_argument("--csv-prefix", type=Path, default=DEFAULT_CSV_PREFIX)
    parser.add_argument(
        "--tenant-id",
        default=DEFAULT_TENANT_ID,
        help=f"the X-Tenant-ID to send (default: {DEFAULT_TENANT_ID})",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.users < 1 or args.run_time < 1 or args.spawn_rate <= 0:
        print(
            "error: --users, --spawn-rate and --run-time must be positive",
            file=sys.stderr,
        )
        return 2
    try:
        uuid.UUID(args.tenant_id)
    except ValueError:
        print(f"error: --tenant-id {args.tenant_id!r} is not a UUID", file=sys.stderr)
        return 2
    if not LOCUSTFILE.is_file():
        print(f"error: {LOCUSTFILE} does not exist", file=sys.stderr)
        return 2

    log_path = args.csv_prefix.with_name(f"{args.csv_prefix.name}_server.log")
    server = nullcontext(args.base_url) if args.base_url else _app(args.port, log_path)
    try:
        with server as base_url:
            credentials = bootstrap(base_url, args.tenant_id)
            print(
                f"load: {args.users} users, {args.run_time}s, "
                f"{base_url} -> {args.csv_prefix}_stats.csv"
            )
            status = run_locust(
                base_url,
                credentials,
                args.csv_prefix,
                args.users,
                args.spawn_rate,
                args.run_time,
            )
    except (LoadTestError, SmokeTestError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if status != 0:
        print(
            f"error: locust exited {status}; requests failed during the run "
            f"(see {args.csv_prefix}_failures.csv)",
            file=sys.stderr,
        )
        return 1

    print(
        f"\nstats written to {args.csv_prefix}_stats.csv — check them with\n"
        "  uv run python scripts/check_load_budget.py"
        + (
            f" --stats {args.csv_prefix}_stats.csv"
            if args.csv_prefix != DEFAULT_CSV_PREFIX
            else ""
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
