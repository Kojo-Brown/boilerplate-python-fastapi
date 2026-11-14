#!/usr/bin/env python
"""Fail the build when a load test's p95 exceeds the budget for its endpoint.

`locust --headless` exits non-zero when a *request* failed and exits 0 at any
latency whatsoever — it reports response times, it does not gate on them — so
the budget has to live somewhere else. This reads the `_stats.csv` that
`locust --csv` writes and turns it into an exit code. The arrangement is the one
`scripts/check_mutation_score.py` already uses for the mutation gate, and for
the same reason.

**The budgets below are regression bounds, not an SLO.** They are set from
measured runs with headroom over the worst of them, because the job runs on a
shared runner where locust, uvicorn and Postgres compete for the same cores and
a percentile is a noisy statistic. What they catch is the change that moves a
response time by a multiple: an N+1 query added to a read path, a dependency
check made synchronous, a middleware that now hashes something per request.
What they deliberately do not catch is a 30% drift, which on this hardware
cannot be told apart from the runner's mood. `docs/load-testing.md` records the
measured numbers, the headroom each budget carries, and how to re-measure.

**Three things are checked besides the percentile**, and each closes a way for a
load test to pass having measured nothing:

- *Every budgeted name must be present.* A task renamed or deleted would
  otherwise silently stop being measured, and the run would still be green.
- *No unbudgeted name may be present.* Adding a task is then a deliberate edit
  here with a number to justify, rather than a row nobody gates on.
- *A floor on the request count per name.* A p95 over thirty samples is not a
  p95, and a profile that barely exercises a path should fail rather than
  report a confident number about it.

Failures are required to be zero outright. A load profile asserts the status of
every response it makes (`loadtest/locustfile.py`), so a failure here is a 401,
a 503 or a 304-that-was-a-200 — in every case the run measured something other
than what it claims to, and such a response is usually *faster* than the one it
replaced.

Usage::

    uv run python scripts/check_load_budget.py
    uv run python scripts/check_load_budget.py --stats build/load/other_stats.csv
    uv run python scripts/check_load_budget.py --headroom 2.0   # a slower box
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final

#: Where `scripts/run_load_test.py` puts locust's CSV by default. The `_stats`
#: suffix is locust's, not ours: `--csv <prefix>` writes `<prefix>_stats.csv`.
DEFAULT_STATS_PATH: Final[Path] = Path("build/load/load_stats.csv")

#: The row locust appends for the whole run. Not budgeted — an aggregate over
#: four endpoints with different costs is a number about the task weights — but
#: it is read for the run-wide failure count.
AGGREGATED_NAME: Final[str] = "Aggregated"


@dataclass(frozen=True, slots=True)
class Budget:
    """What one request name must do to pass.

    `p95_ms` is the 95th percentile ceiling. `min_requests` is the floor on the
    sample count: below it the percentile is not a measurement and the run is
    refused rather than believed.
    """

    p95_ms: float
    min_requests: int


#: The request names, declared here rather than in `loadtest/locustfile.py`,
#: which imports them. The direction is forced — this module must be importable
#: without locust, since the `test` job that runs the unit tests does not
#: install the `loadtest` dependency group — but it is also the right way round:
#: a name is only worth recording if something gates on it, and keeping the two
#: lists in one place means a renamed task cannot quietly stop being measured.
HEALTH: Final[str] = "GET /health"
READY: Final[str] = "GET /health/ready"
PROFILE: Final[str] = "GET /api/v1/users/me"
PROFILE_REVALIDATE: Final[str] = "GET /api/v1/users/me (If-None-Match)"

#: One entry per request name above. Measured values and the reasoning for each
#: ceiling are in `docs/load-testing.md`; the short version is below.
# Each ceiling is roughly seven to twelve times the worst p95 observed across
# the runs recorded in `docs/load-testing.md` — this job's own runner, which
# measured 1/2/5/5ms, and a developer machine over four runs, whose worst was
# 2/5/11/11ms. The multiple is what the hardware spread buys: the tail on the
# runner reaches ~90ms at the 99.9th percentile, so single slow requests happen
# and the gate must not fire on a handful of them, but a p95 moving past these
# numbers needs one request in twenty to be slow, which is a change and not a
# mood.
BUDGETS: Final[dict[str, Budget]] = {
    # No dependency, no authentication: the cost of the middleware stack and
    # the router. This is the floor the other three are read against, and the
    # one that moves when something is added to every request's path.
    # Worst measured p95 2ms.
    HEALTH: Budget(p95_ms=25.0, min_requests=400),
    # Adds a live round trip to every configured dependency — a `SELECT 1` on
    # the async engine and a Redis `PING`. The gap to `/health` is what a
    # readiness check costs, and a check made serial or unpooled shows up here
    # before it shows up anywhere a user can see. Worst measured p95 5ms.
    READY: Budget(p95_ms=40.0, min_requests=400),
    # The real read path: bearer decode, the tenant `set_config` statement, the
    # row fetch, an encrypted column decrypted, the entity tag, serialisation.
    # Worst measured p95 11ms.
    PROFILE: Budget(p95_ms=75.0, min_requests=1200),
    # The same work, answered 304 from the validator instead of serialising a
    # body. Budgeted at the same ceiling as the unconditional read and **not**
    # below it, which is worth stating because the opposite is the tempting
    # mistake: `docs/conditional-get.md` claims revalidation is cheaper, and at
    # this payload — a profile is about 190 bytes — the saving is in bytes on
    # the wire, not in milliseconds. The two measure within a millisecond of
    # each other on both machines, so a ceiling that encoded "the 304 must be
    # faster" would be gating on noise.
    PROFILE_REVALIDATE: Budget(p95_ms=75.0, min_requests=1200),
}

#: The CSV columns this script reads, by the header locust writes them under.
_NAME_COLUMN: Final[str] = "Name"
_TYPE_COLUMN: Final[str] = "Type"
_COUNT_COLUMN: Final[str] = "Request Count"
_FAILURE_COLUMN: Final[str] = "Failure Count"
_P95_COLUMN: Final[str] = "95%"

_REQUIRED_COLUMNS: Final[tuple[str, ...]] = (
    _NAME_COLUMN,
    _TYPE_COLUMN,
    _COUNT_COLUMN,
    _FAILURE_COLUMN,
    _P95_COLUMN,
)


class LoadStatsError(RuntimeError):
    """The stats file is missing, malformed, or describes no requests at all."""


@dataclass(frozen=True, slots=True)
class Measurement:
    """One request name's row, reduced to the numbers the gate needs."""

    name: str
    requests: int
    failures: int
    p95_ms: float


def _parse_int(row: dict[str, str], column: str, name: str) -> int:
    raw = (row.get(column) or "").strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise LoadStatsError(
            f"{name!r}: the {column!r} column is {raw!r}, not an integer"
        ) from exc
    if value < 0:
        raise LoadStatsError(f"{name!r}: the {column!r} column is negative ({value})")
    return value


def _parse_p95(row: dict[str, str], name: str) -> float:
    """Read the 95th-percentile column, refusing locust's "no data" marker.

    locust writes `N/A` for every percentile of a name it recorded no response
    for. Reading that as a zero would be the worst possible failure of this
    gate: a path that answered nothing would post the best number in the table.
    """
    raw = (row.get(_P95_COLUMN) or "").strip()
    if raw in {"", "N/A"}:
        raise LoadStatsError(
            f"{name!r}: locust recorded no response times ({_P95_COLUMN}={raw!r}), "
            "so there is no percentile to check"
        )
    try:
        value = float(raw)
    except ValueError as exc:
        raise LoadStatsError(
            f"{name!r}: the {_P95_COLUMN!r} column is {raw!r}, not a number"
        ) from exc
    if value < 0:
        raise LoadStatsError(f"{name!r}: the {_P95_COLUMN!r} column is negative")
    return value


def parse(rows: list[dict[str, str]]) -> tuple[dict[str, Measurement], Measurement]:
    """Split locust's rows into the per-name measurements and the aggregate.

    Rejects, rather than works around:

    - a file with no header or a header missing a column this gate reads, since
      a locust release that renamed one would otherwise drop every check;
    - a duplicate request name, which cannot happen from one run and means two
      runs' CSVs have been concatenated;
    - a missing `Aggregated` row, which is how a truncated file presents.
    """
    if not rows:
        raise LoadStatsError("the stats file has a header but no rows")

    missing = [column for column in _REQUIRED_COLUMNS if column not in rows[0]]
    if missing:
        raise LoadStatsError(
            f"the stats file is missing the column(s) {missing}; this script and "
            "the installed locust disagree about the CSV format"
        )

    measured: dict[str, Measurement] = {}
    aggregated: Measurement | None = None

    for row in rows:
        name = (row.get(_NAME_COLUMN) or "").strip()
        if not name:
            raise LoadStatsError("the stats file has a row with an empty Name")

        # locust spells the aggregate row's name "Aggregated" with an empty
        # Type. A real request name could collide with it only by being called
        # that, which `BUDGETS` would then reject as unbudgeted.
        label = name if not (row.get(_TYPE_COLUMN) or "").strip() else None
        measurement = Measurement(
            name=name,
            requests=_parse_int(row, _COUNT_COLUMN, name),
            failures=_parse_int(row, _FAILURE_COLUMN, name),
            # The aggregate's percentile is read like any other so that a
            # truncated file fails here rather than at the comparison.
            p95_ms=_parse_p95(row, name),
        )

        if label == AGGREGATED_NAME:
            aggregated = measurement
            continue
        if name in measured:
            raise LoadStatsError(
                f"{name!r} appears twice; this file is not one run's stats"
            )
        measured[name] = measurement

    if aggregated is None:
        raise LoadStatsError(
            f"the stats file has no {AGGREGATED_NAME!r} row, so the run did not "
            "finish writing it"
        )
    if not measured:
        raise LoadStatsError("the stats file describes no request names at all")

    return measured, aggregated


def load(path: Path) -> tuple[dict[str, Measurement], Measurement]:
    """Read and parse the CSV, blaming the file by name on any failure."""
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise LoadStatsError(
            f"{path} does not exist; run `uv run python scripts/run_load_test.py` "
            "before this script"
        ) from exc
    except OSError as exc:
        raise LoadStatsError(f"could not read {path}: {exc}") from exc

    reader = csv.DictReader(text.splitlines())
    if reader.fieldnames is None:
        raise LoadStatsError(f"{path} is empty")
    try:
        return parse([dict(row) for row in reader])
    except LoadStatsError as exc:
        raise LoadStatsError(f"{path}: {exc}") from exc


def evaluate(
    measured: dict[str, Measurement],
    aggregated: Measurement,
    budgets: dict[str, Budget],
    headroom: float,
) -> list[str]:
    """Return one line per breach, empty when the run is within budget.

    `headroom` multiplies every ceiling. It exists for the one legitimate case —
    re-running the same profile on hardware slower than the budgets were set on
    — and defaults to 1.0 so that the CI job gates on the numbers as written.
    """
    problems: list[str] = []

    if aggregated.failures:
        problems.append(
            f"{aggregated.failures} of {aggregated.requests} requests failed; "
            "every task asserts its own status, so the run measured something "
            "other than the responses it claims to (see the _failures.csv "
            "beside the stats)"
        )

    for name in sorted(set(budgets) | set(measured)):
        budget = budgets.get(name)
        measurement = measured.get(name)

        if budget is None:
            problems.append(
                f"{name!r} is in the stats but has no budget; add one to "
                "BUDGETS in this script (and a line to docs/load-testing.md) "
                "or take the task out of loadtest/locustfile.py"
            )
            continue
        if measurement is None:
            problems.append(
                f"{name!r} has a budget but is absent from the stats; the task "
                "was renamed, removed, or never ran"
            )
            continue

        if measurement.requests < budget.min_requests:
            problems.append(
                f"{name!r}: {measurement.requests} requests, below the floor of "
                f"{budget.min_requests}; a p95 over that few samples is not a "
                "measurement — lengthen the run or raise the task's weight"
            )
        ceiling = budget.p95_ms * headroom
        if measurement.p95_ms > ceiling:
            problems.append(
                f"{name!r}: p95 {measurement.p95_ms:.0f}ms exceeds "
                f"{ceiling:.0f}ms ({budget.p95_ms:.0f}ms budget"
                f"{f' x {headroom:g} headroom' if headroom != 1.0 else ''})"
            )

    return problems


def report(
    measured: dict[str, Measurement],
    aggregated: Measurement,
    budgets: dict[str, Budget],
    headroom: float,
) -> str:
    """The one-screen table both outcomes print."""
    width = max(len(name) for name in (*measured, AGGREGATED_NAME))
    lines = [f"{'name'.ljust(width)}  {'reqs':>6} {'fail':>5} {'p95':>7} {'budget':>8}"]
    for name in sorted(measured):
        measurement = measured[name]
        budget = budgets.get(name)
        ceiling = (
            f"{budget.p95_ms * headroom:.0f}ms" if budget is not None else "unbudgeted"
        )
        lines.append(
            f"{name.ljust(width)}  {measurement.requests:>6} "
            f"{measurement.failures:>5} {measurement.p95_ms:>5.0f}ms {ceiling:>8}"
        )
    lines.append(
        f"{AGGREGATED_NAME.ljust(width)}  {aggregated.requests:>6} "
        f"{aggregated.failures:>5} {aggregated.p95_ms:>5.0f}ms {'—':>8}"
    )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__ or None)
    parser.add_argument(
        "--stats",
        type=Path,
        default=DEFAULT_STATS_PATH,
        help=f"locust's <prefix>_stats.csv (default: {DEFAULT_STATS_PATH})",
    )
    parser.add_argument(
        "--headroom",
        type=float,
        default=1.0,
        help=(
            "multiply every budget by this factor, for re-running the profile "
            "on hardware slower than the budgets were measured on (default: 1.0)"
        ),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.headroom < 1.0:
        print(
            "error: --headroom tightens nothing; it must be at least 1.0, got "
            f"{args.headroom}",
            file=sys.stderr,
        )
        return 2

    try:
        measured, aggregated = load(args.stats)
    except LoadStatsError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    print(report(measured, aggregated, BUDGETS, args.headroom))

    problems = evaluate(measured, aggregated, BUDGETS, args.headroom)
    if not problems:
        print(f"\nOK — {len(BUDGETS)} endpoint(s) within budget.")
        return 0

    print(f"\nOVER BUDGET — {len(problems)} problem(s):", file=sys.stderr)
    for problem in problems:
        print(f"  - {problem}", file=sys.stderr)
    print("\nSee docs/load-testing.md § Budgets.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
