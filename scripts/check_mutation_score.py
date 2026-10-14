#!/usr/bin/env python
"""Fail the build when the mutation score drops below the configured floor.

``mutmut run`` exits 0 whether or not mutants survived — it reports, it does not
gate — so the threshold has to live somewhere else. This reads the JSON that
``mutmut export-cicd-stats`` writes to ``mutants/mutmut-cicd-stats.json`` and
turns it into an exit code.

**Timeouts count as detected, alongside killed.** A mutant that makes the test
suite hang has been caught: `while pos < length` became `while pos <= length`
and the scanner stopped terminating, which is exactly the kind of defect this
gate exists to find. mutmut reports timeouts in their own bucket, and whether a
given infinite-loop mutant lands in `timeout` or in `killed` depends on how
loaded the runner is — so counting only `killed` would make the score drift with
CI's mood. Three of the mutants here are timeouts on a fast machine and two on a
slow one; under this definition the score is identical either way.

``suspicious`` and ``segfault`` are *not* counted as detected. Both mean mutmut
could not determine an outcome, and a gate that reads "could not tell" as
"caught it" is a gate that passes when the runner is broken.

Usage::

    uv run python scripts/check_mutation_score.py                  # 95% floor
    uv run python scripts/check_mutation_score.py --threshold 90
    uv run python scripts/check_mutation_score.py --stats path.json
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Final

#: The floor the `mutation` CI job enforces. See docs/mutation-testing.md for
#: how it was chosen and what the surviving mutants are.
DEFAULT_THRESHOLD: Final[float] = 95.0

#: Where `mutmut export-cicd-stats` writes, relative to the project root. The
#: path is mutmut's, not ours — it is not configurable on that side.
DEFAULT_STATS_PATH: Final[Path] = Path("mutants/mutmut-cicd-stats.json")

#: Outcomes that mean the suite noticed the mutant. See the module docstring for
#: why `timeout` is in here and `suspicious` and `segfault` are not.
DETECTED_KEYS: Final[tuple[str, ...]] = ("killed", "timeout")

#: Every other outcome mutmut reports, listed so that a bucket this script has
#: never heard of is an error rather than a silent zero.
UNDETECTED_KEYS: Final[tuple[str, ...]] = (
    "survived",
    "no_tests",
    "skipped",
    "suspicious",
    "segfault",
)


class MutationStatsError(RuntimeError):
    """The stats file is missing, malformed, or describes no mutants at all."""


@dataclass(frozen=True, slots=True)
class MutationScore:
    """A mutation run's outcome, reduced to the numbers the gate needs."""

    detected: int
    total: int
    counts: dict[str, int]

    @property
    def percentage(self) -> float:
        """Detected mutants as a percentage of all of them.

        `total` is never zero: `parse` refuses a run with no mutants, because a
        misconfigured `only_mutate` that matches nothing would otherwise divide
        by zero or, worse, report a perfect score for measuring nothing.
        """
        return 100.0 * self.detected / self.total

    def meets(self, threshold: float) -> bool:
        return self.percentage >= threshold


def parse(payload: object) -> MutationScore:
    """Build a `MutationScore` from the parsed contents of mutmut's JSON.

    Rejects, rather than works around:

    - anything that is not a JSON object;
    - a missing bucket, since a mutmut release that renames one would otherwise
      silently drop those mutants from the denominator;
    - a bucket that is not a non-negative integer;
    - `check_was_interrupted_by_user`, which marks a partial run: its numbers
      describe the mutants that were reached, so the score is a measurement of
      an unknown subset and is not comparable with a threshold;
    - a total of zero.
    """
    if not isinstance(payload, dict):
        raise MutationStatsError(
            f"expected a JSON object of mutant counts, got {type(payload).__name__}"
        )

    if payload.get("check_was_interrupted_by_user"):
        raise MutationStatsError(
            "these stats are from an interrupted run and describe only the "
            "mutants it reached; re-run `mutmut run` to completion"
        )

    counts: dict[str, int] = {}
    for key in (*DETECTED_KEYS, *UNDETECTED_KEYS):
        if key not in payload:
            raise MutationStatsError(
                f"mutmut stats are missing the {key!r} count; this script and "
                "the installed mutmut disagree about the outcome buckets"
            )
        value = payload[key]
        # bool is an int subclass, and `True` would otherwise count as one
        # mutant.
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise MutationStatsError(
                f"the {key!r} count is {value!r}, not a non-negative integer"
            )
        counts[key] = value

    detected = sum(counts[key] for key in DETECTED_KEYS)
    total = sum(counts.values())
    if total == 0:
        raise MutationStatsError(
            "the stats describe no mutants at all, so there is no score to "
            "check; `only_mutate` in pyproject.toml most likely matches nothing"
        )

    return MutationScore(detected=detected, total=total, counts=counts)


def load(path: Path) -> MutationScore:
    """Read and parse the stats file, blaming the file by name on any failure."""
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise MutationStatsError(
            f"{path} does not exist; run `mutmut run` and then "
            "`mutmut export-cicd-stats` before this script"
        ) from exc
    except OSError as exc:
        raise MutationStatsError(f"could not read {path}: {exc}") from exc

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise MutationStatsError(f"{path} is not valid JSON: {exc}") from exc

    return parse(payload)


def report(score: MutationScore, threshold: float) -> str:
    """The one-screen summary both outcomes print."""
    verdict = "PASS" if score.meets(threshold) else "FAIL"
    breakdown = " ".join(
        f"{key}={score.counts[key]}"
        for key in (*DETECTED_KEYS, *UNDETECTED_KEYS)
        if score.counts[key]
    )
    return (
        f"mutation score {score.percentage:.2f}% "
        f"({score.detected}/{score.total} detected), "
        f"threshold {threshold:.2f}% — {verdict}\n"
        f"  {breakdown}"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__ or None)
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help=f"minimum percentage of mutants detected (default: {DEFAULT_THRESHOLD})",
    )
    parser.add_argument(
        "--stats",
        type=Path,
        default=DEFAULT_STATS_PATH,
        help=f"path to mutmut's exported stats (default: {DEFAULT_STATS_PATH})",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if not 0.0 <= args.threshold <= 100.0:
        print(
            f"error: --threshold must be between 0 and 100, got {args.threshold}",
            file=sys.stderr,
        )
        return 2

    try:
        score = load(args.stats)
    except MutationStatsError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    print(report(score, args.threshold))
    return 0 if score.meets(args.threshold) else 1


if __name__ == "__main__":
    raise SystemExit(main())
