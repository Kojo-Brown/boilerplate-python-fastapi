"""The mutation threshold gate: what counts as detected, and what is an error.

`mutmut run` exits 0 whether or not mutants survived, so the whole enforcement
of "the suite must still notice N% of mutants" lives in
`scripts/check_mutation_score.py`. A gate that cannot fail is worse than no gate
— it reports a number nobody checks — so these tests are mostly about the ways
it must refuse to pass.

The arithmetic cases are deliberate about the boundary: a score exactly equal to
the threshold passes, because a threshold is a floor, and a run that re-measures
to precisely the configured number should not block a merge.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from scripts.check_mutation_score import (
    DEFAULT_THRESHOLD,
    MutationStatsError,
    load,
    main,
    parse,
    report,
)


def stats(**overrides: Any) -> dict[str, Any]:
    """A complete, well-formed stats payload, shaped like mutmut's own."""
    payload: dict[str, Any] = {
        "killed": 0,
        "survived": 0,
        "total": 0,
        "no_tests": 0,
        "skipped": 0,
        "suspicious": 0,
        "timeout": 0,
        "check_was_interrupted_by_user": 0,
        "segfault": 0,
    }
    payload.update(overrides)
    return payload


def write(tmp_path: Path, payload: object) -> Path:
    path = tmp_path / "mutmut-cicd-stats.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


class TestWhatCountsAsDetected:
    def test_killed_mutants_are_detected(self) -> None:
        score = parse(stats(killed=9, survived=1))
        assert score.detected == 9
        assert score.total == 10
        assert score.percentage == pytest.approx(90.0)

    def test_a_timeout_counts_as_detected(self) -> None:
        """A mutant that hangs the suite has been caught, not missed.

        `while pos < length` becoming `while pos <= length` is a real defect
        and the suite does notice it — by never finishing. Whether mutmut files
        a given infinite-loop mutant under `timeout` or under `killed` also
        depends on how loaded the runner is, so counting only `killed` would
        make the score drift with CI's mood rather than with the test suite.
        """
        assert parse(stats(killed=8, timeout=2)).percentage == pytest.approx(100.0)
        moved = parse(stats(killed=9, timeout=1))
        assert moved.percentage == pytest.approx(100.0)

    @pytest.mark.parametrize("bucket", ["survived", "no_tests", "skipped"])
    def test_an_unnoticed_mutant_is_not_detected(self, bucket: str) -> None:
        score = parse(stats(killed=1, **{bucket: 1}))
        assert score.detected == 1
        assert score.percentage == pytest.approx(50.0)

    @pytest.mark.parametrize("bucket", ["suspicious", "segfault"])
    def test_an_undetermined_outcome_is_not_detected(self, bucket: str) -> None:
        """ "mutmut could not tell" must never read as "the suite caught it".

        Both buckets mean the run itself went wrong. Counting them as kills is
        how a gate comes back green on a broken runner, which is the single
        worst failure available to it.
        """
        score = parse(stats(killed=1, **{bucket: 1}))
        assert score.detected == 1
        assert score.percentage == pytest.approx(50.0)

    def test_every_bucket_is_in_the_denominator(self) -> None:
        score = parse(
            stats(
                killed=1,
                timeout=1,
                survived=1,
                no_tests=1,
                skipped=1,
                suspicious=1,
                segfault=1,
            )
        )
        assert score.total == 7
        assert score.detected == 2


class TestTheThreshold:
    def test_a_score_above_the_threshold_passes(self) -> None:
        assert parse(stats(killed=96, survived=4)).meets(95.0)

    def test_a_score_exactly_at_the_threshold_passes(self) -> None:
        """A threshold is a floor, not something to be strictly above."""
        assert parse(stats(killed=95, survived=5)).meets(95.0)

    def test_a_score_below_the_threshold_fails(self) -> None:
        assert not parse(stats(killed=94, survived=6)).meets(95.0)

    def test_the_default_threshold_is_the_one_the_docs_quote(self) -> None:
        # Changing this is a deliberate act with a PR of its own; the number is
        # justified in docs/mutation-testing.md.
        assert DEFAULT_THRESHOLD == 95.0


class TestRefusingToGuess:
    def test_a_missing_file_is_an_error_and_not_a_zero(self, tmp_path: Path) -> None:
        with pytest.raises(MutationStatsError, match="does not exist"):
            load(tmp_path / "absent.json")

    def test_invalid_json_is_an_error(self, tmp_path: Path) -> None:
        path = tmp_path / "mutmut-cicd-stats.json"
        path.write_text("{not json", encoding="utf-8")
        with pytest.raises(MutationStatsError, match="not valid JSON"):
            load(path)

    @pytest.mark.parametrize("payload", [[], "killed", 7, None])
    def test_a_payload_that_is_not_an_object_is_an_error(
        self, tmp_path: Path, payload: object
    ) -> None:
        with pytest.raises(MutationStatsError, match="expected a JSON object"):
            load(write(tmp_path, payload))

    def test_a_missing_bucket_is_an_error(self) -> None:
        """A renamed bucket must break the build, not shrink the denominator.

        If mutmut ever renames `survived`, treating the absence as zero would
        quietly drop every surviving mutant out of the total and report a
        perfect score. Naming the buckets and demanding all of them makes that
        a loud failure on the day of the upgrade.
        """
        incomplete = stats(killed=1)
        del incomplete["survived"]
        with pytest.raises(MutationStatsError, match="missing the 'survived' count"):
            parse(incomplete)

    @pytest.mark.parametrize("value", [-1, 1.5, "3", None, [1]])
    def test_a_bucket_that_is_not_a_count_is_an_error(self, value: object) -> None:
        with pytest.raises(MutationStatsError, match="not a non-negative integer"):
            parse(stats(killed=1, survived=value))

    def test_a_boolean_is_not_a_count(self) -> None:
        """`bool` is a subclass of `int`, so `True` would otherwise be one mutant."""
        with pytest.raises(MutationStatsError, match="not a non-negative integer"):
            parse(stats(killed=1, survived=True))

    def test_an_interrupted_run_is_an_error(self) -> None:
        """Its numbers describe the mutants it reached, which is an unknown subset."""
        with pytest.raises(MutationStatsError, match="interrupted run"):
            parse(stats(killed=10, check_was_interrupted_by_user=1))

    def test_no_mutants_at_all_is_an_error(self) -> None:
        """The failure mode this guards is a perfect score for measuring nothing.

        An `only_mutate` pattern that matches no file produces a run with zero
        mutants. Dividing by the total would raise; defaulting the score to
        100% would hand back a green gate for a configuration that tests
        nothing at all.
        """
        with pytest.raises(MutationStatsError, match="no mutants at all"):
            parse(stats())

    def test_the_mutmut_total_field_is_ignored(self) -> None:
        """mutmut reports its own `total`, and this script does not trust it.

        The denominator is the sum of the buckets, so a stats file whose
        `total` disagrees — a future mutmut counting something this script does
        not know about — cannot inflate or deflate the score.
        """
        score = parse(stats(killed=1, survived=1, total=1000))
        assert score.total == 2


class TestTheCommandLine:
    def test_a_passing_run_exits_zero(self, tmp_path: Path) -> None:
        path = write(tmp_path, stats(killed=96, survived=4))
        assert main(["--stats", str(path), "--threshold", "95"]) == 0

    def test_a_failing_run_exits_one(self, tmp_path: Path) -> None:
        path = write(tmp_path, stats(killed=90, survived=10))
        assert main(["--stats", str(path), "--threshold", "95"]) == 1

    def test_a_broken_stats_file_exits_two(self, tmp_path: Path) -> None:
        """Distinct from 1 on purpose.

        "The gate could not run" is not "the gate failed": one is a broken
        runner and the other is a real finding, and a CI log that cannot tell
        them apart sends somebody to read `mutmut results` for a file that was
        never written.
        """
        assert main(["--stats", str(tmp_path / "absent.json")]) == 2

    @pytest.mark.parametrize("threshold", ["-1", "101", "nan"])
    def test_a_threshold_outside_zero_to_a_hundred_exits_two(
        self, tmp_path: Path, threshold: str
    ) -> None:
        path = write(tmp_path, stats(killed=1, survived=1))
        assert main(["--stats", str(path), "--threshold", threshold]) == 2

    def test_the_failure_is_reported_on_stdout(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        path = write(tmp_path, stats(killed=90, survived=9, timeout=1))
        main(["--stats", str(path), "--threshold", "95"])
        out = capsys.readouterr().out
        assert "91.00%" in out
        assert "91/100 detected" in out
        assert "FAIL" in out
        # The breakdown is what tells a contributor where to look.
        assert "survived=9" in out

    def test_the_report_omits_empty_buckets(self) -> None:
        line = report(parse(stats(killed=9, survived=1)), 95.0)
        assert "survived=1" in line
        assert "segfault" not in line
