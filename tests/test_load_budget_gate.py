"""The latency gate: what counts as a breach, and what is an error.

`locust --headless` exits 0 at any response time whatsoever, so the whole
enforcement of "this endpoint still answers in under N milliseconds" lives in
`scripts/check_load_budget.py`. A gate that cannot fail is worse than no gate —
it publishes a number nobody checks — so most of what follows is about the ways
it must refuse to pass.

Three of them are the reason this file is longer than the arithmetic needs:

- **`N/A` percentiles.** locust writes `N/A` for a name it recorded no response
  for. Read as a zero, a path that answered nothing would post the best number
  in the table, which is the worst failure available to a latency gate.
- **A name in the stats with no budget, and a budget with no name.** Either
  direction means the profile and the gate have drifted, and the dangerous one
  is silent: a renamed task simply stops being measured.
- **A sample-count floor.** A p95 over thirty requests is not a p95.

The boundary cases are deliberate: a p95 exactly equal to the ceiling passes,
because a budget is a ceiling and a run that re-measures to precisely the
configured number should not block a merge; a request count exactly equal to
the floor passes, for the same reason.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path

import pytest

from scripts.check_load_budget import (
    AGGREGATED_NAME,
    BUDGETS,
    HEALTH,
    PROFILE,
    PROFILE_REVALIDATE,
    READY,
    Budget,
    LoadStatsError,
    Measurement,
    evaluate,
    load,
    main,
    parse,
    report,
)

LOCUSTFILE = Path(__file__).resolve().parent.parent / "loadtest" / "locustfile.py"

#: The full set of columns locust 2.x writes to `<prefix>_stats.csv`, in order.
#: Spelled out so that a row built here is shaped like a real one and a test
#: cannot accidentally pass by omitting the column under test.
COLUMNS: tuple[str, ...] = (
    "Type",
    "Name",
    "Request Count",
    "Failure Count",
    "Median Response Time",
    "Average Response Time",
    "Min Response Time",
    "Max Response Time",
    "Average Content Size",
    "Requests/s",
    "Failures/s",
    "50%",
    "66%",
    "75%",
    "80%",
    "90%",
    "95%",
    "98%",
    "99%",
    "99.9%",
    "99.99%",
    "100%",
)


def row(
    name: str,
    *,
    type_: str = "GET",
    requests: int | str = 1000,
    failures: int | str = 0,
    p95: float | str = 10,
) -> dict[str, str]:
    """One stats row, with every column locust writes present."""
    values = dict.fromkeys(COLUMNS, "0")
    values["Type"] = type_
    values["Name"] = name
    values["Request Count"] = str(requests)
    values["Failure Count"] = str(failures)
    values["95%"] = str(p95)
    return values


def aggregated(**overrides: object) -> dict[str, str]:
    """The run-wide row: locust spells it `Aggregated` with an empty Type."""
    return row(AGGREGATED_NAME, type_="", **overrides)  # type: ignore[arg-type]


def full_run(
    *, p95: float = 10, failures: int = 0, requests: int = 2000
) -> list[dict[str, str]]:
    """A complete, passing run: every budgeted name plus the aggregate."""
    return [
        row(name, p95=p95, failures=failures, requests=requests) for name in BUDGETS
    ] + [aggregated(p95=p95, failures=failures, requests=requests * len(BUDGETS))]


def write(tmp_path: Path, rows: list[dict[str, str]]) -> Path:
    path = tmp_path / "load_stats.csv"
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=list(COLUMNS))
    writer.writeheader()
    writer.writerows(rows)
    path.write_text(buffer.getvalue(), encoding="utf-8")
    return path


class TestTheGateAndTheProfileAgree:
    """The two lists that must not drift apart."""

    def test_every_budget_names_a_request_the_profile_makes(self) -> None:
        assert set(BUDGETS) == {HEALTH, READY, PROFILE, PROFILE_REVALIDATE}

    def test_the_locustfile_names_no_request_of_its_own(self) -> None:
        """Every `name=` in the profile is one of the imported constants.

        A literal there would be a fifth request name that `BUDGETS` has never
        heard of. The gate does catch that at run time — an unbudgeted row is a
        failure — but only after someone has waited a minute for the run, and
        only on the branch that added it.
        """
        source = LOCUSTFILE.read_text(encoding="utf-8")
        assert 'name="' not in source
        assert "name='" not in source

    def test_every_budget_has_a_usable_floor(self) -> None:
        for name, budget in BUDGETS.items():
            assert budget.p95_ms > 0, name
            # Below a hundred samples a 95th percentile is five requests wide.
            assert budget.min_requests >= 100, name


class TestParsing:
    def test_splits_the_aggregate_from_the_named_rows(self) -> None:
        measured, total = parse(full_run())

        assert set(measured) == set(BUDGETS)
        assert total.name == AGGREGATED_NAME
        assert total.requests == 2000 * len(BUDGETS)

    def test_reads_the_numbers_off_the_right_columns(self) -> None:
        measured, _ = parse(
            [row(HEALTH, requests=7, failures=3, p95=42.5), aggregated()]
        )

        assert measured[HEALTH] == Measurement(
            name=HEALTH, requests=7, failures=3, p95_ms=42.5
        )

    def test_rejects_a_run_with_no_rows(self) -> None:
        with pytest.raises(LoadStatsError, match="no rows"):
            parse([])

    def test_rejects_a_header_missing_a_column_the_gate_reads(self) -> None:
        incomplete = row(HEALTH)
        del incomplete["95%"]

        with pytest.raises(LoadStatsError, match=r"missing the column"):
            parse([incomplete])

    def test_rejects_a_file_with_no_aggregate_row(self) -> None:
        with pytest.raises(LoadStatsError, match="no 'Aggregated' row"):
            parse([row(HEALTH)])

    def test_rejects_two_runs_concatenated(self) -> None:
        with pytest.raises(LoadStatsError, match="appears twice"):
            parse([row(HEALTH), row(HEALTH), aggregated()])

    def test_rejects_a_row_with_no_name(self) -> None:
        with pytest.raises(LoadStatsError, match="empty Name"):
            parse([row("  "), aggregated()])

    def test_rejects_an_aggregate_with_nothing_beside_it(self) -> None:
        with pytest.raises(LoadStatsError, match="no request names at all"):
            parse([aggregated()])

    @pytest.mark.parametrize("marker", ["N/A", ""])
    def test_refuses_locusts_no_data_percentile(self, marker: str) -> None:
        """The failure this gate exists to not have: nothing read as fast."""
        with pytest.raises(LoadStatsError, match="no response times"):
            parse([row(HEALTH, p95=marker), aggregated()])

    @pytest.mark.parametrize("bad", ["", "fast", "1.5"])
    def test_rejects_a_non_integer_request_count(self, bad: str) -> None:
        with pytest.raises(LoadStatsError, match="not an integer"):
            parse([row(HEALTH, requests=bad), aggregated()])

    def test_rejects_a_negative_count(self) -> None:
        with pytest.raises(LoadStatsError, match="negative"):
            parse([row(HEALTH, failures=-1), aggregated()])

    def test_rejects_a_non_numeric_percentile(self) -> None:
        with pytest.raises(LoadStatsError, match="not a number"):
            parse([row(HEALTH, p95="quick"), aggregated()])

    def test_rejects_a_negative_percentile(self) -> None:
        with pytest.raises(LoadStatsError, match="negative"):
            parse([row(HEALTH, p95=-1), aggregated()])


class TestLoading:
    def test_reads_a_real_csv(self, tmp_path: Path) -> None:
        measured, total = load(write(tmp_path, full_run()))

        assert set(measured) == set(BUDGETS)
        assert total.name == AGGREGATED_NAME

    def test_names_the_missing_file_and_says_what_writes_it(
        self, tmp_path: Path
    ) -> None:
        with pytest.raises(LoadStatsError, match="run_load_test.py"):
            load(tmp_path / "absent_stats.csv")

    def test_rejects_an_empty_file(self, tmp_path: Path) -> None:
        path = tmp_path / "empty_stats.csv"
        path.write_text("", encoding="utf-8")

        with pytest.raises(LoadStatsError, match="is empty"):
            load(path)

    def test_blames_the_file_by_name(self, tmp_path: Path) -> None:
        path = write(tmp_path, [row(HEALTH)])

        with pytest.raises(LoadStatsError, match=str(path)):
            load(path)


class TestEvaluation:
    def accept(self, **kwargs: float | int) -> list[str]:
        measured, total = parse(full_run(**kwargs))  # type: ignore[arg-type]
        return evaluate(measured, total, BUDGETS, headroom=1.0)

    def test_a_run_inside_every_budget_has_no_problems(self) -> None:
        assert self.accept(p95=1) == []

    def test_a_p95_exactly_on_the_ceiling_passes(self) -> None:
        ceiling = min(budget.p95_ms for budget in BUDGETS.values())

        assert self.accept(p95=ceiling) == []

    def test_a_p95_over_the_ceiling_is_reported_with_both_numbers(self) -> None:
        ceiling = min(budget.p95_ms for budget in BUDGETS.values())
        problems = self.accept(p95=ceiling + 1)

        assert problems
        assert all("exceeds" in problem for problem in problems)

    def test_a_single_failed_request_fails_the_run(self) -> None:
        measured, total = parse(full_run(p95=1, failures=1))
        problems = evaluate(measured, total, BUDGETS, headroom=1.0)

        assert any("requests failed" in problem for problem in problems)

    def test_a_count_on_the_floor_passes_and_one_below_it_does_not(self) -> None:
        floor = max(budget.min_requests for budget in BUDGETS.values())

        assert self.accept(p95=1, requests=floor) == []
        assert any(
            "below the floor" in problem
            for problem in self.accept(p95=1, requests=floor - 1)
        )

    def test_an_unbudgeted_name_fails_rather_than_being_ignored(self) -> None:
        rows = [*full_run(p95=1), row("GET /api/v1/exports/users", p95=1)]
        measured, total = parse(rows)

        problems = evaluate(measured, total, BUDGETS, headroom=1.0)

        assert any("has no budget" in problem for problem in problems)

    def test_a_budgeted_name_absent_from_the_stats_fails(self) -> None:
        rows = [r for r in full_run(p95=1) if r["Name"] != HEALTH]
        measured, total = parse(rows)

        problems = evaluate(measured, total, BUDGETS, headroom=1.0)

        assert any(
            HEALTH in problem and "absent from the stats" in problem
            for problem in problems
        )

    def test_headroom_loosens_every_ceiling_by_the_same_factor(self) -> None:
        ceiling = min(budget.p95_ms for budget in BUDGETS.values())
        measured, total = parse(full_run(p95=ceiling * 1.5))

        assert evaluate(measured, total, BUDGETS, headroom=1.0)
        assert evaluate(measured, total, BUDGETS, headroom=2.0) == []

    def test_a_breach_names_the_headroom_it_was_already_given(self) -> None:
        measured, total = parse(full_run(p95=10_000))
        problems = evaluate(measured, total, BUDGETS, headroom=2.0)

        assert any("x 2 headroom" in problem for problem in problems)

    def test_an_empty_budget_table_rejects_everything_measured(self) -> None:
        """The misconfiguration that would otherwise gate on nothing."""
        measured, total = parse(full_run(p95=1))

        problems = evaluate(measured, total, {}, headroom=1.0)

        assert len(problems) == len(BUDGETS)


class TestReport:
    def test_prints_a_line_per_name_and_the_aggregate(self) -> None:
        measured, total = parse(full_run(p95=3))

        rendered = report(measured, total, BUDGETS, headroom=1.0)

        # A header, one line per budgeted name, and the aggregate.
        assert len(rendered.splitlines()) == len(BUDGETS) + 2
        for name in BUDGETS:
            assert name in rendered
        assert AGGREGATED_NAME in rendered

    def test_marks_a_measured_name_that_has_no_budget(self) -> None:
        measured, total = parse([*full_run(p95=3), row("GET /unknown", p95=3)])

        assert "unbudgeted" in report(measured, total, BUDGETS, headroom=1.0)


class TestExitCodes:
    def test_zero_when_every_endpoint_is_inside_its_budget(
        self, tmp_path: Path
    ) -> None:
        path = write(tmp_path, full_run(p95=1))

        assert main(["--stats", str(path)]) == 0

    def test_one_when_an_endpoint_is_over_budget(self, tmp_path: Path) -> None:
        path = write(tmp_path, full_run(p95=10_000))

        assert main(["--stats", str(path)]) == 1

    def test_two_when_the_stats_cannot_be_read(self, tmp_path: Path) -> None:
        assert main(["--stats", str(tmp_path / "nope.csv")]) == 2

    def test_two_when_headroom_would_tighten_the_budgets(self, tmp_path: Path) -> None:
        path = write(tmp_path, full_run(p95=1))

        assert main(["--stats", str(path), "--headroom", "0.5"]) == 2

    def test_headroom_of_one_is_accepted(self, tmp_path: Path) -> None:
        path = write(tmp_path, full_run(p95=1))

        assert main(["--stats", str(path), "--headroom", "1.0"]) == 0

    def test_the_table_is_printed_on_both_outcomes(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        main(["--stats", str(write(tmp_path, full_run(p95=10_000)))])
        over = capsys.readouterr()

        main(["--stats", str(write(tmp_path, full_run(p95=1)))])
        under = capsys.readouterr()

        assert HEALTH in over.out and HEALTH in under.out
        assert "OVER BUDGET" in over.err
        assert "within budget" in under.out


class TestBudgetRecord:
    """`Budget` is a value, and the gate reads it from several places."""

    def test_is_frozen(self) -> None:
        budget = Budget(p95_ms=1.0, min_requests=1)

        with pytest.raises(AttributeError):
            budget.p95_ms = 2.0  # type: ignore[misc]

    def test_measurement_is_frozen(self) -> None:
        measurement = Measurement(name="x", requests=1, failures=0, p95_ms=1.0)

        with pytest.raises(AttributeError):
            measurement.requests = 2  # type: ignore[misc]
