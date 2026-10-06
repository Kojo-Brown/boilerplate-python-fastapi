"""The isolation report's logic, and the script that prints it.

`inspect_isolation` is also run against a real database in
`tests/test_tenancy_db.py`, which is where the catalog queries are proven to
mean what this file assumes. What is here is the judgement built on top of
them — in particular that each of the three silent bypasses is reported as a
problem rather than only the one everybody remembers.
"""

from __future__ import annotations

from typing import Any

import pytest

from scripts.check_tenant_isolation import render
from src.tenancy.isolation import IsolationReport, TableIsolation, inspect_isolation

URL = "postgresql+asyncpg://app:hunter2@db.internal:5432/app"


def _table(**overrides: Any) -> TableIsolation:
    fields: dict[str, Any] = {
        "table": "users",
        "exists": True,
        "row_security_enabled": True,
        "row_security_forced": True,
        "policy_count": 1,
    }
    return TableIsolation(**{**fields, **overrides})


def _report(**overrides: Any) -> IsolationReport:
    fields: dict[str, Any] = {
        "role": "app",
        "is_superuser": False,
        "bypasses_rls": False,
        "function_exists": True,
        "tables": (_table(),),
    }
    return IsolationReport(**{**fields, **overrides})


class TestTableIsolation:
    def test_a_fully_configured_table_has_no_problems(self) -> None:
        assert _table().problems == []

    def test_a_missing_table_reports_only_that(self) -> None:
        """Everything else would be noise about a table that is not there."""
        assert _table(exists=False).problems == ["table 'users' does not exist"]

    def test_rls_not_enabled_is_reported(self) -> None:
        assert _table(row_security_enabled=False).problems == [
            "users: row-level security is not enabled"
        ]

    def test_rls_not_forced_is_reported(self) -> None:
        """The owner exemption — the bypass that looks exactly like working."""
        assert _table(row_security_forced=False).problems == [
            "users: row-level security is not FORCEd"
        ]

    def test_a_table_with_no_policy_is_reported(self) -> None:
        """Denies everything, which is safe and is nobody's intention."""
        assert _table(policy_count=0).problems == ["users: no policy is defined"]


class TestIsolationReport:
    def test_a_healthy_report_is_enforced(self) -> None:
        report = _report()
        assert report.enforced
        assert report.problems == []

    def test_a_superuser_is_not_enforced(self) -> None:
        report = _report(is_superuser=True)
        assert not report.enforced
        assert "superuser" in report.problems[0]

    def test_bypassrls_is_not_enforced(self) -> None:
        assert "BYPASSRLS" in _report(bypasses_rls=True).problems[0]

    def test_a_missing_function_is_reported(self) -> None:
        assert (
            "app_current_tenant_id() is missing"
            in _report(function_exists=False).problems[0]
        )

    def test_table_problems_are_included(self) -> None:
        report = _report(tables=(_table(row_security_forced=False),))
        assert report.problems == ["users: row-level security is not FORCEd"]

    def test_every_problem_is_reported_not_just_the_first(self) -> None:
        """An operator fixing one at a time is an operator doing four deploys."""
        report = _report(
            is_superuser=True,
            function_exists=False,
            tables=(_table(policy_count=0), _table(table="refresh_tokens")),
        )
        assert len(report.problems) == 3


class _FakeResult:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def one(self) -> Any:
        return self._rows[0]

    def scalar(self) -> Any:
        return self._rows[0]

    def all(self) -> list[Any]:
        return self._rows


class _Row:
    def __init__(self, **fields: Any) -> None:
        self.__dict__.update(fields)


class _FakeConnection:
    """Answers the three catalog queries in the order they are issued."""

    def __init__(self, results: list[_FakeResult]) -> None:
        self._results = list(results)
        self.calls: list[Any] = []

    async def execute(self, statement: Any, parameters: Any = None) -> Any:
        self.calls.append((statement, parameters))
        return self._results.pop(0)


class TestInspectIsolation:
    async def test_it_reads_the_role_the_function_and_the_tables(self) -> None:
        connection = _FakeConnection(
            [
                _FakeResult([_Row(role="app", rolsuper=False, rolbypassrls=False)]),
                _FakeResult([True]),
                _FakeResult(
                    [
                        _Row(
                            relname="users",
                            relrowsecurity=True,
                            relforcerowsecurity=True,
                            policies=1,
                        )
                    ]
                ),
            ]
        )
        report = await inspect_isolation(connection, tables=("users",))
        assert report.enforced
        assert report.role == "app"
        assert len(connection.calls) == 3

    async def test_a_table_the_catalog_does_not_know_is_reported_missing(self) -> None:
        connection = _FakeConnection(
            [
                _FakeResult([_Row(role="app", rolsuper=False, rolbypassrls=False)]),
                _FakeResult([True]),
                _FakeResult([]),
            ]
        )
        report = await inspect_isolation(connection, tables=("users",))
        assert report.problems == ["table 'users' does not exist"]

    async def test_the_report_keeps_the_order_asked_for(self) -> None:
        """So the output is stable rather than in catalog order."""
        connection = _FakeConnection(
            [
                _FakeResult([_Row(role="app", rolsuper=False, rolbypassrls=False)]),
                _FakeResult([True]),
                _FakeResult([]),
            ]
        )
        report = await inspect_isolation(connection, tables=("b", "a"))
        assert [t.table for t in report.tables] == ["b", "a"]


class TestRender:
    def test_a_healthy_report_says_so_and_names_the_role(self) -> None:
        output = render(_report(), URL)
        assert "OK" in output
        assert "app" in output

    def test_an_unhealthy_report_lists_every_problem(self) -> None:
        output = render(_report(is_superuser=True, function_exists=False), URL)
        assert "NOT ENFORCED" in output
        assert output.count("  - ") == 2

    @pytest.mark.parametrize("report", [_report(), _report(is_superuser=True)])
    def test_the_password_is_never_printed(self, report: IsolationReport) -> None:
        """This runs in CI logs; the URL it is handed carries a credential."""
        assert "hunter2" not in render(report, URL)
