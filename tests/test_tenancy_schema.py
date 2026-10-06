"""The two DDL hooks that make `Base.metadata.create_all` self-sufficient.

Neither fires on a migrated database — the function and the table already
exist — so neither is reached by the rest of the suite. They are still worth
having and therefore worth testing: a schema built straight from the mappers
would otherwise fail on an undefined function while creating `users`, and
then have no bootstrap tenant for the fixtures' foreign keys to resolve
against. Both are driven directly here rather than through a throwaway
database, because what they do is emit one statement each.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from src.database import Base, _create_current_tenant_function
from src.models.tenant import (
    DEFAULT_TENANT_ID,
    DEFAULT_TENANT_NAME,
    DEFAULT_TENANT_SLUG,
    Tenant,
    _seed_default_tenant,
)
from src.tenancy.sql import CURRENT_TENANT_FUNCTION


def _connection(dialect: str) -> MagicMock:
    connection = MagicMock()
    connection.dialect.name = dialect
    return connection


class TestTheFunctionHook:
    def test_it_creates_the_function_on_postgresql(self) -> None:
        connection = _connection("postgresql")
        _create_current_tenant_function(Base.metadata, connection)
        (statement,) = connection.execute.call_args.args
        assert CURRENT_TENANT_FUNCTION in str(statement)

    def test_it_is_a_no_op_on_any_other_dialect(self) -> None:
        """A SQLite metadata — a doc example, a scratch script — must not
        fail on a statement only Postgres understands."""
        connection = _connection("sqlite")
        _create_current_tenant_function(Base.metadata, connection)
        connection.execute.assert_not_called()

    def test_it_is_registered_on_the_metadata(self) -> None:
        from sqlalchemy import event

        assert event.contains(
            Base.metadata, "before_create", _create_current_tenant_function
        )


class TestTheSeedHook:
    def test_it_inserts_the_bootstrap_tenant(self) -> None:
        connection = _connection("postgresql")
        _seed_default_tenant(Tenant.__table__, connection)
        (statement,) = connection.execute.call_args.args
        params = statement.compile().params
        assert params["id"] == DEFAULT_TENANT_ID
        assert params["slug"] == DEFAULT_TENANT_SLUG
        assert params["name"] == DEFAULT_TENANT_NAME

    def test_it_does_nothing_on_conflict(self) -> None:
        """A schema can be created more than once against one database, and a
        duplicate-key error inside `create_all` is a confusing way to learn
        that."""
        connection = _connection("postgresql")
        _seed_default_tenant(Tenant.__table__, connection)
        (statement,) = connection.execute.call_args.args
        assert "ON CONFLICT" in str(statement).upper()

    def test_it_is_registered_on_the_table(self) -> None:
        from sqlalchemy import event

        assert event.contains(Tenant.__table__, "after_create", _seed_default_tenant)
