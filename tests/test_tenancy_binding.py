"""The `begin` listener: what it sends, when, and when it sends nothing.

The statement it sends is measured against a real Postgres in
`tests/test_tenancy_db.py`. What is checked here is the part that is about
registration rather than SQL — that it is attached once however many times it
is asked for, that an engine with no tenant in scope pays nothing, and that
the parameter travels as a parameter rather than as text.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import MagicMock

from sqlalchemy import Engine, create_engine, event

from src.tenancy.binding import apply_tenant, bind_tenant_on_begin
from src.tenancy.context import tenant_scope
from src.tenancy.sql import TENANT_SETTING

ALPHA = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")


def _engine() -> Engine:
    """A sync engine that never connects — nothing here opens a transaction."""
    return create_engine("postgresql+asyncpg://unused/unused")


class TestApplyTenant:
    def test_it_sends_nothing_when_nothing_is_in_scope(self) -> None:
        connection = MagicMock()
        with tenant_scope(None):
            apply_tenant(connection)
        connection.execute.assert_not_called()

    def test_it_sends_the_setting_when_there_is_a_tenant(self) -> None:
        connection = MagicMock()
        with tenant_scope(ALPHA):
            apply_tenant(connection)
        statement, parameters = connection.execute.call_args.args
        assert TENANT_SETTING in str(statement)
        assert parameters == {"tenant_id": str(ALPHA)}

    def test_the_tenant_is_a_bound_parameter_not_statement_text(self) -> None:
        """`SET LOCAL` cannot be parameterised; `set_config` can, and must be."""
        connection = MagicMock()
        with tenant_scope(ALPHA):
            apply_tenant(connection)
        statement, _ = connection.execute.call_args.args
        assert str(ALPHA) not in str(statement)


class TestRegistration:
    def test_it_attaches_to_a_sync_engine(self) -> None:
        engine = _engine()
        bind_tenant_on_begin(engine)
        assert event.contains(engine, "begin", apply_tenant)

    def test_registering_twice_attaches_once(self) -> None:
        """Two round trips per transaction otherwise, and nothing would notice."""
        engine = _engine()
        bind_tenant_on_begin(engine)
        bind_tenant_on_begin(engine)
        event.remove(engine, "begin", apply_tenant)
        assert not event.contains(engine, "begin", apply_tenant)

    def test_an_async_engine_is_unwrapped_to_its_sync_engine(self) -> None:
        from sqlalchemy.ext.asyncio import create_async_engine

        engine = create_async_engine("postgresql+asyncpg://unused/unused")
        bind_tenant_on_begin(engine)
        assert event.contains(engine.sync_engine, "begin", apply_tenant)

    def test_the_application_engine_is_already_bound(self) -> None:
        """Registered at import in `src/database.py`, not in the lifespan."""
        from src.database import engine as app_engine

        assert event.contains(app_engine.sync_engine, "begin", apply_tenant)

    def test_the_suite_binds_every_engine_in_the_process(self) -> None:
        """`tests/conftest.py` registers on the class; see the comment there."""
        assert event.contains(Engine, "begin", apply_tenant)


class TestThroughARealBegin:
    """The listener firing, without a database, via SQLAlchemy's own dispatch."""

    def test_begin_fires_the_listener(self) -> None:
        engine = _engine()
        bind_tenant_on_begin(engine)
        connection = MagicMock()
        with tenant_scope(ALPHA):
            engine.dispatch.begin(connection)
        assert connection.execute.called

    def test_begin_fires_nothing_outside_a_tenant(self) -> None:
        engine = _engine()
        bind_tenant_on_begin(engine)
        connection: Any = MagicMock()
        with tenant_scope(None):
            engine.dispatch.begin(connection)
        connection.execute.assert_not_called()
