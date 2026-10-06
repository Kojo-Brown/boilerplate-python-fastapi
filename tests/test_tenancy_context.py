"""The tenant context variable, and the two properties that make it safe.

Both claims here are about things that would otherwise fail silently and
across requests rather than inside one: a tenant that outlives its block, and
a tenant that one concurrent request can see on another's task. Neither
produces an exception when it breaks; both produce one customer's rows in
another customer's response.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from src.tenancy.context import current_tenant_id, require_tenant_id, tenant_scope
from src.tenancy.errors import TenantRequiredError

ALPHA = uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001")
BETA = uuid.UUID("bbbbbbbb-0000-0000-0000-000000000002")


class TestScope:
    def test_inside_the_block_the_tenant_is_the_one_given(self) -> None:
        with tenant_scope(ALPHA):
            assert current_tenant_id() == ALPHA

    def test_leaving_the_block_restores_what_was_there_before(self) -> None:
        with tenant_scope(ALPHA):
            with tenant_scope(BETA):
                assert current_tenant_id() == BETA
            assert current_tenant_id() == ALPHA

    def test_an_exception_still_restores(self) -> None:
        with tenant_scope(ALPHA):
            with pytest.raises(RuntimeError), tenant_scope(BETA):
                raise RuntimeError("boom")
            assert current_tenant_id() == ALPHA

    def test_none_is_an_unscoped_block_rather_than_a_no_op(self) -> None:
        """A background job stepping deliberately outside its caller's tenant."""
        with tenant_scope(ALPHA), tenant_scope(None):
            assert current_tenant_id() is None


class TestIsolationBetweenTasks:
    async def test_a_tenant_set_in_a_task_does_not_escape_it(self) -> None:
        """`asyncio` copies the context per task, and that is load-bearing."""

        async def worker() -> None:
            with tenant_scope(BETA):
                await asyncio.sleep(0)

        with tenant_scope(ALPHA):
            await asyncio.gather(worker(), worker())
            assert current_tenant_id() == ALPHA

    async def test_concurrent_tasks_do_not_see_each_other(self) -> None:
        """Two requests on one event loop, interleaved on purpose."""
        seen: list[uuid.UUID | None] = []

        async def serve(tenant_id: uuid.UUID) -> None:
            with tenant_scope(tenant_id):
                # Yields control, so the other task runs between the set and
                # the read — the interleaving a module-level global would lose.
                await asyncio.sleep(0)
                seen.append(current_tenant_id())

        await asyncio.gather(serve(ALPHA), serve(BETA))
        assert sorted(str(t) for t in seen) == sorted([str(ALPHA), str(BETA)])

    async def test_a_task_inherits_the_tenant_of_whoever_created_it(self) -> None:
        """Which is what makes an `await` inside a handler keep working."""
        with tenant_scope(ALPHA):
            assert await asyncio.create_task(_read()) == ALPHA


async def _read() -> uuid.UUID | None:
    return current_tenant_id()


class TestRequire:
    def test_it_returns_the_tenant_when_there_is_one(self) -> None:
        with tenant_scope(ALPHA):
            assert require_tenant_id() == ALPHA

    def test_it_raises_when_there_is_not(self) -> None:
        with tenant_scope(None), pytest.raises(TenantRequiredError) as exc:
            require_tenant_id()
        assert exc.value.status_code == 400
        assert exc.value.error_code == "TENANT_REQUIRED"
