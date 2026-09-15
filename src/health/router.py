"""The two probes, and why there are exactly two.

``GET /health`` — *is this process alive?* It touches no dependency at all. A
liveness probe that queried Postgres would turn a brief database blip into the
orchestrator killing and restarting every healthy replica — a recoverable
dependency incident escalated into an outage of its own, by the monitoring.

``GET /health/ready`` — *can this process serve traffic?* It round-trips every
dependency the configuration says a request needs, and answers 503 while any
`required` one is unreachable. This is the endpoint a load balancer and a
Kubernetes `readinessProbe` poll; see `docs/health.md` for the manifest.

There is no third, "startup" probe, and its absence is deliberate rather than
an omission. A startup probe exists to stop a liveness probe killing a process
that is still booting — but uvicorn binds its listening socket only *after* the
lifespan's start-up completes, so during that window there is no socket to
probe and every connection is refused, which every orchestrator already reads
as "not up yet". A start-up endpoint could not be reached in the only window
where it would mean anything.

## The response body

`status` is one of `ready`, `degraded`, `unavailable` and `checks` maps each
check's name to its outcome. `degraded` is served with **200**: something
optional is unreachable, the process can still serve requests, and telling the
load balancer otherwise would shed traffic to no purpose. Alert on the body,
route on the status code.

Both routes send `Cache-Control: no-store`. A cached probe answer is a lie with
a delay on it, and CDNs and reverse proxies do cache unadorned 200s.
"""

from __future__ import annotations

from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Response, status
from pydantic import BaseModel, ConfigDict

from src.health.base import (
    CheckStatus,
    Criticality,
    ReadinessReport,
    ReadinessStatus,
)
from src.health.registry import HealthRegistry
from src.health.wiring import get_health_registry

router = APIRouter(tags=["health"])

HealthRegistryDep = Annotated[HealthRegistry, Depends(get_health_registry)]

#: Probe answers describe this instant and must not be replayed by anything in
#: front of it.
_NO_STORE = "no-store"


class LivenessResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: Literal["ok"]


class CheckReport(BaseModel):
    """One dependency's outcome, as served.

    `error` carries an exception *type* name and never a driver message: this
    endpoint is unauthenticated, and asyncpg and redis-py both quote the
    connection string — credentials included — in theirs.
    """

    model_config = ConfigDict(frozen=True)

    status: CheckStatus
    criticality: Criticality
    duration_ms: float
    description: str
    error: str | None = None


class ReadinessResponse(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: ReadinessStatus
    checks: dict[str, CheckReport]

    @classmethod
    def from_report(cls, report: ReadinessReport) -> ReadinessResponse:
        return cls(
            status=report.status,
            checks={
                outcome.name: CheckReport(
                    status=outcome.status,
                    criticality=outcome.criticality,
                    # Rounded so that a body a human is diffing between two
                    # replicas differs where something differs, rather than in
                    # the ninth decimal place of a duration.
                    duration_ms=round(outcome.duration_ms, 3),
                    description=outcome.description,
                    error=outcome.error,
                )
                for outcome in report.outcomes
            },
        )


@router.get("/health", response_model=LivenessResponse)
async def health(response: Response) -> LivenessResponse:
    """Liveness probe. Deliberately dependency-free."""
    response.headers["Cache-Control"] = _NO_STORE
    return LivenessResponse(status="ok")


@router.get(
    "/health/ready",
    response_model=ReadinessResponse,
    responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ReadinessResponse}},
)
async def readiness(
    response: Response, registry: HealthRegistryDep
) -> ReadinessResponse:
    """Readiness probe. 503 while any required dependency is unreachable."""
    response.headers["Cache-Control"] = _NO_STORE
    report = await registry.report()
    if not report.ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return ReadinessResponse.from_report(report)
