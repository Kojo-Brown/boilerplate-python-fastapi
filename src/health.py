"""Liveness and readiness probes.

The two answer different questions and must not be collapsed into one endpoint:

``GET /health`` — *is this process alive?* It touches no dependency at all. If a
liveness probe went to the database, a brief Postgres outage would make the
orchestrator kill and restart every healthy replica, turning a recoverable
dependency blip into an outage of its own.

``GET /health/ready`` — *can this process serve traffic?* It round-trips a
``SELECT 1`` through the async session, so it reports unavailable while Postgres
is unreachable and recovers by itself once the database comes back. That is the
endpoint a load balancer or a `readinessProbe` should poll, and it is what the
CI start-up smoke test asserts against a real Postgres.
"""

from typing import Annotated, Literal

import structlog
from fastapi import APIRouter, Depends, Response, status
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_db

logger = structlog.get_logger(__name__)

router = APIRouter(tags=["health"])

DbSession = Annotated[AsyncSession, Depends(get_db)]

# A refused TCP connect surfaces as ConnectionRefusedError (an OSError) and never
# reaches SQLAlchemy's wrapping layer, so catching SQLAlchemyError alone would let
# the most common failure — Postgres simply not listening — escape as a 500 and
# make the probe useless exactly when it matters.
_UNREACHABLE = (SQLAlchemyError, OSError)


class LivenessResponse(BaseModel):
    status: Literal["ok"]


class ReadinessResponse(BaseModel):
    status: Literal["ready", "unavailable"]
    database: Literal["ok", "unreachable"]


@router.get("/health", response_model=LivenessResponse)
async def health() -> LivenessResponse:
    """Liveness probe. Deliberately dependency-free."""
    return LivenessResponse(status="ok")


@router.get(
    "/health/ready",
    response_model=ReadinessResponse,
    responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ReadinessResponse}},
)
async def readiness(response: Response, db: DbSession) -> ReadinessResponse:
    """Readiness probe. Reports 503 while the database is unreachable."""
    try:
        await db.execute(text("SELECT 1"))
    except _UNREACHABLE as exc:
        logger.warning(
            "readiness.database_unreachable",
            error=str(exc),
            error_type=type(exc).__name__,
        )
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return ReadinessResponse(status="unavailable", database="unreachable")

    return ReadinessResponse(status="ready", database="ok")
