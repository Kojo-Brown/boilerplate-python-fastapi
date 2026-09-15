"""The health-check contract, free of FastAPI, redis, SQLAlchemy and settings.

A check is a name, a criticality and something to await. It signals failure by
raising, because the natural body of a check is one line — ``await
client.ping()``, ``SELECT 1`` — and asking it to catch, classify and package its
own exception would put that logic in every implementation instead of once in
the registry.

## Criticality is the whole design

A readiness probe exists to answer one question — *should traffic be sent to
this process?* — and the answer is not "is everything perfect". It is "is
anything that a request cannot be served without unreachable". Those are
different questions, and collapsing them is how a warm cache being down takes
an entire deployment out of its load balancer.

So every check declares whether it is `required` or `optional`, and the two
produce different HTTP statuses: a failed required check is a 503, a failed
optional check is a 200 whose body says `degraded`. The body is for a human or
a dashboard; the status code is for the load balancer, which must only ever be
told to stop sending traffic when this process genuinely cannot serve it.

Criticality is not a property of a dependency in the abstract. It is a property
of *this deployment's configuration*, which is why `src/health/wiring.py` reads
it off the settings rather than hard-coding it per backend — see the
`IDEMPOTENCY_FAIL_OPEN` case there, where one boolean decides whether an
unreachable Redis is a 503 or a footnote.

## Why a failure detail is a type name and never a message

The probe is served unauthenticated to anything that can reach the port, and
driver errors quote the connection string: asyncpg and redis-py both put the
host — and, in several failure paths, the user and password — into the message.
So the response carries `type(exc).__name__` and nothing else, which is a code
identifier and cannot contain a credential. The message goes to the log, where
it is useful and not public.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable

#: Whether a request can be served while this dependency is unreachable.
#: `required` failures make the process unready (503); `optional` failures
#: degrade it and leave it serving.
Criticality = Literal["required", "optional"]

#: The outcome of one check. There is no third value: a check either completed
#: or it did not, and anything subtler belongs in `detail`.
CheckStatus = Literal["ok", "failed"]

#: The aggregate. `degraded` is deliberately a 200 — see the module docstring.
ReadinessStatus = Literal["ready", "degraded", "unavailable"]


@runtime_checkable
class HealthCheck(Protocol):
    """One dependency probe.

    Implementations live in `src/health/checks.py`; anything matching this
    shape can be added to a `HealthRegistry`, including a check defined in an
    application that builds on this boilerplate.
    """

    @property
    def name(self) -> str:
        """Stable key for this check in the probe body. Unique per registry."""

    @property
    def criticality(self) -> Criticality:
        """Whether a failure here should stop traffic reaching this process."""

    @property
    def description(self) -> str:
        """Static, credential-free summary of what is being probed.

        Served in the response body, so it names subsystems rather than URLs.
        """

    async def probe(self) -> None:
        """Round-trip the dependency. Raise anything at all to fail the check.

        Must be cancellation-safe: the registry runs every probe under
        `asyncio.timeout`, so a probe is expected to be interrupted at any
        await point and to release whatever it holds on the way out.
        """


@dataclass(frozen=True, slots=True)
class CheckOutcome:
    """What one check did on one run.

    `duration_ms` is measured by the registry rather than reported by the
    check, so it includes everything the check does and cannot be forgotten by
    an implementation.
    """

    name: str
    criticality: Criticality
    status: CheckStatus
    duration_ms: float
    description: str
    #: `None` when the check passed; otherwise the exception's *type* name.
    #: Never a driver message — see the module docstring.
    error: str | None = None

    @property
    def blocks_traffic(self) -> bool:
        """Whether this outcome alone makes the process unready."""
        return self.status == "failed" and self.criticality == "required"


@dataclass(frozen=True, slots=True)
class ReadinessReport:
    """Every check from one run, plus the aggregate they add up to."""

    status: ReadinessStatus
    outcomes: tuple[CheckOutcome, ...]

    @property
    def ready(self) -> bool:
        """Whether traffic should still be routed here.

        True for `degraded`: something optional is down, and a process that
        can still serve requests must not be taken out of rotation for it.
        """
        return self.status != "unavailable"


def aggregate(outcomes: tuple[CheckOutcome, ...]) -> ReadinessStatus:
    """Reduce per-check outcomes to the one word the probe answers with.

    A registry with no checks at all reports `ready`. That is the honest answer
    rather than a defensive `unavailable`: a process configured with in-process
    backends for everything genuinely has no external dependency to wait for,
    and failing its readiness probe would mean it never serves traffic.
    """
    if any(outcome.blocks_traffic for outcome in outcomes):
        return "unavailable"
    if any(outcome.status == "failed" for outcome in outcomes):
        return "degraded"
    return "ready"
