"""The load profile: four read paths, each with its own latency budget.

Run it with `scripts/run_load_test.py`, which boots the app, mints the
credentials this file needs, and writes the CSV that
`scripts/check_load_budget.py` then gates on. `docs/load-testing.md` has the
reasoning for the whole arrangement; what follows is the reasoning for this
file.

## Why these four, and not a write

The four tasks below are `GET /health`, `GET /health/ready` and the profile
read in both its forms. They are chosen so that the *differences* between them
are readable rather than to cover the API surface:

- `GET /health` touches no dependency at all, so its number is the cost of the
  stack itself — five application middlewares, routing, the exception handlers.
  It is the floor every other budget is measured against.
- `GET /health/ready` adds a live round trip to every configured dependency.
  The gap between it and `/health` is what a dependency check costs.
- `GET /api/v1/users/me` is the real read path: bearer decode, the tenant
  binding's `set_config` statement, the row fetch, an encrypted column
  decrypted, the entity tag, serialisation.
- The same URI with `If-None-Match` answers 304 from the same work minus the
  body. The gap between the two is what conditional GET actually saves — and
  measured here it is nothing, because a profile is about 190 bytes and the
  saving is on the wire rather than in the handler. That is the only place in
  this repository where the claim in `docs/conditional-get.md` is checked
  against a number, and the number is recorded there.

There is deliberately **no write task**. `POST /auth/register` and
`POST /auth/login` are rate-limited to five a minute (`src/auth/router.py`) and
every virtual user here shares one source address, so a sustained write load
would measure `slowapi` rejecting it. `PATCH /api/v1/users/me` would be worse
than useless: it requires `If-Match` against the current version and bumps that
version on success, so concurrent users would spend the run losing preconditions
to each other and the gate would be reading the latency of 412s. A write
profile needs an account per user, seeded out of band; it is a worthwhile
separate item and is noted as one in `docs/load-testing.md`.

## FastHttpUser

`FastHttpUser` (geventhttpclient) rather than `HttpUser` (requests), because
this file's output is a *latency* number. The requests-based client costs
several times more CPU per request, and on a runner shared with uvicorn and
Postgres that cost lands inside the measurement — the client would be reporting
its own overhead as the service's p95.

## Credentials come from the environment, and the run fails without them

The token is minted once, before locust starts, by `scripts/run_load_test.py`:
a login costs an argon2 verification on purpose (~75ms, see
`src/auth/password.py`) and is rate-limited, so it can be neither a task nor an
`on_start`. Reading it here rather than defaulting it keeps this file from
having a second, untested bootstrap path — and a missing variable fails at
import, before a single request is sent, rather than turning into 401s that a
budget on latency alone would happily pass.
"""

from __future__ import annotations

import os
from typing import Final

from locust import FastHttpUser, between, task

# The request names the CSV is keyed by, imported from the module that holds the
# budget for each of them. They are named rather than left to the URI so that
# the two profile reads — same method, same path, different work — are two rows
# rather than one average of both, and they live over there so that a rename
# cannot make a task stop being gated without anybody noticing. Every `name=`
# below is one of these four; `tests/test_load_budget_gate.py` pins that no
# fifth one is introduced as a literal here.
from scripts.check_load_budget import HEALTH, PROFILE, PROFILE_REVALIDATE, READY

#: Set by `scripts/run_load_test.py`. See the module docstring.
_TOKEN_ENV: Final[str] = "LOADTEST_ACCESS_TOKEN"
_ETAG_ENV: Final[str] = "LOADTEST_ETAG"
_TENANT_ENV: Final[str] = "LOADTEST_TENANT_ID"


def _required(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"{name} is not set. This locustfile is driven by "
            "`uv run python scripts/run_load_test.py`, which boots the app and "
            "mints the credentials it needs; see docs/load-testing.md to drive "
            "locust directly."
        )
    return value


class ReadPathUser(FastHttpUser):
    """A client doing what a logged-in caller does: polling and reading itself.

    `between(0.05, 0.2)` rather than no wait at all. A zero-wait user is a
    saturation test — it pushes until something queues, and the latency it then
    reports is the depth of that queue rather than the service's response time.
    The budget this file exists to defend is a response time under a defined,
    unsaturated load, so the users are paced and the arrival rate is a property
    of `--users` instead of of how fast the server replies.
    """

    wait_time = between(0.05, 0.2)

    #: Sent on every request. Built per user in `on_start` rather than put in
    #: `default_headers`, which reaches the request through
    #: geventhttpclient's own merge; passing them explicitly keeps "what this
    #: request sent" readable in this file and nowhere else.
    _headers: dict[str, str]
    _etag: str

    def on_start(self) -> None:
        """Read the credentials this run was given, once per virtual user."""
        self._headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {_required(_TOKEN_ENV)}",
            # `src/tenancy/resolver.py` prefers the tenant inside the token and
            # only falls back to this header, so sending it is belt and braces
            # — but a token that turned out to carry no tenant claim would
            # otherwise make every read here a 500 on a not-null column, which
            # is a confusing way to learn that the bootstrap changed.
            "X-Tenant-ID": _required(_TENANT_ENV),
        }
        self._etag = _required(_ETAG_ENV)

    # Weights, not an even split: the two profile reads are the paths whose
    # latency anybody cares about, and a p95 wants a few hundred samples before
    # it means anything. The gate enforces a floor on the sample count per name
    # (`min_requests` in scripts/check_load_budget.py) so that a profile edited
    # to the point where one of these is barely exercised fails rather than
    # reporting a confident number from thirty requests.
    @task(1)
    def liveness(self) -> None:
        with self.client.get(
            "/health", name=HEALTH, headers=self._headers, catch_response=True
        ) as response:
            if response.status_code != 200:
                response.failure(f"expected 200, got {response.status_code}")

    @task(1)
    def readiness(self) -> None:
        with self.client.get(
            "/health/ready", name=READY, headers=self._headers, catch_response=True
        ) as response:
            # 200 covers both `ready` and `degraded` (see src/health/router.py).
            # A 503 means a dependency this run provisioned is unreachable, and
            # a load test against a half-connected app measures nothing.
            if response.status_code != 200:
                response.failure(f"expected 200, got {response.status_code}")

    @task(3)
    def read_profile(self) -> None:
        with self.client.get(
            "/api/v1/users/me",
            name=PROFILE,
            headers=self._headers,
            catch_response=True,
        ) as response:
            # The status check is the whole reason for `catch_response` here. A
            # 401 is both fast and empty, so a budget on latency alone passes
            # with flying colours against a run where nothing was authorised —
            # which is the failure a load test is most likely to actually have.
            if response.status_code != 200:
                response.failure(f"expected 200, got {response.status_code}")

    @task(3)
    def revalidate_profile(self) -> None:
        with self.client.get(
            "/api/v1/users/me",
            name=PROFILE_REVALIDATE,
            headers={**self._headers, "If-None-Match": self._etag},
            catch_response=True,
        ) as response:
            # 304 and not "2xx or 304": a 200 here means the tag this run was
            # given no longer describes the profile, so the task is silently
            # measuring the unconditional read instead of the revalidation and
            # the two budgets have stopped meaning different things.
            if response.status_code != 304:
                response.failure(f"expected 304, got {response.status_code}")
