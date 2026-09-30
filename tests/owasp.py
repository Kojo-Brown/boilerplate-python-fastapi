"""Route inventory for the OWASP checklist's structural gates.

Several of the mitigations in `docs/owasp-api-top-10.md` are properties of the
route table rather than of any one handler: that no route takes an object id
from the client, that every route is either deliberately public or requires a
credential, that nothing serves an API outside `/api/v1`. A test for a property
like that has to enumerate the real application, because its whole value is
catching the *next* route — one added months from now by somebody who never read
the checklist.

Walking `app.routes` is not quite enough for that. FastAPI 0.139 does not flatten
`include_router` into the parent's route list: it inserts an `_IncludedRouter`
holding the child router and the prefix it was mounted under, so a naive walk
sees `/openapi.json` and four opaque objects. `flat_routes` does the recursion
and rebuilds the full paths, which is why the gates go through it rather than
through `app.routes` directly — a gate that silently enumerated five routes
instead of twenty would pass for the wrong reason, and `test_the_inventory_is_not_empty`
is there to make that failure loud.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from fastapi import FastAPI
from fastapi.dependencies.utils import get_flat_dependant
from fastapi.routing import APIRoute, APIWebSocketRoute, _IncludedRouter
from starlette.routing import BaseRoute


@dataclass(frozen=True, slots=True)
class RouteFacts:
    """What the gates need to know about one route.

    Args:
        path: The full path the route is served at, prefixes included.
        methods: The HTTP methods it answers, empty for a WebSocket.
        dependencies: Qualified names of every dependency in the route's
            *flattened* tree, so a guard nested two providers deep still counts.
        endpoint: The handler's qualified name.
        websocket: Whether this is a WebSocket route.
    """

    path: str
    methods: frozenset[str]
    dependencies: frozenset[str]
    endpoint: str
    websocket: bool

    @property
    def authenticates(self) -> bool:
        """Whether resolving this route requires a credential.

        Read off the dependency tree rather than the handler's source: both
        guards this application has — `get_current_user` and the closure
        `require_role` builds around it — are dependencies, so a route that
        forgot one differs from a route that has one exactly here.
        """
        return any(
            name == "get_current_user" or name.startswith("require_role")
            for name in self.dependencies
        )


def _dependency_names(route: APIRoute | APIWebSocketRoute) -> frozenset[str]:
    flat = get_flat_dependant(route.dependant, skip_repeats=True)
    return frozenset(
        # `HTTPBearer` and friends are instances, not functions, so there is no
        # `__qualname__` to read; the class name is what identifies them.
        getattr(dependency.call, "__qualname__", type(dependency.call).__name__)
        for dependency in flat.dependencies
        if dependency.call is not None
    )


def flat_routes(app: FastAPI) -> tuple[RouteFacts, ...]:
    """Every route the app serves, with `include_router` nesting resolved."""

    def walk(routes: list[BaseRoute], prefix: str) -> list[RouteFacts]:
        found: list[RouteFacts] = []
        for route in routes:
            if isinstance(route, _IncludedRouter):
                found += walk(
                    route.original_router.routes,
                    prefix + route.include_context.prefix,
                )
                continue
            if not isinstance(route, APIRoute | APIWebSocketRoute):
                # `/openapi.json` is a plain Starlette route with no dependant.
                continue
            found.append(
                RouteFacts(
                    path=prefix + route.path,
                    methods=frozenset(getattr(route, "methods", None) or ()),
                    dependencies=_dependency_names(route),
                    endpoint=route.endpoint.__qualname__,
                    websocket=isinstance(route, APIWebSocketRoute),
                )
            )
        return found

    return tuple(walk(list(app.routes), ""))


#: Paths served outside `/api/v1` on purpose. Probes and a scrape endpoint are
#: infrastructure rather than API surface, and the three documentation pages
#: describe the API rather than being part of it — see `src/main.py`.
NON_API_PATHS: Final[frozenset[str]] = frozenset(
    {"/health", "/health/ready", "/metrics", "/docs", "/docs/oauth2-redirect", "/redoc"}
)
