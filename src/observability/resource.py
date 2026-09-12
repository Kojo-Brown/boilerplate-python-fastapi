"""What every span, metric and log record says about the process that made it.

A `Resource` is attached once, to the providers, and copied onto everything
they emit. It is the only part of the telemetry pipeline that answers "which
deployment is this?", and getting it wrong is expensive in a way that is hard
to notice later: a backend groups by `service.name`, so two services sharing a
name are one service with confusing latency, and a `service.name` that changes
between releases is two services with half the history each.

`Resource.create` merges what is passed here over the SDK's own detectors,
which read `OTEL_RESOURCE_ATTRIBUTES` and add `telemetry.sdk.*` and
`process.runtime.*`. That merge order is the useful one: a deployment can add
`k8s.pod.name` or `service.instance.id` from the environment without this
module knowing those keys exist, and the three attributes set below still win.
"""

from __future__ import annotations

from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes

from src.config import Settings


def build_resource(settings: Settings) -> Resource:
    """The identity this process reports under.

    `deployment.environment` comes from `ENVIRONMENT`, the same value that
    decides SQLAlchemy echo and the logging renderer, so a span that says
    `production` was produced by a process that believed it was in production.
    A backend filtering on it is filtering on the process's own belief rather
    than on a label applied by whatever deployed it, which is what makes it
    trustworthy when the two disagree.
    """
    return Resource.create(
        {
            ResourceAttributes.SERVICE_NAME: settings.OTEL_SERVICE_NAME,
            ResourceAttributes.SERVICE_VERSION: settings.OTEL_SERVICE_VERSION,
            ResourceAttributes.DEPLOYMENT_ENVIRONMENT: settings.ENVIRONMENT,
        }
    )


__all__ = ["build_resource"]
