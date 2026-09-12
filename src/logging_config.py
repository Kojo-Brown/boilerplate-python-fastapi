import logging
import sys

import structlog

from src.observability.logs import structlog_processors


def configure_logging(log_level: str = "INFO") -> None:
    level = logging.getLevelName(log_level.upper())

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        # Trace correlation, and the bridge into the OpenTelemetry logs
        # pipeline. Both sit here — after the timestamp, before the renderer —
        # and both are no-ops until something is recording, so the chain does
        # not change shape when the SDK is switched on. See
        # src/observability/logs.py.
        *structlog_processors(),
    ]

    if log_level.upper() == "DEBUG":
        renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer()
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )


logger = structlog.get_logger()
