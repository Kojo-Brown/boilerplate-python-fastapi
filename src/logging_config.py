import logging
import sys

import structlog

from src.config import Settings
from src.config import settings as default_settings
from src.observability.logs import structlog_processors
from src.redaction.processor import build_redaction_processor


def configure_logging(
    log_level: str = "INFO", settings: Settings | None = None
) -> None:
    level = logging.getLevelName(log_level.upper())
    settings = settings if settings is not None else default_settings

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        # PII redaction. Position is the decision, not the presence: it is after
        # `merge_contextvars`, so the request-scoped fields bound in
        # src/middleware/request_id.py are in the dict by the time it runs — the
        # raw `query=` string among them — and it is before the two processors
        # below, because one of those is a *sink*. `log_forwarder` mirrors the
        # event into the OpenTelemetry logs pipeline, so a redactor placed after
        # it would clean the copy on stdout and export the original, which is
        # the one order that looks like it works. See src/redaction/processor.py
        # and docs/pii-redaction.md.
        build_redaction_processor(settings),
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
