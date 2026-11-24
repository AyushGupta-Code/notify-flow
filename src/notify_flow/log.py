"""Structured JSON logging with correlation IDs and PII redaction."""

import contextvars
import logging
import sys
import uuid
from collections.abc import MutableMapping
from typing import Any

import structlog

from notify_flow.redaction import redact

correlation_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "correlation_id", default=None
)


def new_correlation_id() -> str:
    return uuid.uuid4().hex


def _add_correlation_id(
    _: Any, __: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    cid = correlation_id.get()
    if cid and "correlation_id" not in event_dict:
        event_dict["correlation_id"] = cid
    return event_dict


def _redact_pii(_: Any, __: str, event_dict: MutableMapping[str, Any]) -> dict[str, Any]:
    return {
        k: (v if k in ("event", "timestamp", "level") else redact(v, k))
        for k, v in event_dict.items()
    }


def configure_logging(level: str = "INFO", service: str = "notify-flow") -> None:
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            _add_correlation_id,
            _redact_pii,
            structlog.processors.format_exc_info,
            structlog.processors.EventRenamer("msg"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )
    structlog.contextvars.bind_contextvars(service=service)


def get_logger(name: str = "notify_flow") -> Any:
    return structlog.get_logger(name)
