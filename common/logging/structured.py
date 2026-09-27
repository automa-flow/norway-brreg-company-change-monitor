"""Logfmt-style structured logging.

Every log line is ``<ts> <LEVEL> <message> key=value ...``. The field set the
Actors are expected to use is:

    actor        which Actor emitted the line
    operation    what it was doing ("company.check", "krz.bootstrap", ...)
    request_id   correlates all lines belonging to one logical unit of work
    source       the upstream data source ("KRZ")
    duration     seconds, float
    status       outcome ("SUCCESS", "FAILED", HTTP status, ...)
    records      how many records the operation produced

Logfmt rather than JSON because these lines are read by humans in the Apify run
log far more often than they are parsed. ``LOG_FORMAT=json`` switches to JSON
lines when something downstream does need to parse them.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from collections.abc import Mapping
from typing import Any, TextIO

Fields = Mapping[str, Any]

REDACTED = "***"

#: Substrings that mark a field as secret. Matching fields are never printed.
_SECRET_HINTS = (
    "token",
    "password",
    "passwd",
    "secret",
    "cookie",
    "authorization",
    "auth",
    "apikey",
    "api_key",
    "session",
    "credential",
)

_RESERVED = frozenset(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {
    "message",
    "asctime",
    "taskName",
}


def redact(fields: Fields) -> dict[str, Any]:
    """Replace values of secret-looking keys with ``***``.

    Applied to every log line, so an accidental ``cookie=...`` in a debug call
    cannot leak a session into the Apify run log.
    """
    out: dict[str, Any] = {}
    for key, value in fields.items():
        lowered = key.lower()
        out[key] = REDACTED if any(hint in lowered for hint in _SECRET_HINTS) else value
    return out


def _render_value(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return f"{value:.3f}"
    text = str(value)
    if text == "":
        return '""'
    if any(char in text for char in ' "=\n\t'):
        return json.dumps(text, ensure_ascii=False)
    return text


class _LogfmtFormatter(logging.Formatter):
    default_time_format = "%Y-%m-%dT%H:%M:%S"
    default_msec_format = "%s.%03dZ"

    def format(self, record: logging.LogRecord) -> str:
        extras = {k: v for k, v in record.__dict__.items() if k not in _RESERVED}
        parts = [
            self.formatTime(record),
            record.levelname,
            _render_value(record.getMessage()),
        ]
        parts += [f"{k}={_render_value(v)}" for k, v in redact(extras).items()]
        line = " ".join(parts)
        if record.exc_info:
            line = f"{line}\n{self.formatException(record.exc_info)}"
        return line


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        extras = {k: v for k, v in record.__dict__.items() if k not in _RESERVED}
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S") + "Z",
            "level": record.levelname,
            "message": record.getMessage(),
            **redact(extras),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


class _ActorFilter(logging.Filter):
    """Stamps every record with the Actor name so lines are attributable."""

    def __init__(self, actor: str) -> None:
        super().__init__()
        self._actor = actor

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "actor"):
            record.actor = self._actor
        return True


def configure_logging(
    actor: str,
    *,
    level: str | int | None = None,
    stream: TextIO | None = None,
    log_format: str | None = None,
) -> logging.Logger:
    """Install a structured handler on the root logger and return the Actor's logger.

    Idempotent: calling it twice replaces the handler rather than duplicating
    output, which matters because the Apify SDK also touches root logging.
    """
    resolved_level = level or os.environ.get("LOG_LEVEL", "INFO")
    resolved_format = (log_format or os.environ.get("LOG_FORMAT", "logfmt")).lower()

    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(_JsonFormatter() if resolved_format == "json" else _LogfmtFormatter())
    handler.set_name("common.structured")

    root = logging.getLogger()
    for existing in list(root.handlers):
        if existing.get_name() == "common.structured":
            root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(resolved_level)

    # httpx logs every request at INFO, which duplicates our own request logs.
    logging.getLogger("httpx").setLevel(logging.WARNING)

    handler.addFilter(_ActorFilter(actor))

    logger = logging.getLogger(actor)
    logger.setLevel(resolved_level)
    return logger


def log_event(
    logger: logging.Logger,
    operation: str,
    *,
    level: int = logging.INFO,
    message: str | None = None,
    exc_info: bool = False,
    **fields: Any,
) -> None:
    """Emit one structured line.

    ``log_event(log, "company.check", request_id=rid, status="SUCCESS", records=1)``
    """
    logger.log(
        level,
        message or operation,
        exc_info=exc_info,
        extra={"operation": operation, **fields},
    )
