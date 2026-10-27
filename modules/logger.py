"""
Custom colorized logger with SUCCESS level, structured JSON output,
and production features.

Usage:
    from modules.logger import get_logger

    # Basic usage (default: JSON output)
    logger = get_logger(__name__)
    logger.info("Hello")
    logger.success("Task completed")

    # With timestamps in output
    logger = get_logger(__name__, show_time=True)

    # Plain text (colored console output)
    logger = get_logger(__name__, json_output=False)

    # With trace ID for distributed tracing
    logger = get_logger(__name__, trace_id="req-abc-123")

    # With static context tags
    logger = get_logger(__name__, context={"service": "api", "env": "prod"})

    # Log to a file with rotation
    logger = get_logger(__name__, log_file="/var/log/app.log")

    # Disable sensitive data redaction (enabled by default)
    logger = get_logger(__name__, redact_sensitive=False)

    # With custom rotation size (accepts "10MB", "500KB", "1GB", or raw int)
    logger = get_logger(__name__, log_file="/var/log/app.log", max_bytes="10MB")

Environment variables:
    LOG_LEVEL     - Set log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
                    Default: DEBUG
    LOG_FORMAT    - Output format: "json" or "text"
                    Default: "json"

Custom log levels:
    SUCCESS (25) — bright green, sits between INFO and WARNING.
"""

import json
import logging
import os
import re
import uuid
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Optional

import colorlog

SUCCESS_LEVEL = 25
"""Custom log level (25) for success messages, between INFO (20) and WARNING (30)."""

logging.addLevelName(SUCCESS_LEVEL, "SUCCESS")


def _success(self: logging.Logger, message: str, *args, **kwargs) -> None:
    """Log a message at SUCCESS level (25)."""
    if self.isEnabledFor(SUCCESS_LEVEL):
        self._log(SUCCESS_LEVEL, message, args, **kwargs)


logging.Logger.success = _success


class WorkerIDFilter(logging.Filter):
    """Attach a worker/PID identifier to every log record.

    Reads ``UVICORN_WORKER`` env var; falls back to current PID.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        worker = os.getenv("UVICORN_WORKER")
        if worker:
            record.worker_id = f"-{worker}"
        else:
            record.worker_id = f"-{os.getpid()}"
        return True


class TraceIDFilter(logging.Filter):
    """Attach a correlation/trace ID to every log record."""

    def __init__(self, trace_id: Optional[str] = None):
        self.trace_id = trace_id or uuid.uuid4().hex[:16]

    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = self.trace_id
        return True


class ContextFilter(logging.Filter):
    """Attach static context key-value pairs to every log record."""

    def __init__(self, context: Optional[Dict[str, Any]] = None):
        self.context = context or {}

    def filter(self, record: logging.LogRecord) -> bool:
        record._context = self.context
        return True


class RedactionFilter(logging.Filter):
    """Redact sensitive data from log messages.

    Covers passwords, secrets, tokens, API keys, Bearer tokens,
    and credit card numbers.
    """

    _PATTERNS = [
        (re.compile(r"(?i)(password|passwd|pwd)\s*[=:]\s*\S+"), r"\1=***"),
        (re.compile(r"(?i)(secret|token|api_key|apikey|api_secret)\s*[=:]\s*\S+"), r"\1=***"),
        (re.compile(r"(?i)(Bearer\s+)\S+"), r"\1***"),
        (re.compile(r"\b\d{4}[- ]?\d{4}[- ]?\d{4}[- ]?\d{4}\b"), "****-****-****-****"),
    ]

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            for pattern, replacement in self._PATTERNS:
                record.msg = pattern.sub(replacement, record.msg)
        return True


class JsonFormatter(logging.Formatter):
    """Format log records as JSON lines for structured logging."""

    def __init__(self, include_time: bool = False):
        super().__init__()
        self.include_time = include_time

    def format(self, record: logging.LogRecord) -> str:
        entry: Dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        if self.include_time:
            entry["timestamp"] = datetime.fromtimestamp(record.created).isoformat()

        if hasattr(record, "worker_id"):
            entry["worker_id"] = record.worker_id
        if hasattr(record, "trace_id"):
            entry["trace_id"] = record.trace_id
        if hasattr(record, "_context"):
            entry["context"] = record._context

        return json.dumps(entry, default=str)


class CustomColoredFormatter(colorlog.ColoredFormatter):
    """Color formatter that pads the log-level name inside the formatted string."""

    def format(self, record: logging.LogRecord) -> str:
        record.levelname_bracket = f"[{record.levelname}]"
        pad = 3 - len(record.levelname)
        record.levelname_pad = " " * pad if pad > 0 else ""
        return super().format(record)


def _parse_bytes(value: int | str) -> int:
    """Parse a human-readable byte size into an integer.

    Accepts formats like ``"10MB"``, ``"500KB"``, ``"1GB"``, or a raw ``int``.
    """
    if isinstance(value, int):
        return value
    value = value.strip().upper()
    units = {"KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}
    for suffix, multiplier in units.items():
        if value.endswith(suffix):
            num = value[: -len(suffix)].strip()
            try:
                return int(float(num) * multiplier)
            except ValueError:
                raise ValueError(f"Invalid byte size: '{value}'")
    try:
        return int(value)
    except ValueError:
        raise ValueError(
            f"Invalid byte size: '{value}'. Use a number or a string like '10MB', '500KB', '1GB'."
        )


def get_logger(
    name: str,
    show_time: bool = False,
    *,
    json_output: Optional[bool] = None,
    log_file: Optional[str] = None,
    max_bytes: int | str = 10 * 1024 * 1024,
    backup_count: int = 5,
    trace_id: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    redact_sensitive: bool = True,
) -> logging.Logger:
    """Return a production-ready logger with structured output,
    rotation, tracing, context, and redaction support.

    Parameters
    ----------
    name : str
        Logger name (typically ``__name__``).
    show_time : bool
        Include ISO-8601 timestamp in output (default: ``False``).

    Keyword Arguments
    -----------------
    json_output : bool | None
        Use JSON lines format. Defaults to env ``LOG_FORMAT=="json"``,
        otherwise ``True``.
    log_file : str | None
        Path to a rotating log file. When set, logs are written both
        to console and to the file.
    max_bytes : int | str
        Max size per log file before rotation. Accepts raw bytes (``10485760``)
        or human-readable strings (``"10MB"``, ``"500KB"``, ``"1GB"``).
        Default: 10 MB.
    backup_count : int
        Number of rotated files to keep (default 5).
    trace_id : str | None
        Correlation ID for distributed tracing. Auto-generated if omitted.
    context : dict | None
        Static key-value pairs attached to every log record
        (e.g. ``{"service": "api", "env": "production"}``).
    redact_sensitive : bool
        Automatically redact passwords, tokens, API keys, and credit
        card numbers from log messages (default: ``True``).

    Returns
    -------
    logging.Logger
        Configured logger instance (handlers are added only once).
    """
    if not name or not isinstance(name, str):
        raise ValueError("Logger name must be a non-empty string")

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    level = os.getenv("LOG_LEVEL", "DEBUG").upper()
    logger.setLevel(level)
    logger.propagate = False

    logger.addFilter(WorkerIDFilter())
    logger.addFilter(TraceIDFilter(trace_id))
    if context:
        logger.addFilter(ContextFilter(context))
    if redact_sensitive:
        logger.addFilter(RedactionFilter())

    if json_output is None:
        json_output = os.getenv("LOG_FORMAT", "json").lower() == "json"

    console = colorlog.StreamHandler()
    if json_output:
        console.setFormatter(JsonFormatter(include_time=show_time))
    else:
        if show_time:
            fmt = "[%(asctime)s] %(log_color)s[%(name)s%(worker_id)s]%(levelname_pad)s %(message)s%(reset)s"
        else:
            fmt = "%(log_color)s[%(name)s%(worker_id)s]%(levelname_pad)s %(message)s%(reset)s"
        console.setFormatter(
            CustomColoredFormatter(
                fmt,
                datefmt="%Y-%m-%d %H:%M:%S" if show_time else None,
                log_colors={
                    "SUCCESS": "bold_green",
                    "INFO": "bold_cyan",
                    "DEBUG": "bold_white",
                    "WARNING": "bold_yellow",
                    "ERROR": "red",
                    "CRITICAL": "bold_red",
                },
                style="%",
            )
        )
    logger.addHandler(console)

    if log_file:
        file_handler = RotatingFileHandler(
            log_file, maxBytes=_parse_bytes(max_bytes), backupCount=backup_count
        )
        file_handler.setFormatter(JsonFormatter(include_time=True))
        logger.addHandler(file_handler)

    return logger


if __name__ == "__main__":
    logger = get_logger("example_app", show_time=True, json_output=False)
    logger.info("Server starting on port 8000")
    logger.success("Database connection established")
    logger.warning("Disk usage at 85%")
    logger.error("Failed to send email: connection timeout")
    logger.debug("Cache miss for key: user_12345")

    json_logger = get_logger("json_app", show_time=True)
    json_logger.info("Server ready")

    ctx_logger = get_logger(
        "order_service",
        show_time=True,
        context={"service": "orders", "env": "staging", "region": "us-east-1"},
        trace_id="ord-7f3a1c",
        json_output=False,
    )
    ctx_logger.info("Processing order #1024")
    ctx_logger.success("Order #1024 completed")