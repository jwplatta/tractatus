"""Structured JSONL logging for the catalog publisher.

Writes JSON-lines logs to ``~/.tractatus/logs/catalog.jsonl`` for ingestion
by Promtail/Loki. Each line is a self-contained JSON object with ``ts``,
``level``, and structured fields.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path.home() / ".tractatus" / "logs"
LOG_FILE = LOG_DIR / "catalog.jsonl"
LOG_ROTATION_SIZE = 10 * 1024 * 1024  # 10 MB
LOG_ROTATION_COUNT = 5


class JsonlFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, object] = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname,
        }

        if isinstance(record.msg, dict):
            entry.update(record.msg)
        else:
            entry["msg"] = record.getMessage()

        if record.exc_info and record.exc_info[1] is not None:
            exc = record.exc_info[1]
            entry["error_class"] = type(exc).__name__
            entry["error_message"] = str(exc)

        return json.dumps(entry, default=str)


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure and return the catalog logger.

    Parameters
    ----------
    verbose:
        If True, also log human-readable output to stdout.
    """
    logger = logging.getLogger("tractatus.catalog")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    jsonl_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=LOG_ROTATION_SIZE,
        backupCount=LOG_ROTATION_COUNT,
    )
    jsonl_handler.setLevel(logging.DEBUG)
    jsonl_handler.setFormatter(JsonlFormatter())
    logger.addHandler(jsonl_handler)

    if verbose:
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        logger.addHandler(console)

    return logger
