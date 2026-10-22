"""Shared utilities for sample quality validation."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

_TRACTATUS_DIR: Path = Path.home() / ".tractatus"


def write_rejected(
    rows: list[dict],
    table_name: str,
    context: dict | None = None,
) -> None:
    """Append dropped rows to ~/.tractatus/rejected_{table_name}.ndjson."""
    log_path = _TRACTATUS_DIR / f"rejected_{table_name}.ndjson"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(tz=UTC).isoformat()
    with log_path.open("a") as fh:
        for row in rows:
            record = {"rejected_at": ts, **(context or {}), **row}
            fh.write(json.dumps(record, default=str) + "\n")
