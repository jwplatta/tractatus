"""Discover compacted options parquet files from the local data directory."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

_FILENAME_PATTERN = re.compile(r"^(.+)_samples_(\d{4}-\d{2}-\d{2})\.parquet$")
_DATE_DIR_PATTERN = re.compile(r"^(\d{4})/(\d{2})/(\d{2})$")


@dataclass
class DiscoveredFile:
    """A compacted options parquet file found on disk."""

    path: Path
    relative_path: str
    provider: str
    root: str
    sample_date: date
    sha256: str
    size_bytes: int


def _sha256(path: Path) -> str:
    """Compute SHA-256 hex digest by streaming the file."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def _parse_parquet_path(path: Path, data_root: Path) -> tuple[str, str, date] | None:
    """Extract (provider, root, sample_date) from a parquet path.

    Expected layout: data_root/{provider}/{YYYY}/{MM}/{DD}/{ROOT}_samples_{YYYY-MM-DD}.parquet
    """
    match = _FILENAME_PATTERN.match(path.name)
    if not match:
        return None

    root = match.group(1)
    try:
        sample_date = date.fromisoformat(match.group(2))
    except ValueError:
        return None

    try:
        rel = path.parent.relative_to(data_root)
    except ValueError:
        return None

    parts = rel.parts
    if len(parts) < 4:
        return None

    provider = parts[0]
    date_dir = "/".join(parts[1:4])
    if not _DATE_DIR_PATTERN.match(date_dir):
        return None

    return provider, root, sample_date


def discover_files(
    data_root: Path,
    providers: list[str] | None = None,
    after: date | None = None,
    before: date | None = None,
) -> list[DiscoveredFile]:
    """Scan data_root for compacted options parquet files.

    Parameters
    ----------
    data_root:
        Root directory containing provider subdirectories with options parquet.
    providers:
        If given, only scan these provider subdirectories.
    after:
        Only include files with sample_date >= after.
    before:
        Only include files with sample_date < before.
    """
    results: list[DiscoveredFile] = []

    if providers:
        search_dirs = [data_root / p for p in providers]
    else:
        search_dirs = (
            [d for d in sorted(data_root.iterdir()) if d.is_dir()] if data_root.is_dir() else []
        )

    for provider_dir in search_dirs:
        if not provider_dir.is_dir():
            continue
        for parquet_path in sorted(provider_dir.rglob("*_samples_*.parquet")):
            if not parquet_path.is_file():
                continue

            size = parquet_path.stat().st_size
            if size == 0:
                continue

            parsed = _parse_parquet_path(parquet_path, data_root)
            if parsed is None:
                continue

            provider, root, sample_date = parsed

            if after is not None and sample_date < after:
                continue
            if before is not None and sample_date >= before:
                continue

            relative = str(parquet_path.relative_to(data_root))

            results.append(
                DiscoveredFile(
                    path=parquet_path,
                    relative_path=relative,
                    provider=provider,
                    root=root,
                    sample_date=sample_date,
                    sha256=_sha256(parquet_path),
                    size_bytes=size,
                )
            )

    return results
