"""Discover parquet files from the local data directory.

Supports per-table directory layouts via ``_PARSERS``.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

_DATE_DIR_PATTERN = re.compile(r"^(\d{4})/(\d{2})/(\d{2})$")
_OPTIONS_FILENAME = re.compile(r"^(.+)_samples_(\d{4}-\d{2}-\d{2})\.parquet$")
_SYMBOL_TIMESTAMP = re.compile(r"^(.+?)_\d+Z\.parquet$")


@dataclass
class DiscoveredFile:
    """A parquet file found on disk."""

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


# ---------------------------------------------------------------------------
# Per-table path parsers: (parquet_path, data_root) → (provider, root, date)
# ---------------------------------------------------------------------------


def _parse_options(path: Path, data_root: Path) -> tuple[str, str, date] | None:
    """Options: provider/YYYY/MM/DD/{ROOT}_samples_{YYYY-MM-DD}.parquet"""
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
    # Accept any .parquet file in the right directory structure
    match = _OPTIONS_FILENAME.match(path.name)
    if match:
        root = match.group(1)
        try:
            sample_date = date.fromisoformat(match.group(2))
        except ValueError:
            return None
    else:
        # Fallback: derive date from directory, root from filename stem
        root = path.stem
        try:
            sample_date = date(int(parts[1]), int(parts[2]), int(parts[3]))
        except ValueError:
            return None
    return provider, root, sample_date


def _parse_symbol_timestamp(path: Path, data_root: Path) -> tuple[str, str, date] | None:
    """Level-one / order-book: provider/YYYY/MM/DD/SYMBOL_TIMESTAMPZ.parquet"""
    match = _SYMBOL_TIMESTAMP.match(path.name)
    if not match:
        return None
    symbol = match.group(1)
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
    try:
        sample_date = date(int(parts[1]), int(parts[2]), int(parts[3]))
    except ValueError:
        return None
    return provider, symbol, sample_date


def _parse_equities_symbol_timestamp(path: Path, data_root: Path) -> tuple[str, str, date] | None:
    """Equities level-one: same as symbol_timestamp but excludes ^ prefix (futures)."""
    if path.stem.startswith("^"):
        return None
    return _parse_symbol_timestamp(path, data_root)


def _parse_futures_symbol_timestamp(path: Path, data_root: Path) -> tuple[str, str, date] | None:
    """Futures level-one: same as symbol_timestamp but only ^ prefix symbols."""
    if not path.stem.startswith("^"):
        return None
    return _parse_symbol_timestamp(path, data_root)


def _parse_candles(path: Path, data_root: Path) -> tuple[str, str, date] | None:
    """Candles: provider/interval/YYYY/SYMBOL.parquet"""
    if not path.name.endswith(".parquet"):
        return None
    try:
        rel = path.relative_to(data_root)
    except ValueError:
        return None
    parts = rel.parts
    # provider / interval / YYYY / SYMBOL.parquet
    if len(parts) < 4:
        return None
    provider = parts[0]
    # interval = parts[1]  # stored in path, used by normalizer
    try:
        year = int(parts[2])
    except ValueError:
        return None
    symbol = path.stem
    sample_date = date(year, 1, 1)
    return provider, symbol, sample_date


def _parse_fundamentals(path: Path, data_root: Path) -> tuple[str, str, date] | None:
    """Fundamentals: provider/YYYY/MM/DD.parquet"""
    if not path.name.endswith(".parquet"):
        return None
    try:
        rel = path.relative_to(data_root)
    except ValueError:
        return None
    parts = rel.parts
    # provider / YYYY / MM / DD.parquet
    if len(parts) < 4:
        return None
    provider = parts[0]
    try:
        sample_date = date(int(parts[1]), int(parts[2]), int(path.stem))
    except ValueError:
        return None
    return provider, "all", sample_date


_PARSERS: dict[str, Callable[[Path, Path], tuple[str, str, date] | None]] = {
    "options": _parse_options,
    "level_one_equities": _parse_equities_symbol_timestamp,
    "level_one_futures": _parse_futures_symbol_timestamp,
    "order_book": _parse_symbol_timestamp,
    "candles": _parse_candles,
    "fundamentals": _parse_fundamentals,
}

# When the on-disk directory name differs from the table name.
_DATA_DIR_NAMES: dict[str, str] = {
    "level_one_equities": "level_one",
    "level_one_futures": "level_one",
}


def data_dir_name(table_name: str) -> str:
    """Return the on-disk directory name for a table (may differ from the table name)."""
    return _DATA_DIR_NAMES.get(table_name, table_name)


def discover_files(
    data_root: Path,
    table_name: str = "options",
    providers: list[str] | None = None,
    after: date | None = None,
    before: date | None = None,
) -> list[DiscoveredFile]:
    """Scan data_root for parquet files matching *table_name*'s layout.

    Parameters
    ----------
    data_root:
        Root directory containing provider subdirectories.
    table_name:
        Which table's directory layout and path parser to use.
    providers:
        If given, only scan these provider subdirectories.
    after:
        Only include files with sample_date >= after.
    before:
        Only include files with sample_date < before.
    """
    parser = _PARSERS.get(table_name)
    if parser is None:
        raise ValueError(
            f"No discovery config for table {table_name!r}. "
            f"Known tables: {', '.join(sorted(_PARSERS))}"
        )

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
        for parquet_path in sorted(provider_dir.rglob("*.parquet")):
            if not parquet_path.is_file():
                continue

            size = parquet_path.stat().st_size
            if size == 0:
                continue

            parsed = parser(parquet_path, data_root)
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
