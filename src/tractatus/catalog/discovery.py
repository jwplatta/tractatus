"""Discover data files from a file store (local or S3-compatible).

Supports per-table directory layouts via ``_PARSERS``.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from pathlib import PurePosixPath

from tractatus.catalog.file_store import FileStore

_DATE_DIR_PATTERN = re.compile(r"^(\d{4})/(\d{2})/(\d{2})$")

# Options CSV: {ROOT}_exp{EXP}_{DATE}_{TIME}.csv
# e.g. SPXW_exp2026-10-18_2026-10-18_14-30-00.csv
_OPTIONS_CSV_FILENAME = re.compile(
    r"^(.+)_exp(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_(\d{2}-\d{2}-\d{2})\.csv$"
)


@dataclass
class DiscoveredFile:
    """A data file found in the store."""

    relative_path: str
    provider: str
    symbol: str
    sample_date: date
    sha256: str
    size_bytes: int
    file_format: str = ""  # "csv" or "ndjson"
    metadata: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Per-table key parsers: (relative_key) → (provider, root, date, metadata)
#
# Keys are relative to the store root and always use forward slashes.
# The table-specific prefix (e.g. "options/") has already been stripped —
# keys start with the provider directory.
# ---------------------------------------------------------------------------

ParseResult = tuple[str, str, date, dict[str, str]] | None


def _parse_options(key: str) -> ParseResult:
    """Options CSV: provider/YYYY/MM/DD/{ROOT}_exp{EXP}_{DATE}_{TIME}.csv"""
    p = PurePosixPath(key)
    parts = p.parent.parts
    if len(parts) < 4:
        return None
    provider = parts[0]
    date_dir = "/".join(parts[1:4])
    if not _DATE_DIR_PATTERN.match(date_dir):
        return None
    match = _OPTIONS_CSV_FILENAME.match(p.name)
    if not match:
        return None
    root = match.group(1)
    try:
        sample_date = date.fromisoformat(match.group(3))
    except ValueError:
        return None
    expiration = match.group(2)
    return provider, root, sample_date, {"expiration": expiration}


def _parse_symbol_ndjson(key: str) -> ParseResult:
    """Level-one / order-book NDJSON: provider/YYYY/MM/DD/SYMBOL.ndjson"""
    p = PurePosixPath(key)
    if p.suffix != ".ndjson":
        return None
    parts = p.parent.parts
    if len(parts) < 4:
        return None
    provider = parts[0]
    date_dir = "/".join(parts[1:4])
    if not _DATE_DIR_PATTERN.match(date_dir):
        return None
    symbol = p.stem
    try:
        sample_date = date(int(parts[1]), int(parts[2]), int(parts[3]))
    except ValueError:
        return None
    return provider, symbol, sample_date, {}


def _parse_equities_ndjson(key: str) -> ParseResult:
    """Equities level-one: same as symbol NDJSON but excludes ^ prefix (futures)."""
    p = PurePosixPath(key)
    if p.stem.startswith("^"):
        return None
    return _parse_symbol_ndjson(key)


def _parse_futures_ndjson(key: str) -> ParseResult:
    """Futures level-one: same as symbol NDJSON but only ^ prefix symbols."""
    p = PurePosixPath(key)
    if not p.stem.startswith("^"):
        return None
    return _parse_symbol_ndjson(key)


def _parse_candles(key: str) -> ParseResult:
    """Candles CSV: provider/YYYY/MM/DD/frequency/SYMBOL.csv"""
    p = PurePosixPath(key)
    if p.suffix != ".csv":
        return None
    parts = p.parts
    # provider / YYYY / MM / DD / frequency / SYMBOL.csv
    if len(parts) < 6:
        return None
    provider = parts[0]
    date_dir = "/".join(parts[1:4])
    if not _DATE_DIR_PATTERN.match(date_dir):
        return None
    try:
        sample_date = date(int(parts[1]), int(parts[2]), int(parts[3]))
    except ValueError:
        return None
    frequency = parts[4]
    symbol = p.stem
    return provider, symbol, sample_date, {"frequency": frequency}


def _parse_fundamentals(key: str) -> ParseResult:
    """Fundamentals NDJSON: provider/YYYY/MM/DD.ndjson"""
    p = PurePosixPath(key)
    if p.suffix != ".ndjson":
        return None
    parts = p.parts
    # provider / YYYY / MM / DD.ndjson
    if len(parts) < 4:
        return None
    provider = parts[0]
    try:
        sample_date = date(int(parts[1]), int(parts[2]), int(p.stem))
    except ValueError:
        return None
    return provider, "all", sample_date, {}


def _parse_economic_events(key: str) -> ParseResult:
    """Economic events NDJSON: source/category/YYYY/MM/DD.ndjson"""
    p = PurePosixPath(key)
    if p.suffix != ".ndjson":
        return None
    parts = p.parts
    # source / category / YYYY / MM / DD.ndjson
    if len(parts) < 5:
        return None
    source = parts[0]
    category = parts[1]
    try:
        sample_date = date(int(parts[2]), int(parts[3]), int(p.stem))
    except ValueError:
        return None
    return source, category, sample_date, {}


_PARSERS: dict[str, Callable[[str], ParseResult]] = {
    "options": _parse_options,
    "level_one_equities": _parse_equities_ndjson,
    "level_one_futures": _parse_futures_ndjson,
    "order_book": _parse_symbol_ndjson,
    "candles": _parse_candles,
    "fundamentals": _parse_fundamentals,
    "economic_events": _parse_economic_events,
}

# When the on-disk directory name differs from the table name.
_DATA_DIR_NAMES: dict[str, str] = {
    "level_one_equities": "level_one",
    "level_one_futures": "level_one",
}

# File extensions to scan per table.
_FILE_EXTENSIONS: dict[str, list[str]] = {
    "options": [".csv"],
    "level_one_equities": [".ndjson"],
    "level_one_futures": [".ndjson"],
    "order_book": [".ndjson"],
    "candles": [".csv"],
    "fundamentals": [".ndjson"],
    "economic_events": [".ndjson"],
}


def data_dir_name(table_name: str) -> str:
    """Return the on-disk directory name for a table (may differ from the table name)."""
    return _DATA_DIR_NAMES.get(table_name, table_name)


def _file_format_from_key(key: str) -> str:
    """Derive file format from extension."""
    if key.endswith(".ndjson"):
        return "ndjson"
    if key.endswith(".csv"):
        return "csv"
    return "parquet"


def discover_files(
    store: FileStore,
    table_name: str = "options",
    providers: list[str] | None = None,
    after: date | None = None,
    before: date | None = None,
) -> list[DiscoveredFile]:
    """Scan *store* for data files matching *table_name*'s layout.

    Parameters
    ----------
    store:
        File store to scan (local or S3-compatible).
    table_name:
        Which table's directory layout and key parser to use.
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

    prefix = data_dir_name(table_name)
    extensions = _FILE_EXTENSIONS[table_name]
    entries = store.list_files(prefix, extensions=extensions, providers=providers)

    results: list[DiscoveredFile] = []
    for full_key, size_bytes in entries:
        # Strip the table prefix to get the key parsers expect
        # e.g. "options/schwab/2026/..." → "schwab/2026/..."
        relative_key = (
            full_key[len(prefix) + 1 :] if full_key.startswith(prefix + "/") else full_key
        )

        parsed = parser(relative_key)
        if parsed is None:
            continue

        provider, root, sample_date, metadata = parsed

        if after is not None and sample_date < after:
            continue
        if before is not None and sample_date >= before:
            continue

        results.append(
            DiscoveredFile(
                relative_path=relative_key,
                provider=provider,
                symbol=root,
                sample_date=sample_date,
                sha256=store.sha256(full_key),
                size_bytes=size_bytes,
                file_format=_file_format_from_key(full_key),
                metadata=metadata,
            )
        )

    return results
