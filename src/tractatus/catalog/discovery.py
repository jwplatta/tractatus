"""Discover data files from a file store (local or S3-compatible).

Supports per-table directory layouts via ``_PARSERS``.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from pathlib import PurePosixPath

from tractatus.catalog.file_store import FileStore

_DATE_DIR_PATTERN = re.compile(r"^(\d{4})/(\d{2})/(\d{2})$")
_OPTIONS_FILENAME = re.compile(r"^(.+)_samples_(\d{4}-\d{2}-\d{2})\.parquet$")
_SYMBOL_TIMESTAMP = re.compile(r"^(.+?)_\d+Z\.parquet$")


@dataclass
class DiscoveredFile:
    """A data file found in the store."""

    relative_path: str
    provider: str
    root: str
    sample_date: date
    sha256: str
    size_bytes: int
    file_format: str = "parquet"  # "parquet", "csv", or "ndjson"
    metadata: dict[str, str] | None = None


# ---------------------------------------------------------------------------
# Per-table key parsers: (relative_key) → (provider, root, date)
#
# Keys are relative to the store root and always use forward slashes.
# The table-specific prefix (e.g. "options/") has already been stripped —
# keys start with the provider directory.
# ---------------------------------------------------------------------------


def _parse_options(key: str) -> tuple[str, str, date] | None:
    """Options: provider/YYYY/MM/DD/{ROOT}_samples_{YYYY-MM-DD}.parquet"""
    p = PurePosixPath(key)
    parts = p.parent.parts
    if len(parts) < 4:
        return None
    provider = parts[0]
    date_dir = "/".join(parts[1:4])
    if not _DATE_DIR_PATTERN.match(date_dir):
        return None
    match = _OPTIONS_FILENAME.match(p.name)
    if match:
        root = match.group(1)
        try:
            sample_date = date.fromisoformat(match.group(2))
        except ValueError:
            return None
    else:
        root = p.stem
        try:
            sample_date = date(int(parts[1]), int(parts[2]), int(parts[3]))
        except ValueError:
            return None
    return provider, root, sample_date


def _parse_symbol_timestamp(key: str) -> tuple[str, str, date] | None:
    """Level-one / order-book: provider/YYYY/MM/DD/SYMBOL_TIMESTAMPZ.parquet"""
    p = PurePosixPath(key)
    match = _SYMBOL_TIMESTAMP.match(p.name)
    if not match:
        return None
    symbol = match.group(1)
    parts = p.parent.parts
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


def _parse_equities_symbol_timestamp(key: str) -> tuple[str, str, date] | None:
    """Equities level-one: same as symbol_timestamp but excludes ^ prefix (futures)."""
    p = PurePosixPath(key)
    if p.stem.startswith("^"):
        return None
    return _parse_symbol_timestamp(key)


def _parse_futures_symbol_timestamp(key: str) -> tuple[str, str, date] | None:
    """Futures level-one: same as symbol_timestamp but only ^ prefix symbols."""
    p = PurePosixPath(key)
    if not p.stem.startswith("^"):
        return None
    return _parse_symbol_timestamp(key)


def _parse_candles(key: str) -> tuple[str, str, date] | None:
    """Candles: provider/interval/YYYY/SYMBOL.{parquet,csv}"""
    p = PurePosixPath(key)
    if p.suffix not in (".parquet", ".csv"):
        return None
    parts = p.parts
    # provider / interval / YYYY / SYMBOL.parquet
    if len(parts) < 4:
        return None
    provider = parts[0]
    try:
        year = int(parts[2])
    except ValueError:
        return None
    symbol = p.stem
    sample_date = date(year, 1, 1)
    return provider, symbol, sample_date


def _parse_fundamentals(key: str) -> tuple[str, str, date] | None:
    """Fundamentals: provider/YYYY/MM/DD.{parquet,ndjson}"""
    p = PurePosixPath(key)
    if p.suffix not in (".parquet", ".ndjson"):
        return None
    parts = p.parts
    # provider / YYYY / MM / DD.{ext}
    if len(parts) < 4:
        return None
    provider = parts[0]
    try:
        sample_date = date(int(parts[1]), int(parts[2]), int(p.stem))
    except ValueError:
        return None
    return provider, "all", sample_date


def _parse_economic_events(key: str) -> tuple[str, str, date] | None:
    """Economic events: source/category/YYYY/MM/DD.{parquet,ndjson}"""
    p = PurePosixPath(key)
    if p.suffix not in (".parquet", ".ndjson"):
        return None
    parts = p.parts
    # source / category / YYYY / MM / DD.{ext}
    if len(parts) < 5:
        return None
    source = parts[0]
    category = parts[1]
    try:
        sample_date = date(int(parts[2]), int(parts[3]), int(p.stem))
    except ValueError:
        return None
    return source, category, sample_date


_PARSERS: dict[str, Callable[[str], tuple[str, str, date] | None]] = {
    "options": _parse_options,
    "level_one_equities": _parse_equities_symbol_timestamp,
    "level_one_futures": _parse_futures_symbol_timestamp,
    "order_book": _parse_symbol_timestamp,
    "candles": _parse_candles,
    "fundamentals": _parse_fundamentals,
    "economic_events": _parse_economic_events,
}

# When the on-disk directory name differs from the table name.
_DATA_DIR_NAMES: dict[str, str] = {
    "level_one_equities": "level_one",
    "level_one_futures": "level_one",
}

# File extensions to scan per table.  Discovery will match any of these.
_FILE_EXTENSIONS: dict[str, list[str]] = {
    "options": [".parquet", ".csv"],
    "level_one_equities": [".parquet", ".ndjson"],
    "level_one_futures": [".parquet", ".ndjson"],
    "order_book": [".parquet", ".ndjson"],
    "candles": [".parquet", ".csv"],
    "fundamentals": [".parquet", ".ndjson"],
    "economic_events": [".parquet", ".ndjson"],
}

_DEFAULT_EXTENSIONS = [".parquet"]


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
    extensions = _FILE_EXTENSIONS.get(table_name, _DEFAULT_EXTENSIONS)
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

        provider, root, sample_date = parsed

        if after is not None and sample_date < after:
            continue
        if before is not None and sample_date >= before:
            continue

        results.append(
            DiscoveredFile(
                relative_path=relative_key,
                provider=provider,
                root=root,
                sample_date=sample_date,
                sha256=store.sha256(full_key),
                size_bytes=size_bytes,
                file_format=_file_format_from_key(full_key),
            )
        )

    return results
