"""Normalize raw data files to canonical Iceberg schemas.

Dispatches to per-table normalizers based on ``table_name``.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from tractatus.catalog.normalize.candles import normalize_candles
from tractatus.catalog.normalize.common import csv_to_arrow, ndjson_to_arrow
from tractatus.catalog.normalize.economic_events import normalize_economic_events
from tractatus.catalog.normalize.fundamentals import normalize_fundamentals
from tractatus.catalog.normalize.level_one import (
    normalize_level_one_equities,
    normalize_level_one_futures,
)
from tractatus.catalog.normalize.options import normalize_options
from tractatus.catalog.normalize.order_book import normalize_order_book

__all__ = [
    "normalize_parquet",
    "normalize_raw",
    "normalize_table",
]


def normalize_table(
    table: pa.Table,
    table_name: str = "options",
    *,
    sample_date: date | None = None,
    symbol: str | None = None,
    interval: str | None = None,
) -> tuple[pa.Table, list[str]]:
    """Normalize an Arrow table to the canonical schema for *table_name*.

    Extra keyword arguments are forwarded to per-table normalizers that need
    metadata derived from the file path (e.g. sample_date, symbol, interval).
    """
    if table_name == "options":
        return normalize_options(table)
    if table_name == "level_one_equities":
        return normalize_level_one_equities(table, sample_date=sample_date)
    if table_name == "level_one_futures":
        return normalize_level_one_futures(table, sample_date=sample_date)
    if table_name == "order_book":
        return normalize_order_book(table, sample_date=sample_date)
    if table_name == "candles":
        return normalize_candles(table, symbol=symbol, interval=interval)
    if table_name == "fundamentals":
        return normalize_fundamentals(table)
    if table_name == "economic_events":
        return normalize_economic_events(table)
    raise ValueError(f"No normalizer registered for table {table_name!r}")


def normalize_raw(
    data: bytes,
    file_format: str,
    table_name: str,
    *,
    sample_date: date | None = None,
    symbol: str | None = None,
    interval: str | None = None,
) -> tuple[pa.Table, list[str]]:
    """Parse raw bytes (CSV or NDJSON) and normalize to canonical schema.

    This is the primary entry point for the publisher pipeline.
    """
    if file_format == "csv":
        table = csv_to_arrow(data)
    elif file_format == "ndjson":
        table = ndjson_to_arrow(data)
    else:
        raise ValueError(f"Unsupported file format: {file_format!r}")

    return normalize_table(
        table, table_name, sample_date=sample_date, symbol=symbol, interval=interval
    )


def normalize_parquet(source: Path, dest: Path, table_name: str = "options") -> list[str]:
    """Read *source* Parquet, normalize, write to *dest*.

    Returns diagnostic warnings. Raises ``ValueError`` if normalization fails.
    """
    table = pq.read_table(source)
    normalized, diagnostics = normalize_table(table, table_name)
    pq.write_table(normalized, dest)
    return diagnostics
