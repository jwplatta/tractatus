"""Order-book normalization.

Parses ``bids_json`` and ``asks_json`` strings into nested
``list<struct<price, size, num_orders, venues>>`` Arrow columns.
"""

from __future__ import annotations

import json
from datetime import date as date_type

import pyarrow as pa

from tractatus.catalog.normalize.common import (
    align_to_schema,
    epoch_ms_to_timestamp,
    get_canonical,
    validate_input_schema,
)
from tractatus.catalog.schemas.order_book import ORDER_BOOK_TABLE

_COLUMNS, _TYPES = get_canonical(ORDER_BOOK_TABLE.schema)

_EXPECTED_INPUT: dict[str, pa.DataType] = {
    "received_at_ms": pa.int64(),
    "symbol": pa.string(),
    "service": pa.string(),
    "book_time_ms": pa.int64(),
    "bids_json": pa.string(),
    "asks_json": pa.string(),
}


def _parse_book_json(json_col: pa.Array, n_rows: int) -> pa.Array:
    """Parse a bids_json or asks_json column into nested Arrow structs."""
    rows: list[list[dict[str, object]] | None] = []
    for i in range(n_rows):
        val = json_col[i].as_py()
        if val is None:
            rows.append(None)
            continue
        levels = json.loads(val)
        parsed_levels: list[dict[str, object]] = []
        for level in levels:
            venues_raw = level.get("3", [])
            venues = [
                {"venue": v.get("0"), "size": v.get("1"), "sequence_id": v.get("2")}
                for v in venues_raw
            ]
            parsed_levels.append(
                {
                    "price": level.get("0"),
                    "size": level.get("1"),
                    "num_orders": level.get("2"),
                    "venues": venues,
                }
            )
        rows.append(parsed_levels)

    return pa.array(rows, type=_TYPES["bids"])


def normalize_order_book(
    table: pa.Table, sample_date: date_type | None = None
) -> tuple[pa.Table, list[str]]:
    """Normalize an order_book Arrow table to the canonical schema."""
    diagnostics: list[str] = []
    n_rows = len(table)

    # Validate input matches tickrake's producer contract
    val_diags = validate_input_schema(table, _EXPECTED_INPUT)
    diagnostics.extend(val_diags)

    # Parse JSON columns into nested structs
    for json_col_name, target_col in [("bids_json", "bids"), ("asks_json", "asks")]:
        if json_col_name in table.column_names:
            parsed = _parse_book_json(table.column(json_col_name), n_rows)
            table = table.append_column(target_col, parsed)
            diagnostics.append(f"Parsed {json_col_name} into nested {target_col}")

    # Convert epoch-ms columns to timestamptz and rename
    _MS_RENAMES = {"received_at_ms": "received_at", "book_time_ms": "book_time"}
    for old_name, new_name in _MS_RENAMES.items():
        if old_name in table.column_names:
            idx = table.column_names.index(old_name)
            converted = epoch_ms_to_timestamp(table.column(old_name))
            table = table.set_column(idx, new_name, converted)
            diagnostics.append(f"Converted {old_name} to timestamptz as {new_name}")

    # Drop original JSON columns
    to_drop = [c for c in ("bids_json", "asks_json") if c in table.column_names]
    if to_drop:
        table = table.drop(to_drop)

    # Add sample_date
    if sample_date is not None and "sample_date" not in table.column_names:
        date_arr = pa.array([sample_date] * n_rows, type=pa.date32())
        table = table.append_column("sample_date", date_arr)
        diagnostics.append("Added sample_date from file path")

    # Align to canonical schema
    table, align_diags = align_to_schema(table, _COLUMNS, _TYPES)
    diagnostics.extend(align_diags)

    return table, diagnostics
