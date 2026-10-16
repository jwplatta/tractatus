"""Normalize source Parquet files to the canonical Iceberg schema.

Handles legacy (31-column) and current (41-column) layouts by casting types,
adding missing columns, and applying fallback logic for timestamp fields.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# Canonical column order matching schema.py field_id order.
CANONICAL_COLUMNS: list[str] = [
    "contract_type",
    "symbol",
    "description",
    "strike",
    "expiration_date",
    "open",
    "high",
    "low",
    "close",
    "mark",
    "bid",
    "bid_size",
    "ask",
    "ask_size",
    "last",
    "last_size",
    "open_interest",
    "total_volume",
    "transactions",
    "delta",
    "gamma",
    "theta",
    "vega",
    "rho",
    "volatility",
    "theoretical_volatility",
    "theoretical_option_value",
    "intrinsic_value",
    "extrinsic_value",
    "underlying_price",
    "sampled_at",
    "trade_at",
    "quote_at",
    "time_value",
    "net_change",
    "percent_change",
    "settlement_type",
    "exercise_type",
    "expiration_type",
    "multiplier",
    "non_standard",
]

# Arrow types for each canonical column.
CANONICAL_ARROW_TYPES: dict[str, pa.DataType] = {
    "contract_type": pa.string(),
    "symbol": pa.string(),
    "description": pa.string(),
    "strike": pa.float64(),
    "expiration_date": pa.date32(),
    "open": pa.float64(),
    "high": pa.float64(),
    "low": pa.float64(),
    "close": pa.float64(),
    "mark": pa.float64(),
    "bid": pa.float64(),
    "bid_size": pa.int64(),
    "ask": pa.float64(),
    "ask_size": pa.int64(),
    "last": pa.float64(),
    "last_size": pa.float64(),
    "open_interest": pa.float64(),
    "total_volume": pa.float64(),
    "transactions": pa.int64(),
    "delta": pa.float64(),
    "gamma": pa.float64(),
    "theta": pa.float64(),
    "vega": pa.float64(),
    "rho": pa.float64(),
    "volatility": pa.float64(),
    "theoretical_volatility": pa.float64(),
    "theoretical_option_value": pa.float64(),
    "intrinsic_value": pa.float64(),
    "extrinsic_value": pa.float64(),
    "underlying_price": pa.float64(),
    "sampled_at": pa.timestamp("us", tz="UTC"),
    "trade_at": pa.timestamp("us", tz="UTC"),
    "quote_at": pa.timestamp("us", tz="UTC"),
    "time_value": pa.float64(),
    "net_change": pa.float64(),
    "percent_change": pa.float64(),
    "settlement_type": pa.string(),
    "exercise_type": pa.string(),
    "expiration_type": pa.string(),
    "multiplier": pa.float64(),
    "non_standard": pa.bool_(),
}


def normalize_table(table: pa.Table) -> tuple[pa.Table, list[str]]:
    """Normalize an in-memory Arrow table to the canonical schema.

    Returns ``(normalized_table, diagnostics)`` where diagnostics is a list of
    warning strings (empty when the file was already conformant).

    Raises ``ValueError`` if the table cannot be normalized (e.g. unparseable
    ``expiration_date`` values).
    """
    diagnostics: list[str] = []
    source_names = set(table.column_names)

    # --- Cast expiration_date from string to date32 if needed -----------------
    if "expiration_date" in source_names:
        col = table.column("expiration_date")
        if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
            try:
                casted = pc.cast(col, pa.date32())
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
                raise ValueError(f"Cannot cast expiration_date to date: {exc}") from exc
            table = table.set_column(
                table.column_names.index("expiration_date"), "expiration_date", casted
            )
            diagnostics.append("Cast expiration_date from string to date32")

    # --- Add missing columns with null/correct type ---------------------------
    for col_name in CANONICAL_COLUMNS:
        if col_name not in source_names:
            null_array = pa.nulls(len(table), type=CANONICAL_ARROW_TYPES[col_name])
            table = table.append_column(col_name, null_array)
            diagnostics.append(f"Added missing column {col_name}")

    # --- Fallback: populate trade_at / quote_at from sampled_at ---------------
    for ts_col in ("trade_at", "quote_at"):
        col = table.column(ts_col)
        sampled = table.column("sampled_at")
        is_null = pc.is_null(col)
        if pc.any(is_null).as_py():
            filled = pc.if_else(is_null, sampled, col)
            table = table.set_column(table.column_names.index(ts_col), ts_col, filled)
            diagnostics.append(f"Filled null {ts_col} from sampled_at")

    # --- Drop extra columns ---------------------------------------------------
    extra = [c for c in table.column_names if c not in CANONICAL_ARROW_TYPES]
    if extra:
        table = table.drop(extra)
        diagnostics.append(f"Dropped extra columns: {', '.join(extra)}")

    # --- Reorder to canonical order -------------------------------------------
    table = table.select(CANONICAL_COLUMNS)

    # --- Cast columns to canonical types where needed -------------------------
    for col_name in CANONICAL_COLUMNS:
        col = table.column(col_name)
        expected = CANONICAL_ARROW_TYPES[col_name]
        if col.type != expected:
            try:
                casted = pc.cast(col, expected)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
                raise ValueError(
                    f"Cannot cast {col_name} from {col.type} to {expected}: {exc}"
                ) from exc
            table = table.set_column(table.column_names.index(col_name), col_name, casted)

    return table, diagnostics


def normalize_parquet(source: Path, dest: Path) -> list[str]:
    """Read *source* Parquet, normalize to the canonical schema, write to *dest*.

    Returns a list of diagnostic warnings (empty if the file was already
    conformant).  Raises ``ValueError`` if the file cannot be normalized.
    """
    table = pq.read_table(source)
    normalized, diagnostics = normalize_table(table)
    pq.write_table(normalized, dest)
    return diagnostics
