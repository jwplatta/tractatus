"""Normalize source Parquet files to the canonical Iceberg schema.

Handles legacy (31-column) and current (41-column) layouts by casting types,
adding missing columns, and applying fallback logic for timestamp fields.

Column names and Arrow types are derived from the Iceberg schema defined in
``schema.py`` so there is a single source of truth.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.io.pyarrow import schema_to_pyarrow

from tractatus.catalog.schema import OPTIONS_TABLE

# Derive canonical columns and Arrow types from the Iceberg schema.
_ARROW_SCHEMA: pa.Schema = schema_to_pyarrow(OPTIONS_TABLE.schema)
CANONICAL_COLUMNS: list[str] = _ARROW_SCHEMA.names
CANONICAL_ARROW_TYPES: dict[str, pa.DataType] = {f.name: f.type for f in _ARROW_SCHEMA}


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
