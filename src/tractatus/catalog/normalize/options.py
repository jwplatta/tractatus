"""Options-specific normalization.

Handles legacy (31-column) and current (41-column) layouts by casting types,
adding missing columns, and applying fallback logic for timestamp fields.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from tractatus.catalog.normalize.common import align_to_schema, get_canonical, validate_input_schema
from tractatus.catalog.schemas.options import OPTIONS_TABLE

_COLUMNS, _TYPES = get_canonical(OPTIONS_TABLE.schema)

# Key columns from tickrake's base ParquetWriter for options.
_EXPECTED_INPUT: dict[str, pa.DataType] = {
    "symbol": pa.string(),
    "expiration_date": pa.string(),
    "strike": pa.float64(),
}


def normalize_options(table: pa.Table) -> tuple[pa.Table, list[str]]:
    """Normalize an options Arrow table to the canonical schema."""
    diagnostics: list[str] = []
    source_names = set(table.column_names)

    # Validate input matches tickrake's producer contract
    val_diags = validate_input_schema(table, _EXPECTED_INPUT)
    diagnostics.extend(val_diags)

    # Cast expiration_date from string to date32 if needed
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

    # Align columns to canonical schema (adds missing, drops extras, reorders, casts)
    table, align_diags = align_to_schema(table, _COLUMNS, _TYPES)
    diagnostics.extend(align_diags)

    # Fallback: populate trade_at / quote_at from sampled_at
    for ts_col in ("trade_at", "quote_at"):
        col = table.column(ts_col)
        sampled = table.column("sampled_at")
        is_null = pc.is_null(col)
        if pc.any(is_null).as_py():
            filled = pc.if_else(is_null, sampled, col)
            table = table.set_column(table.column_names.index(ts_col), ts_col, filled)
            diagnostics.append(f"Filled null {ts_col} from sampled_at")

    return table, diagnostics
