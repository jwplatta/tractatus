"""Options-specific normalization.

Validates the full 41-column CSV contract from tickrake, casts types,
and applies fallback logic for timestamp fields.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from tractatus.catalog.normalize.common import (
    align_to_schema,
    get_canonical,
    iso_string_to_timestamp,
    validate_input_schema,
)
from tractatus.catalog.schemas.options import OPTIONS_TABLE

_COLUMNS, _TYPES = get_canonical(OPTIONS_TABLE.schema)

# Full 41-column CSV contract from tickrake raw_data_contracts.
_EXPECTED_INPUT: dict[str, pa.DataType] = {
    "contract_type": pa.string(),
    "symbol": pa.string(),
    "description": pa.string(),
    "strike": pa.float64(),
    "expiration_date": pa.string(),
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
    "quote_at": pa.string(),
    "trade_at": pa.string(),
    "time_value": pa.float64(),
    "net_change": pa.float64(),
    "percent_change": pa.float64(),
    "settlement_type": pa.string(),
    "exercise_type": pa.string(),
    "expiration_type": pa.string(),
    "multiplier": pa.int64(),
    "non_standard": pa.bool_(),
    "sampled_at": pa.string(),
}

# Timestamp columns that arrive as UTC ISO 8601 strings in the CSV.
_TIMESTAMP_COLS = ("sampled_at", "quote_at", "trade_at")


def normalize_options(table: pa.Table) -> tuple[pa.Table, list[str]]:
    """Normalize an options Arrow table to the canonical schema."""
    diagnostics: list[str] = []
    source_names = set(table.column_names)

    # Validate input matches tickrake's producer contract
    table, val_diags = validate_input_schema(table, _EXPECTED_INPUT)
    diagnostics.extend(val_diags)

    # Cast expiration_date from string to date32
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

    # Cast UTC ISO string timestamps to timestamptz
    for ts_col in _TIMESTAMP_COLS:
        if ts_col not in source_names:
            continue
        col = table.column(ts_col)
        if (pa.types.is_string(col.type) or pa.types.is_large_string(col.type)) and pc.any(
            pc.is_valid(col)
        ).as_py():
            casted = iso_string_to_timestamp(col)
            table = table.set_column(table.column_names.index(ts_col), ts_col, casted)
            diagnostics.append(f"Cast {ts_col} from string to timestamptz")

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
