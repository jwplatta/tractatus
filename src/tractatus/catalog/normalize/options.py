"""Options-specific normalization.

Validates the full 41-column CSV contract from tickrake, casts types,
and applies fallback logic for timestamp fields.
"""

from __future__ import annotations

import re

import pyarrow as pa
import pyarrow.compute as pc

from tractatus.catalog.normalize.common import (
    align_to_schema,
    get_canonical,
    iso_string_to_timestamp,
    set_literal_column,
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

# Mapping from option root → underlying symbol for non-standard index options.
# Equity options not in this map use the option_root directly as underlying_symbol.
_ROOT_TO_UNDERLYING: dict[str, str] = {
    "SPXW": "$SPX",
    "NDXP": "$NDX",
    "RUTW": "$RUT",
    "XSP": "$SPX",
    "NANOS": "$SPX",
}

# Regex to extract the option root from a contract symbol (e.g. "SPXW 261023C00125000" → "SPXW").
_ROOT_RE = re.compile(r"^([A-Z$]+)")


def _parse_option_root(contract_symbol: str | None) -> str | None:
    """Extract the option root from a contract symbol string."""
    if not contract_symbol:
        return None
    m = _ROOT_RE.match(contract_symbol.strip())
    return m.group(1) if m else None


def _derive_underlying(option_root: str | None) -> str | None:
    """Look up the underlying symbol for an option root."""
    if not option_root:
        return None
    return _ROOT_TO_UNDERLYING.get(option_root, option_root)


def normalize_options(
    table: pa.Table,
    *,
    option_root: str | None = None,
    provider: str | None = None,
) -> tuple[pa.Table, list[str]]:
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

    # Populate option_root — use tickrake-provided value if present, else parse from symbol
    if "option_root" not in table.column_names or pc.all(pc.is_null(table["option_root"])).as_py():
        if option_root:
            root_col = pa.array([option_root] * len(table), type=pa.string())
        else:
            symbols = table.column("symbol").to_pylist()
            root_col = pa.array([_parse_option_root(s) for s in symbols], type=pa.string())
        table = table.set_column(table.column_names.index("option_root"), "option_root", root_col)
        diagnostics.append("Populated option_root from symbol")

    # Populate underlying_symbol from option_root via mapping
    if (
        "underlying_symbol" not in table.column_names
        or pc.all(pc.is_null(table["underlying_symbol"])).as_py()
    ):
        roots = table.column("option_root").to_pylist()
        underlying_col = pa.array([_derive_underlying(r) for r in roots], type=pa.string())
        table = table.set_column(
            table.column_names.index("underlying_symbol"), "underlying_symbol", underlying_col
        )
        diagnostics.append("Populated underlying_symbol from option_root mapping")

    table = set_literal_column(table, "provider", provider)

    return table, diagnostics
