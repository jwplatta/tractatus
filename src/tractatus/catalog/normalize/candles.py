"""Candles normalization.

Injects ``symbol`` and ``interval`` columns from file metadata and casts
the ``datetime`` string column to a UTC timestamp.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from tractatus.catalog.normalize.common import align_to_schema, get_canonical, validate_input_schema
from tractatus.catalog.schemas.candles import CANDLES_TABLE

_COLUMNS, _TYPES = get_canonical(CANDLES_TABLE.schema)

_EXPECTED_INPUT: dict[str, pa.DataType] = {
    "datetime": pa.string(),
    "open": pa.float64(),
    "high": pa.float64(),
    "low": pa.float64(),
    "close": pa.float64(),
    "volume": pa.int64(),
}


def normalize_candles(
    table: pa.Table,
    symbol: str | None = None,
    interval: str | None = None,
) -> tuple[pa.Table, list[str]]:
    """Normalize a candles Arrow table to the canonical schema."""
    diagnostics: list[str] = []
    n_rows = len(table)

    # Validate input matches tickrake's producer contract
    val_diags = validate_input_schema(table, _EXPECTED_INPUT)
    diagnostics.extend(val_diags)

    # Inject symbol from filename
    if symbol is not None and "symbol" not in table.column_names:
        table = table.append_column("symbol", pa.array([symbol] * n_rows, type=_TYPES["symbol"]))
        diagnostics.append(f"Injected symbol={symbol}")

    # Inject interval from directory path
    if interval is not None and "interval" not in table.column_names:
        table = table.append_column(
            "interval", pa.array([interval] * n_rows, type=_TYPES["interval"])
        )
        diagnostics.append(f"Injected interval={interval}")

    # Cast datetime string to timestamptz
    if "datetime" in table.column_names:
        col = table.column("datetime")
        if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
            casted = pc.assume_timezone(
                pc.strptime(col, format="%Y-%m-%dT%H:%M:%SZ", unit="us"),
                timezone="UTC",
            )
            table = table.set_column(table.column_names.index("datetime"), "datetime", casted)
            diagnostics.append("Cast datetime from string to timestamptz")

    # Align to canonical schema
    table, align_diags = align_to_schema(table, _COLUMNS, _TYPES)
    diagnostics.extend(align_diags)

    return table, diagnostics
