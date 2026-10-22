"""Fundamentals normalization.

Casts date and timestamp string columns to their canonical Iceberg types.
"""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from tractatus.catalog.normalize.common import align_to_schema, get_canonical, validate_input_schema
from tractatus.catalog.schemas.fundamentals import (
    DATE_COLUMNS,
    FUNDAMENTALS_TABLE,
    TIMESTAMP_COLUMNS,
)

_COLUMNS, _TYPES = get_canonical(FUNDAMENTALS_TABLE.schema)

# Validate only key identifier columns — fundamentals has 69+ columns and
# the full set may evolve; the important thing is the identifiers are present.
_EXPECTED_INPUT: dict[str, pa.DataType] = {
    "symbol": pa.string(),
    "sample_date": pa.string(),
}


def normalize_fundamentals(table: pa.Table) -> tuple[pa.Table, list[str]]:
    """Normalize a fundamentals Arrow table to the canonical schema."""
    diagnostics: list[str] = []

    # Validate input matches tickrake's producer contract
    table, val_diags = validate_input_schema(table, _EXPECTED_INPUT)
    diagnostics.extend(val_diags)

    # Cast date string columns → date32
    for col_name in DATE_COLUMNS:
        if col_name not in table.column_names:
            continue
        col = table.column(col_name)
        if not (pa.types.is_string(col.type) or pa.types.is_large_string(col.type)):
            continue
        # Schwab date formats: "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS.F"
        # Truncate to date portion then cast
        truncated = pc.utf8_slice_codeunits(col, start=0, stop=10)
        casted = pc.cast(truncated, pa.date32())
        table = table.set_column(table.column_names.index(col_name), col_name, casted)
        diagnostics.append(f"Cast {col_name} from string to date32")

    # Cast timestamp string columns → timestamptz
    for col_name in TIMESTAMP_COLUMNS:
        if col_name not in table.column_names:
            continue
        col = table.column(col_name)
        if not (pa.types.is_string(col.type) or pa.types.is_large_string(col.type)):
            continue
        # ISO 8601: "YYYY-MM-DDTHH:MM:SSZ"
        parsed = pc.strptime(col, format="%Y-%m-%dT%H:%M:%SZ", unit="us")
        casted = pc.assume_timezone(parsed, timezone="UTC")
        table = table.set_column(table.column_names.index(col_name), col_name, casted)
        diagnostics.append(f"Cast {col_name} from string to timestamptz")

    # Align to canonical schema
    table, align_diags = align_to_schema(table, _COLUMNS, _TYPES)
    diagnostics.extend(align_diags)

    return table, diagnostics
