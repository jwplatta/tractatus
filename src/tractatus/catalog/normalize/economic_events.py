"""Economic events normalization.

Casts timestamp string columns to their canonical Iceberg types.
"""

from __future__ import annotations

import pyarrow as pa

from tractatus.catalog.normalize.common import (
    align_to_schema,
    get_canonical,
    iso_string_to_timestamp,
    validate_input_schema,
)
from tractatus.catalog.schemas.economic_events import ECONOMIC_EVENTS_TABLE, TIMESTAMP_COLUMNS

_COLUMNS, _TYPES = get_canonical(ECONOMIC_EVENTS_TABLE.schema)

_EXPECTED_INPUT: dict[str, pa.DataType] = {
    "event_name": pa.string(),
    "category": pa.string(),
    "source": pa.string(),
    "event_datetime": pa.string(),
    "sampled_at": pa.string(),
}


def normalize_economic_events(table: pa.Table) -> tuple[pa.Table, list[str]]:
    """Normalize an economic_events Arrow table to the canonical schema."""
    diagnostics: list[str] = []

    val_diags = validate_input_schema(table, _EXPECTED_INPUT)
    diagnostics.extend(val_diags)

    # Cast timestamp string columns to timestamptz
    for col_name in TIMESTAMP_COLUMNS:
        if col_name not in table.column_names:
            continue
        col = table.column(col_name)
        if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
            casted = iso_string_to_timestamp(col)
            table = table.set_column(table.column_names.index(col_name), col_name, casted)
            diagnostics.append(f"Cast {col_name} from string to timestamptz")

    table, align_diags = align_to_schema(table, _COLUMNS, _TYPES)
    diagnostics.extend(align_diags)

    return table, diagnostics
