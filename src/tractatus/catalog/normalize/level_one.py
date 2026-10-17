"""Level-one normalization for equities and futures.

Parses ``extra_json`` into flat columns with proper Schwab API field names.
Separate normalizer entry points for equities and futures since the field
numbering has different semantics per service type.
"""

from __future__ import annotations

import json
from datetime import date as date_type

import pyarrow as pa

from tractatus.catalog.normalize.common import (
    align_to_schema,
    epoch_ms_to_timestamp,
    get_canonical,
)
from tractatus.catalog.schemas.level_one_equities import (
    EXTRA_JSON_FIELDS as EQ_FIELDS,
)
from tractatus.catalog.schemas.level_one_equities import (
    LEVEL_ONE_EQUITIES_TABLE,
)
from tractatus.catalog.schemas.level_one_equities import (
    TIMESTAMP_FIELDS as EQ_TS_FIELDS,
)
from tractatus.catalog.schemas.level_one_futures import (
    EXTRA_JSON_FIELDS as FUT_FIELDS,
)
from tractatus.catalog.schemas.level_one_futures import (
    LEVEL_ONE_FUTURES_TABLE,
)
from tractatus.catalog.schemas.level_one_futures import (
    TIMESTAMP_FIELDS as FUT_TS_FIELDS,
)

_EQ_COLUMNS, _EQ_TYPES = get_canonical(LEVEL_ONE_EQUITIES_TABLE.schema)
_FUT_COLUMNS, _FUT_TYPES = get_canonical(LEVEL_ONE_FUTURES_TABLE.schema)


def _unpack_extra_json(
    table: pa.Table,
    field_map: dict[str, str],
    canonical_types: dict[str, pa.DataType],
    timestamp_fields: set[str],
) -> tuple[pa.Table, list[str]]:
    """Parse extra_json and add flat columns per the field map.

    Mapped keys are promoted to typed columns. Unmapped keys are preserved
    as a residual JSON string in ``extra_json`` so no data is silently lost.
    """
    diagnostics: list[str] = []
    n_rows = len(table)

    if "extra_json" not in table.column_names:
        return table, diagnostics

    extra_col = table.column("extra_json")

    # Collect values per target column + residual JSON
    columns: dict[str, list[object]] = {col_name: [] for col_name in field_map.values()}
    residuals: list[str | None] = []
    mapped_keys = set(field_map.keys())

    for i in range(n_rows):
        val = extra_col[i].as_py()
        if val is None:
            for lst in columns.values():
                lst.append(None)
            residuals.append(None)
            continue

        data = json.loads(val)
        for json_key, col_name in field_map.items():
            columns[col_name].append(data.get(json_key))

        # Preserve unmapped keys
        remaining = {k: v for k, v in data.items() if k not in mapped_keys}
        residuals.append(json.dumps(remaining) if remaining else None)

    # Add promoted columns
    for col_name, values in columns.items():
        target_type = canonical_types[col_name]
        if col_name in timestamp_fields:
            int_arr = pa.array(values, type=pa.int64())
            arr = epoch_ms_to_timestamp(int_arr)
        else:
            arr = pa.array(values, type=target_type)
        table = table.append_column(col_name, arr)

    diagnostics.append(f"Unpacked {len(field_map)} fields from extra_json")

    # Replace extra_json with residual (unmapped keys only)
    idx = table.column_names.index("extra_json")
    table = table.set_column(idx, "extra_json", pa.array(residuals, type=pa.large_string()))

    return table, diagnostics


def _convert_source_timestamps(
    table: pa.Table,
) -> tuple[pa.Table, list[str]]:
    """Convert source parquet epoch-ms columns to timestamptz and rename."""
    diagnostics: list[str] = []
    renames = {
        "received_at_ms": "received_at",
        "quote_time_ms": "quote_time",
        "trade_time_ms": "trade_time",
    }
    for old_name, new_name in renames.items():
        if old_name in table.column_names:
            idx = table.column_names.index(old_name)
            converted = epoch_ms_to_timestamp(table.column(old_name))
            table = table.set_column(idx, new_name, converted)
            diagnostics.append(f"Converted {old_name} to timestamptz as {new_name}")
    return table, diagnostics


def normalize_level_one_equities(
    table: pa.Table, sample_date: date_type | None = None
) -> tuple[pa.Table, list[str]]:
    """Normalize a level_one equities Arrow table to the canonical schema."""
    diagnostics: list[str] = []
    n_rows = len(table)

    # Convert source parquet timestamp columns
    table, ts_diags = _convert_source_timestamps(table)
    diagnostics.extend(ts_diags)

    # Unpack extra_json into flat columns
    table, json_diags = _unpack_extra_json(table, EQ_FIELDS, _EQ_TYPES, EQ_TS_FIELDS)
    diagnostics.extend(json_diags)

    # Add sample_date
    if sample_date is not None and "sample_date" not in table.column_names:
        date_arr = pa.array([sample_date] * n_rows, type=pa.date32())
        table = table.append_column("sample_date", date_arr)
        diagnostics.append("Added sample_date from file path")

    # Align to canonical schema
    table, align_diags = align_to_schema(table, _EQ_COLUMNS, _EQ_TYPES)
    diagnostics.extend(align_diags)

    return table, diagnostics


def normalize_level_one_futures(
    table: pa.Table, sample_date: date_type | None = None
) -> tuple[pa.Table, list[str]]:
    """Normalize a level_one futures Arrow table to the canonical schema."""
    diagnostics: list[str] = []
    n_rows = len(table)

    # Convert source parquet timestamp columns
    table, ts_diags = _convert_source_timestamps(table)
    diagnostics.extend(ts_diags)

    # Unpack extra_json into flat columns
    table, json_diags = _unpack_extra_json(table, FUT_FIELDS, _FUT_TYPES, FUT_TS_FIELDS)
    diagnostics.extend(json_diags)

    # Add sample_date
    if sample_date is not None and "sample_date" not in table.column_names:
        date_arr = pa.array([sample_date] * n_rows, type=pa.date32())
        table = table.append_column("sample_date", date_arr)
        diagnostics.append("Added sample_date from file path")

    # Align to canonical schema
    table, align_diags = align_to_schema(table, _FUT_COLUMNS, _FUT_TYPES)
    diagnostics.extend(align_diags)

    return table, diagnostics
