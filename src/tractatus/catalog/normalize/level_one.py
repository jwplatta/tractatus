"""Level-one normalization.

Parses ``extra_json``, promotes high-value fields to top-level columns,
and packs the rest into a nullable ``extra`` struct.
"""

from __future__ import annotations

import json
from datetime import date as date_type

import pyarrow as pa

from tractatus.catalog.normalize.common import align_to_schema, get_canonical
from tractatus.catalog.schemas.level_one import (
    _EXTRA_STRUCT,
    EXTRA_STRUCT_FIELDS,
    LEVEL_ONE_TABLE,
    PROMOTED_FIELDS,
)

_COLUMNS, _TYPES = get_canonical(LEVEL_ONE_TABLE.schema)

# Build a lookup from struct field name → pyarrow type for the extra struct.
_STRUCT_FIELD_TYPES: dict[str, pa.DataType] = {}
for field in _EXTRA_STRUCT.fields:
    _STRUCT_FIELD_TYPES[field.name] = (
        pa.large_string()
        if field.field_type.__class__.__name__ == "StringType"
        else pa.float64()
        if field.field_type.__class__.__name__ == "DoubleType"
        else pa.int64()
        if field.field_type.__class__.__name__ == "LongType"
        else pa.bool_()
        if field.field_type.__class__.__name__ == "BooleanType"
        else pa.large_string()
    )

_EXTRA_ARROW_TYPE = _TYPES["extra"]
_STRUCT_FIELD_NAMES = [f.name for f in _EXTRA_STRUCT.fields]


def _parse_extra_json(
    extra_json_col: pa.Array, n_rows: int
) -> tuple[dict[str, list[object]], list[dict[str, object] | None]]:
    """Parse extra_json column into promoted fields and struct dicts."""
    promoted: dict[str, list[object]] = {name: [] for name in PROMOTED_FIELDS.values()}
    structs: list[dict[str, object] | None] = []

    for i in range(n_rows):
        val = extra_json_col[i].as_py()
        if val is None:
            for lst in promoted.values():
                lst.append(None)
            structs.append(None)
            continue

        data = json.loads(val)

        # Promoted fields
        for json_key, col_name in PROMOTED_FIELDS.items():
            promoted[col_name].append(data.get(json_key))

        # Struct fields
        struct_dict: dict[str, object] = {}
        for json_key, field_name in EXTRA_STRUCT_FIELDS.items():
            if json_key in data:
                struct_dict[field_name] = data[json_key]
            else:
                struct_dict[field_name] = None
        structs.append(struct_dict)

    return promoted, structs


def normalize_level_one(
    table: pa.Table, sample_date: date_type | None = None
) -> tuple[pa.Table, list[str]]:
    """Normalize a level_one Arrow table to the canonical schema."""
    diagnostics: list[str] = []
    n_rows = len(table)

    # Parse extra_json into promoted columns and struct column
    if "extra_json" in table.column_names:
        extra_col = table.column("extra_json")
        promoted, struct_dicts = _parse_extra_json(extra_col, n_rows)

        # Add promoted columns
        for col_name, values in promoted.items():
            arr = pa.array(values, type=_TYPES[col_name])
            table = table.append_column(col_name, arr)
        diagnostics.append("Promoted fields from extra_json")

        # Build struct column
        struct_arrays = pa.array(struct_dicts, type=_EXTRA_ARROW_TYPE)
        table = table.append_column("extra", struct_arrays)
        diagnostics.append("Built extra struct from extra_json")

        # Drop original extra_json
        table = table.drop(["extra_json"])

    # Add sample_date from caller
    if sample_date is not None and "sample_date" not in table.column_names:
        date_arr = pa.array([sample_date] * n_rows, type=pa.date32())
        table = table.append_column("sample_date", date_arr)
        diagnostics.append("Added sample_date from file path")

    # Align to canonical schema
    table, align_diags = align_to_schema(table, _COLUMNS, _TYPES)
    diagnostics.extend(align_diags)

    return table, diagnostics
