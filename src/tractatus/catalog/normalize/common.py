"""Shared normalization utilities for aligning Arrow tables to Iceberg schemas."""

from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema


def get_canonical(schema: Schema) -> tuple[list[str], dict[str, pa.DataType]]:
    """Derive canonical column names and Arrow types from an Iceberg schema."""
    arrow_schema = schema_to_pyarrow(schema)
    columns = arrow_schema.names
    types = {f.name: f.type for f in arrow_schema}
    return columns, types


def align_to_schema(
    table: pa.Table,
    canonical_columns: list[str],
    canonical_types: dict[str, pa.DataType],
) -> tuple[pa.Table, list[str]]:
    """Add missing columns, drop extras, reorder, and cast to canonical types.

    Returns ``(aligned_table, diagnostics)``.
    """
    diagnostics: list[str] = []
    source_names = set(table.column_names)

    for col_name in canonical_columns:
        if col_name not in source_names:
            null_array = pa.nulls(len(table), type=canonical_types[col_name])
            table = table.append_column(col_name, null_array)
            diagnostics.append(f"Added missing column {col_name}")

    extra = [c for c in table.column_names if c not in canonical_types]
    if extra:
        table = table.drop(extra)
        diagnostics.append(f"Dropped extra columns: {', '.join(extra)}")

    table = table.select(canonical_columns)

    for col_name in canonical_columns:
        col = table.column(col_name)
        expected = canonical_types[col_name]
        if col.type != expected:
            try:
                casted = pc.cast(col, expected)
            except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
                raise ValueError(
                    f"Cannot cast {col_name} from {col.type} to {expected}: {exc}"
                ) from exc
            table = table.set_column(table.column_names.index(col_name), col_name, casted)

    return table, diagnostics
