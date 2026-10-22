"""Shared normalization utilities for aligning Arrow tables to Iceberg schemas."""

from __future__ import annotations

import io
from pathlib import Path
from typing import BinaryIO

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.json as pj
import pyarrow.parquet as pq
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.schema import Schema

_TS_US_UTC = pa.timestamp("us", tz="UTC")

# Type pairs that are logically equivalent for validation purposes.
_STRING_TYPES = {pa.string(), pa.large_string()}


def _types_compatible(actual: pa.DataType, expected: pa.DataType) -> bool:
    """Check if two Arrow types are compatible for validation purposes.

    Treats string/large_string as equivalent, int/float as castable (CSV readers
    commonly infer int64 for values that are logically float), and allows
    date/timestamp variants where string is expected.
    """
    if actual in _STRING_TYPES and expected in _STRING_TYPES:
        return True
    # Numeric types are castable (int64 ↔ float64)
    if (pa.types.is_integer(actual) or pa.types.is_floating(actual)) and (
        pa.types.is_integer(expected) or pa.types.is_floating(expected)
    ):
        return True
    # Allow date/timestamp where string is expected (normalizer handles casts)
    return (pa.types.is_date(actual) or pa.types.is_timestamp(actual)) and expected in _STRING_TYPES


def validate_input_schema(
    table: pa.Table,
    expected: dict[str, pa.DataType],
) -> list[str]:
    """Validate that an input table matches the expected producer schema.

    Raises ``ValueError`` if required columns are missing or have incompatible
    types. Returns diagnostic warnings for unexpected extra columns.
    """
    diagnostics: list[str] = []
    actual_schema = {f.name: f.type for f in table.schema}

    # Check for missing columns
    missing = sorted(set(expected) - set(actual_schema))
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    # Check for type mismatches (string and large_string are treated as compatible)
    for col_name, expected_type in expected.items():
        actual_type = actual_schema[col_name]
        if actual_type != expected_type:
            if _types_compatible(actual_type, expected_type):
                continue
            raise ValueError(f"Column {col_name}: expected {expected_type}, got {actual_type}")

    # Warn about extra columns
    extra = sorted(set(actual_schema) - set(expected))
    if extra:
        diagnostics.append(f"Unexpected extra columns in input: {', '.join(extra)}")

    return diagnostics


def epoch_ms_to_timestamp(col: pa.Array) -> pa.Array:
    """Convert an int64 epoch-milliseconds column to timestamp[us, tz=UTC]."""
    # millis → micros, then interpret as timestamp
    micros = pc.multiply(col.cast(pa.int64()), 1000)
    return micros.cast(_TS_US_UTC)


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


def csv_to_arrow(data: bytes) -> pa.Table:
    """Parse CSV bytes into an Arrow table with type inference."""
    return pcsv.read_csv(io.BytesIO(data))


def ndjson_to_arrow(data: bytes) -> pa.Table:
    """Parse newline-delimited JSON bytes into an Arrow table."""
    return pj.read_json(io.BytesIO(data))


def iso_string_to_timestamp(col: pa.Array) -> pa.Array:
    """Convert a string column of UTC ISO 8601 timestamps to timestamp[us, UTC]."""
    return pc.cast(col, _TS_US_UTC)


def write_canonical_parquet(
    table: pa.Table,
    dest: Path | BinaryIO,
    *,
    sort_keys: list[str],
    row_group_size: int,
    compression: str = "zstd",
    compression_level: int = 3,
) -> None:
    """Sort table and write with canonical physical specs."""
    indices = pc.sort_indices(table, sort_keys=[(k, "ascending") for k in sort_keys])
    table = table.take(indices)
    pq.write_table(
        table,
        dest,
        row_group_size=row_group_size,
        compression=compression,
        compression_level=compression_level,
    )
