"""Tests for Parquet normalization to the canonical Iceberg schema."""

from __future__ import annotations

import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tractatus.catalog.normalize import normalize_parquet, normalize_table
from tractatus.catalog.normalize.options import _COLUMNS as CANONICAL_COLUMNS
from tractatus.catalog.normalize.options import _TYPES as CANONICAL_ARROW_TYPES

_DEFAULT_DATE32 = pa.date32()


def _make_31col_table(
    expiration_date_type: pa.DataType = _DEFAULT_DATE32,
    n_rows: int = 3,
) -> pa.Table:
    """Build a 31-column table matching the legacy layout."""
    cols_31 = CANONICAL_COLUMNS[:31]  # up to sampled_at
    arrays: dict[str, pa.Array] = {}
    for col in cols_31:
        target = CANONICAL_ARROW_TYPES[col]
        if col == "expiration_date":
            if pa.types.is_string(expiration_date_type):
                arrays[col] = pa.array(["2026-06-25"] * n_rows, type=pa.string())
            else:
                arrays[col] = pa.array([datetime.date(2026, 6, 25)] * n_rows, type=target)
        elif pa.types.is_string(target) or pa.types.is_large_string(target):
            arrays[col] = pa.array(["x"] * n_rows, type=target)
        elif pa.types.is_floating(target):
            arrays[col] = pa.array([1.0] * n_rows, type=target)
        elif pa.types.is_integer(target):
            arrays[col] = pa.array([1] * n_rows, type=target)
        elif pa.types.is_boolean(target):
            arrays[col] = pa.array([True] * n_rows, type=target)
        elif pa.types.is_timestamp(target):
            arrays[col] = pa.array(
                [datetime.datetime(2026, 6, 25, 12, 0, tzinfo=datetime.UTC)] * n_rows,
                type=target,
            )
        elif pa.types.is_date(target):
            arrays[col] = pa.array([datetime.date(2026, 6, 25)] * n_rows, type=target)
    return pa.table(arrays)


def _make_41col_table(n_rows: int = 3) -> pa.Table:
    """Build a full 41-column table with correct types."""
    arrays: dict[str, pa.Array] = {}
    for col in CANONICAL_COLUMNS:
        target = CANONICAL_ARROW_TYPES[col]
        if pa.types.is_string(target) or pa.types.is_large_string(target):
            arrays[col] = pa.array(["x"] * n_rows, type=target)
        elif pa.types.is_floating(target):
            arrays[col] = pa.array([1.0] * n_rows, type=target)
        elif pa.types.is_integer(target):
            arrays[col] = pa.array([1] * n_rows, type=target)
        elif pa.types.is_boolean(target):
            arrays[col] = pa.array([True] * n_rows, type=target)
        elif pa.types.is_timestamp(target):
            arrays[col] = pa.array(
                [datetime.datetime(2026, 6, 25, 12, 0, tzinfo=datetime.UTC)] * n_rows,
                type=target,
            )
        elif pa.types.is_date(target):
            arrays[col] = pa.array([datetime.date(2026, 6, 25)] * n_rows, type=target)
    return pa.table(arrays)


class TestNormalizeTable:
    def test_41col_passthrough(self) -> None:
        table = _make_41col_table()
        result, diagnostics = normalize_table(table)
        assert result.column_names == CANONICAL_COLUMNS
        assert len(result) == 3
        assert diagnostics == []

    def test_31col_adds_missing(self) -> None:
        table = _make_31col_table()
        result, diagnostics = normalize_table(table)
        assert result.column_names == CANONICAL_COLUMNS
        assert len(result) == 3
        # 10 columns should have been added
        added = [d for d in diagnostics if d.startswith("Added missing")]
        assert len(added) == 10

    def test_string_expiration_date_cast(self) -> None:
        table = _make_31col_table(expiration_date_type=pa.string())
        result, diagnostics = normalize_table(table)
        col = result.column("expiration_date")
        assert col.type == pa.date32()
        assert col[0].as_py() == datetime.date(2026, 6, 25)
        assert any("Cast expiration_date" in d for d in diagnostics)

    def test_invalid_date_raises(self) -> None:
        table = _make_31col_table(expiration_date_type=pa.string())
        # Replace expiration_date with invalid values
        bad_col = pa.array(["not-a-date"] * 3, type=pa.string())
        idx = table.column_names.index("expiration_date")
        table = table.set_column(idx, "expiration_date", bad_col)

        with pytest.raises(ValueError, match="Cannot cast expiration_date"):
            normalize_table(table)

    def test_trade_at_quote_at_fallback(self) -> None:
        table = _make_41col_table()
        ts = datetime.datetime(2026, 6, 25, 12, 0, tzinfo=datetime.UTC)
        # Set trade_at and quote_at to null
        null_ts = pa.array([None, None, None], type=pa.timestamp("us", tz="UTC"))
        table = table.set_column(table.column_names.index("trade_at"), "trade_at", null_ts)
        table = table.set_column(table.column_names.index("quote_at"), "quote_at", null_ts)

        result, diagnostics = normalize_table(table)
        assert result.column("trade_at")[0].as_py() == ts
        assert result.column("quote_at")[0].as_py() == ts
        assert any("Filled null trade_at" in d for d in diagnostics)
        assert any("Filled null quote_at" in d for d in diagnostics)

    def test_extra_columns_dropped(self) -> None:
        table = _make_41col_table()
        extra = pa.array([1, 2, 3], type=pa.int64())
        table = table.append_column("bonus_column", extra)

        result, diagnostics = normalize_table(table)
        assert "bonus_column" not in result.column_names
        assert result.column_names == CANONICAL_COLUMNS
        assert any("Dropped extra columns" in d for d in diagnostics)

    def test_empty_table(self) -> None:
        table = _make_41col_table(n_rows=0)
        result, diagnostics = normalize_table(table)
        assert len(result) == 0
        assert result.column_names == CANONICAL_COLUMNS


class TestNormalizeParquet:
    def test_roundtrip(self, tmp_path: Path) -> None:
        src = tmp_path / "source.parquet"
        dest = tmp_path / "dest.parquet"
        table = _make_31col_table(expiration_date_type=pa.string())
        pq.write_table(table, src)

        diagnostics = normalize_parquet(src, dest)
        assert len(diagnostics) > 0

        result = pq.read_table(dest)
        assert result.column_names == CANONICAL_COLUMNS
        assert result.column("expiration_date").type == pa.date32()
