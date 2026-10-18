"""Tests for options normalization and the normalize_raw dispatcher."""

from __future__ import annotations

import datetime
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tractatus.catalog.normalize import normalize_parquet, normalize_raw, normalize_table
from tractatus.catalog.normalize.common import write_canonical_parquet
from tractatus.catalog.normalize.options import _COLUMNS as CANONICAL_COLUMNS
from tractatus.catalog.normalize.options import _TYPES as CANONICAL_ARROW_TYPES

_TS = datetime.datetime(2026, 6, 25, 12, 0, tzinfo=datetime.UTC)


def _make_41col_table(n_rows: int = 3) -> pa.Table:
    """Build a full 41-column table matching the CSV contract."""
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
            arrays[col] = pa.array([_TS] * n_rows, type=target)
        elif pa.types.is_date(target):
            arrays[col] = pa.array([datetime.date(2026, 6, 25)] * n_rows, type=target)
    return pa.table(arrays)


def _make_csv_table(n_rows: int = 3) -> pa.Table:
    """Build a table matching the raw CSV contract (strings for timestamps/dates)."""
    arrays: dict[str, pa.Array] = {}
    for col in CANONICAL_COLUMNS:
        target = CANONICAL_ARROW_TYPES[col]
        if col == "expiration_date":
            arrays[col] = pa.array(["2026-06-25"] * n_rows, type=pa.string())
        elif col in ("sampled_at", "quote_at", "trade_at"):
            arrays[col] = pa.array(["2026-06-25T12:00:00Z"] * n_rows, type=pa.string())
        elif pa.types.is_string(target) or pa.types.is_large_string(target):
            arrays[col] = pa.array(["x"] * n_rows, type=target)
        elif pa.types.is_floating(target):
            arrays[col] = pa.array([1.0] * n_rows, type=target)
        elif pa.types.is_integer(target):
            arrays[col] = pa.array([1] * n_rows, type=target)
        elif pa.types.is_boolean(target):
            arrays[col] = pa.array([True] * n_rows, type=target)
    return pa.table(arrays)


class TestNormalizeTable:
    def test_41col_passthrough(self) -> None:
        table = _make_41col_table()
        result, diagnostics = normalize_table(table)
        assert result.column_names == CANONICAL_COLUMNS
        assert len(result) == 3
        assert not any("Missing" in d for d in diagnostics)

    def test_csv_contract_normalizes(self) -> None:
        """A table matching the CSV contract (string dates/timestamps) normalizes correctly."""
        table = _make_csv_table()
        result, diagnostics = normalize_table(table)
        assert result.column_names == CANONICAL_COLUMNS
        assert result.column("expiration_date").type == pa.date32()
        assert result.column("sampled_at")[0].as_py() == _TS
        assert any("Cast expiration_date" in d for d in diagnostics)
        assert any("Cast sampled_at" in d for d in diagnostics)

    def test_string_expiration_date_cast(self) -> None:
        table = _make_csv_table()
        result, diagnostics = normalize_table(table)
        col = result.column("expiration_date")
        assert col.type == pa.date32()
        assert col[0].as_py() == datetime.date(2026, 6, 25)

    def test_invalid_date_raises(self) -> None:
        table = _make_csv_table()
        bad_col = pa.array(["not-a-date"] * 3, type=pa.string())
        idx = table.column_names.index("expiration_date")
        table = table.set_column(idx, "expiration_date", bad_col)
        with pytest.raises(ValueError, match="Cannot cast expiration_date"):
            normalize_table(table)

    def test_trade_at_quote_at_fallback(self) -> None:
        table = _make_41col_table()
        null_ts = pa.array([None, None, None], type=pa.timestamp("us", tz="UTC"))
        table = table.set_column(table.column_names.index("trade_at"), "trade_at", null_ts)
        table = table.set_column(table.column_names.index("quote_at"), "quote_at", null_ts)

        result, diagnostics = normalize_table(table)
        assert result.column("trade_at")[0].as_py() == _TS
        assert result.column("quote_at")[0].as_py() == _TS
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


class TestNormalizeRaw:
    def test_csv_options(self) -> None:
        csv_bytes = (
            b"contract_type,symbol,description,strike,expiration_date,"
            b"open,high,low,close,mark,bid,bid_size,ask,ask_size,last,last_size,"
            b"open_interest,total_volume,transactions,delta,gamma,theta,vega,rho,"
            b"volatility,theoretical_volatility,theoretical_option_value,"
            b"intrinsic_value,extrinsic_value,underlying_price,"
            b"quote_at,trade_at,time_value,net_change,percent_change,"
            b"settlement_type,exercise_type,expiration_type,multiplier,non_standard,sampled_at\n"
            b"CALL,SPXW  261018C05000000,SPX Oct 18 5000 Call,5000.0,2026-10-18,"
            b"1.0,2.0,0.5,1.5,1.25,1.0,10,1.5,20,1.25,5,"
            b"1000,500,100,0.5,0.01,-0.05,0.1,0.02,"
            b"0.25,0.24,1.3,"
            b"0.5,0.75,5800.0,"
            b"2026-10-18T14:30:00Z,2026-10-18T14:29:59Z,0.75,0.1,5.0,"
            b"PM,EUROPEAN,WEEKLY,100,false,2026-10-18T14:30:00Z\n"
        )
        result, diags = normalize_raw(csv_bytes, "csv", "options")
        assert result.column_names == CANONICAL_COLUMNS
        assert len(result) == 1
        assert result.column("contract_type")[0].as_py() == "CALL"
        assert result.column("strike")[0].as_py() == 5000.0

    def test_ndjson_level_one(self) -> None:
        row = {
            "job_type": "level_one",
            "provider": "schwab",
            "received_at": "2026-10-18T14:30:00Z",
            "symbol": "SPY",
            "service": "LEVELONE_EQUITIES",
            "quote_time": "2026-10-18T14:30:00Z",
            "trade_time": "2026-10-18T14:29:59Z",
            "bid": 585.12,
            "ask": 585.14,
            "last": 585.13,
            "mark": 585.13,
            "bid_size": 20,
            "ask_size": 15,
            "volume": 42150120,
            "extra_json": json.dumps({"6": "Q", "unknownKey": "test"}),
        }
        data = (json.dumps(row) + "\n").encode()
        result, diags = normalize_raw(
            data, "ndjson", "level_one_equities", sample_date=datetime.date(2026, 10, 18)
        )
        assert result.column("symbol")[0].as_py() == "SPY"
        assert result.column("ask_id")[0].as_py() == "Q"
        assert "job_type" not in result.column_names

    def test_unsupported_format_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported file format"):
            normalize_raw(b"data", "parquet", "options")


class TestNormalizeParquet:
    def test_roundtrip(self, tmp_path: Path) -> None:
        src = tmp_path / "source.parquet"
        dest = tmp_path / "dest.parquet"
        table = _make_csv_table()
        pq.write_table(table, src)

        diagnostics = normalize_parquet(src, dest)
        assert len(diagnostics) > 0

        result = pq.read_table(dest)
        assert result.column_names == CANONICAL_COLUMNS
        assert result.column("expiration_date").type == pa.date32()


class TestWriteCanonicalParquet:
    def test_sorts_and_writes(self, tmp_path: Path) -> None:
        table = pa.table(
            {
                "a": pa.array([3, 1, 2], type=pa.int64()),
                "b": pa.array(["c", "a", "b"], type=pa.string()),
            }
        )
        dest = tmp_path / "sorted.parquet"
        write_canonical_parquet(table, dest, sort_keys=["a"], row_group_size=2)

        result = pq.read_table(dest)
        assert result.column("a").to_pylist() == [1, 2, 3]
        assert result.column("b").to_pylist() == ["a", "b", "c"]

    def test_compression_and_row_groups(self, tmp_path: Path) -> None:
        table = pa.table({"x": pa.array(list(range(100)), type=pa.int64())})
        dest = tmp_path / "compressed.parquet"
        write_canonical_parquet(
            table, dest, sort_keys=["x"], row_group_size=30, compression_level=5
        )

        meta = pq.read_metadata(dest)
        assert meta.num_row_groups >= 3  # 100 rows / 30 per group
        assert meta.row_group(0).column(0).compression == "ZSTD"
