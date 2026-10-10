"""Tests for the catalog reader (local parquet path)."""

from __future__ import annotations

from datetime import UTC, date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from tractatus.catalog.config import CatalogConfig
from tractatus.catalog.reader import CatalogReader


def _make_config(tmp_path: Path) -> CatalogConfig:
    return CatalogConfig(
        iceberg_bucket="test-bucket",
        iceberg_warehouse="s3://test-bucket/warehouse",
        glue_database="tractatus",
        glue_table="options",
        aws_region="us-east-1",
        aws_profile=None,
        data_root=tmp_path,
        ledger_path=tmp_path / "ledger.db",
    )


def _write_test_parquet(path: Path, rows: int = 5) -> None:
    """Write a minimal options parquet file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(
        {
            "contract_type": ["CALL"] * rows,
            "symbol": ["SPX"] * rows,
            "description": ["test"] * rows,
            "strike": [4000.0 + i for i in range(rows)],
            "expiration_date": ["2026-07-17"] * rows,
            "open": [10.0] * rows,
            "high": [11.0] * rows,
            "low": [9.0] * rows,
            "close": [10.5] * rows,
            "mark": [10.25] * rows,
            "bid": [10.0] * rows,
            "bid_size": [100] * rows,
            "ask": [10.5] * rows,
            "ask_size": [50] * rows,
            "last": [10.3] * rows,
            "last_size": [10.0] * rows,
            "open_interest": [500.0] * rows,
            "total_volume": [1000.0] * rows,
            "transactions": [200] * rows,
            "delta": [0.5] * rows,
            "gamma": [0.01] * rows,
            "theta": [-0.05] * rows,
            "vega": [0.1] * rows,
            "rho": [0.02] * rows,
            "volatility": [0.2] * rows,
            "theoretical_volatility": [0.21] * rows,
            "theoretical_option_value": [10.1] * rows,
            "intrinsic_value": [5.0] * rows,
            "extrinsic_value": [5.1] * rows,
            "underlying_price": [4005.0] * rows,
            "sampled_at": pd.to_datetime(["2026-06-25T14:30:00+00:00"] * rows),
        }
    )
    pq.write_table(table, path)


def test_local_parquet_load_expiry(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    parquet_path = tmp_path / "schwab" / "2026" / "06" / "25" / "SPX_samples_2026-06-25.parquet"
    _write_test_parquet(parquet_path)

    reader = CatalogReader(config)
    df = reader.load_expiry("SPX", date(2026, 6, 25), date(2026, 7, 17))

    assert len(df) == 5
    assert "strike" in df.columns
    assert df["contract_type"].iloc[0] == "CALL"


def test_local_parquet_list_expirations(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    parquet_path = tmp_path / "schwab" / "2026" / "06" / "25" / "SPX_samples_2026-06-25.parquet"
    _write_test_parquet(parquet_path)

    reader = CatalogReader(config)
    expirations = reader.list_expirations("SPX", date(2026, 6, 25))

    assert date(2026, 7, 17) in expirations


def test_local_parquet_load_snapshot(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    parquet_path = tmp_path / "schwab" / "2026" / "06" / "25" / "SPX_samples_2026-06-25.parquet"
    _write_test_parquet(parquet_path)

    from datetime import datetime

    reader = CatalogReader(config)
    df = reader.load_snapshot(
        "SPX",
        date(2026, 6, 25),
        date(2026, 7, 17),
        datetime(2026, 6, 25, 14, 30, tzinfo=UTC),
    )

    assert len(df) == 5


def test_missing_local_returns_none(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    reader = CatalogReader(config)
    # _local_parquet should return None when file doesn't exist
    assert reader._local_parquet("SPX", date(2026, 6, 25)) is None
