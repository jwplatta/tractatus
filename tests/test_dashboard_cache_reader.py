from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

import duckdb
import pandas as pd

from tractatus.reader import CacheReader, OptionSource


def test_inventory_filters_range_and_preserves_remote_revision():
    reader = CacheReader.__new__(CacheReader)
    calls = []

    def listing(bucket, prefix):
        calls.append(prefix)
        return [
            ("options/schwab/2026/10/01/SPY_samples_2026-10-01.parquet", "etag1", 100),
            ("options/schwab/2026/10/02/SPY_samples_2026-10-02.parquet", "etag2", 200),
            ("options/schwab/2026/10/02/QQQ_samples_2026-10-02.parquet", "other", 300),
        ]

    reader._minio = SimpleNamespace(lakehouse_bucket="lake", list_object_metadata=listing)
    sources = reader.list_option_sources("SPY", date(2026, 10, 2), date(2026, 10, 3))
    assert sources == [
        OptionSource(
            date(2026, 10, 2),
            "s3://lake/options/schwab/2026/10/02/SPY_samples_2026-10-02.parquet",
            "etag2:200",
            200,
        )
    ]
    assert calls == ["options/schwab/2026/10/"]


def test_daily_projection_deduplicates_and_selects_nearest_expiry(tmp_path):
    path = tmp_path / "samples.parquet"
    pd.DataFrame(
        [
            {
                "sampled_at": pd.Timestamp(t),
                "expiration_date": exp,
                "strike": 100.0,
                "volatility": iv,
                "delta": 0.25,
                "contract_type": "CALL",
                "underlying_price": 100.0,
            }
            for exp in ("2026-10-30", "2026-12-18")
            for t, iv in [
                ("2026-10-01T14:00Z", 20.0),
                ("2026-10-01T14:35Z", 21.0),
                ("2026-10-01T20:00Z", 22.0),
            ]
        ]
    ).to_parquet(path)
    with duckdb.connect() as conn:
        result = CacheReader.__new__(CacheReader).option_daily_snapshots(
            OptionSource(date(2026, 10, 1), str(path), "v1", path.stat().st_size), 30, conn
        )
    assert list(result.volatility) == [21.0, 22.0]
    assert set(result.expiration_date) == {"2026-10-30"}


def test_root_discovery_uses_shallow_index_listing():
    reader = CacheReader.__new__(CacheReader)
    reader._minio = MagicMock()
    reader._minio.intraday_bucket = "live"
    reader._minio.list_keys.return_value = [
        "intraday/schwab/SPXW.json",
        "intraday/schwab/TSLA.json",
    ]
    assert reader.list_roots() == ["SPXW", "TSLA"]
    reader._minio.list_keys.assert_called_once_with("live", "intraday/schwab/", delimiter="/")
