"""Tests for CacheReader.discover_option_sources and related methods."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tractatus.reader.reader import CacheReader, OptionSource


def _make_reader(objects: list[tuple[str, int]]) -> CacheReader:
    """Return a CacheReader with a mocked MinIOStore.list_objects."""
    cfg = MagicMock()
    cfg.minio_endpoint = "http://localhost:9000"
    cfg.minio_access_key = "minioadmin"
    cfg.minio_secret_key = "minioadmin"
    cfg.minio_lakehouse_bucket = "tractatus-lakehouse"
    cfg.minio_intraday_bucket = "tickrake"

    with patch("tractatus.reader.reader.MinIOStore") as mock_store_cls:
        store = MagicMock()
        store.lakehouse_bucket = "tractatus-lakehouse"
        store.list_objects.return_value = objects
        mock_store_cls.return_value = store
        reader = CacheReader(cfg=cfg)
    return reader


def _key(root: str, d: date, provider: str = "schwab") -> str:
    return (
        f"options/{provider}/{d.year:04d}/{d.month:02d}/{d.day:02d}"
        f"/{root}_samples_{d.isoformat()}.parquet"
    )


# ---------------------------------------------------------------------------
# discover_option_sources
# ---------------------------------------------------------------------------


def test_discover_returns_empty_for_empty_roots():
    reader = _make_reader([])
    assert reader.discover_option_sources([], date(2026, 1, 1), date(2026, 1, 31)) == {}


def test_discover_single_listing_for_multiple_roots():
    d1 = date(2026, 10, 1)
    d2 = date(2026, 10, 2)
    objects = [
        (_key("SPY", d1), 1000),
        (_key("SPXW", d1), 2000),
        (_key("SPY", d2), 1100),
    ]
    reader = _make_reader(objects)

    result = reader.discover_option_sources(["SPY", "SPXW"], d1, d2)

    # list_objects called exactly once — the whole point
    reader._minio.list_objects.assert_called_once()

    assert set(result.keys()) == {"SPY", "SPXW"}
    assert [s.sample_date for s in result["SPY"]] == [d1, d2]
    assert result["SPY"][0].revision == "v1:1000"
    assert result["SPY"][0].size_bytes == 1000
    assert result["SPY"][0].uri == f"s3://tractatus-lakehouse/{_key('SPY', d1)}"
    assert result["SPXW"][0].sample_date == d1


def test_discover_excludes_roots_not_requested():
    d = date(2026, 10, 1)
    objects = [(_key("SPY", d), 500), (_key("TSLA", d), 600)]
    reader = _make_reader(objects)

    result = reader.discover_option_sources(["SPY"], d, d)

    assert "TSLA" not in result
    assert "SPY" in result


def test_discover_filters_by_date_range():
    in_range = date(2026, 10, 15)
    too_early = date(2026, 9, 30)
    too_late = date(2026, 10, 31)
    objects = [
        (_key("SPY", too_early), 100),
        (_key("SPY", in_range), 200),
        (_key("SPY", too_late), 300),
    ]
    reader = _make_reader(objects)

    result = reader.discover_option_sources(
        ["SPY"], date(2026, 10, 1), date(2026, 10, 30)
    )

    assert result["SPY"] == [
        OptionSource(in_range, f"s3://tractatus-lakehouse/{_key('SPY', in_range)}", "v1:200", 200)
    ]


def test_discover_sources_sorted_by_sample_date():
    d1, d2, d3 = date(2026, 10, 3), date(2026, 10, 1), date(2026, 10, 2)
    objects = [(_key("SPY", d) , 100) for d in (d1, d2, d3)]
    reader = _make_reader(objects)

    result = reader.discover_option_sources(["SPY"], d2, d1)

    assert [s.sample_date for s in result["SPY"]] == [d2, d3, d1]


def test_discover_omits_roots_with_no_files():
    d = date(2026, 10, 1)
    objects = [(_key("SPY", d), 100)]
    reader = _make_reader(objects)

    result = reader.discover_option_sources(["SPY", "SPXW"], d, d)

    assert "SPXW" not in result


# ---------------------------------------------------------------------------
# list_option_sources
# ---------------------------------------------------------------------------


def test_list_option_sources_delegates_to_discover():
    d = date(2026, 10, 1)
    objects = [(_key("SPY", d), 100)]
    reader = _make_reader(objects)

    sources = reader.list_option_sources("SPY", d, d)

    assert len(sources) == 1
    assert sources[0].sample_date == d
    reader._minio.list_objects.assert_called_once()


def test_list_option_sources_returns_empty_for_missing_root():
    d = date(2026, 10, 1)
    reader = _make_reader([(_key("SPY", d), 100)])

    assert reader.list_option_sources("TSLA", d, d) == []


# ---------------------------------------------------------------------------
# option_daily_snapshots — uses real DuckDB + temp parquet
# ---------------------------------------------------------------------------


def _write_parquet(path: Path, rows: list[dict]) -> None:
    table = pa.table(
        {
            "strike": pa.array([float(r["strike"]) for r in rows], type=pa.float64()),
            "contract_type": pa.array([r["contract_type"] for r in rows], type=pa.string()),
            "expiration_date": pa.array([r["expiration_date"] for r in rows], type=pa.string()),
            "sampled_at": pa.array([r["sampled_at"] for r in rows], type=pa.timestamp("us")),
            "volatility": pa.array([float(r.get("vol", 0.2)) for r in rows], type=pa.float64()),
            "underlying_price": pa.array(
                [float(r.get("spot", 500.0)) for r in rows], type=pa.float64()
            ),
        }
    )
    pq.write_table(table, str(path))


def test_option_daily_snapshots_selects_closest_expiry(tmp_path: Path):
    sample_date = date(2026, 10, 1)
    # target_dte=30 → closest expiry to 2026-10-31
    exp_30 = "2026-10-31"  # 30 days out
    exp_60 = "2026-11-30"  # 60 days out
    parquet_path = tmp_path / "SPY_samples_2026-10-01.parquet"
    _write_parquet(
        parquet_path,
        [
            {"strike": 500, "contract_type": "CALL", "expiration_date": exp_30, "sampled_at": datetime(2026, 10, 1, 14, 30)},
            {"strike": 510, "contract_type": "PUT",  "expiration_date": exp_30, "sampled_at": datetime(2026, 10, 1, 14, 30)},
            {"strike": 500, "contract_type": "CALL", "expiration_date": exp_60, "sampled_at": datetime(2026, 10, 1, 14, 30)},
        ],
    )
    source = OptionSource(
        sample_date=sample_date,
        uri=str(parquet_path),
        revision="v1:100",
        size_bytes=100,
    )
    conn = duckdb.connect(":memory:")

    cfg = MagicMock()
    cfg.minio_endpoint = "http://localhost:9000"
    cfg.minio_access_key = "x"
    cfg.minio_secret_key = "x"
    cfg.minio_lakehouse_bucket = "b"
    cfg.minio_intraday_bucket = "b"

    with patch("tractatus.reader.reader.MinIOStore"):
        reader = CacheReader(cfg=cfg, conn=conn)

    df = reader.option_daily_snapshots(source, 30, conn)

    assert set(df["expiration_date"].dt.date.unique()) == {date(2026, 10, 31)}
    assert len(df) == 2
    assert set(df["contract_type"].unique()) == {"CALL", "PUT"}


def test_option_daily_snapshots_returns_empty_for_empty_parquet(tmp_path: Path):
    parquet_path = tmp_path / "empty.parquet"
    _write_parquet(parquet_path, [])
    source = OptionSource(
        sample_date=date(2026, 10, 1),
        uri=str(parquet_path),
        revision="v1:0",
        size_bytes=0,
    )
    conn = duckdb.connect(":memory:")

    cfg = MagicMock()
    cfg.minio_endpoint = "http://localhost:9000"
    cfg.minio_access_key = "x"
    cfg.minio_secret_key = "x"
    cfg.minio_lakehouse_bucket = "b"
    cfg.minio_intraday_bucket = "b"

    with patch("tractatus.reader.reader.MinIOStore"):
        reader = CacheReader(cfg=cfg, conn=conn)

    df = reader.option_daily_snapshots(source, 30, conn)
    assert df.empty
