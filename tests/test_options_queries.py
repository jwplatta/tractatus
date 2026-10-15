"""Tests for OptionsQueryClient — all use real DuckDB against real parquet files."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tractatus.tickrake.options.queries import OPTIONS_DTYPES, OptionsQueryClient

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_options_table(rows: list[dict]) -> pa.Table:  # type: ignore[type-arg]
    """Build a pyarrow table with the minimal options schema."""
    return pa.table(
        {
            "strike": pa.array([float(r["strike"]) for r in rows], type=pa.float64()),
            "contract_type": pa.array([r["contract_type"] for r in rows], type=pa.string()),
            "expiration_date": pa.array([r["expiration_date"] for r in rows], type=pa.string()),
            "sampled_at": pa.array([r["sampled_at"] for r in rows], type=pa.timestamp("us")),
            "open_interest": pa.array([float(r.get("oi", 0)) for r in rows], type=pa.float64()),
            "volatility": pa.array([float(r.get("vol", 0.2)) for r in rows], type=pa.float64()),
            "underlying_price": pa.array(
                [float(r.get("spot", 500.0)) for r in rows], type=pa.float64()
            ),
        }
    )


_EXP_A = date(2026, 3, 21)
_EXP_B = date(2026, 3, 28)
_T0 = datetime(2026, 1, 5, 14, 30)
_T1 = datetime(2026, 1, 5, 14, 35)
_T2 = datetime(2026, 1, 5, 14, 40)


@pytest.fixture
def parquet_file(tmp_path: Path) -> Path:
    """Single parquet with two expiries and three sample times."""
    rows = [
        {
            "strike": 500,
            "contract_type": "call",
            "expiration_date": _EXP_A.isoformat(),
            "sampled_at": _T0,
            "oi": 100,
            "vol": 0.20,
        },
        {
            "strike": 505,
            "contract_type": "put",
            "expiration_date": _EXP_A.isoformat(),
            "sampled_at": _T0,
            "oi": 200,
            "vol": 0.22,
        },
        {
            "strike": 500,
            "contract_type": "call",
            "expiration_date": _EXP_A.isoformat(),
            "sampled_at": _T1,
            "oi": 110,
            "vol": 0.21,
        },
        {
            "strike": 500,
            "contract_type": "call",
            "expiration_date": _EXP_A.isoformat(),
            "sampled_at": _T2,
            "oi": 120,
            "vol": 0.23,
        },
        {
            "strike": 510,
            "contract_type": "call",
            "expiration_date": _EXP_B.isoformat(),
            "sampled_at": _T0,
            "oi": 50,
            "vol": 0.18,
        },
    ]
    path = tmp_path / "options.parquet"
    pq.write_table(_make_options_table(rows), path)
    return path


@pytest.fixture
def client() -> OptionsQueryClient:
    return OptionsQueryClient()  # in-memory connection


# ---------------------------------------------------------------------------
# list_snapshot_times
# ---------------------------------------------------------------------------


def test_list_snapshot_times_returns_sorted_datetimes(client, parquet_file):
    times = client.list_snapshot_times(parquet_file, _EXP_A)
    assert times == sorted(times)
    assert len(times) == 3


def test_list_snapshot_times_empty_for_missing_expiry(client, parquet_file):
    times = client.list_snapshot_times(parquet_file, date(2099, 1, 1))
    assert times == []


# ---------------------------------------------------------------------------
# load_snapshot
# ---------------------------------------------------------------------------


def test_load_snapshot_filters_by_expiry_and_sampled_at(client, parquet_file):
    df = client.load_snapshot(parquet_file, _EXP_A, _T0)
    assert not df.empty
    assert set(df["expiration_date"].dt.date.unique()) == {_EXP_A}
    assert len(df) == 2  # call + put at T0


def test_load_snapshot_returns_empty_for_wrong_time(client, parquet_file):
    bad_time = _T0 + timedelta(seconds=1)
    df = client.load_snapshot(parquet_file, _EXP_A, bad_time)
    assert df.empty


# ---------------------------------------------------------------------------
# load_expiry
# ---------------------------------------------------------------------------


def test_load_expiry_returns_all_rows_ordered_by_sampled_at(client, parquet_file):
    df = client.load_expiry(parquet_file, _EXP_A)
    assert not df.empty
    # 2 rows at T0, 1 at T1, 1 at T2
    assert len(df) == 4
    times = df["sampled_at"].tolist()
    assert times == sorted(times)


def test_load_expiry_empty_for_missing_expiry(client, parquet_file):
    df = client.load_expiry(parquet_file, date(2099, 1, 1))
    assert df.empty


# ---------------------------------------------------------------------------
# latest_window
# ---------------------------------------------------------------------------


def test_latest_window_returns_max_sampled_at_per_expiry(client, parquet_file):
    results = client.latest_window(parquet_file, _EXP_A, _EXP_B)
    as_dict = dict(results)
    assert _EXP_A in as_dict
    assert _EXP_B in as_dict
    # EXP_A's latest is T2
    assert as_dict[_EXP_A] == _T2
    # EXP_B only has T0
    assert as_dict[_EXP_B] == _T0


def test_latest_window_empty_when_no_expirations_in_range(client, parquet_file):
    results = client.latest_window(parquet_file, date(2099, 1, 1), date(2099, 12, 31))
    assert results == []


# ---------------------------------------------------------------------------
# list_expirations
# ---------------------------------------------------------------------------


def test_list_expirations_returns_only_dates_gte_min(client, parquet_file):
    exps = client.list_expirations(parquet_file, _EXP_A)
    assert _EXP_A in exps
    assert _EXP_B in exps
    assert exps == sorted(exps)


def test_list_expirations_excludes_dates_before_min(client, parquet_file):
    exps = client.list_expirations(parquet_file, _EXP_B)
    assert _EXP_A not in exps
    assert _EXP_B in exps


def test_list_expirations_empty_when_none_match(client, parquet_file):
    exps = client.list_expirations(parquet_file, date(2099, 1, 1))
    assert exps == []


# ---------------------------------------------------------------------------
# load_lookback
# ---------------------------------------------------------------------------


def test_load_lookback_returns_data_for_expiry_range(client, parquet_file):
    df = client.load_lookback(str(parquet_file), (_EXP_A, _EXP_B), interval_minutes=5)
    assert not df.empty
    exps = set(df["expiration_date"].dt.date.unique())
    assert _EXP_A in exps
    assert _EXP_B in exps


def test_load_lookback_deduplicates_within_interval_bucket(client, tmp_path):
    """Two rows with same strike/contract_type in the same 60-min bucket → one row out."""
    t_base = datetime(2026, 1, 5, 14, 0)
    rows = [
        {
            "strike": 500,
            "contract_type": "call",
            "expiration_date": _EXP_A.isoformat(),
            "sampled_at": t_base,
            "oi": 100,
        },
        # same bucket, same key — later timestamp wins
        {
            "strike": 500,
            "contract_type": "call",
            "expiration_date": _EXP_A.isoformat(),
            "sampled_at": t_base + timedelta(minutes=30),
            "oi": 200,
        },
    ]
    path = tmp_path / "dedup.parquet"
    pq.write_table(_make_options_table(rows), path)
    df = client.load_lookback(str(path), (_EXP_A, _EXP_A), interval_minutes=60)
    assert len(df) == 1
    assert float(df["open_interest"].iloc[0]) == 200.0


# ---------------------------------------------------------------------------
# load_sample_window
# ---------------------------------------------------------------------------


def test_load_sample_window_filters_by_start_date(client, parquet_file):
    # load_sample_window filters WHERE sampled_at >= TIMESTAMPTZ '{sample_start}'
    # Using the date 2026-01-05 means midnight, so T0/T1/T2 all pass (same day)
    df = client.load_sample_window(str(parquet_file), _T0.date(), interval_minutes=5)
    assert not df.empty
    # Result covers both expiries
    exps = set(pd.to_datetime(df["expiration_date"]).dt.date.unique())
    assert _EXP_A in exps


# ---------------------------------------------------------------------------
# load_expiry_lookback
# ---------------------------------------------------------------------------


def test_load_expiry_lookback_single_expiry(client, parquet_file):
    df = client.load_expiry_lookback(str(parquet_file), _EXP_A, interval_minutes=5)
    assert not df.empty
    assert set(df["expiration_date"].dt.date.unique()) == {_EXP_A}


def test_load_expiry_lookback_empty_for_missing_expiry(client, parquet_file):
    df = client.load_expiry_lookback(str(parquet_file), date(2099, 1, 1), interval_minutes=5)
    assert df.empty


# ---------------------------------------------------------------------------
# _normalize_df — contract_type uppercased
# ---------------------------------------------------------------------------


def test_normalize_df_uppercases_contract_type(client, parquet_file):
    df = client.load_snapshot(parquet_file, _EXP_A, _T0)
    assert all(v == v.upper() for v in df["contract_type"])


# ---------------------------------------------------------------------------
# OPTIONS_DTYPES export
# ---------------------------------------------------------------------------


def test_options_dtypes_exported():
    assert "strike" in OPTIONS_DTYPES
    assert "gamma" in OPTIONS_DTYPES
    assert len(OPTIONS_DTYPES) >= 10
