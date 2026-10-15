"""Tests for options sub-clients (filesystem, archive)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from tractatus.tickrake.options.archive import ArchiveClient
from tractatus.tickrake.options.filesystem import FilesystemClient, parse_snapshot_filename


def test_list_roots(cfg, options_dir):
    client = FilesystemClient(cfg)
    roots = client.list_roots("test-provider")
    assert set(roots) == {"SPY", "SPXW"}


def test_list_sample_dates(cfg, options_dir):
    client = FilesystemClient(cfg)
    assert client.list_sample_dates("SPY", "test-provider") == [date(2026, 1, 4)]


def test_scan_snapshots(cfg, options_dir):
    client = FilesystemClient(cfg)
    snapshots = client.scan_snapshots("SPY", date(2026, 1, 5), provider="test-provider")
    assert len(snapshots) == 1
    fetch_dt, path = snapshots[0]
    assert path.name == "SPY_exp2026-01-10_2026-01-05_14-30-00.csv"


def test_scan_snapshots_for_expiry(cfg, options_dir):
    client = FilesystemClient(cfg)
    snapshots = client.scan_snapshots_for_expiry(
        "SPY", date(2026, 1, 10), date(2026, 1, 5), provider="test-provider"
    )
    assert len(snapshots) == 1

    snapshots = client.scan_snapshots_for_expiry(
        "SPY", date(2026, 1, 11), date(2026, 1, 5), provider="test-provider"
    )
    assert len(snapshots) == 0


def test_scan_snapshots_empty_date(cfg, options_dir):
    client = FilesystemClient(cfg)
    snapshots = client.scan_snapshots("SPY", date(2026, 1, 6), provider="test-provider")
    assert snapshots == []


def test_parse_snapshot_filename(tmp_path):
    p = Path("SPY_exp2026-01-10_2026-01-05_14-30-00.csv")
    result = parse_snapshot_filename(p)
    assert result is not None
    exp_date, fetch_dt = result
    assert exp_date == date(2026, 1, 10)
    assert fetch_dt.hour == 14
    assert fetch_dt.minute == 30


def test_parse_snapshot_filename_invalid():
    assert parse_snapshot_filename(Path("bad_file.csv")) is None


def test_archive_get_root_index(cfg, options_dir):
    client = ArchiveClient(cfg)
    index = client.get_root_index("SPY", "test-provider")
    assert index["root"] == "SPY"
    assert len(index["historical"]) == 1


def test_archive_get_root_index_missing(cfg, options_dir):
    client = ArchiveClient(cfg)
    assert client.get_root_index("NONEXISTENT", "test-provider") == {}


def test_archive_get_parquet_path_local_cache_hit(cfg, options_dir):
    client = ArchiveClient(cfg)
    path = client.get_parquet_path("SPY", date(2026, 1, 4), provider="test-provider")
    assert path is not None
    assert path.exists()
    assert path.suffix == ".parquet"


def test_archive_descriptor_lookup_supports_live_list_schema(cfg, monkeypatch):
    client = ArchiveClient(cfg)
    monkeypatch.setattr(
        client,
        "get_root_index",
        lambda *_args, **_kwargs: {
            "historical": [
                {
                    "sample_date": "2025-12-18",
                    "files": [
                        {"format": "csv", "uri": "s3://tickrake/SPXW_samples_2025-12-18.csv"},
                        {
                            "format": "parquet",
                            "uri": "s3://tickrake/SPXW_samples_2025-12-18.parquet",
                        },
                    ],
                }
            ]
        },
    )

    descriptor = client._find_parquet_descriptor("SPXW", date(2025, 12, 18), "schwab", False)

    assert descriptor == {
        "format": "parquet",
        "uri": "s3://tickrake/SPXW_samples_2025-12-18.parquet",
    }
