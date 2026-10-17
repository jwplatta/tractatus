"""Tests for catalog file discovery."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from tractatus.catalog.discovery import discover_files


def _create_parquet(base: Path, provider: str, d: date, root: str) -> Path:
    """Create a minimal parquet file in the expected directory layout."""
    path = base / provider / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}" / f"{root}_samples_{d}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"PAR1fake parquet content for testing")
    return path


def test_discover_finds_parquet(tmp_path: Path) -> None:
    _create_parquet(tmp_path, "schwab", date(2026, 6, 25), "SPX")
    _create_parquet(tmp_path, "schwab", date(2026, 6, 25), "AAPL")

    files = discover_files(tmp_path)
    assert len(files) == 2
    roots = {f.root for f in files}
    assert roots == {"SPX", "AAPL"}

    for f in files:
        assert f.provider == "schwab"
        assert f.sample_date == date(2026, 6, 25)
        assert f.size_bytes > 0
        assert len(f.sha256) == 64


def test_discover_filters_provider(tmp_path: Path) -> None:
    _create_parquet(tmp_path, "schwab", date(2026, 6, 25), "SPX")
    _create_parquet(tmp_path, "other", date(2026, 6, 25), "AAPL")

    files = discover_files(tmp_path, providers=["schwab"])
    assert len(files) == 1
    assert files[0].provider == "schwab"


def test_discover_skips_empty_files(tmp_path: Path) -> None:
    path = tmp_path / "schwab" / "2026" / "06" / "25" / "SPX_samples_2026-06-25.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")

    files = discover_files(tmp_path)
    assert len(files) == 0


def test_discover_accepts_any_parquet(tmp_path: Path) -> None:
    """Options discovery now accepts any .parquet in the right directory structure."""
    path = tmp_path / "schwab" / "2026" / "06" / "25" / "not_a_sample.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"some content")

    files = discover_files(tmp_path)
    assert len(files) == 1
    assert files[0].root == "not_a_sample"
    assert files[0].sample_date == date(2026, 6, 25)


def test_discover_multiple_dates(tmp_path: Path) -> None:
    _create_parquet(tmp_path, "schwab", date(2026, 6, 24), "SPX")
    _create_parquet(tmp_path, "schwab", date(2026, 6, 25), "SPX")

    files = discover_files(tmp_path)
    assert len(files) == 2
    dates = {f.sample_date for f in files}
    assert dates == {date(2026, 6, 24), date(2026, 6, 25)}


def test_discover_relative_path(tmp_path: Path) -> None:
    _create_parquet(tmp_path, "schwab", date(2026, 6, 25), "SPX")

    files = discover_files(tmp_path)
    assert len(files) == 1
    assert files[0].relative_path == "schwab/2026/06/25/SPX_samples_2026-06-25.parquet"


def test_discover_empty_dir(tmp_path: Path) -> None:
    files = discover_files(tmp_path)
    assert files == []


def test_discover_nonexistent_dir(tmp_path: Path) -> None:
    files = discover_files(tmp_path / "nope")
    assert files == []


def _create_level_one(base: Path, provider: str, d: date, symbol: str) -> Path:
    """Create a minimal level_one parquet file."""
    path = base / provider / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}" / f"{symbol}_152516Z.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"PAR1fake")
    return path


def _create_candles(base: Path, provider: str, interval: str, year: int, symbol: str) -> Path:
    """Create a minimal candles parquet file."""
    path = base / provider / interval / str(year) / f"{symbol}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"PAR1fake")
    return path


def _create_fundamentals(base: Path, provider: str, d: date) -> Path:
    """Create a minimal fundamentals parquet file."""
    path = base / provider / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"PAR1fake")
    return path


def test_discover_level_one_equities(tmp_path: Path) -> None:
    _create_level_one(tmp_path, "schwab", date(2026, 10, 15), "SPY")
    _create_level_one(tmp_path, "schwab", date(2026, 10, 15), "QQQ")
    # Futures file should be excluded
    _create_level_one(tmp_path, "schwab", date(2026, 10, 15), "^ES")

    files = discover_files(tmp_path, table_name="level_one_equities")
    assert len(files) == 2
    roots = {f.root for f in files}
    assert roots == {"SPY", "QQQ"}
    assert all(f.sample_date == date(2026, 10, 15) for f in files)


def test_discover_level_one_futures(tmp_path: Path) -> None:
    _create_level_one(tmp_path, "schwab", date(2026, 10, 15), "^ES")
    _create_level_one(tmp_path, "schwab", date(2026, 10, 15), "^NQ")
    # Equities file should be excluded
    _create_level_one(tmp_path, "schwab", date(2026, 10, 15), "SPY")

    files = discover_files(tmp_path, table_name="level_one_futures")
    assert len(files) == 2
    roots = {f.root for f in files}
    assert roots == {"^ES", "^NQ"}
    assert all(f.sample_date == date(2026, 10, 15) for f in files)


def test_discover_candles(tmp_path: Path) -> None:
    _create_candles(tmp_path, "schwab", "5min", 2026, "SPX")
    _create_candles(tmp_path, "schwab", "day", 2026, "SPX")

    files = discover_files(tmp_path, table_name="candles")
    assert len(files) == 2
    assert all(f.provider == "schwab" for f in files)
    assert {f.root for f in files} == {"SPX"}


def test_discover_fundamentals(tmp_path: Path) -> None:
    _create_fundamentals(tmp_path, "schwab", date(2026, 10, 15))

    files = discover_files(tmp_path, table_name="fundamentals")
    assert len(files) == 1
    assert files[0].sample_date == date(2026, 10, 15)
    assert files[0].root == "all"


def test_discover_unknown_table_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="No discovery config"):
        discover_files(tmp_path, table_name="nonexistent")


def test_discover_skips_json_files(tmp_path: Path) -> None:
    """JSON index files in the provider directory should be ignored."""
    _create_parquet(tmp_path, "schwab", date(2026, 6, 25), "SPX")
    # Simulate a ticker index JSON file at the provider level
    (tmp_path / "schwab" / "SPX.json").write_text("{}")

    files = discover_files(tmp_path)
    assert len(files) == 1
