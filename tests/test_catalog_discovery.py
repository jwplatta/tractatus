"""Tests for catalog file discovery."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from tractatus.catalog.discovery import discover_files
from tractatus.catalog.file_store import LocalFileStore


def _create_options_csv(base: Path, provider: str, d: date, root: str, exp: str) -> Path:
    """Create a minimal options CSV file in the expected directory layout."""
    path = (
        base
        / "options"
        / provider
        / f"{d:%Y}"
        / f"{d:%m}"
        / f"{d:%d}"
        / f"{root}_exp{exp}_{d}_14-30-00.csv"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("contract_type,symbol,strike\nCALL,SPX,5000\n")
    return path


def test_discover_finds_csv(tmp_path: Path) -> None:
    _create_options_csv(tmp_path, "schwab", date(2026, 6, 25), "SPX", "2026-07-18")
    _create_options_csv(tmp_path, "schwab", date(2026, 6, 25), "AAPL", "2026-07-18")

    store = LocalFileStore(tmp_path)
    files = discover_files(store)
    assert len(files) == 2
    roots = {f.root for f in files}
    assert roots == {"SPX", "AAPL"}

    for f in files:
        assert f.provider == "schwab"
        assert f.sample_date == date(2026, 6, 25)
        assert f.size_bytes > 0
        assert len(f.sha256) == 64
        assert f.file_format == "csv"
        assert f.metadata.get("expiration") == "2026-07-18"


def test_discover_filters_provider(tmp_path: Path) -> None:
    _create_options_csv(tmp_path, "schwab", date(2026, 6, 25), "SPX", "2026-07-18")
    _create_options_csv(tmp_path, "other", date(2026, 6, 25), "AAPL", "2026-07-18")

    store = LocalFileStore(tmp_path)
    files = discover_files(store, providers=["schwab"])
    assert len(files) == 1
    assert files[0].provider == "schwab"


def test_discover_skips_empty_files(tmp_path: Path) -> None:
    path = (
        tmp_path
        / "options"
        / "schwab"
        / "2026"
        / "06"
        / "25"
        / "SPX_exp2026-07-18_2026-06-25_14-30-00.csv"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"")

    store = LocalFileStore(tmp_path)
    files = discover_files(store)
    assert len(files) == 0


def test_discover_multiple_dates(tmp_path: Path) -> None:
    _create_options_csv(tmp_path, "schwab", date(2026, 6, 24), "SPX", "2026-07-18")
    _create_options_csv(tmp_path, "schwab", date(2026, 6, 25), "SPX", "2026-07-18")

    store = LocalFileStore(tmp_path)
    files = discover_files(store)
    assert len(files) == 2
    dates = {f.sample_date for f in files}
    assert dates == {date(2026, 6, 24), date(2026, 6, 25)}


def test_discover_relative_path(tmp_path: Path) -> None:
    _create_options_csv(tmp_path, "schwab", date(2026, 6, 25), "SPX", "2026-07-18")

    store = LocalFileStore(tmp_path)
    files = discover_files(store)
    assert len(files) == 1
    assert files[0].relative_path == "schwab/2026/06/25/SPX_exp2026-07-18_2026-06-25_14-30-00.csv"


def test_discover_empty_dir(tmp_path: Path) -> None:
    store = LocalFileStore(tmp_path)
    files = discover_files(store)
    assert files == []


def test_discover_nonexistent_dir(tmp_path: Path) -> None:
    store = LocalFileStore(tmp_path / "nope")
    files = discover_files(store)
    assert files == []


def _create_ndjson(base: Path, table: str, provider: str, d: date, symbol: str) -> Path:
    """Create a minimal NDJSON file."""
    path = base / table / provider / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}" / f"{symbol}.ndjson"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"symbol":"' + symbol + '"}\n')
    return path


def _create_candles_csv(base: Path, provider: str, frequency: str, symbol: str) -> Path:
    """Create a minimal candles CSV file."""
    path = base / "candles" / provider / frequency / f"{symbol}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("datetime,open,high,low,close,volume\n2026-10-18T14:30:00Z,1,2,3,4,100\n")
    return path


def _create_fundamentals(base: Path, provider: str, d: date) -> Path:
    """Create a minimal fundamentals NDJSON file."""
    path = base / "fundamentals" / provider / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}.ndjson"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"symbol":"AAPL"}\n')
    return path


def _create_economic_events(base: Path, source: str, category: str, d: date) -> Path:
    """Create a minimal economic events NDJSON file."""
    path = base / "economic_events" / source / category / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}.ndjson"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"event_name":"CPI"}\n')
    return path


def test_discover_level_one_equities(tmp_path: Path) -> None:
    _create_ndjson(tmp_path, "level_one", "schwab", date(2026, 10, 15), "SPY")
    _create_ndjson(tmp_path, "level_one", "schwab", date(2026, 10, 15), "QQQ")
    # Futures file should be excluded
    _create_ndjson(tmp_path, "level_one", "schwab", date(2026, 10, 15), "^ES")

    store = LocalFileStore(tmp_path)
    files = discover_files(store, table_name="level_one_equities")
    assert len(files) == 2
    roots = {f.root for f in files}
    assert roots == {"SPY", "QQQ"}
    assert all(f.sample_date == date(2026, 10, 15) for f in files)
    assert all(f.file_format == "ndjson" for f in files)


def test_discover_level_one_futures(tmp_path: Path) -> None:
    _create_ndjson(tmp_path, "level_one", "schwab", date(2026, 10, 15), "^ES")
    _create_ndjson(tmp_path, "level_one", "schwab", date(2026, 10, 15), "^NQ")
    # Equities file should be excluded
    _create_ndjson(tmp_path, "level_one", "schwab", date(2026, 10, 15), "SPY")

    store = LocalFileStore(tmp_path)
    files = discover_files(store, table_name="level_one_futures")
    assert len(files) == 2
    roots = {f.root for f in files}
    assert roots == {"^ES", "^NQ"}
    assert all(f.sample_date == date(2026, 10, 15) for f in files)


def test_discover_candles(tmp_path: Path) -> None:
    _create_candles_csv(tmp_path, "schwab", "5min", "SPX")
    _create_candles_csv(tmp_path, "schwab", "day", "SPX")

    store = LocalFileStore(tmp_path)
    files = discover_files(store, table_name="candles")
    assert len(files) == 2
    assert all(f.provider == "schwab" for f in files)
    assert {f.root for f in files} == {"SPX"}
    assert all(f.file_format == "csv" for f in files)
    frequencies = {f.metadata.get("frequency") for f in files}
    assert frequencies == {"5min", "day"}


def test_discover_fundamentals(tmp_path: Path) -> None:
    _create_fundamentals(tmp_path, "schwab", date(2026, 10, 15))

    store = LocalFileStore(tmp_path)
    files = discover_files(store, table_name="fundamentals")
    assert len(files) == 1
    assert files[0].sample_date == date(2026, 10, 15)
    assert files[0].root == "all"
    assert files[0].file_format == "ndjson"


def test_discover_economic_events(tmp_path: Path) -> None:
    _create_economic_events(tmp_path, "bls", "economic", date(2026, 10, 14))
    _create_economic_events(tmp_path, "fred", "fomc", date(2026, 9, 17))

    store = LocalFileStore(tmp_path)
    files = discover_files(store, table_name="economic_events")
    assert len(files) == 2
    sources = {f.provider for f in files}
    assert sources == {"bls", "fred"}
    categories = {f.root for f in files}
    assert categories == {"economic", "fomc"}


def test_discover_unknown_table_raises(tmp_path: Path) -> None:
    store = LocalFileStore(tmp_path)
    with pytest.raises(ValueError, match="No discovery config"):
        discover_files(store, table_name="nonexistent")


def test_discover_ignores_non_matching_extensions(tmp_path: Path) -> None:
    """JSON and parquet files in the options directory should be ignored."""
    _create_options_csv(tmp_path, "schwab", date(2026, 6, 25), "SPX", "2026-07-18")
    # Non-matching files
    (tmp_path / "options" / "schwab" / "SPX.json").write_text("{}")
    parquet_path = (
        tmp_path / "options" / "schwab" / "2026" / "06" / "25" / "SPX_samples_2026-06-25.parquet"
    )
    parquet_path.write_bytes(b"PAR1fake")

    store = LocalFileStore(tmp_path)
    files = discover_files(store)
    assert len(files) == 1
    assert files[0].file_format == "csv"
