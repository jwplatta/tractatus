"""Tests for catalog file discovery."""

from __future__ import annotations

from datetime import date
from pathlib import Path

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


def test_discover_skips_non_matching_files(tmp_path: Path) -> None:
    path = tmp_path / "schwab" / "2026" / "06" / "25" / "not_a_sample.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"some content")

    files = discover_files(tmp_path)
    assert len(files) == 0


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


def test_discover_skips_json_files(tmp_path: Path) -> None:
    """JSON index files in the provider directory should be ignored."""
    _create_parquet(tmp_path, "schwab", date(2026, 6, 25), "SPX")
    # Simulate a ticker index JSON file at the provider level
    (tmp_path / "schwab" / "SPX.json").write_text("{}")

    files = discover_files(tmp_path)
    assert len(files) == 1
