"""Tests for the catalog publisher pipeline."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from tractatus.catalog.config import CatalogConfig
from tractatus.catalog.publisher import _PARQUET_SPECS, Publisher, _metadata_from_path
from tractatus.catalog.schemas import TABLE_DEFS


def _make_config(tmp_path: Path, cache_bucket: str | None = None) -> CatalogConfig:
    data_root = tmp_path / "data"
    data_root.mkdir()
    return CatalogConfig(
        iceberg_bucket="test-bucket",
        iceberg_warehouse="s3://test-bucket",
        glue_database="tractatus",
        aws_region="us-east-1",
        aws_profile=None,
        data_root=data_root,
        database_url=str(tmp_path / "ledger.db"),
        s3_storage_class="STANDARD",
        cache_bucket=cache_bucket,
    )


def _create_options_csv(data_root: Path, provider: str, d: date, root: str) -> Path:
    """Write a minimal options CSV file with enough columns to pass validation."""
    path = (
        data_root
        / "options"
        / provider
        / f"{d:%Y}"
        / f"{d:%m}"
        / f"{d:%d}"
        / f"{root}_exp2026-07-18_{d}_14-30-00.csv"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("contract_type,symbol,strike\nCALL,SPX,5000\n")
    return path


def _create_level_one_ndjson(data_root: Path, provider: str, d: date, symbol: str) -> Path:
    """Write a minimal level_one NDJSON file."""
    path = (
        data_root / "level_one" / provider / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}" / f"{symbol}.ndjson"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('{"symbol":"AAPL","received_at":"2026-06-25T14:30:00Z"}\n')
    return path


# --- Discovery tests ---


@patch("tractatus.catalog.publisher.normalize_table", return_value=(pa.table({"x": [1]}), []))
@patch("tractatus.catalog.publisher.load_catalog")
def test_discover_only_dry_run(
    mock_load_catalog: MagicMock, _mock_normalize: MagicMock, tmp_path: Path
) -> None:
    config = _make_config(tmp_path)
    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "SPX")
    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "AAPL")

    publisher = Publisher(config)
    result = publisher.run(table_name="options", dry_run=True)
    publisher.close()

    assert result.discovered == 2
    assert result.committed == 0
    assert result.cached == 0


def test_unknown_table_raises(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    publisher = Publisher(config)
    try:
        with pytest.raises(ValueError, match="Unknown table"):
            publisher.run(table_name="nonexistent")
    finally:
        publisher.close()


# --- Append pipeline tests ---


@patch("tractatus.catalog.publisher.load_catalog")
def test_append_csv_file(mock_load_catalog: MagicMock, tmp_path: Path) -> None:
    """CSV files should go through normalize_raw path."""
    config = _make_config(tmp_path)
    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "SPX")

    mock_iceberg_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_iceberg_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    # Discover first
    publisher.run(table_name="options", dry_run=True)

    # Now mock normalize_raw for the append
    normalized = pa.table({"x": [1]})
    with patch(
        "tractatus.catalog.publisher.normalize_raw", return_value=(normalized, [])
    ) as mock_norm:
        result = publisher.run(table_name="options")

    publisher.close()

    mock_norm.assert_called_once()
    assert mock_norm.call_args[0][1] == "csv"  # file_format
    mock_iceberg_table.append.assert_called_once_with(normalized)
    assert result.committed == 1


@patch("tractatus.catalog.publisher.load_catalog")
def test_append_ndjson_file(mock_load_catalog: MagicMock, tmp_path: Path) -> None:
    """NDJSON files should go through normalize_raw path."""
    config = _make_config(tmp_path)
    _create_level_one_ndjson(config.data_root, "schwab", date(2026, 6, 25), "AAPL")

    mock_iceberg_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_iceberg_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    publisher.run(table_name="level_one_equities", dry_run=True)

    normalized = pa.table({"x": [1]})
    with patch(
        "tractatus.catalog.publisher.normalize_raw", return_value=(normalized, [])
    ) as mock_norm:
        result = publisher.run(table_name="level_one_equities")

    publisher.close()

    mock_norm.assert_called_once()
    assert mock_norm.call_args[0][1] == "ndjson"
    assert result.committed == 1


@patch("tractatus.catalog.publisher.load_catalog")
def test_append_error_marks_ledger(mock_load_catalog: MagicMock, tmp_path: Path) -> None:
    """Files that fail normalization should be marked as errors."""
    config = _make_config(tmp_path)
    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "SPX")

    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = MagicMock()
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    publisher.run(table_name="options", dry_run=True)

    with patch("tractatus.catalog.publisher.normalize_raw", side_effect=ValueError("bad data")):
        result = publisher.run(table_name="options")

    publisher.close()
    assert result.errors == 1
    assert result.committed == 0


@patch("tractatus.catalog.publisher.load_catalog")
def test_append_with_limit(mock_load_catalog: MagicMock, tmp_path: Path) -> None:
    """Limit should cap the number of files appended."""
    config = _make_config(tmp_path)
    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "SPX")
    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "AAPL")

    mock_iceberg_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_iceberg_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    publisher.run(table_name="options", dry_run=True)

    normalized = pa.table({"x": [1]})
    with patch("tractatus.catalog.publisher.normalize_raw", return_value=(normalized, [])):
        result = publisher.run(table_name="options", limit=1)

    publisher.close()
    assert result.committed == 1


# --- Cache tests ---


@patch("tractatus.catalog.publisher.load_catalog")
def test_cache_to_minio(mock_load_catalog: MagicMock, tmp_path: Path) -> None:
    """When cache_bucket is configured, files should be cached."""
    config = _make_config(tmp_path, cache_bucket="test-cache")

    mock_iceberg_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_iceberg_table
    mock_load_catalog.return_value = mock_catalog

    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "SPX")

    publisher = Publisher(config)
    publisher.run(table_name="options", dry_run=True)

    normalized = pa.table({"x": pa.array([1], type=pa.int64())})
    mock_cache_client = MagicMock()
    publisher._cache_client = mock_cache_client

    with (
        patch("tractatus.catalog.publisher.normalize_raw", return_value=(normalized, [])),
        patch("tractatus.catalog.publisher.write_canonical_parquet"),
    ):
        result = publisher.run(table_name="options")

    publisher.close()

    assert result.cached == 1
    mock_cache_client.put_object.assert_called_once()
    call_kwargs = mock_cache_client.put_object.call_args
    assert call_kwargs[1]["Bucket"] == "test-cache"
    assert call_kwargs[1]["Key"].startswith("options/")
    assert call_kwargs[1]["Key"].endswith(".parquet")


@patch("tractatus.catalog.publisher.load_catalog")
def test_no_cache_without_config(mock_load_catalog: MagicMock, tmp_path: Path) -> None:
    """Without cache_bucket, no caching should happen."""
    config = _make_config(tmp_path)  # no cache_bucket

    mock_iceberg_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_iceberg_table
    mock_load_catalog.return_value = mock_catalog

    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "SPX")

    publisher = Publisher(config)
    publisher.run(table_name="options", dry_run=True)

    normalized = pa.table({"x": [1]})
    with patch("tractatus.catalog.publisher.normalize_raw", return_value=(normalized, [])):
        result = publisher.run(table_name="options")

    publisher.close()
    assert result.cached == 0
    assert result.committed == 1


# --- Metadata extraction tests ---


def test_metadata_from_path_level_one() -> None:
    path = "schwab/2026/06/25/AAPL.ndjson"
    sample_dt, symbol, interval = _metadata_from_path(path, "level_one_equities")
    assert sample_dt == date(2026, 6, 25)
    assert symbol is None
    assert interval is None


def test_metadata_from_path_candles() -> None:
    # _metadata_from_path expects len(parts) >= 4 for candles
    path = "schwab/daily/subdir/AAPL.csv"
    sample_dt, symbol, interval = _metadata_from_path(path, "candles")
    assert symbol == "AAPL"
    assert interval == "daily"


def test_metadata_from_path_options() -> None:
    sample_dt, symbol, interval = _metadata_from_path("schwab/foo.csv", "options")
    assert sample_dt is None


# --- Parquet specs tests ---


def test_all_tables_have_specs() -> None:
    for table_name in TABLE_DEFS:
        assert table_name in _PARQUET_SPECS, f"Missing parquet spec for {table_name}"
