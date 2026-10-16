"""Tests for the catalog publisher pipeline."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tractatus.catalog.config import CatalogConfig
from tractatus.catalog.ledger import Ledger
from tractatus.catalog.publisher import Publisher


def _make_config(tmp_path: Path) -> CatalogConfig:
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
    )


def _create_parquet(data_root: Path, table: str, provider: str, d: date, root: str) -> Path:
    """Write a minimal valid Parquet file."""
    path = (
        data_root
        / table
        / provider
        / f"{d:%Y}"
        / f"{d:%m}"
        / f"{d:%d}"
        / f"{root}_samples_{d}.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    # Write a real parquet file so normalization can read it
    tbl = pa.table({"x": [1]})
    pq.write_table(tbl, path)
    return path


@patch("tractatus.catalog.publisher.normalize_table", return_value=(pa.table({"x": [1]}), []))
@patch("tractatus.catalog.publisher.load_catalog")
def test_discover_only_dry_run(
    mock_load_catalog: MagicMock, _mock_normalize: MagicMock, tmp_path: Path
) -> None:
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "options", "schwab", date(2026, 6, 25), "SPX")
    _create_parquet(config.data_root, "options", "schwab", date(2026, 6, 25), "AAPL")

    publisher = Publisher(config)
    result = publisher.run(table_name="options", dry_run=True)
    publisher.close()

    assert result.discovered == 2
    assert result.committed == 0


@patch("tractatus.catalog.publisher.normalize_table", return_value=(pa.table({"x": [1]}), []))
@patch("tractatus.catalog.publisher.load_catalog")
def test_full_pipeline(
    mock_load_catalog: MagicMock, _mock_normalize: MagicMock, tmp_path: Path
) -> None:
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "options", "schwab", date(2026, 6, 25), "SPX")

    mock_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    result = publisher.run(table_name="options")
    publisher.close()

    assert result.discovered == 1
    assert result.committed == 1
    assert result.errors == 0

    mock_table.append.assert_called_once()


@patch("tractatus.catalog.publisher.normalize_table", return_value=(pa.table({"x": [1]}), []))
@patch("tractatus.catalog.publisher.load_catalog")
def test_append_error_marks_file(
    mock_load_catalog: MagicMock, _mock_normalize: MagicMock, tmp_path: Path
) -> None:
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "options", "schwab", date(2026, 6, 25), "SPX")

    mock_table = MagicMock()
    mock_table.append.side_effect = Exception("Iceberg error")
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    result = publisher.run(table_name="options")
    publisher.close()

    assert result.discovered == 1
    assert result.committed == 0
    assert result.errors == 1

    ledger = Ledger(config.database_url)
    record = ledger.get("schwab/2026/06/25/SPX_samples_2026-06-25.parquet", "options")
    assert record is not None
    assert record.status == "error"
    assert "Iceberg error" in record.error  # type: ignore[operator]
    ledger.close()


def test_unknown_table_raises(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    publisher = Publisher(config)
    try:
        with pytest.raises(ValueError, match="Unknown table"):
            publisher.run(table_name="nonexistent")
    finally:
        publisher.close()


@patch("tractatus.catalog.publisher.normalize_table", return_value=(pa.table({"x": [1]}), []))
@patch("tractatus.catalog.publisher.load_catalog")
def test_bounded_publish_leaves_out_of_scope_rows(
    mock_load_catalog: MagicMock, _mock_normalize: MagicMock, tmp_path: Path
) -> None:
    """A date-bounded publish must not append out-of-scope pending rows."""
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "options", "schwab", date(2026, 10, 15), "SPX")
    _create_parquet(config.data_root, "options", "schwab", date(2026, 12, 15), "SPX")

    # Discover all files
    publisher = Publisher(config)
    publisher.run(table_name="options", dry_run=True)
    publisher.close()

    ledger = Ledger(config.database_url)
    assert len(ledger.pending_upload("options")) == 2
    ledger.close()

    # Run a December-bounded publish
    mock_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    result = publisher.run(table_name="options", after=date(2026, 12, 1), before=date(2027, 1, 1))
    publisher.close()

    assert result.committed == 1

    # October row should still be pending
    ledger = Ledger(config.database_url)
    oct_record = ledger.get("schwab/2026/10/15/SPX_samples_2026-10-15.parquet", "options")
    assert oct_record is not None
    assert oct_record.status == "discovered"
    ledger.close()


@patch("tractatus.catalog.publisher.normalize_table", return_value=(pa.table({"x": [1]}), []))
@patch("tractatus.catalog.publisher.load_catalog")
def test_bounded_publish_provider_filter(
    mock_load_catalog: MagicMock, _mock_normalize: MagicMock, tmp_path: Path
) -> None:
    """A provider-bounded publish must not append out-of-scope provider rows."""
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "options", "schwab", date(2026, 10, 15), "SPX")
    _create_parquet(config.data_root, "options", "tradier", date(2026, 10, 15), "SPX")

    # Discover all files
    publisher = Publisher(config)
    publisher.run(table_name="options", dry_run=True)
    publisher.close()

    mock_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    result = publisher.run(table_name="options", providers=["schwab"])
    publisher.close()

    assert result.committed == 1

    # Tradier row should still be pending
    ledger = Ledger(config.database_url)
    tradier_record = ledger.get("tradier/2026/10/15/SPX_samples_2026-10-15.parquet", "options")
    assert tradier_record is not None
    assert tradier_record.status == "discovered"
    ledger.close()


def test_resume_after_failure(tmp_path: Path) -> None:
    """Files that failed append can be retried on next run."""
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "options", "schwab", date(2026, 6, 25), "SPX")

    publisher = Publisher(config)
    publisher.run(table_name="options", dry_run=True)
    publisher.close()

    ledger = Ledger(config.database_url)
    pending = ledger.pending_upload("options")
    assert len(pending) == 1
    ledger.close()
