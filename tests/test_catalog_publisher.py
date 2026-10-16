"""Tests for the catalog publisher pipeline."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

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
    path.write_bytes(b"PAR1fake parquet content")
    return path


def test_discover_only_dry_run(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "options", "schwab", date(2026, 6, 25), "SPX")
    _create_parquet(config.data_root, "options", "schwab", date(2026, 6, 25), "AAPL")

    publisher = Publisher(config)
    result = publisher.run(table_name="options", dry_run=True)
    publisher.close()

    assert result.discovered == 2
    assert result.uploaded == 0
    assert result.committed == 0


@patch("tractatus.catalog.publisher.normalize_parquet", return_value=[])
@patch("tractatus.catalog.publisher.load_catalog")
@patch("tractatus.catalog.publisher.boto3.Session")
def test_full_pipeline(
    mock_session_cls: MagicMock,
    mock_load_catalog: MagicMock,
    _mock_normalize: MagicMock,
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "options", "schwab", date(2026, 6, 25), "SPX")

    mock_s3 = MagicMock()
    mock_s3.head_object.side_effect = Exception("Not found")
    mock_session_cls.return_value.client.return_value = mock_s3

    mock_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    result = publisher.run(table_name="options")
    publisher.close()

    assert result.discovered == 1
    assert result.uploaded == 1
    assert result.committed == 1
    assert result.errors == 0

    mock_s3.upload_file.assert_called_once()
    mock_table.add_files.assert_called_once()


@patch("tractatus.catalog.publisher.normalize_parquet", return_value=[])
@patch("tractatus.catalog.publisher.boto3.Session")
def test_upload_error_marks_file(
    mock_session_cls: MagicMock, _mock_normalize: MagicMock, tmp_path: Path
) -> None:
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "options", "schwab", date(2026, 6, 25), "SPX")

    mock_s3 = MagicMock()
    mock_s3.head_object.side_effect = Exception("Not found")
    mock_s3.upload_file.side_effect = Exception("S3 error")
    mock_session_cls.return_value.client.return_value = mock_s3

    publisher = Publisher(config)
    result = publisher.run(table_name="options")
    publisher.close()

    assert result.discovered == 1
    assert result.uploaded == 0
    assert result.errors == 1


@patch("tractatus.catalog.publisher.normalize_parquet", return_value=[])
@patch("tractatus.catalog.publisher.load_catalog")
@patch("tractatus.catalog.publisher.boto3.Session")
def test_commit_error_marks_files(
    mock_session_cls: MagicMock,
    mock_load_catalog: MagicMock,
    _mock_normalize: MagicMock,
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "options", "schwab", date(2026, 6, 25), "SPX")

    mock_s3 = MagicMock()
    mock_s3.head_object.side_effect = Exception("Not found")
    mock_session_cls.return_value.client.return_value = mock_s3

    mock_table = MagicMock()
    mock_table.add_files.side_effect = Exception("Iceberg error")
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    result = publisher.run(table_name="options")
    publisher.close()

    assert result.uploaded == 1
    assert result.committed == 0
    assert result.errors == 1


def test_unknown_table_raises(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    publisher = Publisher(config)
    try:
        with pytest.raises(ValueError, match="Unknown table"):
            publisher.run(table_name="nonexistent")
    finally:
        publisher.close()


@patch("tractatus.catalog.publisher.normalize_parquet", return_value=[])
@patch("tractatus.catalog.publisher.load_catalog")
@patch("tractatus.catalog.publisher.boto3.Session")
def test_bounded_publish_leaves_out_of_scope_rows(
    mock_session_cls: MagicMock,
    mock_load_catalog: MagicMock,
    _mock_normalize: MagicMock,
    tmp_path: Path,
) -> None:
    """A date-bounded publish must not upload or commit out-of-scope pending rows."""
    config = _make_config(tmp_path)
    # Create October and December files
    _create_parquet(config.data_root, "options", "schwab", date(2026, 10, 15), "SPX")
    _create_parquet(config.data_root, "options", "schwab", date(2026, 12, 15), "SPX")

    # First: discover all files (unbounded)
    publisher = Publisher(config)
    publisher.run(table_name="options", dry_run=True)
    publisher.close()

    # Verify both are pending
    ledger = Ledger(config.database_url)
    assert len(ledger.pending_upload("options")) == 2
    ledger.close()

    # Now run a December-bounded publish — should only touch December
    mock_s3 = MagicMock()
    mock_s3.head_object.side_effect = Exception("Not found")
    mock_session_cls.return_value.client.return_value = mock_s3

    mock_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    result = publisher.run(table_name="options", after=date(2026, 12, 1), before=date(2027, 1, 1))
    publisher.close()

    # December file discovered=0 (already in ledger), uploaded=1, committed=1
    assert result.uploaded == 1
    assert result.committed == 1

    # October row should still be pending (discovered, not uploaded)
    ledger = Ledger(config.database_url)
    oct_record = ledger.get("schwab/2026/10/15/SPX_samples_2026-10-15.parquet", "options")
    assert oct_record is not None
    assert oct_record.status == "discovered"
    ledger.close()


@patch("tractatus.catalog.publisher.normalize_parquet", return_value=[])
@patch("tractatus.catalog.publisher.load_catalog")
@patch("tractatus.catalog.publisher.boto3.Session")
def test_bounded_publish_provider_filter(
    mock_session_cls: MagicMock,
    mock_load_catalog: MagicMock,
    _mock_normalize: MagicMock,
    tmp_path: Path,
) -> None:
    """A provider-bounded publish must not upload out-of-scope provider rows."""
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "options", "schwab", date(2026, 10, 15), "SPX")
    _create_parquet(config.data_root, "options", "tradier", date(2026, 10, 15), "SPX")

    # Discover all files
    publisher = Publisher(config)
    publisher.run(table_name="options", dry_run=True)
    publisher.close()

    mock_s3 = MagicMock()
    mock_s3.head_object.side_effect = Exception("Not found")
    mock_session_cls.return_value.client.return_value = mock_s3

    mock_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    result = publisher.run(table_name="options", providers=["schwab"])
    publisher.close()

    assert result.uploaded == 1
    assert result.committed == 1

    # Tradier row should still be pending
    ledger = Ledger(config.database_url)
    tradier_record = ledger.get("tradier/2026/10/15/SPX_samples_2026-10-15.parquet", "options")
    assert tradier_record is not None
    assert tradier_record.status == "discovered"
    ledger.close()


@patch("tractatus.catalog.publisher.normalize_parquet", return_value=[])
@patch("tractatus.catalog.publisher.load_catalog")
@patch("tractatus.catalog.publisher.boto3.Session")
def test_commit_failure_cleans_up_s3(
    mock_session_cls: MagicMock,
    mock_load_catalog: MagicMock,
    _mock_normalize: MagicMock,
    tmp_path: Path,
) -> None:
    """On commit failure, orphaned S3 objects are deleted and records reset to discovered."""
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "options", "schwab", date(2026, 6, 25), "SPX")

    mock_s3 = MagicMock()
    mock_s3.head_object.side_effect = Exception("Not found")
    mock_session_cls.return_value.client.return_value = mock_s3

    mock_table = MagicMock()
    mock_table.add_files.side_effect = Exception("Iceberg error")
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    result = publisher.run(table_name="options")
    publisher.close()

    assert result.uploaded == 1
    assert result.committed == 0
    assert result.errors == 1

    # S3 delete should have been called for the orphaned object
    mock_s3.delete_object.assert_called_once()

    # Record should be reset to discovered (not error) for retry
    ledger = Ledger(config.database_url)
    record = ledger.get("schwab/2026/06/25/SPX_samples_2026-06-25.parquet", "options")
    assert record is not None
    assert record.status == "discovered"
    assert record.s3_key is None
    ledger.close()


@patch("tractatus.catalog.publisher.normalize_parquet", return_value=[])
@patch("tractatus.catalog.publisher.load_catalog")
@patch("tractatus.catalog.publisher.boto3.Session")
def test_commit_failure_partial_cleanup(
    mock_session_cls: MagicMock,
    mock_load_catalog: MagicMock,
    _mock_normalize: MagicMock,
    tmp_path: Path,
) -> None:
    """When S3 cleanup also fails, record stays in error with orphan info."""
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "options", "schwab", date(2026, 6, 25), "SPX")

    mock_s3 = MagicMock()
    mock_s3.head_object.side_effect = Exception("Not found")
    # delete_object fails — orphan cannot be cleaned
    mock_s3.delete_object.side_effect = Exception("Access denied")
    mock_session_cls.return_value.client.return_value = mock_s3

    mock_table = MagicMock()
    mock_table.add_files.side_effect = Exception("Iceberg error")
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    result = publisher.run(table_name="options")
    publisher.close()

    assert result.committed == 0
    assert result.errors == 1

    # Record should be in error state with orphan info
    ledger = Ledger(config.database_url)
    record = ledger.get("schwab/2026/06/25/SPX_samples_2026-06-25.parquet", "options")
    assert record is not None
    assert record.status == "error"
    assert "orphan cleanup failed" in record.error  # type: ignore[operator]
    ledger.close()


def test_resume_after_upload_failure(tmp_path: Path) -> None:
    """Files that failed upload can be retried on next run."""
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "options", "schwab", date(2026, 6, 25), "SPX")

    publisher = Publisher(config)
    publisher.run(table_name="options", dry_run=True)
    publisher.close()

    ledger = Ledger(config.database_url)
    pending = ledger.pending_upload("options")
    assert len(pending) == 1
    ledger.close()
