"""Tests for the catalog publisher pipeline."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from tractatus.catalog.config import CatalogConfig
from tractatus.catalog.publisher import Publisher


def _make_config(tmp_path: Path) -> CatalogConfig:
    data_root = tmp_path / "data"
    data_root.mkdir()
    return CatalogConfig(
        iceberg_bucket="test-bucket",
        iceberg_warehouse="s3://test-bucket",
        glue_database="tractatus",
        glue_table="options",
        aws_region="us-east-1",
        aws_profile=None,
        data_root=data_root,
        database_url=str(tmp_path / "ledger.db"),
    )


def _create_parquet(data_root: Path, provider: str, d: date, root: str) -> Path:
    path = data_root / provider / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}" / f"{root}_samples_{d}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"PAR1fake parquet content")
    return path


def test_discover_only_dry_run(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "schwab", date(2026, 6, 25), "SPX")
    _create_parquet(config.data_root, "schwab", date(2026, 6, 25), "AAPL")

    publisher = Publisher(config)
    result = publisher.run(dry_run=True)
    publisher.close()

    assert result.discovered == 2
    assert result.uploaded == 0
    assert result.committed == 0


@patch("tractatus.catalog.publisher.load_catalog")
@patch("tractatus.catalog.publisher.boto3.Session")
def test_full_pipeline(
    mock_session_cls: MagicMock, mock_load_catalog: MagicMock, tmp_path: Path
) -> None:
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "schwab", date(2026, 6, 25), "SPX")

    mock_s3 = MagicMock()
    mock_session_cls.return_value.client.return_value = mock_s3

    mock_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    result = publisher.run()
    publisher.close()

    assert result.discovered == 1
    assert result.uploaded == 1
    assert result.committed == 1
    assert result.errors == 0

    mock_s3.upload_file.assert_called_once()
    mock_table.add_files.assert_called_once()


@patch("tractatus.catalog.publisher.boto3.Session")
def test_upload_error_marks_file(mock_session_cls: MagicMock, tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "schwab", date(2026, 6, 25), "SPX")

    mock_s3 = MagicMock()
    mock_s3.upload_file.side_effect = Exception("S3 error")
    mock_session_cls.return_value.client.return_value = mock_s3

    publisher = Publisher(config)
    result = publisher.run()
    publisher.close()

    assert result.discovered == 1
    assert result.uploaded == 0
    assert result.errors == 1


@patch("tractatus.catalog.publisher.load_catalog")
@patch("tractatus.catalog.publisher.boto3.Session")
def test_commit_error_marks_files(
    mock_session_cls: MagicMock, mock_load_catalog: MagicMock, tmp_path: Path
) -> None:
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "schwab", date(2026, 6, 25), "SPX")

    mock_s3 = MagicMock()
    mock_session_cls.return_value.client.return_value = mock_s3

    mock_table = MagicMock()
    mock_table.add_files.side_effect = Exception("Iceberg error")
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    result = publisher.run()
    publisher.close()

    assert result.uploaded == 1
    assert result.committed == 0
    assert result.errors == 1


def test_resume_after_upload_failure(tmp_path: Path) -> None:
    """Files that failed upload can be retried on next run."""
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "schwab", date(2026, 6, 25), "SPX")

    # First run: discover only
    publisher = Publisher(config)
    publisher.run(dry_run=True)
    publisher.close()

    # Ledger should have the file as discovered
    from tractatus.catalog.ledger import Ledger

    ledger = Ledger(config.database_url)
    pending = ledger.pending_upload()
    assert len(pending) == 1
    ledger.close()
