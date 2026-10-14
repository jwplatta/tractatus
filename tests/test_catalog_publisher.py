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
        iceberg_warehouse="s3://test-bucket/warehouse",
        glue_database="tractatus",
        glue_table="options",
        aws_region="us-east-1",
        aws_profile=None,
        data_root=data_root,
        database_url="postgresql://test:test@localhost:5432/test",
    )


def _create_parquet(data_root: Path, provider: str, d: date, root: str) -> Path:
    path = data_root / provider / f"{d:%Y}" / f"{d:%m}" / f"{d:%d}" / f"{root}_samples_{d}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"PAR1fake parquet content")
    return path


@patch("tractatus.catalog.publisher.Ledger")
def test_discover_only_dry_run(mock_ledger_cls: MagicMock, tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "schwab", date(2026, 6, 25), "SPX")
    _create_parquet(config.data_root, "schwab", date(2026, 6, 25), "AAPL")

    mock_ledger = MagicMock()
    mock_ledger_cls.return_value = mock_ledger

    publisher = Publisher(config)
    result = publisher.run(dry_run=True)
    publisher.close()

    assert result.discovered == 2
    assert result.uploaded == 0
    assert result.committed == 0
    assert mock_ledger.record_discovered.call_count == 2


@patch("tractatus.catalog.publisher.load_catalog")
@patch("tractatus.catalog.publisher.boto3.Session")
@patch("tractatus.catalog.publisher.Ledger")
def test_full_pipeline(
    mock_ledger_cls: MagicMock,
    mock_session_cls: MagicMock,
    mock_load_catalog: MagicMock,
    tmp_path: Path,
) -> None:
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "schwab", date(2026, 6, 25), "SPX")

    mock_ledger = MagicMock()
    mock_ledger_cls.return_value = mock_ledger

    # After discover, pending_upload returns one record
    from tractatus.catalog.ledger import FileRecord

    record = FileRecord(
        path="schwab/2026/06/25/SPX_samples_2026-06-25.parquet",
        sha256="abc",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-06-25",
        s3_key=None,
        status="discovered",
        error=None,
        discovered_at="2026-06-25T00:00:00",
        uploaded_at=None,
        committed_at=None,
        updated_at="2026-06-25T00:00:00",
    )
    uploaded_record = FileRecord(
        path=record.path,
        sha256=record.sha256,
        size_bytes=record.size_bytes,
        provider=record.provider,
        root=record.root,
        sample_date=record.sample_date,
        s3_key="warehouse/tractatus/options/data/schwab/2026/06/25/SPX_samples_2026-06-25.parquet",
        status="uploaded",
        error=None,
        discovered_at=record.discovered_at,
        uploaded_at="2026-06-25T00:00:00",
        committed_at=None,
        updated_at="2026-06-25T00:00:00",
    )
    mock_ledger.pending_upload.return_value = [record]
    mock_ledger.pending_commit.return_value = [uploaded_record]

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
@patch("tractatus.catalog.publisher.Ledger")
def test_upload_error_marks_file(
    mock_ledger_cls: MagicMock, mock_session_cls: MagicMock, tmp_path: Path
) -> None:
    config = _make_config(tmp_path)
    _create_parquet(config.data_root, "schwab", date(2026, 6, 25), "SPX")

    mock_ledger = MagicMock()
    mock_ledger_cls.return_value = mock_ledger

    from tractatus.catalog.ledger import FileRecord

    record = FileRecord(
        path="schwab/2026/06/25/SPX_samples_2026-06-25.parquet",
        sha256="abc",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-06-25",
        s3_key=None,
        status="discovered",
        error=None,
        discovered_at="2026-06-25T00:00:00",
        uploaded_at=None,
        committed_at=None,
        updated_at="2026-06-25T00:00:00",
    )
    mock_ledger.pending_upload.return_value = [record]
    mock_ledger.pending_commit.return_value = []

    mock_s3 = MagicMock()
    mock_s3.upload_file.side_effect = Exception("S3 error")
    mock_session_cls.return_value.client.return_value = mock_s3

    publisher = Publisher(config)
    result = publisher.run()
    publisher.close()

    assert result.discovered == 1
    assert result.uploaded == 0
    assert result.errors == 1
    mock_ledger.mark_error.assert_called_once()
