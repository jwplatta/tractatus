"""Tests for the catalog publisher pipeline.

Note: Full pipeline tests (append, error handling, bounded publish, etc.) will be
rewritten in Phase 5 when the publisher is updated to handle raw CSV/NDJSON files.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from tractatus.catalog.config import CatalogConfig
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


def _create_options_csv(data_root: Path, provider: str, d: date, root: str) -> Path:
    """Write a minimal options CSV file."""
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


def test_unknown_table_raises(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    publisher = Publisher(config)
    try:
        with pytest.raises(ValueError, match="Unknown table"):
            publisher.run(table_name="nonexistent")
    finally:
        publisher.close()
