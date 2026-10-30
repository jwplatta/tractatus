"""Tests for the catalog publisher pipeline."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from tractatus.catalog.config import CatalogConfig
from tractatus.catalog.publisher import Publisher, _metadata_from_path
from tractatus.catalog.schemas import TABLE_DEFS
from tractatus.catalog.validate import QualityReport


def _make_config(tmp_path: Path, cache_bucket: str | None = None) -> CatalogConfig:
    data_root = tmp_path / "data"
    data_root.mkdir()
    return CatalogConfig(
        catalog_type="glue",
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


def _create_options_csv(
    data_root: Path, provider: str, d: date, root: str, expiry: str = "2026-07-18"
) -> Path:
    """Write a minimal options CSV file with enough columns to pass validation."""
    path = (
        data_root
        / "options"
        / provider
        / f"{d:%Y}"
        / f"{d:%m}"
        / f"{d:%d}"
        / f"{root}_exp{expiry}_{d}_14-30-00.csv"
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


@patch("tractatus.catalog.publisher._sort_table", side_effect=lambda t, k: t)
@patch("tractatus.catalog.publisher.load_catalog")
def test_append_csv_file(
    mock_load_catalog: MagicMock, _mock_sort: MagicMock, tmp_path: Path
) -> None:
    """CSV files should be parsed and grouped before normalizing."""
    config = _make_config(tmp_path)
    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "SPX")

    mock_iceberg_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_iceberg_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    publisher.run(table_name="options", dry_run=True)

    normalized = pa.table({"x": [1]})

    with (
        patch("tractatus.catalog.publisher.normalize_table", return_value=(normalized, [])),
        patch(
            "tractatus.catalog.publisher.validate_options",
            return_value=(normalized, QualityReport(1, 0, set())),
        ),
    ):
        result = publisher.run(table_name="options")

    publisher.close()

    mock_iceberg_table.append.assert_called_once()
    assert result.committed == 1


@patch("tractatus.catalog.publisher._sort_table", side_effect=lambda t, k: t)
@patch("tractatus.catalog.publisher.load_catalog")
def test_append_ndjson_file(
    mock_load_catalog: MagicMock, _mock_sort: MagicMock, tmp_path: Path
) -> None:
    """NDJSON files should be parsed and grouped before normalizing."""
    config = _make_config(tmp_path)
    _create_level_one_ndjson(config.data_root, "schwab", date(2026, 6, 25), "AAPL")

    mock_iceberg_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_iceberg_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    publisher.run(table_name="level_one_equities", dry_run=True)

    normalized = pa.table({"x": [1]})
    with patch("tractatus.catalog.publisher.normalize_table", return_value=(normalized, [])):
        result = publisher.run(table_name="level_one_equities")

    publisher.close()

    assert result.committed == 1


@patch("tractatus.catalog.publisher._sort_table", side_effect=lambda t, k: t)
@patch("tractatus.catalog.publisher.load_catalog")
def test_append_error_marks_ledger(
    mock_load_catalog: MagicMock, _mock_sort: MagicMock, tmp_path: Path
) -> None:
    """Files that fail normalization should be marked as errors."""
    config = _make_config(tmp_path)
    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "SPX")

    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = MagicMock()
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    publisher.run(table_name="options", dry_run=True)

    with patch("tractatus.catalog.publisher.normalize_table", side_effect=ValueError("bad data")):
        result = publisher.run(table_name="options")

    publisher.close()
    assert result.errors == 1
    assert result.committed == 0


@patch("tractatus.catalog.publisher._sort_table", side_effect=lambda t, k: t)
@patch("tractatus.catalog.publisher.load_catalog")
def test_append_with_limit(
    mock_load_catalog: MagicMock, _mock_sort: MagicMock, tmp_path: Path
) -> None:
    """Limit should cap the number of groups (tickers) appended, not individual files."""
    config = _make_config(tmp_path)
    # Two CSVs for SPX (same group), one for AAPL (second group)
    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "SPX", expiry="2026-07-18")
    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "SPX", expiry="2026-07-25")
    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "AAPL")

    mock_iceberg_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_iceberg_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    publisher.run(table_name="options", dry_run=True)

    normalized = pa.table({"x": [1]})

    with (
        patch("tractatus.catalog.publisher.normalize_table", return_value=(normalized, [])),
        patch(
            "tractatus.catalog.publisher.validate_options",
            return_value=(normalized, QualityReport(1, 0, set())),
        ),
    ):
        result = publisher.run(table_name="options", limit=1)

    publisher.close()
    # limit=1 means 1 group (SPX with its 2 CSVs) — AAPL should not be committed
    assert result.committed == 1
    mock_iceberg_table.append.assert_called_once()


@patch("tractatus.catalog.publisher._sort_table", side_effect=lambda t, k: t)
@patch("tractatus.catalog.publisher.load_catalog")
def test_append_groups_all_csvs_for_ticker(
    mock_load_catalog: MagicMock, _mock_sort: MagicMock, tmp_path: Path
) -> None:
    """All CSVs for the same (provider, root, sample_date) must be committed as one group."""
    config = _make_config(tmp_path)
    # Three CSVs for SPX — different expiries, same sample date → one group
    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "SPX", expiry="2026-07-18")
    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "SPX", expiry="2026-07-25")
    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "SPX", expiry="2026-08-01")

    mock_iceberg_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_iceberg_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    publisher.run(table_name="options", dry_run=True)

    normalized = pa.table({"x": [1]})

    with (
        patch("tractatus.catalog.publisher.normalize_table", return_value=(normalized, [])),
        patch(
            "tractatus.catalog.publisher.validate_options",
            return_value=(normalized, QualityReport(1, 0, set())),
        ),
    ):
        result = publisher.run(table_name="options")

    publisher.close()
    # 3 CSVs → 1 group → 1 Iceberg append
    assert result.committed == 1
    mock_iceberg_table.append.assert_called_once()


@patch("tractatus.catalog.publisher.load_catalog")
def test_append_sorts_before_iceberg(mock_load_catalog: MagicMock, tmp_path: Path) -> None:
    """Normalized table should be sorted by table_def.sort_keys before append."""
    config = _make_config(tmp_path)
    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "SPX")

    mock_iceberg_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_iceberg_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    publisher.run(table_name="options", dry_run=True)

    # Table with a sort key column in descending order
    normalized = pa.table({"x": [3, 1, 2]})

    with (
        patch("tractatus.catalog.publisher.normalize_table", return_value=(normalized, [])),
        patch(
            "tractatus.catalog.publisher.validate_options",
            return_value=(normalized, QualityReport(1, 0, set())),
        ),
        patch("tractatus.catalog.publisher._sort_table", wraps=lambda t, k: t) as mock_sort,
    ):
        publisher.run(table_name="options")

    publisher.close()
    mock_sort.assert_called_once()
    # sort_keys should come from TABLE_DEFS["options"].sort_keys
    assert mock_sort.call_args[0][1] == TABLE_DEFS["options"].sort_keys


# --- Cache tests ---


@patch("tractatus.catalog.publisher._sort_table", side_effect=lambda t, k: t)
@patch("tractatus.catalog.publisher.load_catalog")
def test_cache_to_minio(
    mock_load_catalog: MagicMock, _mock_sort: MagicMock, tmp_path: Path
) -> None:
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
        patch("tractatus.catalog.publisher.normalize_table", return_value=(normalized, [])),
        patch(
            "tractatus.catalog.publisher.validate_options",
            return_value=(normalized, QualityReport(1, 0, set())),
        ),
    ):
        result = publisher.run(table_name="options", cache=True)

    publisher.close()

    assert result.cached == 1
    mock_cache_client.put_object.assert_called_once()
    call_kwargs = mock_cache_client.put_object.call_args
    assert call_kwargs[1]["Bucket"] == "test-cache"
    assert call_kwargs[1]["Key"].startswith("options/")
    assert call_kwargs[1]["Key"].endswith(".parquet")


@patch("tractatus.catalog.publisher._sort_table", side_effect=lambda t, k: t)
@patch("tractatus.catalog.publisher.load_catalog")
def test_no_cache_without_config(
    mock_load_catalog: MagicMock, _mock_sort: MagicMock, tmp_path: Path
) -> None:
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

    with (
        patch("tractatus.catalog.publisher.normalize_table", return_value=(normalized, [])),
        patch(
            "tractatus.catalog.publisher.validate_options",
            return_value=(normalized, QualityReport(1, 0, set())),
        ),
    ):
        result = publisher.run(table_name="options")

    publisher.close()
    assert result.cached == 0
    assert result.committed == 1


# --- cache_artifacts tests ---


@patch("tractatus.catalog.publisher.load_catalog")
def test_cache_artifacts_writes_to_minio_and_upserts_ledger(
    mock_load_catalog: MagicMock, tmp_path: Path
) -> None:
    """cache_artifacts should write parquet to MinIO and upsert published_artifacts."""
    config = _make_config(tmp_path, cache_bucket="test-cache")
    publisher = Publisher(config)

    normalized = pa.table(
        {
            "provider": pa.array(["schwab", "schwab"], type=pa.string()),
            "option_root": pa.array(["SPX", "SPX"], type=pa.string()),
            "sampled_at": pa.array(
                [datetime(2026, 6, 25, 14, 30, tzinfo=UTC)] * 2,
                type=pa.timestamp("us", tz="UTC"),
            ),
        }
    )

    mock_iceberg_table = MagicMock()
    mock_scan = MagicMock()
    mock_scan.to_arrow.return_value = normalized
    mock_iceberg_table.scan.return_value = mock_scan
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_iceberg_table
    mock_load_catalog.return_value = mock_catalog

    mock_cache_client = MagicMock()
    publisher._cache_client = mock_cache_client

    cached = publisher.cache_artifacts(
        table_name="options",
        after=date(2026, 6, 25),
        before=date(2026, 6, 26),
    )
    publisher.close()

    assert cached == 1
    mock_cache_client.put_object.assert_called_once()
    call_kwargs = mock_cache_client.put_object.call_args[1]
    assert call_kwargs["Bucket"] == "test-cache"
    assert call_kwargs["Key"] == "options/schwab/2026/06/25/SPX_samples_2026-06-25.parquet"

    # Verify published_artifacts was upserted
    from tractatus.catalog.ledger import Ledger  # noqa: PLC0415

    ledger = Ledger(str(tmp_path / "ledger.db"))
    artifact = ledger.get_published("options/schwab/2026/06/25/SPX_samples_2026-06-25.parquet")
    ledger.close()
    assert artifact is not None
    assert artifact.cached is True
    assert artifact.row_count == 2


@patch("tractatus.catalog.publisher.load_catalog")
def test_cache_artifacts_failure_on_one_does_not_stop_others(
    mock_load_catalog: MagicMock, tmp_path: Path
) -> None:
    """A failure writing one artifact should not stop others from being cached."""
    config = _make_config(tmp_path, cache_bucket="test-cache")
    publisher = Publisher(config)

    normalized = pa.table(
        {
            "provider": pa.array(["schwab", "schwab", "schwab", "schwab"], type=pa.string()),
            "option_root": pa.array(["SPX", "SPX", "AAPL", "AAPL"], type=pa.string()),
            "sampled_at": pa.array(
                [datetime(2026, 6, 25, 14, 30, tzinfo=UTC)] * 4,
                type=pa.timestamp("us", tz="UTC"),
            ),
        }
    )

    mock_iceberg_table = MagicMock()
    mock_scan = MagicMock()
    mock_scan.to_arrow.return_value = normalized
    mock_iceberg_table.scan.return_value = mock_scan
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_iceberg_table
    mock_load_catalog.return_value = mock_catalog

    call_count = 0

    def _put_object_side_effect(**kwargs: object) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("MinIO connection error")

    mock_cache_client = MagicMock()
    mock_cache_client.put_object.side_effect = _put_object_side_effect
    publisher._cache_client = mock_cache_client

    cached = publisher.cache_artifacts(
        table_name="options",
        after=date(2026, 6, 25),
        before=date(2026, 6, 26),
    )
    publisher.close()

    # 2 groups total; one fails, one succeeds
    assert cached == 1
    assert mock_cache_client.put_object.call_count == 2


# --- Metadata extraction tests ---


def test_metadata_from_path_level_one() -> None:
    path = "schwab/2026/06/25/AAPL.ndjson"
    sample_dt, symbol, interval = _metadata_from_path(path, "level_one_equities")
    assert sample_dt == date(2026, 6, 25)
    assert symbol is None
    assert interval is None


def test_metadata_from_path_candles() -> None:
    path = "schwab/2026/10/18/5min/AAPL.csv"
    sample_dt, symbol, interval = _metadata_from_path(path, "candles")
    assert symbol == "AAPL"
    assert interval == "5min"
    assert sample_dt == date(2026, 10, 18)


def test_metadata_from_path_options() -> None:
    sample_dt, symbol, interval = _metadata_from_path("schwab/foo.csv", "options")
    assert sample_dt is None


# --- Table definition tests ---


def test_all_tables_have_sort_keys() -> None:
    for table_name, table_def in TABLE_DEFS.items():
        assert table_def.sort_keys, f"Missing sort_keys for {table_name}"


def test_all_tables_have_write_properties() -> None:
    for table_name, table_def in TABLE_DEFS.items():
        assert "write.parquet.compression-codec" in table_def.properties, (
            f"Missing compression-codec for {table_name}"
        )
        assert "write.parquet.row-group-limit" in table_def.properties, (
            f"Missing row-group-limit for {table_name}"
        )


# --- Telemetry / byte measurement tests ---


def test_cache_to_minio_returns_bytes(tmp_path: Path) -> None:
    """_cache_to_minio should return (cache_key, byte_count) when client is configured."""
    config = _make_config(tmp_path, cache_bucket="test-cache")
    publisher = Publisher(config)
    publisher._cache_client = MagicMock()

    table = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
    cache_key, cache_bytes = publisher._cache_to_minio(
        table, "options", "schwab/2026/06/25/SPX_exp2026-07-18_2026-06-25_14-30-00.csv"
    )
    publisher.close()

    assert cache_key is not None and cache_key.endswith(".parquet")
    assert isinstance(cache_bytes, int) and cache_bytes > 0


def test_cache_to_minio_returns_none_none_without_client(tmp_path: Path) -> None:
    """_cache_to_minio should return (None, None) when no cache client is configured."""
    config = _make_config(tmp_path)  # no cache_bucket
    publisher = Publisher(config)

    table = pa.table({"x": pa.array([1], type=pa.int64())})
    cache_key, cache_bytes = publisher._cache_to_minio(table, "options", "some/path.csv")
    publisher.close()

    assert cache_key is None
    assert cache_bytes is None


@patch("tractatus.catalog.publisher._sort_table", side_effect=lambda t, k: t)
@patch("tractatus.catalog.publisher.load_catalog")
def test_group_committed_log_contains_all_byte_fields(
    mock_load_catalog: MagicMock, _mock_sort: MagicMock, tmp_path: Path
) -> None:
    """group_committed event should include group_id, input_bytes, iceberg_bytes, cache_bytes."""
    config = _make_config(tmp_path, cache_bucket="test-cache")
    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "SPX")

    mock_snap = MagicMock()
    mock_snap.summary = {"added-files-size": "12345"}
    mock_iceberg_table = MagicMock()
    mock_iceberg_table.current_snapshot.return_value = mock_snap
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_iceberg_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    publisher.run(table_name="options", dry_run=True)

    normalized = pa.table({"x": pa.array([1], type=pa.int64())})
    publisher._cache_client = MagicMock()

    logged_events: list[dict] = []

    with (
        patch("tractatus.catalog.publisher.normalize_table", return_value=(normalized, [])),
        patch(
            "tractatus.catalog.publisher.validate_options",
            return_value=(normalized, QualityReport(1, 0, set())),
        ),
        patch("tractatus.catalog.publisher.logger") as mock_logger,
    ):
        publisher.run(table_name="options", cache=True)
        for call in mock_logger.info.call_args_list:
            arg = call[0][0]
            if isinstance(arg, dict) and arg.get("event") == "group_committed":
                logged_events.append(arg)

    publisher.close()

    assert len(logged_events) == 1
    ev = logged_events[0]
    assert ev["group_id"] == "options/schwab/SPX/2026-06-25"
    assert isinstance(ev["input_bytes"], int) and ev["input_bytes"] > 0
    assert ev["iceberg_bytes"] == 12345
    assert isinstance(ev["cache_bytes"], int) and ev["cache_bytes"] > 0
    assert ev["cached"] is True


@patch("tractatus.catalog.publisher._sort_table", side_effect=lambda t, k: t)
@patch("tractatus.catalog.publisher.load_catalog")
def test_group_committed_iceberg_bytes_none_when_snapshot_unavailable(
    mock_load_catalog: MagicMock, _mock_sort: MagicMock, tmp_path: Path
) -> None:
    """iceberg_bytes should be None when the snapshot is not available after append."""
    config = _make_config(tmp_path)
    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "SPX")

    mock_iceberg_table = MagicMock()
    mock_iceberg_table.current_snapshot.return_value = None
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_iceberg_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    publisher.run(table_name="options", dry_run=True)

    normalized = pa.table({"x": pa.array([1], type=pa.int64())})

    logged_events: list[dict] = []

    with (
        patch("tractatus.catalog.publisher.normalize_table", return_value=(normalized, [])),
        patch(
            "tractatus.catalog.publisher.validate_options",
            return_value=(normalized, QualityReport(1, 0, set())),
        ),
        patch("tractatus.catalog.publisher.logger") as mock_logger,
    ):
        publisher.run(table_name="options")
        for call in mock_logger.info.call_args_list:
            arg = call[0][0]
            if isinstance(arg, dict) and arg.get("event") == "group_committed":
                logged_events.append(arg)

    publisher.close()

    assert len(logged_events) == 1
    assert logged_events[0]["iceberg_bytes"] is None


@patch("tractatus.catalog.publisher._sort_table", side_effect=lambda t, k: t)
@patch("tractatus.catalog.publisher.load_catalog")
def test_group_committed_cache_bytes_none_when_no_cache(
    mock_load_catalog: MagicMock, _mock_sort: MagicMock, tmp_path: Path
) -> None:
    """cache_bytes should be None when caching is not requested."""
    config = _make_config(tmp_path)  # no cache_bucket
    _create_options_csv(config.data_root, "schwab", date(2026, 6, 25), "SPX")

    mock_iceberg_table = MagicMock()
    mock_catalog = MagicMock()
    mock_catalog.load_table.return_value = mock_iceberg_table
    mock_load_catalog.return_value = mock_catalog

    publisher = Publisher(config)
    publisher.run(table_name="options", dry_run=True)

    normalized = pa.table({"x": pa.array([1], type=pa.int64())})

    logged_events: list[dict] = []

    with (
        patch("tractatus.catalog.publisher.normalize_table", return_value=(normalized, [])),
        patch(
            "tractatus.catalog.publisher.validate_options",
            return_value=(normalized, QualityReport(1, 0, set())),
        ),
        patch("tractatus.catalog.publisher.logger") as mock_logger,
    ):
        publisher.run(table_name="options")
        for call in mock_logger.info.call_args_list:
            arg = call[0][0]
            if isinstance(arg, dict) and arg.get("event") == "group_committed":
                logged_events.append(arg)

    publisher.close()

    assert len(logged_events) == 1
    ev = logged_events[0]
    assert ev["cache_bytes"] is None
    assert ev["cached"] is False
