"""Tests for the catalog publication ledger (SQLite backend)."""

from __future__ import annotations

from pathlib import Path

import pytest

from tractatus.catalog.ledger import Ledger


@pytest.fixture()
def ledger(tmp_path: Path) -> Ledger:  # type: ignore[misc]
    led = Ledger(str(tmp_path / "ledger.db"))
    yield led  # type: ignore[misc]
    led.close()


def test_record_and_retrieve(ledger: Ledger) -> None:
    ledger.record_discovered(
        "schwab/2026/06/25/SPX_exp2026-07-18_2026-06-25_14-30-00.csv",
        table_name="options",
        size_bytes=1024,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-06-25",
        file_format="csv",
    )

    record = ledger.get("schwab/2026/06/25/SPX_exp2026-07-18_2026-06-25_14-30-00.csv", "options")
    assert record is not None
    assert record.status == "discovered"
    assert record.table_name == "options"
    assert record.size_bytes == 1024
    assert record.provider == "schwab"
    assert record.symbol == "SPX"
    assert record.sample_date == "2026-06-25"
    assert record.file_format == "csv"


def test_idempotent_rediscover(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-01-01",
        file_format="csv",
    )
    ledger.mark_committed(["a.csv"], "options")

    # Re-discover — should not reset status
    ledger.record_discovered(
        "a.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-01-01",
        file_format="csv",
    )

    record = ledger.get("a.csv", "options")
    assert record is not None
    assert record.status == "committed"


def test_status_transitions(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-01-01",
        file_format="csv",
    )
    assert ledger.get("a.csv", "options").status == "discovered"  # type: ignore[union-attr]

    ledger.mark_committed(["a.csv"], "options")
    assert ledger.get("a.csv", "options").status == "committed"  # type: ignore[union-attr]


def test_mark_error(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-01-01",
        file_format="csv",
    )
    ledger.mark_error("a.csv", "options", "append failed")

    record = ledger.get("a.csv", "options")
    assert record is not None
    assert record.status == "error"
    assert record.error == "append failed"


def test_pending(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-01-01",
        file_format="csv",
    )
    ledger.record_discovered(
        "b.csv",
        table_name="options",
        size_bytes=200,
        provider="schwab",
        symbol="AAPL",
        sample_date="2026-01-02",
        file_format="csv",
    )
    ledger.mark_committed(["b.csv"], "options")

    assert len(ledger.pending("options")) == 1
    assert ledger.pending("options")[0].path == "a.csv"


def test_summary(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-01-01",
        file_format="csv",
    )
    ledger.record_discovered(
        "b.csv",
        table_name="options",
        size_bytes=200,
        provider="schwab",
        symbol="AAPL",
        sample_date="2026-01-02",
        file_format="csv",
    )
    ledger.mark_committed(["b.csv"], "options")

    summary = ledger.summary("options")
    assert summary == {"discovered": 1, "committed": 1}


def test_summary_all_tables(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-01-01",
        file_format="csv",
    )
    ledger.record_discovered(
        "b.ndjson",
        table_name="level_one_equities",
        size_bytes=200,
        provider="schwab",
        symbol="SPY",
        sample_date="2026-01-02",
        file_format="ndjson",
    )

    summary = ledger.summary()
    assert summary == {"discovered": 2}


def test_tables_are_isolated(ledger: Ledger) -> None:
    """Same path in different tables should be independent records."""
    ledger.record_discovered(
        "a.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-01-01",
        file_format="csv",
    )
    ledger.record_discovered(
        "a.csv",
        table_name="level_one_equities",
        size_bytes=200,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-01-01",
        file_format="ndjson",
    )

    assert ledger.get("a.csv", "options").size_bytes == 100  # type: ignore[union-attr]
    assert ledger.get("a.csv", "level_one_equities").size_bytes == 200  # type: ignore[union-attr]


def test_mark_committed_empty(ledger: Ledger) -> None:
    ledger.mark_committed([], "options")  # should not raise


def test_delete_all(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-01-01",
        file_format="csv",
    )
    ledger.record_discovered(
        "b.ndjson",
        table_name="level_one_equities",
        size_bytes=200,
        provider="schwab",
        symbol="SPY",
        sample_date="2026-01-02",
        file_format="ndjson",
    )

    count = ledger.delete_all("options")
    assert count == 1
    assert ledger.get("a.csv", "options") is None
    assert ledger.get("b.ndjson", "level_one_equities") is not None


def test_delete_filtered_by_date(ledger: Ledger) -> None:
    for path, sd in [("a.csv", "2026-01-15"), ("b.csv", "2026-02-15"), ("c.csv", "2026-03-15")]:
        ledger.record_discovered(
            path,
            table_name="options",
            size_bytes=100,
            provider="schwab",
            symbol="SPX",
            sample_date=sd,
            file_format="csv",
        )

    count = ledger.delete_filtered("options", after="2026-02-01", before="2026-03-01")
    assert count == 1
    assert ledger.get("a.csv", "options") is not None
    assert ledger.get("b.csv", "options") is None
    assert ledger.get("c.csv", "options") is not None


def test_delete_filtered_by_provider(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-01-01",
        file_format="csv",
    )
    ledger.record_discovered(
        "b.csv",
        table_name="options",
        size_bytes=200,
        provider="tradier",
        symbol="SPX",
        sample_date="2026-01-01",
        file_format="csv",
    )

    count = ledger.delete_filtered("options", provider="schwab")
    assert count == 1
    assert ledger.get("a.csv", "options") is None
    assert ledger.get("b.csv", "options") is not None


def test_pending_filtered_by_date(ledger: Ledger) -> None:
    ledger.record_discovered(
        "oct.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-10-15",
        file_format="csv",
    )
    ledger.record_discovered(
        "dec.csv",
        table_name="options",
        size_bytes=200,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-12-15",
        file_format="csv",
    )

    result = ledger.pending("options", after="2026-10-01", before="2026-11-01")
    assert [r.path for r in result] == ["oct.csv"]


def test_pending_filtered_by_provider(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-10-15",
        file_format="csv",
    )
    ledger.record_discovered(
        "b.csv",
        table_name="options",
        size_bytes=200,
        provider="tradier",
        symbol="SPX",
        sample_date="2026-10-15",
        file_format="csv",
    )

    result = ledger.pending("options", providers=["schwab"])
    assert [r.path for r in result] == ["a.csv"]


def test_pending_no_filters_returns_all(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-10-15",
        file_format="csv",
    )
    ledger.record_discovered(
        "b.csv",
        table_name="options",
        size_bytes=200,
        provider="tradier",
        symbol="SPX",
        sample_date="2026-12-15",
        file_format="csv",
    )

    result = ledger.pending("options")
    assert len(result) == 2


def test_get_s3_keys_filtered(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2025-12-15",
        file_format="csv",
    )
    ledger.record_discovered(
        "b.csv",
        table_name="options",
        size_bytes=200,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-01-15",
        file_format="csv",
    )
    # Manually set s3_keys via mark_committed (s3_key set elsewhere in real flow)
    ledger._execute(
        "UPDATE catalog_files SET s3_key = %s WHERE path = %s AND table_name = %s",
        ("s3/a", "a.csv", "options"),
    )
    ledger._execute(
        "UPDATE catalog_files SET s3_key = %s WHERE path = %s AND table_name = %s",
        ("s3/b", "b.csv", "options"),
    )

    keys = ledger.get_s3_keys("options", after="2025-12-01", before="2026-01-01")
    assert keys == ["s3/a"]

    all_keys = ledger.get_s3_keys("options")
    assert sorted(all_keys) == ["s3/a", "s3/b"]


# --- Cache management tests ---


def test_mark_cached(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-01-01",
        file_format="csv",
    )
    ledger.mark_cached("a.csv", "options", "cache/options/a.parquet")

    record = ledger.get("a.csv", "options")
    assert record is not None
    assert record.cache_key == "cache/options/a.parquet"
    assert record.cached_at is not None
    assert record.last_accessed_at is not None


def test_mark_cache_accessed(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-01-01",
        file_format="csv",
    )
    ledger.mark_cached("a.csv", "options", "cache/a.parquet")

    original = ledger.get("a.csv", "options")
    assert original is not None
    original_accessed = original.last_accessed_at

    ledger.mark_cache_accessed("a.csv", "options")
    updated = ledger.get("a.csv", "options")
    assert updated is not None
    assert updated.last_accessed_at >= original_accessed  # type: ignore[operator]


def test_clear_cache_entry(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-01-01",
        file_format="csv",
    )
    ledger.mark_cached("a.csv", "options", "cache/a.parquet")
    ledger.clear_cache_entry("a.csv", "options")

    record = ledger.get("a.csv", "options")
    assert record is not None
    assert record.cache_key is None
    assert record.cached_at is None
    assert record.last_accessed_at is None


def test_cached_files(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-01-01",
        file_format="csv",
    )
    ledger.record_discovered(
        "b.csv",
        table_name="options",
        size_bytes=200,
        provider="schwab",
        symbol="AAPL",
        sample_date="2026-01-01",
        file_format="csv",
    )
    ledger.mark_cached("a.csv", "options", "cache/a.parquet")

    cached = ledger.cached_files(table_name="options")
    assert len(cached) == 1
    assert cached[0].path == "a.csv"


def test_cache_summary(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.csv",
        table_name="options",
        size_bytes=100,
        provider="schwab",
        symbol="SPX",
        sample_date="2026-01-01",
        file_format="csv",
    )
    ledger.record_discovered(
        "b.csv",
        table_name="options",
        size_bytes=200,
        provider="schwab",
        symbol="AAPL",
        sample_date="2026-01-01",
        file_format="csv",
    )
    ledger.mark_cached("a.csv", "options", "cache/a.parquet")
    ledger.mark_cached("b.csv", "options", "cache/b.parquet")

    summary = ledger.cache_summary("options")
    assert summary == {"cached_files": 2, "cached_bytes": 300}


def test_cache_summary_empty(ledger: Ledger) -> None:
    summary = ledger.cache_summary("options")
    assert summary == {"cached_files": 0, "cached_bytes": 0}
