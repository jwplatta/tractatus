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
        "schwab/2026/06/25/SPX_samples_2026-06-25.parquet",
        table_name="options",
        sha256="abc123",
        size_bytes=1024,
        provider="schwab",
        root="SPX",
        sample_date="2026-06-25",
    )

    record = ledger.get("schwab/2026/06/25/SPX_samples_2026-06-25.parquet", "options")
    assert record is not None
    assert record.status == "discovered"
    assert record.table_name == "options"
    assert record.sha256 == "abc123"
    assert record.size_bytes == 1024
    assert record.provider == "schwab"
    assert record.root == "SPX"
    assert record.sample_date == "2026-06-25"


def test_idempotent_same_hash(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        table_name="options",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )
    ledger.mark_uploaded("a.parquet", "options", "s3/key")

    # Re-discover with same hash — should not reset status
    ledger.record_discovered(
        "a.parquet",
        table_name="options",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )

    record = ledger.get("a.parquet", "options")
    assert record is not None
    assert record.status == "uploaded"
    assert record.s3_key == "s3/key"


def test_hash_change_resets_status(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        table_name="options",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )
    ledger.mark_uploaded("a.parquet", "options", "s3/key")

    # Re-discover with different hash — should reset to discovered
    ledger.record_discovered(
        "a.parquet",
        table_name="options",
        sha256="bbb",
        size_bytes=200,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )

    record = ledger.get("a.parquet", "options")
    assert record is not None
    assert record.status == "discovered"
    assert record.s3_key is None
    assert record.uploaded_at is None


def test_status_transitions(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        table_name="options",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )

    assert ledger.get("a.parquet", "options") is not None
    assert ledger.get("a.parquet", "options").status == "discovered"  # type: ignore[union-attr]

    ledger.mark_uploaded("a.parquet", "options", "s3/key")
    assert ledger.get("a.parquet", "options").status == "uploaded"  # type: ignore[union-attr]

    ledger.mark_committed(["a.parquet"], "options")
    assert ledger.get("a.parquet", "options").status == "committed"  # type: ignore[union-attr]


def test_mark_error(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        table_name="options",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )
    ledger.mark_error("a.parquet", "options", "upload failed")

    record = ledger.get("a.parquet", "options")
    assert record is not None
    assert record.status == "error"
    assert record.error == "upload failed"


def test_pending_queries(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        table_name="options",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )
    ledger.record_discovered(
        "b.parquet",
        table_name="options",
        sha256="bbb",
        size_bytes=200,
        provider="schwab",
        root="AAPL",
        sample_date="2026-01-02",
    )
    ledger.mark_uploaded("b.parquet", "options", "s3/b")

    assert len(ledger.pending_upload("options")) == 1
    assert ledger.pending_upload("options")[0].path == "a.parquet"
    assert len(ledger.pending_commit("options")) == 1
    assert ledger.pending_commit("options")[0].path == "b.parquet"


def test_summary(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        table_name="options",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )
    ledger.record_discovered(
        "b.parquet",
        table_name="options",
        sha256="bbb",
        size_bytes=200,
        provider="schwab",
        root="AAPL",
        sample_date="2026-01-02",
    )
    ledger.mark_uploaded("b.parquet", "options", "s3/b")

    summary = ledger.summary("options")
    assert summary == {"discovered": 1, "uploaded": 1}


def test_summary_all_tables(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        table_name="options",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )
    ledger.record_discovered(
        "b.parquet",
        table_name="level_one",
        sha256="bbb",
        size_bytes=200,
        provider="schwab",
        root="SPY",
        sample_date="2026-01-02",
    )

    summary = ledger.summary()
    assert summary == {"discovered": 2}


def test_tables_are_isolated(ledger: Ledger) -> None:
    """Same path in different tables should be independent records."""
    ledger.record_discovered(
        "a.parquet",
        table_name="options",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )
    ledger.record_discovered(
        "a.parquet",
        table_name="level_one",
        sha256="bbb",
        size_bytes=200,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )

    assert ledger.get("a.parquet", "options") is not None
    assert ledger.get("a.parquet", "level_one") is not None
    assert ledger.get("a.parquet", "options").sha256 == "aaa"  # type: ignore[union-attr]
    assert ledger.get("a.parquet", "level_one").sha256 == "bbb"  # type: ignore[union-attr]


def test_mark_committed_empty(ledger: Ledger) -> None:
    ledger.mark_committed([], "options")  # should not raise


def test_delete_all(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        table_name="options",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )
    ledger.record_discovered(
        "b.parquet",
        table_name="level_one",
        sha256="bbb",
        size_bytes=200,
        provider="schwab",
        root="SPY",
        sample_date="2026-01-02",
    )

    count = ledger.delete_all("options")
    assert count == 1
    assert ledger.get("a.parquet", "options") is None
    assert ledger.get("b.parquet", "level_one") is not None


def test_delete_filtered_by_date(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        table_name="options",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-15",
    )
    ledger.record_discovered(
        "b.parquet",
        table_name="options",
        sha256="bbb",
        size_bytes=200,
        provider="schwab",
        root="SPX",
        sample_date="2026-02-15",
    )
    ledger.record_discovered(
        "c.parquet",
        table_name="options",
        sha256="ccc",
        size_bytes=300,
        provider="schwab",
        root="SPX",
        sample_date="2026-03-15",
    )

    count = ledger.delete_filtered("options", after="2026-02-01", before="2026-03-01")
    assert count == 1
    assert ledger.get("a.parquet", "options") is not None
    assert ledger.get("b.parquet", "options") is None
    assert ledger.get("c.parquet", "options") is not None


def test_delete_filtered_by_provider(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        table_name="options",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )
    ledger.record_discovered(
        "b.parquet",
        table_name="options",
        sha256="bbb",
        size_bytes=200,
        provider="tradier",
        root="SPX",
        sample_date="2026-01-01",
    )

    count = ledger.delete_filtered("options", provider="schwab")
    assert count == 1
    assert ledger.get("a.parquet", "options") is None
    assert ledger.get("b.parquet", "options") is not None


def test_delete_filtered_combined(ledger: Ledger) -> None:
    """Combine date and provider filters."""
    ledger.record_discovered(
        "a.parquet",
        table_name="options",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2025-12-15",
    )
    ledger.record_discovered(
        "b.parquet",
        table_name="options",
        sha256="bbb",
        size_bytes=200,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-15",
    )
    ledger.record_discovered(
        "c.parquet",
        table_name="options",
        sha256="ccc",
        size_bytes=300,
        provider="tradier",
        root="SPX",
        sample_date="2025-12-15",
    )

    # Only schwab in Dec 2025
    count = ledger.delete_filtered(
        "options", after="2025-12-01", before="2026-01-01", provider="schwab"
    )
    assert count == 1
    assert ledger.get("a.parquet", "options") is None
    assert ledger.get("b.parquet", "options") is not None
    assert ledger.get("c.parquet", "options") is not None


def test_pending_upload_filtered_by_date(ledger: Ledger) -> None:
    ledger.record_discovered(
        "oct.parquet",
        table_name="options",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-10-15",
    )
    ledger.record_discovered(
        "dec.parquet",
        table_name="options",
        sha256="bbb",
        size_bytes=200,
        provider="schwab",
        root="SPX",
        sample_date="2026-12-15",
    )

    result = ledger.pending_upload("options", after="2026-10-01", before="2026-11-01")
    assert [r.path for r in result] == ["oct.parquet"]


def test_pending_upload_filtered_by_provider(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        table_name="options",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-10-15",
    )
    ledger.record_discovered(
        "b.parquet",
        table_name="options",
        sha256="bbb",
        size_bytes=200,
        provider="tradier",
        root="SPX",
        sample_date="2026-10-15",
    )

    result = ledger.pending_upload("options", providers=["schwab"])
    assert [r.path for r in result] == ["a.parquet"]


def test_pending_commit_filtered(ledger: Ledger) -> None:
    ledger.record_discovered(
        "oct.parquet",
        table_name="options",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-10-15",
    )
    ledger.record_discovered(
        "dec.parquet",
        table_name="options",
        sha256="bbb",
        size_bytes=200,
        provider="schwab",
        root="SPX",
        sample_date="2026-12-15",
    )
    ledger.mark_uploaded("oct.parquet", "options", "s3/oct")
    ledger.mark_uploaded("dec.parquet", "options", "s3/dec")

    result = ledger.pending_commit("options", after="2026-12-01")
    assert [r.path for r in result] == ["dec.parquet"]


def test_pending_upload_no_filters_returns_all(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        table_name="options",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-10-15",
    )
    ledger.record_discovered(
        "b.parquet",
        table_name="options",
        sha256="bbb",
        size_bytes=200,
        provider="tradier",
        root="SPX",
        sample_date="2026-12-15",
    )

    result = ledger.pending_upload("options")
    assert len(result) == 2


def test_get_s3_keys_filtered(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        table_name="options",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2025-12-15",
    )
    ledger.record_discovered(
        "b.parquet",
        table_name="options",
        sha256="bbb",
        size_bytes=200,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-15",
    )
    ledger.mark_uploaded("a.parquet", "options", "s3/a")
    ledger.mark_uploaded("b.parquet", "options", "s3/b")

    keys = ledger.get_s3_keys("options", after="2025-12-01", before="2026-01-01")
    assert keys == ["s3/a"]

    all_keys = ledger.get_s3_keys("options")
    assert sorted(all_keys) == ["s3/a", "s3/b"]
