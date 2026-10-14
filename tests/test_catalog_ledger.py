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
