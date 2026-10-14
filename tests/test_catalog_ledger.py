"""Tests for the catalog publication ledger.

Requires a PostgreSQL instance. Set CATALOG_DATABASE_URL to run these tests.
Defaults to the quant-infra dev instance.
"""

from __future__ import annotations

import os

import psycopg
import pytest

from tractatus.catalog.ledger import Ledger

_TEST_DB_URL = os.environ.get(
    "CATALOG_DATABASE_URL",
    "postgresql://tractatus_user:tractatus@localhost:5433/tractatus",
)


def _pg_available() -> bool:
    try:
        conn = psycopg.connect(_TEST_DB_URL, connect_timeout=2)
        conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _pg_available(), reason="PostgreSQL not available")


@pytest.fixture()
def ledger() -> Ledger:  # type: ignore[misc]
    """Create a ledger connected to the test database with a clean table."""
    conn = psycopg.connect(_TEST_DB_URL, autocommit=True)
    conn.execute("DROP TABLE IF EXISTS catalog_files")
    conn.close()

    led = Ledger(_TEST_DB_URL)  # creates the table via _ensure_schema
    yield led  # type: ignore[misc]
    led.close()

    conn = psycopg.connect(_TEST_DB_URL, autocommit=True)
    conn.execute("DROP TABLE IF EXISTS catalog_files")
    conn.close()


def test_record_and_retrieve(ledger: Ledger) -> None:
    ledger.record_discovered(
        "schwab/2026/06/25/SPX_samples_2026-06-25.parquet",
        sha256="abc123",
        size_bytes=1024,
        provider="schwab",
        root="SPX",
        sample_date="2026-06-25",
    )

    record = ledger.get("schwab/2026/06/25/SPX_samples_2026-06-25.parquet")
    assert record is not None
    assert record.status == "discovered"
    assert record.sha256 == "abc123"
    assert record.size_bytes == 1024
    assert record.provider == "schwab"
    assert record.root == "SPX"
    assert record.sample_date == "2026-06-25"


def test_idempotent_same_hash(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )
    ledger.mark_uploaded("a.parquet", "s3/key")

    # Re-discover with same hash — should not reset status
    ledger.record_discovered(
        "a.parquet",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )

    record = ledger.get("a.parquet")
    assert record is not None
    assert record.status == "uploaded"
    assert record.s3_key == "s3/key"


def test_hash_change_resets_status(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )
    ledger.mark_uploaded("a.parquet", "s3/key")

    # Re-discover with different hash — should reset to discovered
    ledger.record_discovered(
        "a.parquet",
        sha256="bbb",
        size_bytes=200,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )

    record = ledger.get("a.parquet")
    assert record is not None
    assert record.status == "discovered"
    assert record.s3_key is None
    assert record.uploaded_at is None


def test_status_transitions(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )

    assert ledger.get("a.parquet") is not None
    assert ledger.get("a.parquet").status == "discovered"  # type: ignore[union-attr]

    ledger.mark_uploaded("a.parquet", "s3/key")
    assert ledger.get("a.parquet").status == "uploaded"  # type: ignore[union-attr]

    ledger.mark_committed(["a.parquet"])
    assert ledger.get("a.parquet").status == "committed"  # type: ignore[union-attr]


def test_mark_error(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )
    ledger.mark_error("a.parquet", "upload failed")

    record = ledger.get("a.parquet")
    assert record is not None
    assert record.status == "error"
    assert record.error == "upload failed"


def test_pending_queries(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )
    ledger.record_discovered(
        "b.parquet",
        sha256="bbb",
        size_bytes=200,
        provider="schwab",
        root="AAPL",
        sample_date="2026-01-02",
    )
    ledger.mark_uploaded("b.parquet", "s3/b")

    assert len(ledger.pending_upload()) == 1
    assert ledger.pending_upload()[0].path == "a.parquet"
    assert len(ledger.pending_commit()) == 1
    assert ledger.pending_commit()[0].path == "b.parquet"


def test_summary(ledger: Ledger) -> None:
    ledger.record_discovered(
        "a.parquet",
        sha256="aaa",
        size_bytes=100,
        provider="schwab",
        root="SPX",
        sample_date="2026-01-01",
    )
    ledger.record_discovered(
        "b.parquet",
        sha256="bbb",
        size_bytes=200,
        provider="schwab",
        root="AAPL",
        sample_date="2026-01-02",
    )
    ledger.mark_uploaded("b.parquet", "s3/b")

    summary = ledger.summary()
    assert summary == {"discovered": 1, "uploaded": 1}


def test_mark_committed_empty(ledger: Ledger) -> None:
    ledger.mark_committed([])  # should not raise
