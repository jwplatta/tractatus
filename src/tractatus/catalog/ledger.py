"""PostgreSQL-backed ledger for tracking file discovery and publication progress."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import psycopg
from psycopg.rows import class_row


@dataclass
class FileRecord:
    """A row in the publication ledger."""

    path: str
    sha256: str
    size_bytes: int
    provider: str
    root: str
    sample_date: str
    s3_key: str | None
    status: str
    error: str | None
    discovered_at: str
    uploaded_at: str | None
    committed_at: str | None
    updated_at: str


_CREATE_TABLE = """\
CREATE TABLE IF NOT EXISTS catalog_files (
    path          TEXT PRIMARY KEY,
    sha256        TEXT NOT NULL,
    size_bytes    BIGINT NOT NULL,
    provider      TEXT NOT NULL,
    root          TEXT NOT NULL,
    sample_date   TEXT NOT NULL,
    s3_key        TEXT,
    status        TEXT NOT NULL DEFAULT 'discovered',
    error         TEXT,
    discovered_at TEXT NOT NULL,
    uploaded_at   TEXT,
    committed_at  TEXT,
    updated_at    TEXT NOT NULL
)
"""

_CREATE_INDEX = "CREATE INDEX IF NOT EXISTS idx_catalog_files_status ON catalog_files (status)"


class Ledger:
    """Persistent publication ledger backed by PostgreSQL.

    Connects to the ``tractatus`` database in the shared quant-infra
    PostgreSQL instance.  Creates the ``catalog_files`` table on first
    connection if it does not already exist.
    """

    def __init__(self, database_url: str) -> None:
        self._conn = psycopg.connect(database_url, autocommit=True)
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Create the catalog_files table and indexes if they don't exist."""
        self._conn.execute(_CREATE_TABLE)
        self._conn.execute(_CREATE_INDEX)

    def close(self) -> None:
        self._conn.close()

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat()

    def record_discovered(
        self,
        path: str,
        sha256: str,
        size_bytes: int,
        provider: str,
        root: str,
        sample_date: str,
    ) -> None:
        """Insert a new file or update an existing one if the hash changed."""
        now = self._now()
        self._conn.execute(
            """\
            INSERT INTO catalog_files (path, sha256, size_bytes, provider, root, sample_date,
                                       status, discovered_at, updated_at)
            VALUES (%(path)s, %(sha256)s, %(size_bytes)s, %(provider)s, %(root)s,
                    %(sample_date)s, 'discovered', %(now)s, %(now)s)
            ON CONFLICT (path) DO UPDATE SET
                sha256 = EXCLUDED.sha256,
                size_bytes = EXCLUDED.size_bytes,
                status = CASE
                    WHEN catalog_files.sha256 != EXCLUDED.sha256 THEN 'discovered'
                    ELSE catalog_files.status
                END,
                error = CASE
                    WHEN catalog_files.sha256 != EXCLUDED.sha256 THEN NULL
                    ELSE catalog_files.error
                END,
                s3_key = CASE
                    WHEN catalog_files.sha256 != EXCLUDED.sha256 THEN NULL
                    ELSE catalog_files.s3_key
                END,
                uploaded_at = CASE
                    WHEN catalog_files.sha256 != EXCLUDED.sha256 THEN NULL
                    ELSE catalog_files.uploaded_at
                END,
                committed_at = CASE
                    WHEN catalog_files.sha256 != EXCLUDED.sha256 THEN NULL
                    ELSE catalog_files.committed_at
                END,
                updated_at = %(now)s
            """,
            {
                "path": path,
                "sha256": sha256,
                "size_bytes": size_bytes,
                "provider": provider,
                "root": root,
                "sample_date": sample_date,
                "now": now,
            },
        )

    def mark_uploaded(self, path: str, s3_key: str) -> None:
        """Transition a file to uploaded status."""
        now = self._now()
        self._conn.execute(
            "UPDATE catalog_files SET status = 'uploaded', s3_key = %(s3_key)s, "
            "uploaded_at = %(now)s, error = NULL, updated_at = %(now)s "
            "WHERE path = %(path)s",
            {"s3_key": s3_key, "now": now, "path": path},
        )

    def mark_committed(self, paths: list[str]) -> None:
        """Batch-transition files to committed status."""
        if not paths:
            return
        now = self._now()
        self._conn.execute(
            "UPDATE catalog_files SET status = 'committed', committed_at = %(now)s, "
            "error = NULL, updated_at = %(now)s "
            "WHERE path = ANY(%(paths)s)",
            {"now": now, "paths": paths},
        )

    def mark_error(self, path: str, error: str) -> None:
        """Record an error for a file."""
        now = self._now()
        self._conn.execute(
            "UPDATE catalog_files SET status = 'error', error = %(error)s, "
            "updated_at = %(now)s WHERE path = %(path)s",
            {"error": error, "now": now, "path": path},
        )

    def pending_upload(self) -> list[FileRecord]:
        """Return files with status=discovered."""
        return self._query_status("discovered")

    def pending_commit(self) -> list[FileRecord]:
        """Return files with status=uploaded."""
        return self._query_status("uploaded")

    def summary(self) -> dict[str, int]:
        """Return counts by status."""
        rows = self._conn.execute(
            "SELECT status, COUNT(*) FROM catalog_files GROUP BY status"
        ).fetchall()
        return {row[0]: row[1] for row in rows}

    def get(self, path: str) -> FileRecord | None:
        """Look up a single file record."""
        with self._conn.cursor(row_factory=class_row(FileRecord)) as cur:
            cur.execute(
                "SELECT path, sha256, size_bytes, provider, root, sample_date, "
                "s3_key, status, error, discovered_at, uploaded_at, committed_at, updated_at "
                "FROM catalog_files WHERE path = %(path)s",
                {"path": path},
            )
            return cur.fetchone()

    def _query_status(self, status: str) -> list[FileRecord]:
        with self._conn.cursor(row_factory=class_row(FileRecord)) as cur:
            cur.execute(
                "SELECT path, sha256, size_bytes, provider, root, sample_date, "
                "s3_key, status, error, discovered_at, uploaded_at, committed_at, updated_at "
                "FROM catalog_files WHERE status = %(status)s ORDER BY path",
                {"status": status},
            )
            return cur.fetchall()
