"""SQLite-backed ledger for tracking file discovery and publication progress."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


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
CREATE TABLE IF NOT EXISTS files (
    path          TEXT PRIMARY KEY,
    sha256        TEXT NOT NULL,
    size_bytes    INTEGER NOT NULL,
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


class Ledger:
    """Persistent publication ledger backed by SQLite."""

    def __init__(self, db_path: Path | str) -> None:
        self._db_path = str(db_path)
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(_CREATE_TABLE)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def _now(self) -> str:
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
            INSERT INTO files (path, sha256, size_bytes, provider, root, sample_date,
                               status, discovered_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 'discovered', ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                sha256 = excluded.sha256,
                size_bytes = excluded.size_bytes,
                status = CASE
                    WHEN files.sha256 != excluded.sha256 THEN 'discovered'
                    ELSE files.status
                END,
                error = CASE
                    WHEN files.sha256 != excluded.sha256 THEN NULL
                    ELSE files.error
                END,
                s3_key = CASE
                    WHEN files.sha256 != excluded.sha256 THEN NULL
                    ELSE files.s3_key
                END,
                uploaded_at = CASE
                    WHEN files.sha256 != excluded.sha256 THEN NULL
                    ELSE files.uploaded_at
                END,
                committed_at = CASE
                    WHEN files.sha256 != excluded.sha256 THEN NULL
                    ELSE files.committed_at
                END,
                updated_at = excluded.updated_at
            """,
            (path, sha256, size_bytes, provider, root, sample_date, now, now),
        )
        self._conn.commit()

    def mark_uploaded(self, path: str, s3_key: str) -> None:
        """Transition a file to uploaded status."""
        now = self._now()
        self._conn.execute(
            "UPDATE files SET status = 'uploaded', s3_key = ?, uploaded_at = ?, "
            "error = NULL, updated_at = ? WHERE path = ?",
            (s3_key, now, now, path),
        )
        self._conn.commit()

    def mark_committed(self, paths: list[str]) -> None:
        """Batch-transition files to committed status."""
        if not paths:
            return
        now = self._now()
        placeholders = ",".join("?" for _ in paths)
        self._conn.execute(
            f"UPDATE files SET status = 'committed', committed_at = ?, "
            f"error = NULL, updated_at = ? WHERE path IN ({placeholders})",
            [now, now, *paths],
        )
        self._conn.commit()

    def mark_error(self, path: str, error: str) -> None:
        """Record an error for a file."""
        now = self._now()
        self._conn.execute(
            "UPDATE files SET status = 'error', error = ?, updated_at = ? WHERE path = ?",
            (error, now, path),
        )
        self._conn.commit()

    def pending_upload(self) -> list[FileRecord]:
        """Return files with status=discovered."""
        return self._query_status("discovered")

    def pending_commit(self) -> list[FileRecord]:
        """Return files with status=uploaded."""
        return self._query_status("uploaded")

    def summary(self) -> dict[str, int]:
        """Return counts by status."""
        rows = self._conn.execute("SELECT status, COUNT(*) FROM files GROUP BY status").fetchall()
        return {row[0]: row[1] for row in rows}

    def get(self, path: str) -> FileRecord | None:
        """Look up a single file record."""
        row = self._conn.execute("SELECT * FROM files WHERE path = ?", (path,)).fetchone()
        return FileRecord(*row) if row else None

    def _query_status(self, status: str) -> list[FileRecord]:
        rows = self._conn.execute(
            "SELECT * FROM files WHERE status = ? ORDER BY path", (status,)
        ).fetchall()
        return [FileRecord(*row) for row in rows]
