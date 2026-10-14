"""Publication ledger for tracking file discovery and publication progress.

Uses SQLite by default (for local development and testing). Connects to
PostgreSQL when the database URL starts with ``postgresql://``.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


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

_COLUMNS = (
    "path, sha256, size_bytes, provider, root, sample_date, "
    "s3_key, status, error, discovered_at, uploaded_at, committed_at, updated_at"
)


class Ledger:
    """Persistent publication ledger backed by SQLite or PostgreSQL.

    Pass a file path or ``sqlite://`` URL for SQLite, or a
    ``postgresql://`` URL for PostgreSQL.
    """

    def __init__(self, database_url: str) -> None:
        self._is_pg = database_url.startswith("postgresql://")
        if self._is_pg:
            import psycopg

            self._conn: Any = psycopg.connect(database_url, autocommit=True)
        else:
            path = (
                database_url.removeprefix("sqlite:///")
                if database_url.startswith("sqlite:")
                else database_url
            )
            self._conn = sqlite3.connect(path)
            self._conn.execute("PRAGMA journal_mode=WAL")
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Create the catalog_files table and indexes if they don't exist."""
        self._conn.execute(_CREATE_TABLE)
        self._conn.execute(_CREATE_INDEX)
        if not self._is_pg:
            self._conn.commit()

    def _execute(self, sql: str, params: tuple[Any, ...] | list[Any] = ()) -> Any:
        if not self._is_pg:
            sql = sql.replace("%s", "?")
        result = self._conn.execute(sql, params)
        if not self._is_pg:
            self._conn.commit()
        return result

    def _fetchall(self, sql: str, params: tuple[Any, ...] | list[Any] = ()) -> list[Any]:
        if not self._is_pg:
            sql = sql.replace("%s", "?")
        return list(self._conn.execute(sql, params).fetchall())

    def _fetchone(self, sql: str, params: tuple[Any, ...] | list[Any] = ()) -> Any:
        if not self._is_pg:
            sql = sql.replace("%s", "?")
        return self._conn.execute(sql, params).fetchone()

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
        self._execute(
            """\
            INSERT INTO catalog_files (path, sha256, size_bytes, provider, root, sample_date,
                                       status, discovered_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, 'discovered', %s, %s)
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
                updated_at = EXCLUDED.updated_at
            """,
            (path, sha256, size_bytes, provider, root, sample_date, now, now),
        )

    def mark_uploaded(self, path: str, s3_key: str) -> None:
        """Transition a file to uploaded status."""
        now = self._now()
        self._execute(
            "UPDATE catalog_files SET status = 'uploaded', s3_key = %s, "
            "uploaded_at = %s, error = NULL, updated_at = %s WHERE path = %s",
            (s3_key, now, now, path),
        )

    def mark_committed(self, paths: list[str]) -> None:
        """Batch-transition files to committed status."""
        if not paths:
            return
        now = self._now()
        if self._is_pg:
            self._execute(
                "UPDATE catalog_files SET status = 'committed', committed_at = %s, "
                "error = NULL, updated_at = %s WHERE path = ANY(%s)",
                (now, now, paths),
            )
        else:
            placeholders = ",".join("?" for _ in paths)
            self._conn.execute(
                f"UPDATE catalog_files SET status = 'committed', committed_at = ?, "
                f"error = NULL, updated_at = ? WHERE path IN ({placeholders})",
                [now, now, *paths],
            )
            self._conn.commit()

    def mark_error(self, path: str, error: str) -> None:
        """Record an error for a file."""
        now = self._now()
        self._execute(
            "UPDATE catalog_files SET status = 'error', error = %s, "
            "updated_at = %s WHERE path = %s",
            (error, now, path),
        )

    def pending_upload(self) -> list[FileRecord]:
        """Return files with status=discovered."""
        return self._query_status("discovered")

    def pending_commit(self) -> list[FileRecord]:
        """Return files with status=uploaded."""
        return self._query_status("uploaded")

    def summary(self) -> dict[str, int]:
        """Return counts by status."""
        rows = self._fetchall("SELECT status, COUNT(*) FROM catalog_files GROUP BY status")
        return {row[0]: row[1] for row in rows}

    def get(self, path: str) -> FileRecord | None:
        """Look up a single file record."""
        row = self._fetchone(f"SELECT {_COLUMNS} FROM catalog_files WHERE path = %s", (path,))
        return FileRecord(*row) if row else None

    def _query_status(self, status: str) -> list[FileRecord]:
        rows = self._fetchall(
            f"SELECT {_COLUMNS} FROM catalog_files WHERE status = %s ORDER BY path",
            (status,),
        )
        return [FileRecord(*row) for row in rows]
