"""Publication ledger for tracking file discovery and publication progress.

Uses SQLite by default (for local development and testing). Connects to
PostgreSQL when the database URL starts with ``postgresql://``.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg


@dataclass
class FileRecord:
    """A row in the publication ledger."""

    path: str
    table_name: str
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
    path          TEXT NOT NULL,
    table_name    TEXT NOT NULL,
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
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (path, table_name)
)
"""

_CREATE_INDEX = "CREATE INDEX IF NOT EXISTS idx_catalog_files_status ON catalog_files (status)"

_COLUMNS = (
    "path, table_name, sha256, size_bytes, provider, root, sample_date, "
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
            self._conn: Any = psycopg.connect(database_url, autocommit=True)
        else:
            path = (
                database_url.removeprefix("sqlite:///")
                if database_url.startswith("sqlite:")
                else database_url
            )
            Path(path).parent.mkdir(parents=True, exist_ok=True)
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
        table_name: str,
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
            INSERT INTO catalog_files (path, table_name, sha256, size_bytes, provider, root,
                                       sample_date, status, discovered_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'discovered', %s, %s)
            ON CONFLICT (path, table_name) DO UPDATE SET
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
            (path, table_name, sha256, size_bytes, provider, root, sample_date, now, now),
        )

    def mark_uploaded(self, path: str, table_name: str, s3_key: str) -> None:
        """Transition a file to uploaded status."""
        now = self._now()
        self._execute(
            "UPDATE catalog_files SET status = 'uploaded', s3_key = %s, "
            "uploaded_at = %s, error = NULL, updated_at = %s "
            "WHERE path = %s AND table_name = %s",
            (s3_key, now, now, path, table_name),
        )

    def mark_committed(self, paths: list[str], table_name: str) -> None:
        """Batch-transition files to committed status."""
        if not paths:
            return
        now = self._now()
        if self._is_pg:
            self._execute(
                "UPDATE catalog_files SET status = 'committed', committed_at = %s, "
                "error = NULL, updated_at = %s "
                "WHERE path = ANY(%s) AND table_name = %s",
                (now, now, paths, table_name),
            )
        else:
            placeholders = ",".join("?" for _ in paths)
            self._conn.execute(
                f"UPDATE catalog_files SET status = 'committed', committed_at = ?, "
                f"error = NULL, updated_at = ? "
                f"WHERE path IN ({placeholders}) AND table_name = ?",
                [now, now, *paths, table_name],
            )
            self._conn.commit()

    def mark_discovered_for_retry(self, path: str, table_name: str) -> None:
        """Reset a file to discovered status after commit failure cleanup.

        Clears s3_key, uploaded_at, committed_at, and error so the full
        upload → commit cycle will be retried on the next run.
        """
        now = self._now()
        self._execute(
            "UPDATE catalog_files SET status = 'discovered', s3_key = NULL, "
            "uploaded_at = NULL, committed_at = NULL, error = NULL, updated_at = %s "
            "WHERE path = %s AND table_name = %s",
            (now, path, table_name),
        )

    def mark_error(self, path: str, table_name: str, error: str) -> None:
        """Record an error for a file."""
        now = self._now()
        self._execute(
            "UPDATE catalog_files SET status = 'error', error = %s, "
            "updated_at = %s WHERE path = %s AND table_name = %s",
            (error, now, path, table_name),
        )

    def pending_upload(
        self,
        table_name: str,
        after: str | None = None,
        before: str | None = None,
        providers: list[str] | None = None,
    ) -> list[FileRecord]:
        """Return files with status=discovered for the given table."""
        return self._query_status(
            "discovered", table_name, after=after, before=before, providers=providers
        )

    def pending_commit(
        self,
        table_name: str,
        after: str | None = None,
        before: str | None = None,
        providers: list[str] | None = None,
    ) -> list[FileRecord]:
        """Return files with status=uploaded for the given table."""
        return self._query_status(
            "uploaded", table_name, after=after, before=before, providers=providers
        )

    def summary(self, table_name: str | None = None) -> dict[str, int]:
        """Return counts by status, optionally filtered by table."""
        if table_name:
            rows = self._fetchall(
                "SELECT status, COUNT(*) FROM catalog_files WHERE table_name = %s GROUP BY status",
                (table_name,),
            )
        else:
            rows = self._fetchall("SELECT status, COUNT(*) FROM catalog_files GROUP BY status")
        return {row[0]: row[1] for row in rows}

    def get(self, path: str, table_name: str) -> FileRecord | None:
        """Look up a single file record."""
        row = self._fetchone(
            f"SELECT {_COLUMNS} FROM catalog_files WHERE path = %s AND table_name = %s",
            (path, table_name),
        )
        return FileRecord(*row) if row else None

    def delete_all(self, table_name: str) -> int:
        """Delete all records for a table. Returns the count deleted."""
        return self.delete_filtered(table_name)

    def delete_filtered(
        self,
        table_name: str,
        after: str | None = None,
        before: str | None = None,
        provider: str | None = None,
    ) -> int:
        """Delete records matching the given filters. Returns the count deleted."""
        where = "WHERE table_name = %s"
        params: list[str] = [table_name]

        if after:
            where += " AND sample_date >= %s"
            params.append(after)
        if before:
            where += " AND sample_date < %s"
            params.append(before)
        if provider:
            where += " AND provider = %s"
            params.append(provider)

        rows = self._fetchall(f"SELECT COUNT(*) FROM catalog_files {where}", tuple(params))
        count = rows[0][0] if rows else 0
        self._execute(f"DELETE FROM catalog_files {where}", tuple(params))
        return count

    def get_s3_keys(
        self,
        table_name: str,
        after: str | None = None,
        before: str | None = None,
        provider: str | None = None,
    ) -> list[str]:
        """Return S3 keys for records matching the given filters."""
        where = "WHERE table_name = %s AND s3_key IS NOT NULL"
        params: list[str] = [table_name]

        if after:
            where += " AND sample_date >= %s"
            params.append(after)
        if before:
            where += " AND sample_date < %s"
            params.append(before)
        if provider:
            where += " AND provider = %s"
            params.append(provider)

        rows = self._fetchall(f"SELECT s3_key FROM catalog_files {where}", tuple(params))
        return [row[0] for row in rows]

    def _query_status(
        self,
        status: str,
        table_name: str,
        after: str | None = None,
        before: str | None = None,
        providers: list[str] | None = None,
    ) -> list[FileRecord]:
        where = "WHERE status = %s AND table_name = %s"
        params: list[Any] = [status, table_name]

        if after:
            where += " AND sample_date >= %s"
            params.append(after)
        if before:
            where += " AND sample_date < %s"
            params.append(before)
        if providers:
            if self._is_pg:
                where += " AND provider = ANY(%s)"
                params.append(providers)
            else:
                placeholders = ",".join("%s" for _ in providers)
                where += f" AND provider IN ({placeholders})"
                params.extend(providers)

        rows = self._fetchall(
            f"SELECT {_COLUMNS} FROM catalog_files {where} ORDER BY path",
            tuple(params),
        )
        return [FileRecord(*row) for row in rows]
