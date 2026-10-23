"""Publication ledger for tracking file discovery, publication, and cache state.

Uses SQLite by default (for local development and testing). Connects to
PostgreSQL when the database URL starts with ``postgresql://``.

Status flow: discovered → committed | error
"""
# NOTE: If you drop and recreate the ledger, repopulate with:
#   tractatus catalog sync-ledger --table options

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg

from tractatus.catalog.validate.options import QualityReport


@dataclass
class PublishedArtifact:
    """A row in the published_artifacts table."""

    artifact_key: str
    table_name: str | None
    provider: str | None
    root: str | None
    sample_date: str | None
    interval: str | None
    committed_at: str | None
    cached: bool
    cached_at: str | None
    row_count: int | None
    size_bytes: int | None
    updated_at: str


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
    file_format: str
    s3_key: str | None
    status: str
    error: str | None
    discovered_at: str
    committed_at: str | None
    cache_key: str | None
    cached_at: str | None
    last_accessed_at: str | None
    updated_at: str
    rows_total: int | None
    rows_degraded: int | None
    quality_flags: str | None


_CREATE_TABLE = """\
CREATE TABLE IF NOT EXISTS catalog_files (
    path             TEXT NOT NULL,
    table_name       TEXT NOT NULL,
    sha256           TEXT NOT NULL,
    size_bytes       BIGINT NOT NULL,
    provider         TEXT NOT NULL,
    root             TEXT NOT NULL,
    sample_date      TEXT NOT NULL,
    file_format      TEXT NOT NULL DEFAULT '',
    s3_key           TEXT,
    status           TEXT NOT NULL DEFAULT 'discovered',
    error            TEXT,
    discovered_at    TEXT NOT NULL,
    committed_at     TEXT,
    cache_key        TEXT,
    cached_at        TEXT,
    last_accessed_at TEXT,
    updated_at       TEXT NOT NULL,
    rows_total       INTEGER,
    rows_degraded    INTEGER,
    quality_flags    TEXT,
    PRIMARY KEY (path, table_name)
)
"""

_CREATE_INDEX = "CREATE INDEX IF NOT EXISTS idx_catalog_files_status ON catalog_files (status)"

_CREATE_PUBLISHED_FILES = """\
CREATE TABLE IF NOT EXISTS published_artifacts (
    artifact_key  TEXT PRIMARY KEY,
    table_name    TEXT,
    provider      TEXT,
    root          TEXT,
    sample_date   TEXT,
    interval      TEXT,
    committed_at  TEXT,
    cached        INTEGER NOT NULL DEFAULT 0,
    cached_at     TEXT,
    row_count     INTEGER,
    size_bytes    INTEGER,
    updated_at    TEXT NOT NULL
)
"""

_CREATE_PUBLISHED_INDEX = (
    "CREATE INDEX IF NOT EXISTS idx_published_artifacts_root_date "
    "ON published_artifacts (table_name, root, sample_date)"
)

_PUBLISHED_COLUMNS = (
    "artifact_key, table_name, provider, root, sample_date, interval, "
    "committed_at, cached, cached_at, row_count, size_bytes, updated_at"
)

_COLUMNS = (
    "path, table_name, sha256, size_bytes, provider, root, sample_date, "
    "file_format, s3_key, status, error, discovered_at, committed_at, "
    "cache_key, cached_at, last_accessed_at, updated_at, "
    "rows_total, rows_degraded, quality_flags"
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
        """Create tables and indexes if they don't exist."""
        self._conn.execute(_CREATE_TABLE)
        self._conn.execute(_CREATE_INDEX)
        self._conn.execute(_CREATE_PUBLISHED_FILES)
        self._conn.execute(_CREATE_PUBLISHED_INDEX)
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

    # --- Published files ---

    def upsert_published(
        self,
        artifact_key: str,
        table_name: str | None = None,
        provider: str | None = None,
        root: str | None = None,
        sample_date: str | None = None,
        interval: str | None = None,
        committed_at: str | None = None,
        cached: bool = False,
        cached_at: str | None = None,
        row_count: int | None = None,
        size_bytes: int | None = None,
    ) -> None:
        """Insert or update a published_artifacts record."""
        now = self._now()
        self._execute(
            """\
            INSERT INTO published_artifacts (artifact_key, table_name, provider, root, sample_date,
                interval, committed_at, cached, cached_at, row_count, size_bytes, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (artifact_key) DO UPDATE SET
                table_name   = COALESCE(EXCLUDED.table_name, published_artifacts.table_name),
                provider     = COALESCE(EXCLUDED.provider, published_artifacts.provider),
                root         = COALESCE(EXCLUDED.root, published_artifacts.root),
                sample_date  = COALESCE(EXCLUDED.sample_date, published_artifacts.sample_date),
                interval     = COALESCE(EXCLUDED.interval, published_artifacts.interval),
                committed_at = COALESCE(EXCLUDED.committed_at, published_artifacts.committed_at),
                cached       = EXCLUDED.cached,
                cached_at    = COALESCE(EXCLUDED.cached_at, published_artifacts.cached_at),
                row_count    = COALESCE(EXCLUDED.row_count, published_artifacts.row_count),
                size_bytes   = COALESCE(EXCLUDED.size_bytes, published_artifacts.size_bytes),
                updated_at   = EXCLUDED.updated_at
            """,
            (
                artifact_key,
                table_name,
                provider,
                root,
                sample_date,
                interval,
                committed_at,
                int(cached),
                cached_at,
                row_count,
                size_bytes,
                now,
            ),
        )

    def get_published(self, artifact_key: str) -> PublishedArtifact | None:
        """Look up a single published_artifacts record."""
        row = self._fetchone(
            f"SELECT {_PUBLISHED_COLUMNS} FROM published_artifacts WHERE artifact_key = %s",
            (artifact_key,),
        )
        if not row:
            return None
        return PublishedArtifact(
            artifact_key=row[0],
            table_name=row[1],
            provider=row[2],
            root=row[3],
            sample_date=row[4],
            interval=row[5],
            committed_at=row[6],
            cached=bool(row[7]),
            cached_at=row[8],
            row_count=row[9],
            size_bytes=row[10],
            updated_at=row[11],
        )

    def artifact_summary(self, table_name: str | None = None) -> dict[str, int]:
        """Return counts of total and cached published artifacts."""
        where = "WHERE 1=1"
        params: list[Any] = []
        if table_name:
            where += " AND table_name = %s"
            params.append(table_name)
        row = self._fetchone(
            f"SELECT COUNT(*), SUM(cached) FROM published_artifacts {where}",
            tuple(params),
        )
        return {"total": row[0] or 0, "cached": row[1] or 0}

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat()

    # --- Discovery & publication ---

    def record_discovered(
        self,
        path: str,
        table_name: str,
        sha256: str,
        size_bytes: int,
        provider: str,
        root: str,
        sample_date: str,
        file_format: str = "",
    ) -> None:
        """Insert a new file or update an existing one if the hash changed."""
        now = self._now()
        self._execute(
            """\
            INSERT INTO catalog_files (path, table_name, sha256, size_bytes, provider, root,
                                       sample_date, file_format, status, discovered_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'discovered', %s, %s)
            ON CONFLICT (path, table_name) DO UPDATE SET
                sha256 = EXCLUDED.sha256,
                size_bytes = EXCLUDED.size_bytes,
                file_format = EXCLUDED.file_format,
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
                committed_at = CASE
                    WHEN catalog_files.sha256 != EXCLUDED.sha256 THEN NULL
                    ELSE catalog_files.committed_at
                END,
                updated_at = EXCLUDED.updated_at
            """,
            (
                path,
                table_name,
                sha256,
                size_bytes,
                provider,
                root,
                sample_date,
                file_format,
                now,
                now,
            ),
        )

    def mark_committed(
        self,
        paths: list[str],
        table_name: str,
        quality: QualityReport | None = None,
    ) -> None:
        """Batch-transition files to committed status.

        Pass a ``QualityReport`` to record per-file quality stats. When
        processing one file per call (the normal publisher loop), ``paths``
        will contain a single entry and the report applies to that file.
        """
        if not paths:
            return
        now = self._now()
        rows_total = quality.rows_total if quality else None
        rows_degraded = quality.rows_degraded if quality else None
        flags = ",".join(sorted(quality.flags_seen)) if quality and quality.flags_seen else None
        if self._is_pg:
            self._execute(
                "UPDATE catalog_files SET status = 'committed', committed_at = %s, "
                "error = NULL, updated_at = %s, "
                "rows_total = %s, rows_degraded = %s, quality_flags = %s "
                "WHERE path = ANY(%s) AND table_name = %s",
                (now, now, rows_total, rows_degraded, flags, paths, table_name),
            )
        else:
            placeholders = ",".join("?" for _ in paths)
            self._conn.execute(
                f"UPDATE catalog_files SET status = 'committed', committed_at = ?, "
                f"error = NULL, updated_at = ?, "
                f"rows_total = ?, rows_degraded = ?, quality_flags = ? "
                f"WHERE path IN ({placeholders}) AND table_name = ?",
                [now, now, rows_total, rows_degraded, flags, *paths, table_name],
            )
            self._conn.commit()

    def mark_error(self, path: str, table_name: str, error: str) -> None:
        """Record an error for a file."""
        now = self._now()
        self._execute(
            "UPDATE catalog_files SET status = 'error', error = %s, "
            "updated_at = %s WHERE path = %s AND table_name = %s",
            (error, now, path, table_name),
        )

    def pending(
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

    # --- Cache management ---

    def mark_cached(self, path: str, table_name: str, cache_key: str) -> None:
        """Record that a file has been cached in local MinIO."""
        now = self._now()
        self._execute(
            "UPDATE catalog_files SET cache_key = %s, cached_at = %s, "
            "last_accessed_at = %s, updated_at = %s "
            "WHERE path = %s AND table_name = %s",
            (cache_key, now, now, now, path, table_name),
        )

    def mark_cache_accessed(self, path: str, table_name: str) -> None:
        """Update last_accessed_at for a cached file."""
        now = self._now()
        self._execute(
            "UPDATE catalog_files SET last_accessed_at = %s, updated_at = %s "
            "WHERE path = %s AND table_name = %s",
            (now, now, path, table_name),
        )

    def clear_cache_entry(self, path: str, table_name: str) -> None:
        """Clear cache fields for a file (after eviction)."""
        now = self._now()
        self._execute(
            "UPDATE catalog_files SET cache_key = NULL, cached_at = NULL, "
            "last_accessed_at = NULL, updated_at = %s "
            "WHERE path = %s AND table_name = %s",
            (now, path, table_name),
        )

    def cached_files(
        self,
        table_name: str | None = None,
        older_than_days: int | None = None,
        provider: str | None = None,
        root: str | None = None,
    ) -> list[FileRecord]:
        """Return files that have cache entries, with optional filters."""
        where = "WHERE cache_key IS NOT NULL"
        params: list[Any] = []

        if table_name:
            where += " AND table_name = %s"
            params.append(table_name)
        if older_than_days is not None:
            where += " AND last_accessed_at < %s"
            cutoff = (datetime.now(UTC) - timedelta(days=older_than_days)).isoformat()
            params.append(cutoff)
        if provider:
            where += " AND provider = %s"
            params.append(provider)
        if root:
            where += " AND root = %s"
            params.append(root)

        rows = self._fetchall(
            f"SELECT {_COLUMNS} FROM catalog_files {where} ORDER BY path",
            tuple(params),
        )
        return [FileRecord(*row) for row in rows]

    def cache_summary(self, table_name: str | None = None) -> dict[str, int]:
        """Return cache stats: total cached files and total bytes."""
        where = "WHERE cache_key IS NOT NULL"
        params: list[Any] = []
        if table_name:
            where += " AND table_name = %s"
            params.append(table_name)

        row = self._fetchone(
            f"SELECT COUNT(*), COALESCE(SUM(size_bytes), 0) FROM catalog_files {where}",
            tuple(params),
        )
        return {"cached_files": row[0], "cached_bytes": row[1]}

    # --- Queries ---

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

    def quality_summary(
        self,
        table_name: str | None = None,
        provider: str | None = None,
    ) -> list[FileRecord]:
        """Return committed files that have at least one degraded row."""
        where = "WHERE status = 'committed' AND rows_degraded > 0"
        params: list[Any] = []
        if table_name:
            where += " AND table_name = %s"
            params.append(table_name)
        if provider:
            where += " AND provider = %s"
            params.append(provider)
        rows = self._fetchall(
            f"SELECT {_COLUMNS} FROM catalog_files {where} ORDER BY sample_date DESC",
            tuple(params),
        )
        return [FileRecord(*row) for row in rows]

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
