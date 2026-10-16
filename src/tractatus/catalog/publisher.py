"""Orchestrates discovery, upload, and Iceberg commit of parquet files."""

from __future__ import annotations

import logging
import tempfile
import time
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import boto3
from pyiceberg.catalog import Catalog, load_catalog

from tractatus.catalog.config import CatalogConfig
from tractatus.catalog.discovery import discover_files
from tractatus.catalog.ledger import FileRecord, Ledger
from tractatus.catalog.normalize import normalize_parquet
from tractatus.catalog.schema import TABLE_DEFS

logger = logging.getLogger("tractatus.catalog")


@dataclass
class PublishResult:
    """Summary of a publish run."""

    discovered: int = 0
    uploaded: int = 0
    committed: int = 0
    skipped: int = 0
    errors: int = 0
    details: list[str] = field(default_factory=list)


class Publisher:
    """Discover, upload, and commit parquet files to an Iceberg table."""

    def __init__(self, config: CatalogConfig) -> None:
        self._config = config
        self._ledger = Ledger(config.database_url)

    def close(self) -> None:
        self._ledger.close()

    def run(
        self,
        table_name: str,
        dry_run: bool = False,
        providers: list[str] | None = None,
        limit: int | None = None,
        after: date | None = None,
        before: date | None = None,
    ) -> PublishResult:
        """Execute the full discover → upload → commit pipeline for a table."""
        if table_name not in TABLE_DEFS:
            raise ValueError(
                f"Unknown table {table_name!r}. Registered tables: {', '.join(sorted(TABLE_DEFS))}"
            )

        run_start = time.monotonic()
        result = PublishResult()

        logger.info(
            {
                "event": "run_started",
                "table": table_name,
                "dry_run": dry_run,
                "limit": limit,
                "after": after,
                "before": before,
                "providers": providers,
            }
        )

        result.discovered = self._discover(table_name, providers, after=after, before=before)
        if dry_run:
            result.details.append("Dry run — skipping upload and commit.")
            return result

        after_str = after.isoformat() if after else None
        before_str = before.isoformat() if before else None

        result.uploaded, result.skipped, upload_errors = self._upload(
            table_name, limit=limit, after=after_str, before=before_str, providers=providers
        )
        result.errors += upload_errors

        committed, commit_errors = self._commit(
            table_name, limit=limit, after=after_str, before=before_str, providers=providers
        )
        result.committed = committed
        result.errors += commit_errors

        duration_ms = int((time.monotonic() - run_start) * 1000)
        logger.info(
            {
                "event": "run_completed",
                "table": table_name,
                "discovered": result.discovered,
                "uploaded": result.uploaded,
                "skipped": result.skipped,
                "committed": result.committed,
                "errors": result.errors,
                "duration_ms": duration_ms,
            }
        )

        return result

    def _discover(
        self,
        table_name: str,
        providers: list[str] | None = None,
        after: date | None = None,
        before: date | None = None,
    ) -> int:
        """Scan data_root/{table_name} for parquet files and record them in the ledger."""
        table_dir = self._config.data_root / table_name
        files = discover_files(table_dir, providers=providers, after=after, before=before)
        for f in files:
            self._ledger.record_discovered(
                path=f.relative_path,
                table_name=table_name,
                sha256=f.sha256,
                size_bytes=f.size_bytes,
                provider=f.provider,
                root=f.root,
                sample_date=f.sample_date.isoformat(),
            )
        logger.info(
            {
                "event": "discovery_completed",
                "table": table_name,
                "file_count": len(files),
                "total_bytes": sum(f.size_bytes for f in files),
            }
        )
        return len(files)

    def _upload(
        self,
        table_name: str,
        limit: int | None = None,
        after: str | None = None,
        before: str | None = None,
        providers: list[str] | None = None,
    ) -> tuple[int, int, int]:
        """Upload pending files to S3. Returns (uploaded, skipped, errors)."""
        pending = self._ledger.pending_upload(
            table_name, after=after, before=before, providers=providers
        )
        if limit is not None:
            pending = pending[:limit]
        if not pending:
            return 0, 0, 0

        session = self._boto_session()
        s3 = session.client("s3")
        uploaded = 0
        skipped = 0
        errors = 0
        uploaded_bytes = 0
        table_dir = self._config.data_root / table_name

        for record in pending:
            s3_key = self._s3_key(table_name, record)
            local_path = table_dir / record.path

            if not local_path.exists():
                self._ledger.mark_error(record.path, table_name, "Local file not found")
                logger.error(
                    {
                        "event": "upload_error",
                        "table": table_name,
                        "path": record.path,
                        "error": "Local file not found",
                    }
                )
                errors += 1
                continue

            try:
                if self._s3_key_exists(s3, s3_key):
                    logger.info(
                        {
                            "event": "upload_skipped",
                            "table": table_name,
                            "path": record.path,
                            "s3_key": s3_key,
                            "reason": "already_exists",
                        }
                    )
                    skipped += 1
                else:
                    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
                        tmp_path = Path(tmp.name)
                    try:
                        diagnostics = normalize_parquet(local_path, tmp_path)
                        if diagnostics:
                            logger.info(
                                {
                                    "event": "normalize_applied",
                                    "table": table_name,
                                    "path": record.path,
                                    "diagnostics": diagnostics,
                                }
                            )
                        s3.upload_file(
                            str(tmp_path),
                            self._config.iceberg_bucket,
                            s3_key,
                            ExtraArgs={"StorageClass": self._config.s3_storage_class},
                        )
                    except ValueError as exc:
                        self._ledger.mark_error(
                            record.path, table_name, f"Normalization failed: {exc}"
                        )
                        logger.error(
                            {
                                "event": "normalize_error",
                                "table": table_name,
                                "path": record.path,
                                "error": str(exc),
                            }
                        )
                        errors += 1
                        continue
                    finally:
                        tmp_path.unlink(missing_ok=True)
                    uploaded_bytes += record.size_bytes
                    logger.info(
                        {
                            "event": "file_uploaded",
                            "table": table_name,
                            "path": record.path,
                            "s3_key": s3_key,
                            "size_bytes": record.size_bytes,
                            "provider": record.provider,
                            "root": record.root,
                            "sample_date": record.sample_date,
                        }
                    )
                    uploaded += 1
                self._ledger.mark_uploaded(record.path, table_name, s3_key)
            except Exception as exc:
                self._ledger.mark_error(record.path, table_name, str(exc))
                logger.error(
                    {
                        "event": "upload_error",
                        "table": table_name,
                        "path": record.path,
                        "error_class": type(exc).__name__,
                        "error_message": str(exc),
                    }
                )
                errors += 1

        logger.info(
            {
                "event": "upload_batch_completed",
                "table": table_name,
                "uploaded": uploaded,
                "skipped": skipped,
                "errors": errors,
                "uploaded_bytes": uploaded_bytes,
            }
        )
        return uploaded, skipped, errors

    def _commit(
        self,
        table_name: str,
        limit: int | None = None,
        after: str | None = None,
        before: str | None = None,
        providers: list[str] | None = None,
    ) -> tuple[int, int]:
        """Commit uploaded files to Iceberg. Returns (committed_count, error_count)."""
        pending = self._ledger.pending_commit(
            table_name, after=after, before=before, providers=providers
        )
        if limit is not None:
            pending = pending[:limit]
        if not pending:
            return 0, 0

        catalog = self._iceberg_catalog()
        full_name = f"{self._config.glue_database}.{table_name}"
        table = catalog.load_table(full_name)

        s3_paths = [
            f"s3://{self._config.iceberg_bucket}/{r.s3_key}"
            for r in pending
            if r.s3_key is not None
        ]

        if not s3_paths:
            return 0, 0

        try:
            table.add_files(file_paths=s3_paths, check_duplicate_files=True)
            committed_paths = [r.path for r in pending if r.s3_key is not None]
            self._ledger.mark_committed(committed_paths, table_name)
            logger.info(
                {
                    "event": "commit_completed",
                    "table": table_name,
                    "file_count": len(committed_paths),
                }
            )
            return len(committed_paths), 0
        except Exception as exc:
            logger.error(
                {
                    "event": "commit_error",
                    "table": table_name,
                    "file_count": len(s3_paths),
                    "error_class": type(exc).__name__,
                    "error_message": str(exc),
                }
            )

            # Clean up orphaned S3 objects from this batch
            s3_keys_to_delete = [r.s3_key for r in pending if r.s3_key]
            cleanup_failures = self._cleanup_orphaned_s3(s3_keys_to_delete)

            for record in pending:
                if record.s3_key and record.s3_key in cleanup_failures:
                    self._ledger.mark_error(
                        record.path,
                        table_name,
                        f"Commit failed and orphan cleanup failed for {record.s3_key}: {exc}",
                    )
                else:
                    # S3 object cleaned up — safe to retry full cycle
                    self._ledger.mark_discovered_for_retry(record.path, table_name)

            return 0, len(pending)

    def _cleanup_orphaned_s3(self, s3_keys: list[str]) -> set[str]:
        """Delete orphaned S3 objects after a failed commit.

        Returns the set of keys that could NOT be deleted.
        """
        if not s3_keys:
            return set()
        session = self._boto_session()
        s3 = session.client("s3")
        failures: set[str] = set()
        for key in s3_keys:
            try:
                s3.delete_object(Bucket=self._config.iceberg_bucket, Key=key)
                logger.info({"event": "orphan_cleaned", "s3_key": key})
            except Exception as del_exc:
                logger.error(
                    {
                        "event": "orphan_cleanup_failed",
                        "s3_key": key,
                        "error": str(del_exc),
                    }
                )
                failures.add(key)
        return failures

    def _s3_key_exists(self, s3: Any, s3_key: str) -> bool:
        """Check if an S3 key already exists via HEAD request."""
        try:
            s3.head_object(Bucket=self._config.iceberg_bucket, Key=s3_key)
            return True
        except Exception:
            return False

    def _s3_key(self, table_name: str, record: FileRecord) -> str:
        """Build a deterministic S3 key for a file."""
        return f"{self._config.glue_database}/{table_name}/{record.path}"

    def _boto_session(self) -> boto3.Session:
        session_kwargs: dict[str, str] = {"region_name": self._config.aws_region}
        if self._config.aws_profile:
            session_kwargs["profile_name"] = self._config.aws_profile
        return boto3.Session(**session_kwargs)  # type: ignore[arg-type]

    def _iceberg_catalog(self) -> Catalog:
        """Load PyIceberg Glue catalog."""
        props: dict[str, str] = {
            "type": "glue",
            "glue.region": self._config.aws_region,
            "s3.region": self._config.aws_region,
            "warehouse": self._config.iceberg_warehouse,
        }
        if self._config.aws_profile:
            props["glue.profile-name"] = self._config.aws_profile
            props["s3.profile-name"] = self._config.aws_profile
        return load_catalog("glue", **props)
