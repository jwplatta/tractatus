"""Orchestrates discovery, upload, and Iceberg commit of parquet files."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import boto3
from pyiceberg.catalog import Catalog, load_catalog

from tractatus.catalog.config import CatalogConfig
from tractatus.catalog.discovery import discover_files
from tractatus.catalog.ledger import FileRecord, Ledger
from tractatus.catalog.schema import TABLE_DEFS, ensure_table

logger = logging.getLogger(__name__)


@dataclass
class PublishResult:
    """Summary of a publish run."""

    discovered: int = 0
    uploaded: int = 0
    committed: int = 0
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
    ) -> PublishResult:
        """Execute the full discover → upload → commit pipeline for a table."""
        if table_name not in TABLE_DEFS:
            raise ValueError(
                f"Unknown table {table_name!r}. Registered tables: {', '.join(sorted(TABLE_DEFS))}"
            )

        result = PublishResult()

        result.discovered = self._discover(table_name, providers)
        if dry_run:
            result.details.append("Dry run — skipping upload and commit.")
            return result

        result.uploaded, upload_errors = self._upload(table_name, limit=limit)
        result.errors += upload_errors

        committed, commit_errors = self._commit(table_name, limit=limit)
        result.committed = committed
        result.errors += commit_errors

        return result

    def _discover(self, table_name: str, providers: list[str] | None = None) -> int:
        """Scan data_root/{table_name} for parquet files and record them in the ledger."""
        table_dir = self._config.data_root / table_name
        files = discover_files(table_dir, providers=providers)
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
        logger.info("Discovered %d parquet files for table %s", len(files), table_name)
        return len(files)

    def _upload(self, table_name: str, limit: int | None = None) -> tuple[int, int]:
        """Upload pending files to S3. Returns (uploaded_count, error_count)."""
        pending = self._ledger.pending_upload(table_name)
        if limit is not None:
            pending = pending[:limit]
        if not pending:
            return 0, 0

        session = self._boto_session()
        s3 = session.client("s3")
        uploaded = 0
        errors = 0
        table_dir = self._config.data_root / table_name

        for record in pending:
            s3_key = self._s3_key(table_name, record)
            local_path = table_dir / record.path

            if not local_path.exists():
                self._ledger.mark_error(record.path, table_name, "Local file not found")
                errors += 1
                continue

            try:
                s3.upload_file(str(local_path), self._config.iceberg_bucket, s3_key)
                self._ledger.mark_uploaded(record.path, table_name, s3_key)
                uploaded += 1
                logger.info(
                    "Uploaded %s → s3://%s/%s", record.path, self._config.iceberg_bucket, s3_key
                )
            except Exception as exc:
                self._ledger.mark_error(record.path, table_name, str(exc))
                errors += 1
                logger.error("Upload failed for %s: %s", record.path, exc)

        return uploaded, errors

    def _commit(self, table_name: str, limit: int | None = None) -> tuple[int, int]:
        """Commit uploaded files to Iceberg. Returns (committed_count, error_count)."""
        pending = self._ledger.pending_commit(table_name)
        if limit is not None:
            pending = pending[:limit]
        if not pending:
            return 0, 0

        catalog = self._iceberg_catalog()
        table = ensure_table(catalog, self._config.glue_database, table_name)

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
            logger.info("Committed %d files to Iceberg table %s", len(committed_paths), table_name)
            return len(committed_paths), 0
        except Exception as exc:
            logger.error("Iceberg commit failed for %s: %s", table_name, exc)
            for record in pending:
                self._ledger.mark_error(record.path, table_name, f"Commit failed: {exc}")
            return 0, len(pending)

    def _s3_key(self, table_name: str, record: FileRecord) -> str:
        """Build a deterministic S3 key for a file."""
        return f"{self._config.glue_database}/{table_name}/data/{record.path}"

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
