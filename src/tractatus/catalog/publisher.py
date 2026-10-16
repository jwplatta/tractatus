"""Orchestrates discovery, normalization, and Iceberg append of parquet files."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import date

import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog, load_catalog

from tractatus.catalog.config import CatalogConfig
from tractatus.catalog.discovery import discover_files
from tractatus.catalog.ledger import Ledger
from tractatus.catalog.normalize import normalize_table
from tractatus.catalog.schema import TABLE_DEFS

logger = logging.getLogger("tractatus.catalog")


@dataclass
class PublishResult:
    """Summary of a publish run."""

    discovered: int = 0
    committed: int = 0
    errors: int = 0
    details: list[str] = field(default_factory=list)


class Publisher:
    """Discover, normalize, and append parquet files to an Iceberg table."""

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
        """Execute the full discover → normalize → append pipeline for a table."""
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
            result.details.append("Dry run — skipping append.")
            return result

        after_str = after.isoformat() if after else None
        before_str = before.isoformat() if before else None

        committed, errors = self._append(
            table_name, limit=limit, after=after_str, before=before_str, providers=providers
        )
        result.committed = committed
        result.errors = errors

        duration_ms = int((time.monotonic() - run_start) * 1000)
        logger.info(
            {
                "event": "run_completed",
                "table": table_name,
                "discovered": result.discovered,
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

    def _append(
        self,
        table_name: str,
        limit: int | None = None,
        after: str | None = None,
        before: str | None = None,
        providers: list[str] | None = None,
    ) -> tuple[int, int]:
        """Normalize and append pending files to Iceberg. Returns (committed, errors)."""
        pending = self._ledger.pending_upload(
            table_name, after=after, before=before, providers=providers
        )
        if limit is not None:
            pending = pending[:limit]
        if not pending:
            return 0, 0

        catalog = self._iceberg_catalog()
        full_name = f"{self._config.glue_database}.{table_name}"
        iceberg_table = catalog.load_table(full_name)
        table_dir = self._config.data_root / table_name

        committed = 0
        errors = 0

        for record in pending:
            local_path = table_dir / record.path

            if not local_path.exists():
                self._ledger.mark_error(record.path, table_name, "Local file not found")
                logger.error(
                    {
                        "event": "append_error",
                        "table": table_name,
                        "path": record.path,
                        "error": "Local file not found",
                    }
                )
                errors += 1
                continue

            try:
                arrow_table = pq.read_table(local_path)
                normalized, diagnostics = normalize_table(arrow_table)
                if diagnostics:
                    logger.info(
                        {
                            "event": "normalize_applied",
                            "table": table_name,
                            "path": record.path,
                            "diagnostics": diagnostics,
                        }
                    )
                iceberg_table.append(normalized)
                self._ledger.mark_committed([record.path], table_name)
                committed += 1
                logger.info(
                    {
                        "event": "file_committed",
                        "table": table_name,
                        "path": record.path,
                        "size_bytes": record.size_bytes,
                        "provider": record.provider,
                        "root": record.root,
                        "sample_date": record.sample_date,
                    }
                )
            except Exception as exc:
                self._ledger.mark_error(record.path, table_name, str(exc))
                logger.error(
                    {
                        "event": "append_error",
                        "table": table_name,
                        "path": record.path,
                        "error_class": type(exc).__name__,
                        "error_message": str(exc),
                    }
                )
                errors += 1

        logger.info(
            {
                "event": "append_batch_completed",
                "table": table_name,
                "committed": committed,
                "errors": errors,
            }
        )
        return committed, errors

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
