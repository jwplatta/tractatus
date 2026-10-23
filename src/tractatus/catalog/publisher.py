"""Orchestrates discovery, normalization, and Iceberg append of data files."""

from __future__ import annotations

import io
import logging
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date
from pathlib import PurePosixPath
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog, load_catalog

from tractatus.catalog.config import CatalogConfig
from tractatus.catalog.discovery import data_dir_name, discover_files
from tractatus.catalog.file_store import FileStore, LocalFileStore, S3FileStore
from tractatus.catalog.ledger import Ledger
from tractatus.catalog.normalize import normalize_table
from tractatus.catalog.normalize.common import csv_to_arrow, ndjson_to_arrow
from tractatus.catalog.schemas import TABLE_DEFS
from tractatus.catalog.validate import QualityReport, validate_options

logger = logging.getLogger("tractatus.catalog")

_DATE_DIR_RE = re.compile(r"(\d{4})/(\d{2})/(\d{2})")


def _sort_table(table: pa.Table, sort_keys: list[str]) -> pa.Table:
    """Sort an Arrow table by the given keys. No-op if sort_keys is empty."""
    if not sort_keys:
        return table
    indices = pc.sort_indices(table, sort_keys=[(k, "ascending") for k in sort_keys])
    return table.take(indices)


def _metadata_from_path(
    relative_path: str, table_name: str
) -> tuple[date | None, str | None, str | None]:
    """Derive (sample_date, symbol, interval) from a file's relative path."""
    parts = PurePosixPath(relative_path).parts

    if table_name in ("level_one_equities", "level_one_futures", "order_book"):
        m = _DATE_DIR_RE.search(relative_path)
        if m:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3))), None, None
        return None, None, None

    if table_name == "candles":
        # provider/YYYY/MM/DD/frequency/SYMBOL.csv
        if len(parts) >= 6:
            m = _DATE_DIR_RE.search(relative_path)
            if m:
                sample_dt = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                interval = parts[4]
                symbol = PurePosixPath(parts[-1]).stem
                return sample_dt, symbol, interval
        return None, None, None

    return None, None, None


@dataclass
class PublishResult:
    """Summary of a publish run."""

    discovered: int = 0
    committed: int = 0
    cached: int = 0
    errors: int = 0
    details: list[str] = field(default_factory=list)


class Publisher:
    """Discover, normalize, and append data files to an Iceberg table."""

    def __init__(self, config: CatalogConfig) -> None:
        self._config = config
        self._ledger = Ledger(config.database_url)
        self._store = self._build_store()
        self._cache_client = self._build_cache_client()

    def _build_store(self) -> FileStore:
        """Build the appropriate file store from config."""
        if self._config.minio_endpoint:
            return S3FileStore(
                endpoint_url=self._config.minio_endpoint,
                bucket=self._config.minio_bucket or "",
                access_key=self._config.minio_access_key or "",
                secret_key=self._config.minio_secret_key or "",
            )
        return LocalFileStore(self._config.data_root)

    def _build_cache_client(self) -> Any:
        """Build an S3 client for the local cache MinIO bucket, if configured."""
        if not (self._config.minio_endpoint and self._config.cache_bucket):
            return None
        return boto3.client(
            "s3",
            endpoint_url=self._config.minio_endpoint,
            aws_access_key_id=self._config.minio_access_key or "",
            aws_secret_access_key=self._config.minio_secret_key or "",
            region_name="us-east-1",
        )

    def close(self) -> None:
        self._ledger.close()

    def run(
        self,
        table_name: str,
        dry_run: bool = False,
        skip_discover: bool = False,
        providers: list[str] | None = None,
        limit: int | None = None,
        after: date | None = None,
        before: date | None = None,
    ) -> PublishResult:
        """Execute the discover → normalize → append pipeline for a table.

        Pass ``skip_discover=True`` to skip the MinIO scan and only process
        files already recorded in the ledger as ``discovered``.
        """
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

        if not skip_discover:
            result.discovered = self._discover(table_name, providers, after=after, before=before)
        if dry_run:
            result.details.append("Dry run — skipping append.")
            return result

        after_str = after.isoformat() if after else None
        before_str = before.isoformat() if before else None

        committed, cached, errors = self._append(
            table_name, limit=limit, after=after_str, before=before_str, providers=providers
        )
        result.committed = committed
        result.cached = cached
        result.errors = errors

        duration_ms = int((time.monotonic() - run_start) * 1000)
        logger.info(
            {
                "event": "run_completed",
                "table": table_name,
                "discovered": result.discovered,
                "committed": result.committed,
                "cached": result.cached,
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
        """Scan the file store for data files and record them in the ledger."""
        discover_start = time.monotonic()
        files = discover_files(
            self._store, table_name=table_name, providers=providers, after=after, before=before
        )
        discover_ms = int((time.monotonic() - discover_start) * 1000)

        ledger_start = time.monotonic()
        for f in files:
            self._ledger.record_discovered(
                path=f.relative_path,
                table_name=table_name,
                sha256=f.sha256,
                size_bytes=f.size_bytes,
                provider=f.provider,
                root=f.root,
                sample_date=f.sample_date.isoformat(),
                file_format=f.file_format,
            )
        ledger_ms = int((time.monotonic() - ledger_start) * 1000)

        logger.info(
            {
                "event": "discovery_completed",
                "table": table_name,
                "file_count": len(files),
                "total_bytes": sum(f.size_bytes for f in files),
                "discover_ms": discover_ms,
                "ledger_ms": ledger_ms,
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
    ) -> tuple[int, int, int]:
        """Normalize and append pending files to Iceberg. Returns (committed, cached, errors)."""
        pending = self._ledger.pending(table_name, after=after, before=before, providers=providers)
        if not pending:
            return 0, 0, 0

        catalog = self._iceberg_catalog()
        full_name = f"{self._config.glue_database}.{table_name}"
        iceberg_table = catalog.load_table(full_name)
        table_prefix = data_dir_name(table_name)
        table_def = TABLE_DEFS[table_name]

        committed = 0
        cached = 0
        errors = 0

        # Group all pending records by (provider, root, sample_date) so all
        # CSVs for a given ticker on a given day are concatenated into a single
        # Arrow table and committed as one Iceberg file.
        groups: dict[tuple[str, str, str], list[Any]] = defaultdict(list)
        for record in pending:
            groups[(record.provider, record.root, record.sample_date)].append(record)

        group_items = list(groups.items())
        if limit is not None:
            group_items = group_items[:limit]

        for (provider, root, sample_date_str), group_records in group_items:
            paths = [r.path for r in group_records]
            try:
                sample_dt = (
                    date.fromisoformat(sample_date_str)
                    if isinstance(sample_date_str, str)
                    else sample_date_str
                )

                # Read and parse each file into a raw Arrow table (pre-normalize)
                tables = []
                for record in group_records:
                    full_key = f"{table_prefix}/{record.path}"
                    if record.file_format == "csv":
                        tables.append(csv_to_arrow(self._store.read_raw(full_key)))
                    elif record.file_format == "ndjson":
                        tables.append(ndjson_to_arrow(self._store.read_raw(full_key)))
                    else:
                        tables.append(self._store.read_parquet(full_key))

                # Concatenate then normalize once for the whole group
                _, _, interval = _metadata_from_path(group_records[0].path, table_name)
                combined = pa.concat_tables(tables, promote_options="default")
                normalized, diagnostics = normalize_table(
                    combined, table_name, sample_date=sample_dt, symbol=root, interval=interval
                )

                if diagnostics:
                    logger.info(
                        {
                            "event": "normalize_applied",
                            "table": table_name,
                            "group": f"{provider}/{root}/{sample_date_str}",
                            "files": len(group_records),
                            "diagnostics": diagnostics,
                        }
                    )

                # Validate once for the whole group
                quality: QualityReport | None = None
                if table_name == "options":
                    normalized, quality = validate_options(
                        normalized,
                        context={
                            "provider": provider,
                            "root": root,
                            "sample_date": sample_date_str,
                        },
                    )
                    if quality.rows_degraded:
                        logger.warning(
                            {
                                "event": "sample_quality_degraded",
                                "table": table_name,
                                "group": f"{provider}/{root}/{sample_date_str}",
                                "rows_total": quality.rows_total,
                                "rows_degraded": quality.rows_degraded,
                                "flags": sorted(quality.flags_seen),
                            }
                        )

                # Pre-sort before Iceberg append (PyIceberg doesn't sort)
                normalized = _sort_table(normalized, table_def.sort_keys)

                iceberg_table.append(normalized)
                self._ledger.mark_committed(paths, table_name, quality=quality)
                committed += 1

                try:
                    snap = iceberg_table.current_snapshot()
                    raw = (
                        snap.summary["added-files-size"]
                        if snap and snap.summary and "added-files-size" in snap.summary
                        else None
                    )
                    parquet_bytes_added: int | None = int(raw) if raw is not None else None
                except Exception:
                    parquet_bytes_added = None

                # Upload combined table to cache MinIO if configured
                cache_key = self._cache_to_minio(normalized, table_name, group_records[0].path)
                if cache_key:
                    for path in paths:
                        self._ledger.mark_cached(path, table_name, cache_key)
                    cached += 1

                # Record the published artifact
                artifact_key = (
                    cache_key
                    or f"{table_name}/{provider}/{sample_date_str[:4]}/{sample_date_str[5:7]}/{sample_date_str[8:10]}/{root}_samples_{sample_date_str}.parquet"
                )
                try:
                    self._ledger.upsert_published(
                        artifact_key=artifact_key,
                        table_name=table_name,
                        provider=provider,
                        root=root,
                        sample_date=sample_date_str,
                        interval=interval,
                        committed_at=self._ledger._now(),
                        cached=cache_key is not None,
                        cached_at=self._ledger._now() if cache_key else None,
                        row_count=normalized.num_rows,
                        size_bytes=parquet_bytes_added,
                    )
                except Exception as pub_exc:
                    logger.warning(
                        {
                            "event": "published_artifacts_write_failed",
                            "artifact_key": artifact_key,
                            "error": str(pub_exc),
                        }
                    )

                logger.info(
                    {
                        "event": "group_committed",
                        "table": table_name,
                        "provider": provider,
                        "root": root,
                        "sample_date": sample_date_str,
                        "files": len(group_records),
                        "parquet_bytes_added": parquet_bytes_added,
                        "cached": cache_key is not None,
                    }
                )
            except Exception as exc:
                for path in paths:
                    self._ledger.mark_error(path, table_name, str(exc))
                logger.error(
                    {
                        "event": "append_error",
                        "table": table_name,
                        "group": f"{provider}/{root}/{sample_date_str}",
                        "error_class": type(exc).__name__,
                        "error_message": str(exc),
                    }
                )
                errors += len(group_records)

        logger.info(
            {
                "event": "append_batch_completed",
                "table": table_name,
                "committed": committed,
                "cached": cached,
                "errors": errors,
            }
        )
        return committed, cached, errors

    def _cache_to_minio(
        self,
        table: pa.Table,
        table_name: str,
        path: str,
    ) -> str | None:
        """Write parquet to the cache MinIO bucket. Returns cache_key or None."""
        if self._cache_client is None:
            return None

        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        # Cache key mirrors the staging path but with .parquet extension
        stem = PurePosixPath(path).stem
        parent = str(PurePosixPath(path).parent)
        cache_key = f"{table_name}/{parent}/{stem}.parquet"

        self._cache_client.put_object(
            Bucket=self._config.cache_bucket,
            Key=cache_key,
            Body=buf.getvalue(),
        )
        return cache_key

    def _write_to_cache(
        self, table: pa.Table, cache_key: str, overwrite: bool = True
    ) -> int | None:
        """Write an Arrow table as parquet to the cache MinIO bucket at cache_key.

        If overwrite is False and the object already exists, returns None without writing.
        Returns the number of bytes written, or None if no cache client is configured or skipped.
        """
        if self._cache_client is None:
            return None

        if not overwrite:
            try:
                self._cache_client.head_object(Bucket=self._config.cache_bucket, Key=cache_key)
                # Object exists — skip
                print(f"Skipped {cache_key} — already cached. Re-run with --overwrite to replace.")
                return None
            except Exception:
                pass  # Object does not exist; proceed with write

        buf = io.BytesIO()
        pq.write_table(table, buf)
        data = buf.getvalue()

        self._cache_client.put_object(
            Bucket=self._config.cache_bucket,
            Key=cache_key,
            Body=data,
        )
        return len(data)

    # --- Table grain definitions for cache_artifacts ---
    #
    # Maps table_name → (date_column | None, grain_columns)
    # date_column: the Iceberg column used for --after/--before filtering (None if N/A)
    # grain_columns: PyArrow column names that define each artifact group
    _TABLE_GRAIN: dict[str, tuple[str | None, tuple[str, ...]]] = {
        "options": ("sampled_at", ("provider", "root", "sample_date")),
        "level_one_equities": ("sampled_at", ("provider", "root", "sample_date")),
        "level_one_futures": ("sampled_at", ("provider", "root", "sample_date")),
        "order_book": ("sampled_at", ("provider", "root", "sample_date")),
        "candles": (None, ("provider", "interval", "root")),
        "fundamentals": ("sampled_at", ("provider", "sample_date")),
    }

    @staticmethod
    def _build_cache_key(table_name: str, group: dict[str, str | None]) -> str:
        """Construct the MinIO cache key for a given artifact group."""
        provider = group.get("provider") or "unknown"
        root = group.get("root") or ""
        sample_date = group.get("sample_date") or ""
        interval = group.get("interval") or ""

        if table_name == "options":
            yyyy, mm, dd = sample_date[:4], sample_date[5:7], sample_date[8:10]
            return f"options/{provider}/{yyyy}/{mm}/{dd}/{root}_samples_{sample_date}.parquet"

        if table_name in ("level_one_equities", "level_one_futures", "order_book"):
            yyyy, mm, dd = sample_date[:4], sample_date[5:7], sample_date[8:10]
            return f"{table_name}/{provider}/{yyyy}/{mm}/{dd}/{root}.parquet"

        if table_name == "candles":
            return f"candles/{provider}/{interval}/{root}.parquet"

        if table_name == "fundamentals":
            yyyy, mm, dd = sample_date[:4], sample_date[5:7], sample_date[8:10]
            return f"fundamentals/{provider}/{yyyy}/{mm}/{dd}.parquet"

        raise ValueError(f"Unknown table for cache key: {table_name!r}")

    def cache_artifacts(
        self,
        table_name: str,
        providers: list[str] | None = None,
        after: date | None = None,
        before: date | None = None,
        roots: list[str] | None = None,
        interval: str | None = None,
        overwrite: bool = True,
    ) -> int:
        """Read from Iceberg, group by artifact grain, write to MinIO cache, upsert ledger.

        Returns the count of artifacts successfully cached.
        """
        if table_name not in TABLE_DEFS:
            raise ValueError(
                f"Unknown table {table_name!r}. Registered tables: {', '.join(sorted(TABLE_DEFS))}"
            )
        if self._cache_client is None:
            raise RuntimeError(
                "No MinIO cache client configured. Set MINIO_ENDPOINT and MINIO_CACHE_BUCKET."
            )

        grain_def = self._TABLE_GRAIN.get(table_name)
        if grain_def is None:
            raise ValueError(f"No artifact grain defined for table {table_name!r}")

        date_col, grain_cols = grain_def

        catalog = self._iceberg_catalog()
        full_name = f"{self._config.glue_database}.{table_name}"
        iceberg_table = catalog.load_table(full_name)

        # Build PyIceberg row filter
        filter_parts: list[str] = []
        if date_col and after:
            filter_parts.append(f"{date_col} >= '{after.isoformat()}T00:00:00+00:00'")
        if date_col and before:
            filter_parts.append(f"{date_col} < '{before.isoformat()}T00:00:00+00:00'")
        row_filter: Any = " and ".join(filter_parts) if filter_parts else None

        scan = iceberg_table.scan(row_filter=row_filter) if row_filter else iceberg_table.scan()
        arrow_table = scan.to_arrow()

        if arrow_table.num_rows == 0:
            logger.info({"event": "cache_artifacts_no_rows", "table": table_name})
            return 0

        # Apply provider / root / interval filters in Arrow (not pushed to Iceberg)
        if providers and "provider" in arrow_table.schema.names:
            mask = pc.is_in(
                arrow_table["provider"], value_set=pa.array(providers, type=pa.string())
            )
            arrow_table = arrow_table.filter(mask)
        if roots and "root" in arrow_table.schema.names:
            mask = pc.is_in(arrow_table["root"], value_set=pa.array(roots, type=pa.string()))
            arrow_table = arrow_table.filter(mask)
        if interval and "interval" in arrow_table.schema.names:
            mask = pc.equal(arrow_table["interval"], interval)
            arrow_table = arrow_table.filter(mask)

        if arrow_table.num_rows == 0:
            logger.info({"event": "cache_artifacts_filtered_empty", "table": table_name})
            return 0

        # Group by grain columns, building unique groups via a dict keyed by grain tuple
        present_grain_cols = [c for c in grain_cols if c in arrow_table.schema.names]
        group_dict: dict[tuple[str, ...], list[int]] = {}
        for i in range(arrow_table.num_rows):
            key = tuple((arrow_table[c][i].as_py() or "") for c in present_grain_cols)
            if key not in group_dict:
                group_dict[key] = []
            group_dict[key].append(i)

        cached_count = 0
        for key_values, row_indices in group_dict.items():
            group_meta: dict[str, str | None] = dict(
                zip(present_grain_cols, key_values, strict=True)
            )
            # Fill in any grain cols not present in table
            for col in grain_cols:
                if col not in group_meta:
                    group_meta[col] = None

            indices_array = pa.array(row_indices, type=pa.int64())
            group_table = arrow_table.take(indices_array)

            try:
                cache_key = self._build_cache_key(table_name, group_meta)
                size_bytes = self._write_to_cache(group_table, cache_key, overwrite=overwrite)
                if size_bytes is None and not overwrite:
                    # Skipped — object already exists
                    continue

                # Normalize sample_date to YYYY-MM-DD
                sample_date_str: str | None = group_meta.get("sample_date")
                if sample_date_str and "T" in sample_date_str:
                    sample_date_str = sample_date_str[:10]

                self._ledger.upsert_published(
                    artifact_key=cache_key,
                    table_name=table_name,
                    provider=group_meta.get("provider"),
                    root=group_meta.get("root"),
                    sample_date=sample_date_str,
                    interval=group_meta.get("interval"),
                    cached=True,
                    cached_at=self._ledger._now(),
                    row_count=group_table.num_rows,
                    size_bytes=size_bytes,
                )
                cached_count += 1
                logger.info(
                    {
                        "event": "artifact_cached",
                        "table": table_name,
                        "cache_key": cache_key,
                        "rows": group_table.num_rows,
                        "size_bytes": size_bytes,
                    }
                )
            except Exception as exc:
                logger.error(
                    {
                        "event": "cache_artifact_error",
                        "table": table_name,
                        "group": group_meta,
                        "error_class": type(exc).__name__,
                        "error_message": str(exc),
                    }
                )

        logger.info(
            {
                "event": "cache_artifacts_completed",
                "table": table_name,
                "cached": cached_count,
                "groups": len(group_dict),
            }
        )
        return cached_count

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
