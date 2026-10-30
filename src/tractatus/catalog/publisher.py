"""Orchestrates discovery, normalization, and Iceberg append of data files."""

from __future__ import annotations

import io
import logging
import re
import time
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import PurePosixPath
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog, load_catalog
from tqdm import tqdm

from tractatus.catalog.config import CatalogConfig
from tractatus.catalog.discovery import DiscoveredFile, data_dir_name, discover_files
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

    def __init__(self, config: CatalogConfig, verbose: bool = False) -> None:
        self._config = config
        self._verbose = verbose
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
        cache: bool = False,
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
            table_name,
            limit=limit,
            after=after_str,
            before=before_str,
            providers=providers,
            cache=cache,
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
        """Scan the file store for data files and record them in the ledger.

        When a date range is given, iterates one day at a time and records
        results incrementally — each day's files are written to the ledger
        before moving to the next day.
        """
        total_files = 0
        total_bytes = 0
        discover_start = time.monotonic()

        date_range_days = (before - after).days if after and before else None
        gen = discover_files(
            self._store, table_name=table_name, providers=providers, after=after, before=before
        )

        iterable: Iterable[tuple[date, list[DiscoveredFile]]] = (
            tqdm(gen, total=date_range_days, desc=f"discover {table_name}", unit="day")
            if self._verbose
            else gen
        )

        for _scan_date, day_files in iterable:
            for f in day_files:
                self._ledger.record_discovered(
                    path=f.relative_path,
                    table_name=table_name,
                    size_bytes=f.size_bytes,
                    provider=f.provider,
                    symbol=f.symbol,
                    sample_date=f.sample_date.isoformat(),
                    file_format=f.file_format,
                )
            total_files += len(day_files)
            total_bytes += sum(f.size_bytes for f in day_files)

        discover_ms = int((time.monotonic() - discover_start) * 1000)
        logger.info(
            {
                "event": "discovery_completed",
                "table": table_name,
                "file_count": total_files,
                "total_bytes": total_bytes,
                "discover_ms": discover_ms,
            }
        )
        return total_files

    def _append(
        self,
        table_name: str,
        limit: int | None = None,
        after: str | None = None,
        before: str | None = None,
        providers: list[str] | None = None,
        cache: bool = False,
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

        # Group all pending records by (provider, option_root, sample_date) so all
        # CSVs for a given option_root on a given day are concatenated into a single
        # Arrow table and committed as one Iceberg file per option_root per day.
        # For non-options tables, record.symbol is the underlying ticker (same role).
        groups: dict[tuple[str, str, str], list[Any]] = defaultdict(list)
        for record in pending:
            groups[(record.provider, record.symbol, record.sample_date)].append(record)

        group_items = list(groups.items())
        if limit is not None:
            group_items = group_items[:limit]

        for (provider, root, sample_date_str), group_records in group_items:
            paths = [r.path for r in group_records]

            # Computed before any I/O — stable across retries for the same input set
            group_id = f"{table_name}/{provider}/{root}/{sample_date_str}"
            input_bytes: int = sum(r.size_bytes for r in group_records)

            # Stage 1: normalize, validate, append to Iceberg
            # Failures here mark files as errors and skip to the next group.
            normalized: pa.Table | None = None
            interval: str | None = None
            iceberg_bytes: int | None = None
            snapshot_id: int | None = None
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
                    combined,
                    table_name,
                    sample_date=sample_dt,
                    symbol=root,
                    interval=interval,
                    provider=provider,
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
                    if snap:
                        snapshot_id = snap.snapshot_id
                        raw = snap.summary.get("added-files-size") if snap.summary else None
                        iceberg_bytes = int(raw) if raw is not None else None
                except Exception:
                    pass

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
                continue

            # Stage 2: cache write
            # Committed files are never marked as errors here. A cache failure logs a
            # warning and sets cache_outcome="cache_failed" but does not affect the
            # committed state or suppress the group_committed event.
            cache_key: str | None = None
            cache_bytes: int | None = None
            cache_outcome = "not_attempted"
            if cache and normalized is not None:
                try:
                    cache_key, cache_bytes = self._cache_to_minio(normalized, table_name, group_records[0].path)
                    if cache_key:
                        for path in paths:
                            self._ledger.mark_cached(path, table_name, cache_key)
                        cached += 1
                        cache_outcome = "cached"
                except Exception as cache_exc:
                    cache_outcome = "cache_failed"
                    logger.warning(
                        {
                            "event": "cache_write_failed",
                            "group_id": group_id,
                            "table": table_name,
                            "error_class": type(cache_exc).__name__,
                            "error_message": str(cache_exc),
                        }
                    )

            # Stage 3: record and emit
            # group_committed is always emitted when Iceberg commit succeeded,
            # regardless of cache outcome.
            artifact_key = (
                cache_key
                or f"{table_name}/{provider}/{sample_date_str[:4]}/{sample_date_str[5:7]}/{sample_date_str[8:10]}/{root}_samples_{sample_date_str}.parquet"
            )
            try:
                self._ledger.upsert_published(
                    artifact_key=artifact_key,
                    table_name=table_name,
                    provider=provider,
                    symbol=root,
                    sample_date=sample_date_str,
                    interval=interval,
                    committed_at=self._ledger._now(),
                    cached=cache_key is not None,
                    cached_at=self._ledger._now() if cache_key else None,
                    row_count=normalized.num_rows,
                    size_bytes=iceberg_bytes,
                )
            except Exception as pub_exc:
                logger.warning(
                    {
                        "event": "published_artifacts_write_failed",
                        "artifact_key": artifact_key,
                        "error": str(pub_exc),
                    }
                )

            # Byte field semantics:
            #   input_bytes   — always int; sum of raw file sizes from ledger
            #   iceberg_bytes — None if snapshot unavailable; int if measured
            #   cache_bytes   — None if not attempted or failed; int if measured
            #   snapshot_id   — Iceberg snapshot ID; unique per commit; deduplicate retries
            #   cache_outcome — "not_attempted" | "cached" | "cache_failed"
            logger.info(
                {
                    "event": "group_committed",
                    "group_id": group_id,
                    "snapshot_id": snapshot_id,
                    "table": table_name,
                    "provider": provider,
                    "root": root,
                    "sample_date": sample_date_str,
                    "files": len(group_records),
                    "input_bytes": input_bytes,
                    "iceberg_bytes": iceberg_bytes,
                    "cache_bytes": cache_bytes,
                    "cache_outcome": cache_outcome,
                    "cached": cache_key is not None,
                }
            )

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
    ) -> tuple[str | None, int | None]:
        """Write parquet to the cache MinIO bucket. Returns (cache_key, bytes) or (None, None)."""
        if self._cache_client is None:
            return None, None

        buf = io.BytesIO()
        pq.write_table(table, buf)
        data = buf.getvalue()

        # Cache key mirrors the staging path but with .parquet extension
        stem = PurePosixPath(path).stem
        parent = str(PurePosixPath(path).parent)
        cache_key = f"{table_name}/{parent}/{stem}.parquet"

        self._cache_client.put_object(
            Bucket=self._config.cache_bucket,
            Key=cache_key,
            Body=data,
        )
        return cache_key, len(data)

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
    # grain_columns reference actual Iceberg schema column names.
    # For tables with sampled_at, a derived "sample_date" (YYYY-MM-DD string) column
    # is added from sampled_at before grouping — it is not a native schema column.
    _TABLE_GRAIN: dict[str, tuple[str | None, tuple[str, ...]]] = {
        "options": ("sampled_at", ("provider", "option_root", "sample_date")),
        "level_one_equities": ("received_at", ("provider", "symbol", "sample_date")),
        "level_one_futures": ("received_at", ("provider", "symbol", "sample_date")),
        "order_book": ("received_at", ("provider", "symbol", "sample_date")),
        "candles": (None, ("provider", "interval", "symbol")),
        "fundamentals": ("sampled_at", ("provider", "sample_date")),
    }

    @staticmethod
    def _build_cache_key(table_name: str, group: dict[str, str | None]) -> str:
        """Construct the MinIO cache key for a given artifact group."""
        provider = group.get("provider") or "unknown"
        # options groups by option_root; all other tables group by symbol
        symbol = group.get("option_root") or group.get("symbol") or ""
        sample_date = group.get("sample_date") or ""
        interval = group.get("interval") or ""

        if table_name == "options":
            yyyy, mm, dd = sample_date[:4], sample_date[5:7], sample_date[8:10]
            return f"options/{provider}/{yyyy}/{mm}/{dd}/{symbol}_samples_{sample_date}.parquet"

        if table_name in ("level_one_equities", "level_one_futures", "order_book"):
            yyyy, mm, dd = sample_date[:4], sample_date[5:7], sample_date[8:10]
            return f"{table_name}/{provider}/{yyyy}/{mm}/{dd}/{symbol}.parquet"

        if table_name == "candles":
            return f"candles/{provider}/{interval}/{symbol}.parquet"

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

        # For tables with a date column, iterate one day at a time to avoid loading
        # the entire table into memory. For candles (no date_col), load all at once.
        if date_col:
            scan_start = after or date(2000, 1, 1)
            scan_end = before or date(2100, 1, 1)
            current = scan_start
            dates_to_scan: list[date] = []
            while current < scan_end:
                dates_to_scan.append(current)
                current = date.fromordinal(current.toordinal() + 1)
        else:
            dates_to_scan = [None]  # type: ignore[list-item]

        root_col = "option_root" if table_name == "options" else "symbol"
        cached_count = 0

        iterable = (
            tqdm(dates_to_scan, desc=f"cache {table_name}", unit="day")
            if self._verbose
            else dates_to_scan
        )
        for scan_date in iterable:
            if scan_date is not None:
                day_filter = (
                    f"{date_col} >= '{scan_date.isoformat()}T00:00:00+00:00'"
                    f" and {date_col} < '{date.fromordinal(scan_date.toordinal() + 1).isoformat()}T00:00:00+00:00'"
                )
                arrow_table = iceberg_table.scan(row_filter=day_filter).to_arrow()
            else:
                arrow_table = iceberg_table.scan().to_arrow()

            if arrow_table.num_rows == 0:
                continue

            # Apply in-Arrow filters
            if providers and "provider" in arrow_table.schema.names:
                mask = pc.is_in(
                    arrow_table["provider"], value_set=pa.array(providers, type=pa.string())
                )
                arrow_table = arrow_table.filter(mask)
            if roots and root_col in arrow_table.schema.names:
                mask = pc.is_in(arrow_table[root_col], value_set=pa.array(roots, type=pa.string()))
                arrow_table = arrow_table.filter(mask)
            if interval and "interval" in arrow_table.schema.names:
                mask = pc.equal(arrow_table["interval"], interval)
                arrow_table = arrow_table.filter(mask)

            if arrow_table.num_rows == 0:
                continue

            # Derive sample_date (YYYY-MM-DD string) from the table's date column for grain grouping
            if (
                date_col
                and date_col in arrow_table.schema.names
                and "sample_date" not in arrow_table.schema.names
            ):
                dates = pc.strftime(arrow_table[date_col], format="%Y-%m-%d")
                arrow_table = arrow_table.append_column("sample_date", dates)

            # Group by grain columns using pandas groupby (avoids O(n) Python row loop)
            present_grain_cols = [c for c in grain_cols if c in arrow_table.schema.names]
            pdf = arrow_table.to_pandas()

            for key_values, group_df in pdf.groupby(present_grain_cols, sort=False):
                if not isinstance(key_values, tuple):
                    key_values = (key_values,)
                group_meta: dict[str, str | None] = dict(
                    zip(present_grain_cols, key_values, strict=True)
                )
                for col in grain_cols:
                    if col not in group_meta:
                        group_meta[col] = None

                group_table = pa.Table.from_pandas(group_df, preserve_index=False)

                try:
                    cache_key = self._build_cache_key(table_name, group_meta)
                    size_bytes = self._write_to_cache(group_table, cache_key, overwrite=overwrite)
                    if size_bytes is None and not overwrite:
                        continue

                    # Normalize sample_date to YYYY-MM-DD
                    sample_date_str: str | None = group_meta.get("sample_date")
                    if sample_date_str and "T" in sample_date_str:
                        sample_date_str = sample_date_str[:10]

                    self._ledger.upsert_published(
                        artifact_key=cache_key,
                        table_name=table_name,
                        provider=group_meta.get("provider"),
                        symbol=group_meta.get("option_root") or group_meta.get("symbol"),
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
            }
        )
        return cached_count

    def expire_snapshots(self, table_name: str) -> int:
        """Expire all snapshots older than now for the given table.

        Returns the number of snapshots expired.
        """
        catalog = self._iceberg_catalog()
        full_name = f"{self._config.glue_database}.{table_name}"
        iceberg_table = catalog.load_table(full_name)
        before_count = len(iceberg_table.history())
        iceberg_table.maintenance.expire_snapshots().older_than(dt=datetime.now(tz=UTC)).commit()
        iceberg_table = catalog.load_table(full_name)
        after_count = len(iceberg_table.history())
        expired = before_count - after_count
        logger.info(
            "expire_snapshots table=%s before=%d after=%d expired=%d",
            table_name,
            before_count,
            after_count,
            expired,
        )
        return expired

    def _iceberg_catalog(self) -> Catalog:
        """Load PyIceberg catalog — SQL (local) or Glue (AWS) based on config."""
        if self._config.catalog_type == "sql":
            if not self._config.iceberg_postgres_url:
                raise ValueError("catalog_type=sql requires ICEBERG_POSTGRES_URL to be set")
            props: dict[str, str] = {
                "type": "sql",
                "uri": self._config.iceberg_postgres_url,
                "warehouse": self._config.iceberg_warehouse,
            }
            if self._config.minio_endpoint:
                props["s3.endpoint"] = self._config.minio_endpoint
                props["s3.path-style-access"] = "true"
            if self._config.minio_access_key:
                props["s3.access-key-id"] = self._config.minio_access_key
            if self._config.minio_secret_key:
                props["s3.secret-access-key"] = self._config.minio_secret_key
            return load_catalog("sql", **props)

        props = {
            "type": "glue",
            "glue.region": self._config.aws_region,
            "s3.region": self._config.aws_region,
            "warehouse": self._config.iceberg_warehouse,
        }
        if self._config.aws_profile:
            props["glue.profile-name"] = self._config.aws_profile
            props["s3.profile-name"] = self._config.aws_profile
        return load_catalog("glue", **props)
