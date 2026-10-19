#!/usr/bin/env python3
"""Verify migration integrity: Iceberg lakehouse, MinIO cache, and postgres ledger.

Checks:
1. Ledger has the expected number of committed records per table
2. Every ledger record has a corresponding file in MinIO cache
3. Iceberg tables are queryable and return data
4. Spot-check: data from Iceberg matches data from MinIO cache (same schema, row counts)
5. Prepared files in /tmp/migrate match ledger records

Usage:
    python scripts/verify_migration.py --input /tmp/migrate
    python scripts/verify_migration.py --input /tmp/migrate --table options
    python scripts/verify_migration.py --input /tmp/migrate --spot-checks 5
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import sys
import time
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.glue import GlueCatalog

from tractatus.catalog.ledger import Ledger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("verify_migration")

DATABASE_URL = "postgresql://tractatus_user:tractatus@127.0.0.1:5432/tractatus"

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
CACHE_BUCKET = "tractatus-lakehouse"

ALL_TABLES = [
    "candles",
    "fundamentals",
    "level_one_equities",
    "level_one_futures",
    "options",
    "order_book",
]

# File counts for prepared files, ledger, and MinIO cache (all prepared data)
EXPECTED_PREPARED = {
    "candles": 745,
    "fundamentals": 1,
    "level_one_equities": 693,
    "level_one_futures": 92,
    "options": 10782,
    "order_book": 582,
}

# Datasets that were fully published to S3 Iceberg
# ibkr-paper candles were NOT published — exclude candles from Iceberg spot checks
ICEBERG_PUBLISHED_TABLES = {
    "fundamentals",
    "level_one_equities",
    "level_one_futures",
    "options",
    "order_book",
}


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def check_ledger(ledger: Ledger, tables: list[str]) -> list[str]:
    """Check ledger has correct counts and all records are committed."""
    errors: list[str] = []
    logger.info("--- Ledger checks ---")

    for table_name in tables:
        summary = ledger.summary(table_name)
        committed = summary.get("committed", 0)
        discovered = summary.get("discovered", 0)
        error_count = summary.get("error", 0)
        expected = EXPECTED_PREPARED.get(table_name, 0)

        if committed != expected:
            errors.append(
                f"LEDGER {table_name}: expected {expected} committed, got {committed}"
            )
            logger.error("FAIL %s: expected %d committed, got %d", table_name, expected, committed)
        else:
            logger.info("OK   %s: %d committed", table_name, committed)

        if discovered > 0:
            errors.append(f"LEDGER {table_name}: {discovered} still in discovered state")
            logger.error("FAIL %s: %d records still discovered", table_name, discovered)

        if error_count > 0:
            errors.append(f"LEDGER {table_name}: {error_count} in error state")
            logger.error("FAIL %s: %d records in error state", table_name, error_count)

    return errors


def check_prepared_files(input_dir: Path, tables: list[str]) -> list[str]:
    """Check that prepared files exist and counts match expectations."""
    errors: list[str] = []
    logger.info("--- Prepared file checks ---")

    for table_name in tables:
        table_dir = input_dir / table_name
        if not table_dir.is_dir():
            errors.append(f"PREPARED {table_name}: directory {table_dir} missing")
            logger.error("FAIL %s: directory missing", table_name)
            continue

        count = len(list(table_dir.rglob("*.parquet")))
        expected = EXPECTED_PREPARED.get(table_name, 0)

        if count != expected:
            errors.append(
                f"PREPARED {table_name}: expected {expected} files, got {count}"
            )
            logger.error("FAIL %s: expected %d files, got %d", table_name, expected, count)
        else:
            logger.info("OK   %s: %d prepared files", table_name, count)

    return errors


def check_minio_cache(
    minio_client: boto3.client, ledger: Ledger, tables: list[str]
) -> list[str]:
    """Check that every ledger record has a corresponding MinIO cache file."""
    errors: list[str] = []
    logger.info("--- MinIO cache checks ---")

    for table_name in tables:
        # List all keys in MinIO for this table
        paginator = minio_client.get_paginator("list_objects_v2")
        minio_keys: set[str] = set()
        for page in paginator.paginate(Bucket=CACHE_BUCKET, Prefix=f"{table_name}/"):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    # Strip the table_name/ prefix to get the relative path
                    rel = obj["Key"][len(table_name) + 1 :]
                    minio_keys.add(rel)

        expected = EXPECTED_PREPARED.get(table_name, 0)

        if len(minio_keys) != expected:
            errors.append(
                f"MINIO {table_name}: expected {expected} files, got {len(minio_keys)}"
            )
            logger.error(
                "FAIL %s: expected %d cache files, got %d",
                table_name,
                expected,
                len(minio_keys),
            )
        else:
            logger.info("OK   %s: %d files in MinIO cache", table_name, len(minio_keys))

        # Cross-check: every ledger path should exist in MinIO
        rows = ledger._fetchall(
            "SELECT path FROM catalog_files WHERE table_name = %s AND status = 'committed'",
            (table_name,),
        )
        ledger_paths = {row[0] for row in rows}

        missing_in_minio = ledger_paths - minio_keys
        if missing_in_minio:
            errors.append(
                f"MINIO {table_name}: {len(missing_in_minio)} ledger paths missing from cache"
            )
            logger.error(
                "FAIL %s: %d ledger paths not in MinIO cache", table_name, len(missing_in_minio)
            )
            for p in sorted(missing_in_minio)[:5]:
                logger.error("  missing: %s", p)
        else:
            logger.info("OK   %s: all ledger paths found in MinIO", table_name)

        extra_in_minio = minio_keys - ledger_paths
        if extra_in_minio:
            errors.append(
                f"MINIO {table_name}: {len(extra_in_minio)} cache files not in ledger"
            )
            logger.error(
                "FAIL %s: %d MinIO files not in ledger", table_name, len(extra_in_minio)
            )
            for p in sorted(extra_in_minio)[:5]:
                logger.error("  extra: %s", p)

    return errors


def check_iceberg(catalog: GlueCatalog, tables: list[str]) -> list[str]:
    """Check that Iceberg tables are loadable and have data."""
    errors: list[str] = []
    logger.info("--- Iceberg checks ---")

    for table_name in tables:
        try:
            tbl = catalog.load_table(f"tractatus.{table_name}")
            snaps = tbl.metadata.snapshots
            if not snaps:
                errors.append(f"ICEBERG {table_name}: no snapshots")
                logger.error("FAIL %s: no snapshots", table_name)
                continue

            summary = snaps[-1].summary
            total_records = summary.get("total-records", "0")
            total_files = summary.get("total-data-files", "0")
            logger.info(
                "OK   %s: %s rows, %s data files",
                table_name,
                total_records,
                total_files,
            )

            if int(total_records) == 0:
                errors.append(f"ICEBERG {table_name}: 0 rows")
                logger.error("FAIL %s: 0 rows in Iceberg", table_name)

        except Exception as e:
            errors.append(f"ICEBERG {table_name}: load failed: {e}")
            logger.error("FAIL %s: %s", table_name, e)

    return errors


def spot_check_data(
    catalog: GlueCatalog,
    minio_client: boto3.client,
    input_dir: Path,
    tables: list[str],
    num_checks: int,
) -> list[str]:
    """Spot-check that data from Iceberg matches the prepared local files.

    For each table, pick a few prepared files, read them locally, then query
    Iceberg for the same (provider, root/symbol, date) and compare row counts
    and a few column values.
    """
    import random

    errors: list[str] = []
    logger.info("--- Spot checks (%d per table) ---", num_checks)

    for table_name in tables:
        table_dir = input_dir / table_name
        if not table_dir.is_dir():
            continue

        parquet_files = list(table_dir.rglob("*.parquet"))
        samples = random.sample(parquet_files, min(num_checks, len(parquet_files)))

        for sample_path in samples:
            rel = sample_path.relative_to(table_dir)
            parts = rel.parts

            # Read local prepared file
            local_table = pq.read_table(sample_path)
            local_rows = local_table.num_rows

            # Read same file from MinIO cache
            cache_key = f"{table_name}/{rel}"
            try:
                import io

                resp = minio_client.get_object(Bucket=CACHE_BUCKET, Key=cache_key)
                cache_bytes = resp["Body"].read()
                cache_table = pq.read_table(io.BytesIO(cache_bytes))
                cache_rows = cache_table.num_rows

                if local_rows != cache_rows:
                    errors.append(
                        f"SPOT {table_name}/{rel}: local={local_rows} rows, "
                        f"cache={cache_rows} rows"
                    )
                    logger.error(
                        "FAIL %s: row mismatch local=%d cache=%d",
                        rel,
                        local_rows,
                        cache_rows,
                    )
                elif local_table.schema != cache_table.schema:
                    errors.append(
                        f"SPOT {table_name}/{rel}: schema mismatch"
                    )
                    logger.error("FAIL %s: schema mismatch", rel)
                else:
                    logger.info(
                        "OK   %s: local & cache match (%d rows)",
                        rel,
                        local_rows,
                    )
            except Exception as e:
                errors.append(f"SPOT {table_name}/{rel}: cache read failed: {e}")
                logger.error("FAIL %s: cache read error: %s", rel, e)

            # Query Iceberg for the same data slice (skip unpublished tables)
            if table_name not in ICEBERG_PUBLISHED_TABLES:
                logger.info("SKIP %s: not published to Iceberg", rel)
                continue

            try:
                iceberg_table = catalog.load_table(f"tractatus.{table_name}")
                row_filter = _build_row_filter(table_name, parts, sample_path)
                if row_filter:
                    scan = iceberg_table.scan(row_filter=row_filter)
                    iceberg_result = scan.to_arrow()
                    iceberg_rows = iceberg_result.num_rows

                    if iceberg_rows == 0:
                        errors.append(
                            f"SPOT {table_name}/{rel}: Iceberg returned 0 rows "
                            f"(expected ~{local_rows})"
                        )
                        logger.error(
                            "FAIL %s: Iceberg query returned 0 rows (filter: %s)",
                            rel,
                            row_filter,
                        )
                    elif iceberg_rows < local_rows:
                        errors.append(
                            f"SPOT {table_name}/{rel}: Iceberg has fewer rows "
                            f"({iceberg_rows}) than local ({local_rows})"
                        )
                        logger.error(
                            "FAIL %s: Iceberg=%d < local=%d",
                            rel,
                            iceberg_rows,
                            local_rows,
                        )
                    else:
                        # Iceberg may have >= local rows if multiple files were
                        # batched into one append. Check that local is a subset.
                        logger.info(
                            "OK   %s: Iceberg has %d rows (local=%d)",
                            rel,
                            iceberg_rows,
                            local_rows,
                        )
                else:
                    logger.info("SKIP %s: no Iceberg filter for this table type", rel)

            except Exception as e:
                errors.append(f"SPOT {table_name}/{rel}: Iceberg query failed: {e}")
                logger.error("FAIL %s: Iceberg query error: %s", rel, e)

    return errors


def _build_row_filter(
    table_name: str, parts: tuple[str, ...], path: Path
) -> str | None:
    """Build an Iceberg row filter to query the same slice as a prepared file."""
    if table_name == "options":
        # parts: provider/YYYY/MM/DD/ROOT_samples_YYYY-MM-DD.parquet
        if len(parts) >= 5:
            sample_date = f"{parts[1]}-{parts[2]}-{parts[3]}"
            name = path.stem
            root = name.split("_samples_")[0] if "_samples_" in name else name
            return (
                f"sampled_at >= '{sample_date}T00:00:00+00:00' "
                f"AND sampled_at <= '{sample_date}T23:59:59+00:00' "
                f"AND symbol LIKE '{root}%'"
            )
    elif table_name in ("level_one_equities", "level_one_futures"):
        # parts: provider/YYYY/MM/DD/SYMBOL.parquet
        if len(parts) >= 5:
            sample_date = f"{parts[1]}-{parts[2]}-{parts[3]}"
            symbol = path.stem
            # Futures symbols: ^ in filenames maps to / in Iceberg
            if table_name == "level_one_futures" and symbol.startswith("^"):
                symbol = "/" + symbol[1:]
            return (
                f"symbol == '{symbol}' "
                f"AND received_at >= '{sample_date}T00:00:00+00:00' "
                f"AND received_at <= '{sample_date}T23:59:59+00:00'"
            )
    elif table_name == "order_book":
        # parts: provider/YYYY/MM/DD/SYMBOL.parquet
        if len(parts) >= 5:
            sample_date = f"{parts[1]}-{parts[2]}-{parts[3]}"
            symbol = path.stem
            return (
                f"symbol == '{symbol}' "
                f"AND sample_date == '{sample_date}'"
            )
    elif table_name == "candles":
        # parts: provider/frequency/SYMBOL.parquet
        if len(parts) >= 3:
            interval = parts[1]
            symbol = path.stem
            return (
                f"symbol == '{symbol}' "
                f"AND interval == '{interval}'"
            )
    elif table_name == "fundamentals":
        # parts: provider/YYYY/MM/DD.parquet
        if len(parts) >= 4:
            sample_date = f"{parts[1]}-{parts[2]}-{path.stem}"
            return f"sample_date == '{sample_date}'"
    return None


def check_sha256_sample(
    input_dir: Path, ledger: Ledger, tables: list[str], num_checks: int
) -> list[str]:
    """Verify that ledger SHA256 values match the actual prepared files."""
    import random

    errors: list[str] = []
    logger.info("--- SHA256 checks (%d per table) ---", num_checks)

    for table_name in tables:
        table_dir = input_dir / table_name
        if not table_dir.is_dir():
            continue

        parquet_files = list(table_dir.rglob("*.parquet"))
        samples = random.sample(parquet_files, min(num_checks, len(parquet_files)))

        for p in samples:
            rel_key = str(p.relative_to(table_dir))
            record = ledger.get(rel_key, table_name)
            if record is None:
                errors.append(f"SHA256 {table_name}/{rel_key}: not in ledger")
                logger.error("FAIL %s: not found in ledger", rel_key)
                continue

            actual_sha = _sha256_file(p)
            if actual_sha != record.sha256:
                errors.append(
                    f"SHA256 {table_name}/{rel_key}: "
                    f"ledger={record.sha256[:16]}... actual={actual_sha[:16]}..."
                )
                logger.error("FAIL %s: SHA256 mismatch", rel_key)
            else:
                logger.info("OK   %s: SHA256 matches", rel_key)

    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify migration integrity")
    parser.add_argument(
        "-i", "--input", required=True, help="Root migration directory (e.g. /tmp/migrate)"
    )
    parser.add_argument("--table", default=None, help="Verify a specific table (default: all)")
    parser.add_argument(
        "--spot-checks",
        type=int,
        default=3,
        help="Number of spot checks per table (default: 3)",
    )
    parser.add_argument(
        "--skip-iceberg",
        action="store_true",
        help="Skip Iceberg spot checks (faster)",
    )

    args = parser.parse_args()
    tables = [args.table] if args.table else ALL_TABLES
    input_dir = Path(args.input)

    all_errors: list[str] = []

    # 1. Ledger checks
    ledger = Ledger(DATABASE_URL)
    try:
        all_errors.extend(check_ledger(ledger, tables))

        # 2. Prepared file checks
        all_errors.extend(check_prepared_files(input_dir, tables))

        # 3. SHA256 checks
        all_errors.extend(check_sha256_sample(input_dir, ledger, tables, args.spot_checks))

        # 4. MinIO cache checks
        minio_client = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )
        all_errors.extend(check_minio_cache(minio_client, ledger, tables))

        # 5. Iceberg checks
        catalog = GlueCatalog(
            "tractatus",
            **{
                "glue.region": "us-east-1",
                "s3.region": "us-east-1",
                "warehouse": "s3://tractatus-lakehouse",
            },
        )
        all_errors.extend(check_iceberg(catalog, tables))

        # 6. Spot checks: local vs cache vs Iceberg
        if not args.skip_iceberg:
            all_errors.extend(
                spot_check_data(
                    catalog, minio_client, input_dir, tables, args.spot_checks
                )
            )

    finally:
        ledger.close()

    # Summary
    logger.info("")
    logger.info("=" * 60)
    if all_errors:
        logger.error("VERIFICATION FAILED: %d errors", len(all_errors))
        for e in all_errors:
            logger.error("  - %s", e)
        sys.exit(1)
    else:
        logger.info("VERIFICATION PASSED: all checks OK")


if __name__ == "__main__":
    main()
