#!/usr/bin/env python3
"""Publish prepared migration parquets to the Iceberg lakehouse.

Reads normalized parquet files from the local directory produced by
migrate_prepare.py and appends them to the Iceberg table.

Usage:
    python scripts/migrate_publish.py --table options --input /tmp/migrate/options
    python scripts/migrate_publish.py --table level_one_equities --input /tmp/migrate/level_one_equities
    python scripts/migrate_publish.py --table order_book --input /tmp/migrate/order_book --limit 10
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog

from tractatus.catalog.schemas import TABLE_DEFS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("migrate_publish")

TABLE_NAMES = sorted(TABLE_DEFS.keys())


def publish(
    table_name: str,
    input_dir: Path,
    iceberg_bucket: str,
    glue_database: str,
    aws_region: str,
    aws_profile: str | None,
    limit: int | None = None,
    batch_size: int = 50,
    dry_run: bool = False,
) -> None:
    if table_name not in TABLE_DEFS:
        logger.error("Unknown table: %s", table_name)
        sys.exit(1)

    if not input_dir.is_dir():
        logger.error("Input directory does not exist: %s", input_dir)
        sys.exit(1)

    parquet_files = sorted(input_dir.rglob("*.parquet"))
    logger.info("Found %d parquet files in %s", len(parquet_files), input_dir)

    if limit:
        parquet_files = parquet_files[:limit]

    if dry_run:
        total_rows = 0
        for p in parquet_files:
            meta = pq.read_metadata(p)
            total_rows += meta.num_rows
            logger.info(
                "  [dry-run] %s (%d rows, %d row_groups)",
                p.relative_to(input_dir),
                meta.num_rows,
                meta.num_row_groups,
            )
        logger.info("Dry run: %d files, %d total rows", len(parquet_files), total_rows)
        return

    props: dict[str, str] = {
        "type": "glue",
        "glue.region": aws_region,
        "s3.region": aws_region,
        "warehouse": f"s3://{iceberg_bucket}",
    }
    if aws_profile:
        props["glue.profile-name"] = aws_profile
        props["s3.profile-name"] = aws_profile
    logger.info("Connecting to Glue catalog...")
    catalog = load_catalog("glue", **props)
    logger.info("Loading Iceberg table %s.%s...", glue_database, table_name)
    iceberg_table = catalog.load_table(f"{glue_database}.{table_name}")
    logger.info("Table loaded. Starting publish...")

    batches_committed = 0
    files_committed = 0
    errors = 0
    total_rows = 0
    start = time.monotonic()

    # Process files in batches to reduce Iceberg append overhead
    for i in range(0, len(parquet_files), batch_size):
        batch_paths = parquet_files[i : i + batch_size]
        try:
            tables = [pq.read_table(p) for p in batch_paths]
            combined = pa.concat_tables(tables, promote_options="permissive")
            iceberg_table.append(combined)
            batches_committed += 1
            files_committed += len(batch_paths)
            total_rows += len(combined)

            elapsed = time.monotonic() - start
            logger.info(
                "Batch %d: %d files, %d rows (total: %d files, %d rows, %.1fs)",
                batches_committed,
                len(batch_paths),
                len(combined),
                files_committed,
                total_rows,
                elapsed,
            )

        except Exception:
            logger.exception(
                "Failed batch %d (%s ... %s)", i // batch_size + 1, batch_paths[0], batch_paths[-1]
            )
            errors += 1

    elapsed = time.monotonic() - start
    logger.info(
        "Publish complete: %d batches (%d files), %d errors, %d total rows (%.1fs)",
        batches_committed,
        files_committed,
        errors,
        total_rows,
        elapsed,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish prepared migration parquets to Iceberg lakehouse"
    )
    parser.add_argument("--table", required=True, choices=TABLE_NAMES)
    parser.add_argument(
        "-i", "--input", required=True, help="Directory with prepared parquet files"
    )
    parser.add_argument("--limit", type=int, default=None, help="Max files to publish")
    parser.add_argument(
        "--batch-size", type=int, default=50, help="Files per Iceberg append (default 50)"
    )
    parser.add_argument("--dry-run", action="store_true", help="List files without publishing")
    parser.add_argument("--iceberg-bucket", default=None, help="S3 bucket (or set ICEBERG_BUCKET)")
    parser.add_argument("--glue-database", default="tractatus")
    parser.add_argument("--aws-region", default="us-east-1")
    parser.add_argument("--aws-profile", default=None)

    args = parser.parse_args()

    iceberg_bucket = args.iceberg_bucket or os.environ.get("ICEBERG_BUCKET")
    aws_profile = args.aws_profile or os.environ.get("AWS_PROFILE")
    if not iceberg_bucket and not args.dry_run:
        parser.error("--iceberg-bucket or ICEBERG_BUCKET env var required")

    publish(
        table_name=args.table,
        input_dir=Path(args.input),
        iceberg_bucket=iceberg_bucket or "",
        glue_database=args.glue_database,
        aws_region=args.aws_region,
        aws_profile=aws_profile,
        limit=args.limit,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
