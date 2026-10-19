#!/usr/bin/env python3
"""Upload prepared migration parquets to the MinIO cache.

Reads normalized parquet files from the local directory produced by
migrate_prepare.py and uploads them to MinIO, mirroring the directory
structure as cache keys.

Usage:
    python scripts/migrate_cache.py --table options --input /tmp/migrate/options
    python scripts/migrate_cache.py --table level_one_equities --input /tmp/migrate/level_one_equities
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import boto3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("migrate_cache")

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
CACHE_BUCKET = "tractatus-lakehouse"


def upload_to_cache(
    table_name: str,
    input_dir: Path,
    dry_run: bool = False,
    limit: int | None = None,
) -> None:
    if not input_dir.is_dir():
        logger.error("Input directory does not exist: %s", input_dir)
        sys.exit(1)

    parquet_files = sorted(input_dir.rglob("*.parquet"))
    logger.info("Found %d parquet files in %s", len(parquet_files), input_dir)

    if limit:
        parquet_files = parquet_files[:limit]

    if dry_run:
        for p in parquet_files:
            cache_key = f"{table_name}/{p.relative_to(input_dir)}"
            logger.info("  [dry-run] %s -> %s", p.name, cache_key)
        logger.info("Dry run: %d files", len(parquet_files))
        return

    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    # Ensure bucket exists
    try:
        client.head_bucket(Bucket=CACHE_BUCKET)
    except client.exceptions.ClientError:
        logger.info("Creating bucket %s", CACHE_BUCKET)
        client.create_bucket(Bucket=CACHE_BUCKET)

    uploaded = 0
    errors = 0
    total_bytes = 0
    start = time.monotonic()

    for p in parquet_files:
        cache_key = f"{table_name}/{p.relative_to(input_dir)}"
        try:
            size = p.stat().st_size
            client.upload_file(str(p), CACHE_BUCKET, cache_key)
            uploaded += 1
            total_bytes += size

            if uploaded % 500 == 0:
                elapsed = time.monotonic() - start
                logger.info(
                    "Progress: %d uploaded, %d errors, %.1f MB (%.1fs)",
                    uploaded,
                    errors,
                    total_bytes / 1024 / 1024,
                    elapsed,
                )

        except Exception:
            logger.exception("Failed to upload %s", p)
            errors += 1

    elapsed = time.monotonic() - start
    logger.info(
        "Cache upload complete: %d uploaded, %d errors, %.1f MB (%.1fs)",
        uploaded,
        errors,
        total_bytes / 1024 / 1024,
        elapsed,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload prepared migration parquets to MinIO cache"
    )
    parser.add_argument("--table", required=True, help="Table name (used as cache key prefix)")
    parser.add_argument("-i", "--input", required=True, help="Directory with prepared parquet files")
    parser.add_argument("--limit", type=int, default=None, help="Max files to upload")
    parser.add_argument("--dry-run", action="store_true", help="List files without uploading")

    args = parser.parse_args()

    upload_to_cache(
        table_name=args.table,
        input_dir=Path(args.input),
        dry_run=args.dry_run,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
