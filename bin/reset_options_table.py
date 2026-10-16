#!/usr/bin/env python3
"""Reset the options Iceberg table for schema migration.

Drops the existing table from Glue, deletes S3 data files, resets the ledger,
and (optionally) re-publishes all files through the corrected schema.

Usage:
    # Dry run — show what would be deleted
    python bin/reset_options_table.py --dry-run

    # Execute reset only (drop table, delete S3 objects, clear ledger)
    python bin/reset_options_table.py

    # Execute reset and re-publish all files
    python bin/reset_options_table.py --republish
"""

from __future__ import annotations

import argparse
import sys

import boto3
from pyiceberg.catalog import load_catalog

from tractatus.catalog.config import CatalogConfig
from tractatus.catalog.ledger import Ledger
from tractatus.catalog.publisher import Publisher
from tractatus.catalog.schema import ensure_table

TABLE_NAME = "options"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reset the options Iceberg table.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes.",
    )
    parser.add_argument(
        "--republish",
        action="store_true",
        help="After reset, re-discover and publish all files.",
    )
    return parser.parse_args()


def _iceberg_catalog(config: CatalogConfig) -> load_catalog:
    props: dict[str, str] = {
        "type": "glue",
        "glue.region": config.aws_region,
        "s3.region": config.aws_region,
        "warehouse": config.iceberg_warehouse,
    }
    if config.aws_profile:
        props["glue.profile-name"] = config.aws_profile
        props["s3.profile-name"] = config.aws_profile
    return load_catalog("glue", **props)


def _delete_s3_prefix(config: CatalogConfig, prefix: str, dry_run: bool) -> int:
    """Delete all objects under a prefix. Returns count deleted."""
    session_kwargs: dict[str, str] = {"region_name": config.aws_region}
    if config.aws_profile:
        session_kwargs["profile_name"] = config.aws_profile
    session = boto3.Session(**session_kwargs)
    s3 = session.client("s3")

    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=config.iceberg_bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if dry_run:
                print(f"  [dry-run] would delete s3://{config.iceberg_bucket}/{key}")
            else:
                s3.delete_object(Bucket=config.iceberg_bucket, Key=key)
            deleted += 1
    return deleted


def main() -> None:
    args = _parse_args()
    config = CatalogConfig.load()
    full_table = f"{config.glue_database}.{TABLE_NAME}"

    print(f"=== Options table reset {'(DRY RUN)' if args.dry_run else ''} ===")
    print(f"  Glue table:  {full_table}")
    print(f"  S3 bucket:   {config.iceberg_bucket}")
    print(f"  Ledger:      {config.database_url}")
    print()

    # 1. Drop Iceberg table from Glue
    print("Step 1: Drop Iceberg table from Glue catalog")
    catalog = _iceberg_catalog(config)
    try:
        catalog.load_table(full_table)
        if args.dry_run:
            print(f"  [dry-run] would drop {full_table}")
        else:
            catalog.drop_table(full_table)
            print(f"  Dropped {full_table}")
    except Exception:
        print(f"  Table {full_table} does not exist — skipping")

    # 2. Delete S3 data files
    print()
    print("Step 2: Delete S3 data files")
    # Iceberg data lives under {glue_database}/{table_name}/
    s3_prefix = f"{config.glue_database}/{TABLE_NAME}/"
    count = _delete_s3_prefix(config, s3_prefix, args.dry_run)
    print(f"  {'Would delete' if args.dry_run else 'Deleted'} {count} S3 objects under {s3_prefix}")

    # 3. Reset ledger
    print()
    print("Step 3: Reset ledger records")
    ledger = Ledger(config.database_url)
    summary = ledger.summary(TABLE_NAME)
    total = sum(summary.values())
    if args.dry_run:
        print(f"  [dry-run] would delete {total} ledger records: {summary}")
    else:
        deleted = ledger.delete_all(TABLE_NAME)
        print(f"  Deleted {deleted} ledger records")
    ledger.close()

    # 4. Recreate table with corrected schema
    print()
    print("Step 4: Recreate table with corrected schema")
    if args.dry_run:
        print(f"  [dry-run] would create {full_table} with DateType expiration_date")
    else:
        table = ensure_table(catalog, config.glue_database, TABLE_NAME)
        print(f"  Created {full_table} (schema has {len(table.schema().fields)} fields)")

    # 5. Optional: re-publish
    if args.republish:
        print()
        print("Step 5: Re-publish all files")
        if args.dry_run:
            publisher = Publisher(config)
            result = publisher.run(TABLE_NAME, dry_run=True)
            publisher.close()
            print(f"  [dry-run] would discover {result.discovered} files")
        else:
            publisher = Publisher(config)
            result = publisher.run(TABLE_NAME)
            publisher.close()
            print(
                f"  Discovered: {result.discovered}, "
                f"Uploaded: {result.uploaded}, "
                f"Committed: {result.committed}, "
                f"Errors: {result.errors}"
            )
            if result.errors:
                print(f"  WARNING: {result.errors} errors during re-publish", file=sys.stderr)

    print()
    print("Done.")


if __name__ == "__main__":
    main()
