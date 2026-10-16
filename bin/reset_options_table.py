#!/usr/bin/env python3
"""Reset the options Iceberg table for schema migration.

Uses PyIceberg to purge all snapshots and data files, then drops the Glue
table, resets the ledger, and recreates the table with the corrected schema.

Usage:
    # Dry run — show what would be done
    python bin/reset_options_table.py --dry-run

    # Execute reset
    python bin/reset_options_table.py
"""

from __future__ import annotations

import argparse

from pyiceberg.catalog import Catalog, load_catalog

from tractatus.catalog.config import CatalogConfig
from tractatus.catalog.ledger import Ledger
from tractatus.catalog.schema import ensure_table

TABLE_NAME = "options"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reset the options Iceberg table.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes.",
    )
    return parser.parse_args()


def _iceberg_catalog(config: CatalogConfig) -> Catalog:
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


def main() -> None:
    args = _parse_args()
    config = CatalogConfig.load()
    full_table = f"{config.glue_database}.{TABLE_NAME}"

    print(f"=== Options table reset {'(DRY RUN)' if args.dry_run else ''} ===")
    print(f"  Glue table:  {full_table}")
    print(f"  S3 bucket:   {config.iceberg_bucket}")
    print(f"  Ledger:      {config.database_url}")
    print()

    # 1. Purge table data via Iceberg, then drop from Glue
    print("Step 1: Drop Iceberg table (purge=True removes data and metadata from S3)")
    catalog = _iceberg_catalog(config)
    try:
        catalog.load_table(full_table)
        if args.dry_run:
            print(f"  [dry-run] would drop {full_table} with purge=True")
        else:
            catalog.drop_table(full_table, purge_requested=True)
            print(f"  Dropped and purged {full_table}")
    except Exception:
        print(f"  Table {full_table} does not exist — skipping")

    # 2. Reset ledger
    print()
    print("Step 2: Reset ledger records")
    ledger = Ledger(config.database_url)
    summary = ledger.summary(TABLE_NAME)
    total = sum(summary.values())
    if args.dry_run:
        print(f"  [dry-run] would delete {total} ledger records: {summary}")
    else:
        deleted = ledger.delete_all(TABLE_NAME)
        print(f"  Deleted {deleted} ledger records")
    ledger.close()

    # 3. Recreate table with corrected schema
    print()
    print("Step 3: Recreate table with corrected schema")
    if args.dry_run:
        print(f"  [dry-run] would create {full_table} with DateType expiration_date")
    else:
        table = ensure_table(catalog, config.glue_database, TABLE_NAME)
        print(f"  Created {full_table} (schema has {len(table.schema().fields)} fields)")

    print()
    print("Done.")


if __name__ == "__main__":
    main()
