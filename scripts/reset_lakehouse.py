#!/usr/bin/env python3
"""Reset the lakehouse: purge all Iceberg tables and clear the postgres ledger.

Usage:
    python scripts/reset_lakehouse.py --yes
    python scripts/reset_lakehouse.py --table options --yes
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

import psycopg
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError

from tractatus.catalog.schemas import TABLE_DEFS, ensure_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("reset_lakehouse")

DATABASE_URL = "postgresql://tractatus_user:tractatus@127.0.0.1:5432/tractatus"


def reset(
    tables: list[str],
    iceberg_bucket: str,
    glue_database: str,
    aws_region: str,
    aws_profile: str | None,
) -> None:
    props: dict[str, str] = {
        "type": "glue",
        "glue.region": aws_region,
        "s3.region": aws_region,
        "warehouse": f"s3://{iceberg_bucket}",
    }
    if aws_profile:
        props["glue.profile-name"] = aws_profile
        props["s3.profile-name"] = aws_profile

    catalog = load_catalog("glue", **props)

    # 1. Purge Iceberg tables
    for table_name in tables:
        full_name = f"{glue_database}.{table_name}"
        try:
            catalog.load_table(full_name)
            catalog.purge_table(full_name)
            logger.info("Purged Iceberg table %s", full_name)
        except NoSuchTableError:
            logger.info("Iceberg table %s not found, skipping purge", full_name)

    # 2. Clear postgres ledger
    conn = psycopg.connect(DATABASE_URL, autocommit=True)
    try:
        for table_name in tables:
            result = conn.execute("DELETE FROM catalog_files WHERE table_name = %s", (table_name,))
            logger.info("Deleted %d ledger records for %s", result.rowcount, table_name)
    finally:
        conn.close()

    # 3. Recreate Iceberg tables with new properties
    for table_name in tables:
        full_name = f"{glue_database}.{table_name}"
        table = ensure_table(catalog, glue_database, table_name)
        logger.info(
            "Recreated %s (%d fields, properties: %s)",
            full_name,
            len(table.schema().fields),
            TABLE_DEFS[table_name].properties,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset lakehouse tables and ledger")
    parser.add_argument(
        "--table",
        default=None,
        help="Reset a specific table (default: all tables)",
    )
    parser.add_argument("--yes", "-y", action="store_true", help="Skip confirmation")
    parser.add_argument("--iceberg-bucket", default=None)
    parser.add_argument("--glue-database", default="tractatus")
    parser.add_argument("--aws-region", default="us-east-1")
    parser.add_argument("--aws-profile", default=None)

    args = parser.parse_args()

    iceberg_bucket = args.iceberg_bucket or os.environ.get("ICEBERG_BUCKET")
    aws_profile = args.aws_profile or os.environ.get("AWS_PROFILE")
    if not iceberg_bucket:
        parser.error("--iceberg-bucket or ICEBERG_BUCKET env var required")

    if args.table:
        if args.table not in TABLE_DEFS:
            parser.error(f"Unknown table: {args.table}. Known: {', '.join(sorted(TABLE_DEFS))}")
        tables = [args.table]
    else:
        tables = sorted(TABLE_DEFS.keys())

    print(f"Tables to reset: {', '.join(tables)}")
    print(f"Iceberg bucket: {iceberg_bucket}")
    print(f"Postgres: {DATABASE_URL}")
    print()

    if not args.yes:
        confirm = input("This will DELETE all data. Continue? [y/N] ")
        if confirm.lower() != "y":
            print("Aborted.")
            sys.exit(0)

    reset(
        tables=tables,
        iceberg_bucket=iceberg_bucket,
        glue_database=args.glue_database,
        aws_region=args.aws_region,
        aws_profile=aws_profile,
    )
    logger.info("Reset complete.")


if __name__ == "__main__":
    main()
