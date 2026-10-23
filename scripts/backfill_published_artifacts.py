#!/usr/bin/env python3
"""Backfill published_artifacts from the tractatus-lakehouse MinIO bucket.

Walks all table prefixes, parses artifact metadata from the object key,
and upserts into published_artifacts. Sets cached=True and size_bytes
from object metadata.

Usage:
    source .venv/bin/activate
    python scripts/backfill_published_artifacts.py
"""

from __future__ import annotations

import re

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

import os  # noqa: E402

from tractatus.catalog.config import CatalogConfig  # noqa: E402
from tractatus.catalog.ledger import Ledger  # noqa: E402

# Patterns per table type — each yields (table_name, provider, root, sample_date, interval)
# with None for fields not present in that path structure.
_PATTERNS: list[tuple[str, re.Pattern]] = [
    # options/{provider}/{YYYY}/{MM}/{DD}/{root}_samples_{date}.parquet
    (
        "options",
        re.compile(
            r"^options/([^/]+)/\d{4}/\d{2}/\d{2}/(.+)_samples_(\d{4}-\d{2}-\d{2})\.parquet$"
        ),
    ),
    # level_one_equities/{provider}/{YYYY}/{MM}/{DD}/{root}.parquet
    (
        "level_one_equities",
        re.compile(r"^level_one_equities/([^/]+)/(\d{4})/(\d{2})/(\d{2})/([^/]+)\.parquet$"),
    ),
    # level_one_futures/{provider}/{YYYY}/{MM}/{DD}/{root}.parquet
    (
        "level_one_futures",
        re.compile(r"^level_one_futures/([^/]+)/(\d{4})/(\d{2})/(\d{2})/([^/]+)\.parquet$"),
    ),
    # order_book/{provider}/{YYYY}/{MM}/{DD}/{root}.parquet
    (
        "order_book",
        re.compile(r"^order_book/([^/]+)/(\d{4})/(\d{2})/(\d{2})/([^/]+)\.parquet$"),
    ),
    # candles/{provider}/{interval}/{root}.parquet
    (
        "candles",
        re.compile(r"^candles/([^/]+)/([^/]+)/([^/]+)\.parquet$"),
    ),
    # fundamentals/{provider}/{YYYY}/{MM}/{DD}.parquet
    (
        "fundamentals",
        re.compile(r"^fundamentals/([^/]+)/(\d{4})/(\d{2})/(\d{2})\.parquet$"),
    ),
]


def _parse_key(key: str) -> dict | None:
    """Parse an artifact key into metadata fields. Returns None if unrecognised."""
    for table_name, pattern in _PATTERNS:
        m = pattern.match(key)
        if not m:
            continue
        groups = m.groups()

        if table_name == "options":
            provider, root, sample_date = groups
            return {
                "table_name": table_name,
                "provider": provider,
                "root": root,
                "sample_date": sample_date,
                "interval": None,
            }

        if table_name == "level_one_equities":
            provider, yyyy, mm, dd, root = groups
            return {
                "table_name": table_name,
                "provider": provider,
                "root": root,
                "sample_date": f"{yyyy}-{mm}-{dd}",
                "interval": None,
            }

        if table_name in ("level_one_futures", "order_book"):
            provider, yyyy, mm, dd, root = groups
            return {
                "table_name": table_name,
                "provider": provider,
                "root": root,
                "sample_date": f"{yyyy}-{mm}-{dd}",
                "interval": None,
            }

        if table_name == "candles":
            provider, interval, root = groups
            return {
                "table_name": table_name,
                "provider": provider,
                "root": root,
                "sample_date": None,
                "interval": interval,
            }

        if table_name == "fundamentals":
            provider, yyyy, mm, dd = groups
            return {
                "table_name": table_name,
                "provider": provider,
                "root": None,
                "sample_date": f"{yyyy}-{mm}-{dd}",
                "interval": None,
            }

    return None


def main() -> None:
    config = CatalogConfig.load()
    ledger = Ledger(config.database_url)

    client = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )

    bucket = os.environ.get("MINIO_CACHE_BUCKET", "tractatus-lakehouse")
    paginator = client.get_paginator("list_objects_v2")

    upserted = 0
    skipped = 0
    by_table: dict[str, int] = {}

    try:
        pages = paginator.paginate(Bucket=bucket)
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                parsed = _parse_key(key)
                if not parsed:
                    skipped += 1
                    continue

                ledger.upsert_published(
                    artifact_key=key,
                    cached=True,
                    size_bytes=obj["Size"],
                    **parsed,
                )
                upserted += 1
                by_table[parsed["table_name"]] = by_table.get(parsed["table_name"], 0) + 1

        print(f"Upserted: {upserted}")
        print(f"Skipped:  {skipped}")
        for table, count in sorted(by_table.items()):
            print(f"  {table}: {count}")
    finally:
        ledger.close()


if __name__ == "__main__":
    main()
