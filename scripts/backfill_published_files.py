#!/usr/bin/env python3
"""Backfill published_artifacts from the tractatus-lakehouse MinIO bucket.

Walks options/ under schwab and massive providers, parses artifact_key,
provider, root, and sample_date from the object key, and upserts into
published_artifacts. Sets cached=True and size_bytes from object metadata.

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

# options/{provider}/{YYYY}/{MM}/{DD}/{root}_samples_{date}.parquet
_KEY_RE = re.compile(
    r"^(options)/([^/]+)/(\d{4})/(\d{2})/(\d{2})/(.+)_samples_(\d{4}-\d{2}-\d{2})\.parquet$"
)

PROVIDERS = {"schwab", "massive"}


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

    bucket = os.environ.get("MINIO_BUCKET", "tractatus-lakehouse")
    paginator = client.get_paginator("list_objects_v2")

    upserted = 0
    skipped = 0

    try:
        for provider in PROVIDERS:
            pages = paginator.paginate(Bucket=bucket, Prefix=f"options/{provider}/")
            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    m = _KEY_RE.match(key)
                    if not m:
                        skipped += 1
                        continue

                    table_name, prov, _y, _m, _d, root, sample_date = m.groups()
                    ledger.upsert_published(
                        artifact_key=key,
                        table_name=table_name,
                        provider=prov,
                        root=root,
                        sample_date=sample_date,
                        cached=True,
                        size_bytes=obj["Size"],
                    )
                    upserted += 1

        print(f"Upserted: {upserted}")
        print(f"Skipped:  {skipped}")
    finally:
        ledger.close()


if __name__ == "__main__":
    main()
