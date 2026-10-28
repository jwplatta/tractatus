"""Backfill local Iceberg lakehouse from MinIO cache parquets.

Reads existing parquet files from tractatus-lakehouse (hand-crafted paths),
adds missing columns (provider, option_root, underlying_symbol, sample_quality,
quality_flags), then appends to the local Iceberg table in tractatus-iceberg.

Usage:
    uv run python scripts/backfill_local_lakehouse_from_cache.py
    uv run python scripts/backfill_local_lakehouse_from_cache.py --after 2026-06-01
    uv run python scripts/backfill_local_lakehouse_from_cache.py --before 2026-10-17
    uv run python scripts/backfill_local_lakehouse_from_cache.py --dry-run
"""

from __future__ import annotations

import argparse
import io
import os
import sys
from datetime import UTC, date, datetime
from pathlib import PurePosixPath

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from tqdm import tqdm

from tractatus.catalog.config import CatalogConfig
from tractatus.catalog.ledger import Ledger

load_dotenv()

# ---------------------------------------------------------------------------
# Config from env
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
CACHE_BUCKET = os.environ["MINIO_CACHE_BUCKET"]
ICEBERG_BUCKET = os.environ["MINIO_ICEBERG_BUCKET"]
POSTGRES_URL = os.environ["ICEBERG_POSTGRES_URL"]
GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "tractatus")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# Option root → underlying symbol mapping (mirrors normalize/options.py)
# ---------------------------------------------------------------------------
_ROOT_TO_UNDERLYING: dict[str, str] = {
    "SPX": "$SPX",
    "SPXW": "$SPX",
    "NDX": "$NDX",
    "NDXP": "$NDX",
    "RUT": "$RUT",
    "RUTW": "$RUT",
    "XSP": "$XSP",
    "VIX": "$VIX",
    "VIXW": "$VIX",
    "DJX": "$DJX",
    "DJXW": "$DJX",
}


def _derive_underlying(option_root: str) -> str:
    return _ROOT_TO_UNDERLYING.get(option_root, option_root)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_date_from_key(key: str) -> date | None:
    """Extract sample date from path: options/schwab/YYYY/MM/DD/..."""
    parts = PurePosixPath(key).parts
    # parts: ('options', 'schwab', '2026', '01', '02', 'SPXW_samples_2026-01-02.parquet')
    if len(parts) < 6:
        return None
    try:
        return date(int(parts[2]), int(parts[3]), int(parts[4]))
    except ValueError:
        return None


def _parse_provider_from_key(key: str) -> str:
    parts = PurePosixPath(key).parts
    return parts[1] if len(parts) >= 2 else "unknown"


def _parse_option_root_from_filename(key: str) -> str | None:
    """Extract option_root from filename: SPXW_samples_2026-01-02.parquet → SPXW"""
    stem = PurePosixPath(key).stem  # e.g. SPXW_samples_2026-01-02
    return stem.split("_")[0] if "_" in stem else None


def _add_missing_columns(table: pa.Table, option_root: str, provider: str) -> pa.Table:
    """Add provider, option_root, underlying_symbol, sample_quality, quality_flags."""
    n = len(table)
    underlying = _derive_underlying(option_root)

    for col_name, value, col_type in [
        ("provider", provider, pa.string()),
        ("option_root", option_root, pa.string()),
        ("underlying_symbol", underlying, pa.string()),
        ("sample_quality", None, pa.string()),
        ("quality_flags", None, pa.string()),
    ]:
        if col_name not in table.column_names:
            if value is not None:
                arr = pa.array([value] * n, type=col_type)
            else:
                arr = pa.array([None] * n, type=col_type)
            table = table.append_column(col_name, arr)

    return table


def _list_cache_keys(
    s3: boto3.client,
    after: date | None,
    before: date | None,
) -> list[str]:
    """List all options parquet keys in the cache bucket, filtered by date."""
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=CACHE_BUCKET, Prefix="options/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            sample_date = _parse_date_from_key(key)
            if sample_date is None:
                continue
            if after and sample_date < after:
                continue
            if before and sample_date >= before:
                continue
            keys.append(key)
    return sorted(keys)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--after", help="Only include sample_date >= DATE (YYYY-MM-DD)")
    parser.add_argument("--before", help="Only include sample_date < DATE (YYYY-MM-DD)")
    parser.add_argument(
        "--dry-run", action="store_true", help="List files without writing to Iceberg"
    )
    args = parser.parse_args()

    after = date.fromisoformat(args.after) if args.after else None
    before = date.fromisoformat(args.before) if args.before else None

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    # Load local Iceberg catalog
    catalog = load_catalog(
        "sql",
        **{
            "type": "sql",
            "uri": POSTGRES_URL,
            "warehouse": f"s3://{ICEBERG_BUCKET}",
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.path-style-access": "true",
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
            "s3.region": AWS_REGION,
        },
    )
    iceberg_table = catalog.load_table(f"{GLUE_DATABASE}.options")
    ledger = Ledger(CatalogConfig.load().database_url)

    keys = _list_cache_keys(s3, after, before)
    print(f"Found {len(keys)} parquet files to backfill.")
    if args.dry_run:
        for k in keys:
            print(" ", k)
        return

    committed = 0
    errors = 0

    for key in tqdm(keys, unit="file"):
        option_root = _parse_option_root_from_filename(key)
        provider = _parse_provider_from_key(key)

        if not option_root:
            print(f"SKIP (no option_root): {key}", file=sys.stderr)
            errors += 1
            continue

        if ledger.get(key, "options") is not None:
            tqdm.write(f"SKIP (already committed): {key}")
            continue

        try:
            obj = s3.get_object(Bucket=CACHE_BUCKET, Key=key)
            buf = io.BytesIO(obj["Body"].read())
            table = pq.read_table(buf)

            table = _add_missing_columns(table, option_root, provider)

            # Cast large_string → string for Iceberg compatibility
            new_cols = {}
            for col_name in table.column_names:
                col = table.column(col_name)
                if pa.types.is_large_string(col.type):
                    new_cols[col_name] = pc.cast(col, pa.string())
            for col_name, arr in new_cols.items():
                table = table.set_column(table.column_names.index(col_name), col_name, arr)

            sample_date = _parse_date_from_key(key)
            iceberg_table.append(table)

            ledger.upsert_published(
                artifact_key=key,
                table_name="options",
                provider=provider,
                symbol=option_root,
                sample_date=sample_date.isoformat() if sample_date else None,
                committed_at=datetime.now(UTC).isoformat(),
                cached=True,
                cached_at=datetime.now(UTC).isoformat(),
                row_count=len(table),
            )

            committed += 1
            tqdm.write(f"OK [{committed:>4}] {key} ({len(table)} rows)")

        except Exception as exc:
            tqdm.write(f"ERROR: {key}: {exc}", file=sys.stderr)
            errors += 1

    ledger.close()
    print(f"\nDone. Committed: {committed}, Errors: {errors}")

    print("\nExpiring snapshots...")
    iceberg_table.maintenance.expire_snapshots().older_than(dt=datetime.now(UTC)).commit()
    print("Snapshots expired.")


if __name__ == "__main__":
    main()
