"""Validate local options lakehouse after migration.

Checks:
1. Iceberg row counts per date — spot check and full coverage
2. published_artifacts coverage — one row per (option_root, sample_date)
3. Cache bucket parquet schema — verify provider/option_root/underlying_symbol present
4. Row count agreement between Iceberg and cache bucket per (option_root, sample_date)
5. catalog_files — confirm only Oct 19+ entries

Usage:
    uv run python scripts/validate_options_lakehouse.py
    uv run python scripts/validate_options_lakehouse.py --spot-dates 2026-01-15 2026-06-02 2026-10-23
    uv run python scripts/validate_options_lakehouse.py --skip-rowcount-match  # faster, skips per-file row count check
"""

from __future__ import annotations

import argparse
import io
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import date

import boto3
import pyarrow.compute as pc
import pyarrow.parquet as pq
from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog

load_dotenv()

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
CACHE_BUCKET = os.environ["MINIO_CACHE_BUCKET"]
ICEBERG_BUCKET = os.environ["MINIO_ICEBERG_BUCKET"]
POSTGRES_URL = os.environ["ICEBERG_POSTGRES_URL"]
GLUE_DATABASE = os.environ.get("GLUE_DATABASE", "tractatus")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
LEDGER_URL = os.environ.get(
    "LEDGER_DATABASE_URL",
    str(__import__("pathlib").Path.home() / ".tractatus" / "catalog_ledger.db"),
)

REQUIRED_COLUMNS = {"provider", "option_root", "underlying_symbol"}

SPOT_DATES_DEFAULT = [
    "2025-12-18",  # first day of historical data
    "2026-01-02",
    "2026-03-21",
    "2026-06-01",
    "2026-09-15",
    "2026-10-16",  # last day of backfill
    "2026-10-22",
    "2026-10-23",  # most recent
]


def load_iceberg():
    return load_catalog(
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


def check_iceberg_spot_dates(iceberg_table, spot_dates: list[str]) -> list[str]:
    errors = []
    print("\n=== 1. Iceberg spot date row counts ===")
    for d in spot_dates:
        next_d = date.fromisoformat(d)
        next_d = date.fromordinal(next_d.toordinal() + 1)
        row_filter = (
            f"sampled_at >= '{d}T00:00:00+00:00'"
            f" and sampled_at < '{next_d.isoformat()}T00:00:00+00:00'"
        )
        try:
            t = iceberg_table.scan(row_filter=row_filter).to_arrow()
            roots = set(t["option_root"].to_pylist()) if "option_root" in t.schema.names else set()
            print(f"  {d}: {t.num_rows:>8} rows  roots={sorted(roots)}")
            if t.num_rows == 0:
                errors.append(f"WARN: {d} has 0 rows in Iceberg")
        except Exception as e:
            errors.append(f"ERROR scanning {d}: {e}")
            print(f"  {d}: ERROR — {e}")
    return errors


def check_iceberg_date_coverage(iceberg_table) -> tuple[list[str], dict[str, int]]:
    """Scan Iceberg by month to get row counts per sample_date. Returns (errors, date_rows)."""
    errors = []
    date_rows: dict[str, int] = {}
    print("\n=== 2. Iceberg full date coverage (by month) ===")

    # Scan month by month to avoid loading everything at once
    start = date(2025, 12, 1)
    end = date(2026, 11, 1)
    current_month = start
    while current_month < end:
        if current_month.month == 12:
            next_month = date(current_month.year + 1, 1, 1)
        else:
            next_month = date(current_month.year, current_month.month + 1, 1)

        row_filter = (
            f"sampled_at >= '{current_month.isoformat()}T00:00:00+00:00'"
            f" and sampled_at < '{next_month.isoformat()}T00:00:00+00:00'"
        )
        try:
            t = iceberg_table.scan(row_filter=row_filter).to_arrow()
            if t.num_rows > 0:
                dates_col = pc.strftime(t["sampled_at"], format="%Y-%m-%d")
                t_with_date = t.append_column("_date", dates_col)
                pdf = t_with_date.to_pandas()
                for d, grp in pdf.groupby("_date"):
                    date_rows[str(d)] = date_rows.get(str(d), 0) + len(grp)
            print(f"  {current_month.strftime('%Y-%m')}: {t.num_rows:>9} rows")
        except Exception as e:
            errors.append(f"ERROR scanning {current_month.strftime('%Y-%m')}: {e}")
            print(f"  {current_month.strftime('%Y-%m')}: ERROR — {e}")

        current_month = next_month

    return errors, date_rows


def check_published_artifacts(ledger_path: str, date_rows: dict[str, int]) -> list[str]:
    errors = []
    print("\n=== 3. published_artifacts coverage ===")
    con = sqlite3.connect(ledger_path)
    cur = con.cursor()

    # Total count
    cur.execute("SELECT COUNT(*) FROM published_artifacts WHERE table_name='options'")
    total = cur.fetchone()[0]
    print(f"  Total rows: {total}")

    # Date range
    cur.execute(
        "SELECT MIN(sample_date), MAX(sample_date) FROM published_artifacts WHERE table_name='options'"
    )
    mn, mx = cur.fetchone()
    print(f"  Date range: {mn} → {mx}")

    # Dates present in Iceberg but missing from published_artifacts
    cur.execute(
        "SELECT DISTINCT sample_date FROM published_artifacts WHERE table_name='options' ORDER BY sample_date"
    )
    artifact_dates = {row[0] for row in cur.fetchall()}
    iceberg_dates = set(date_rows.keys())
    missing_in_artifacts = iceberg_dates - artifact_dates
    if missing_in_artifacts:
        errors.append(
            f"Dates in Iceberg but missing from published_artifacts: {sorted(missing_in_artifacts)[:20]}"
        )
        print(
            f"  WARN: {len(missing_in_artifacts)} dates in Iceberg missing from published_artifacts"
        )
    else:
        print(f"  OK: all {len(iceberg_dates)} Iceberg dates present in published_artifacts")

    # Per-date artifact counts
    cur.execute(
        "SELECT sample_date, COUNT(*) as cnt FROM published_artifacts WHERE table_name='options' GROUP BY sample_date ORDER BY sample_date"
    )
    rows = cur.fetchall()
    low_artifact_dates = [(d, c) for d, c in rows if c < 5]
    if low_artifact_dates:
        print(
            f"  WARN: {len(low_artifact_dates)} dates with fewer than 5 artifacts (expected ~11 roots):"
        )
        for d, c in low_artifact_dates[:10]:
            print(f"    {d}: {c} artifacts")

    con.close()
    return errors


def check_catalog_files(ledger_path: str) -> list[str]:
    errors = []
    print("\n=== 4. catalog_files (CSV source records) ===")
    con = sqlite3.connect(ledger_path)
    cur = con.cursor()

    cur.execute("SELECT COUNT(*) FROM catalog_files WHERE table_name='options'")
    total = cur.fetchone()[0]
    print(f"  Total rows: {total}")

    cur.execute(
        "SELECT MIN(sample_date), MAX(sample_date) FROM catalog_files WHERE table_name='options'"
    )
    mn, mx = cur.fetchone()
    print(f"  Date range: {mn} → {mx}")

    if mn and mn < "2026-10-19":
        errors.append(
            f"catalog_files has entries before Oct 19 ({mn}) — unexpected for CSV pipeline"
        )
        print("  WARN: entries before Oct 19 — were ledger records migrated from an older format?")
    else:
        print("  OK: no entries before Oct 19 (CSV pipeline only)")

    con.close()
    return errors


def check_cache_schema(s3, spot_keys: list[str]) -> list[str]:
    """Check a sample of cache parquets for required columns."""
    errors = []
    print("\n=== 5. Cache bucket schema spot check ===")
    for key in spot_keys[:20]:
        try:
            obj = s3.get_object(Bucket=CACHE_BUCKET, Key=key)
            buf = io.BytesIO(obj["Body"].read())
            schema = pq.read_schema(buf)
            col_names = set(schema.names)
            missing = REQUIRED_COLUMNS - col_names
            if missing:
                errors.append(f"Cache file missing columns {missing}: {key}")
                print(f"  MISSING {missing}: {key}")
            else:
                print(f"  OK: {key}")
        except Exception as e:
            errors.append(f"ERROR reading {key}: {e}")
            print(f"  ERROR: {key}: {e}")
    return errors


def list_cache_keys_for_dates(s3, dates: list[str]) -> dict[str, list[str]]:
    """List cache keys for given dates. Returns {date: [keys]}."""
    result: dict[str, list[str]] = defaultdict(list)
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=CACHE_BUCKET, Prefix="options/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            # options/schwab/YYYY/MM/DD/ROOT_samples_YYYY-MM-DD.parquet
            parts = key.split("/")
            if len(parts) >= 5:
                d = f"{parts[2]}-{parts[3]}-{parts[4]}"
                if d in dates:
                    result[d].append(key)
    return result


def check_cache_rowcounts(s3, iceberg_table, spot_dates: list[str]) -> list[str]:
    """For spot dates, compare per-(option_root, sample_date) row counts between Iceberg and cache."""
    errors = []
    print("\n=== 6. Row count agreement: Iceberg vs cache ===")

    cache_keys_by_date = list_cache_keys_for_dates(s3, spot_dates)

    for d in spot_dates:
        next_d = date.fromordinal(date.fromisoformat(d).toordinal() + 1)
        row_filter = (
            f"sampled_at >= '{d}T00:00:00+00:00'"
            f" and sampled_at < '{next_d.isoformat()}T00:00:00+00:00'"
        )
        try:
            t = iceberg_table.scan(row_filter=row_filter).to_arrow()
        except Exception as e:
            errors.append(f"ERROR scanning Iceberg for {d}: {e}")
            continue

        if t.num_rows == 0:
            if cache_keys_by_date.get(d):
                errors.append(f"{d}: 0 rows in Iceberg but cache files exist")
            continue

        # Iceberg counts per option_root
        pdf = t.to_pandas()
        iceberg_counts: dict[str, int] = pdf.groupby("option_root").size().to_dict()

        # Cache counts per option_root
        cache_counts: dict[str, int] = {}
        for key in cache_keys_by_date.get(d, []):
            root = key.split("/")[-1].replace(f"_samples_{d}.parquet", "")
            try:
                obj = s3.get_object(Bucket=CACHE_BUCKET, Key=key)
                buf = io.BytesIO(obj["Body"].read())
                pf = pq.read_table(buf)
                cache_counts[root] = pf.num_rows
            except Exception as e:
                errors.append(f"ERROR reading cache {key}: {e}")

        # Compare
        all_roots = set(iceberg_counts) | set(cache_counts)
        mismatches = []
        for root in sorted(all_roots):
            ic = iceberg_counts.get(root, 0)
            ca = cache_counts.get(root, 0)
            if ic != ca:
                mismatches.append(f"{root}: iceberg={ic} cache={ca}")

        if mismatches:
            errors.append(f"{d} row count mismatches: {mismatches}")
            print(f"  {d}: MISMATCH — {mismatches}")
        else:
            print(f"  {d}: OK ({len(all_roots)} roots, {t.num_rows} rows)")

    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spot-dates", nargs="+", default=SPOT_DATES_DEFAULT)
    parser.add_argument(
        "--skip-rowcount-match",
        action="store_true",
        help="Skip per-file row count comparison (faster)",
    )
    parser.add_argument(
        "--skip-full-coverage",
        action="store_true",
        help="Skip full month-by-month Iceberg scan (faster)",
    )
    args = parser.parse_args()

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    catalog = load_iceberg()
    iceberg_table = catalog.load_table(f"{GLUE_DATABASE}.options")

    all_errors: list[str] = []

    # 1. Spot date row counts
    all_errors += check_iceberg_spot_dates(iceberg_table, args.spot_dates)

    # 2. Full date coverage
    date_rows: dict[str, int] = {}
    if not args.skip_full_coverage:
        errs, date_rows = check_iceberg_date_coverage(iceberg_table)
        all_errors += errs

    # 3. published_artifacts
    if date_rows:
        all_errors += check_published_artifacts(LEDGER_URL, date_rows)

    # 4. catalog_files
    all_errors += check_catalog_files(LEDGER_URL)

    # 5. Cache schema spot check — list a few keys for spot dates
    cache_keys_for_schema = list_cache_keys_for_dates(s3, args.spot_dates)
    flat_keys = [k for keys in cache_keys_for_schema.values() for k in keys]
    all_errors += check_cache_schema(s3, flat_keys)

    # 6. Row count agreement
    if not args.skip_rowcount_match:
        all_errors += check_cache_rowcounts(s3, iceberg_table, args.spot_dates)

    # Summary
    print("\n=== SUMMARY ===")
    if all_errors:
        print(f"  {len(all_errors)} issue(s) found:")
        for e in all_errors:
            print(f"  - {e}")
        sys.exit(1)
    else:
        print("  All checks passed.")


if __name__ == "__main__":
    main()
