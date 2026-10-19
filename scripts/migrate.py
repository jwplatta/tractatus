#!/usr/bin/env python3
"""Migrate existing ~/.tickrake/data/ parquets into the Iceberg lakehouse.

Reads old-format parquets, converts columns to match the new normalizer
expectations, then feeds through normalize_table → sort → iceberg append.

Usage:
    python scripts/migrate.py --table options --provider schwab --limit 10 --dry-run
    python scripts/migrate.py --table level_one_equities --after 2026-10-01
    python scripts/migrate.py --table order_book --provider schwab
    python scripts/migrate.py --table candles --provider schwab
    python scripts/migrate.py --table fundamentals --provider schwab
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import sys
import time
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog

from tractatus.catalog.ledger import Ledger
from tractatus.catalog.normalize import normalize_table
from tractatus.catalog.publisher import _sort_table
from tractatus.catalog.schemas import TABLE_DEFS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("migrate")

DATABASE_URL = "postgresql://tractatus_user:tractatus@127.0.0.1:5432/tractatus"

_DATE_DIR_RE = re.compile(r"(\d{4})/(\d{2})/(\d{2})")

# Old options filename: {ROOT}_samples_{DATE}.parquet
_OPTIONS_FILENAME_RE = re.compile(r"^(.+)_samples_(\d{4}-\d{2}-\d{2})\.parquet$")

# Old level_one / order_book filename: {SYMBOL}_{HHMMSSZ}.parquet
_SNAPSHOT_FILENAME_RE = re.compile(r"^(.+)_\d{6}Z\.parquet$")


# ---------------------------------------------------------------------------
# Per-dataset column converters: old parquet → normalizer-compatible Arrow
# ---------------------------------------------------------------------------


def _epoch_ms_to_iso_string(col: pa.Array) -> pa.Array:
    """Convert int64 epoch-milliseconds to UTC ISO 8601 string column."""
    micros = pc.multiply(col.cast(pa.int64()), 1000)
    ts = micros.cast(pa.timestamp("us", tz="UTC"))
    # Format as "YYYY-MM-DDTHH:MM:SSZ"
    return pc.strftime(ts, format="%Y-%m-%dT%H:%M:%SZ")


def convert_options(table: pa.Table) -> pa.Table:
    """Convert old options parquet to normalizer-compatible format.

    Old parquets vary from 26 to 31 columns. The normalizer's
    validate_input_schema requires all 41 expected columns, so we add
    any missing ones as null here to pass validation. align_to_schema
    handles final column ordering and type casting.
    """
    from tractatus.catalog.normalize.options import _EXPECTED_INPUT  # noqa: PLC0415

    n = len(table)
    for col_name, col_type in _EXPECTED_INPUT.items():
        if col_name not in table.column_names:
            table = table.append_column(col_name, pa.nulls(n, type=col_type))
    return table


def convert_level_one(table: pa.Table) -> pa.Table:
    """Convert old level_one parquet to normalizer-compatible format.

    Old: received_at_ms (int64), quote_time_ms (int64), trade_time_ms (int64)
    New: received_at (string), quote_time (string), trade_time (string)
    """
    renames = {
        "received_at_ms": "received_at",
        "quote_time_ms": "quote_time",
        "trade_time_ms": "trade_time",
    }
    for old_name, new_name in renames.items():
        if old_name in table.column_names:
            idx = table.column_names.index(old_name)
            col = table.column(old_name)
            converted = _epoch_ms_to_iso_string(col)
            table = table.set_column(idx, new_name, converted)
    return table


def convert_order_book(table: pa.Table) -> pa.Table:
    """Convert old order_book parquet to normalizer-compatible format.

    Old: received_at_ms (int64), book_time_ms (int64)
    New: received_at (string), book_time (string)
    """
    renames = {
        "received_at_ms": "received_at",
        "book_time_ms": "book_time",
    }
    for old_name, new_name in renames.items():
        if old_name in table.column_names:
            idx = table.column_names.index(old_name)
            col = table.column(old_name)
            converted = _epoch_ms_to_iso_string(col)
            table = table.set_column(idx, new_name, converted)
    return table


def convert_candles(table: pa.Table) -> pa.Table:
    """Convert old candles parquet — already compatible."""
    return table


def convert_fundamentals(table: pa.Table) -> pa.Table:
    """Convert old fundamentals parquet: rename fetched_at → sampled_at."""
    if "fetched_at" in table.column_names:
        idx = table.column_names.index("fetched_at")
        col = table.column("fetched_at")
        table = table.set_column(idx, "sampled_at", col)
    return table


_CONVERTERS: dict[str, callable] = {
    "options": convert_options,
    "level_one_equities": convert_level_one,
    "level_one_futures": convert_level_one,
    "order_book": convert_order_book,
    "candles": convert_candles,
    "fundamentals": convert_fundamentals,
}

# Data directory names on disk (may differ from table name)
_DATA_DIRS: dict[str, str] = {
    "level_one_equities": "level_one",
    "level_one_futures": "level_one",
}


# ---------------------------------------------------------------------------
# File discovery for old parquets
# ---------------------------------------------------------------------------


def _discover_old_parquets(
    data_root: Path,
    table_name: str,
    provider: str | None = None,
    after: date | None = None,
    before: date | None = None,
) -> list[tuple[Path, str, str, date]]:
    """Find old parquet files. Returns (path, provider, root, sample_date)."""
    dir_name = _DATA_DIRS.get(table_name, table_name)
    base = data_root / dir_name

    if not base.is_dir():
        return []

    providers = [provider] if provider else [d.name for d in sorted(base.iterdir()) if d.is_dir()]

    results: list[tuple[Path, str, str, date]] = []

    for prov in providers:
        prov_dir = base / prov
        if not prov_dir.is_dir():
            continue

        for parquet_path in sorted(prov_dir.rglob("*.parquet")):
            parsed = _parse_old_path(parquet_path, prov, table_name, data_root)
            if parsed is None:
                continue
            _, root, sample_dt = parsed
            if after and sample_dt < after:
                continue
            if before and sample_dt >= before:
                continue

            # For level_one, filter equities vs futures by symbol prefix
            if table_name == "level_one_equities" and root.startswith("^"):
                continue
            if table_name == "level_one_futures" and not root.startswith("^"):
                continue

            results.append((parquet_path, prov, root, sample_dt))

    return results


def _parse_old_path(
    path: Path, provider: str, table_name: str, data_root: Path
) -> tuple[str, str, date] | None:
    """Parse an old parquet path into (provider, root, sample_date)."""
    if table_name == "options":
        m = _OPTIONS_FILENAME_RE.match(path.name)
        if not m:
            return None
        root = m.group(1)
        try:
            sample_dt = date.fromisoformat(m.group(2))
        except ValueError:
            return None
        return provider, root, sample_dt

    if table_name in ("level_one_equities", "level_one_futures", "order_book"):
        # Path: provider/YYYY/MM/DD/{SYMBOL}_{HHMMSSZ}.parquet
        m = _SNAPSHOT_FILENAME_RE.match(path.name)
        root = m.group(1) if m else path.stem
        # Extract date from directory
        rel = path.relative_to(data_root / _DATA_DIRS.get(table_name, table_name) / provider)
        parts = rel.parent.parts
        if len(parts) >= 3:
            try:
                sample_dt = date(int(parts[0]), int(parts[1]), int(parts[2]))
            except ValueError:
                return None
            return provider, root, sample_dt
        return None

    if table_name == "candles":
        # Path: provider/frequency/[year/]SYMBOL.parquet
        rel = path.relative_to(data_root / "candles" / provider)
        parts = rel.parts
        if len(parts) >= 2:
            root = path.stem
            # Candles don't have a meaningful sample_date, use epoch
            return provider, root, date(1970, 1, 1)
        return None

    if table_name == "fundamentals":
        # Path: provider/YYYY/MM/DD.parquet
        rel = path.relative_to(data_root / "fundamentals" / provider)
        parts = rel.parts
        if len(parts) >= 3:
            try:
                sample_dt = date(int(parts[0]), int(parts[1]), int(path.stem))
            except ValueError:
                return None
            return provider, "all", sample_dt
        return None

    return None


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def _candle_metadata(path: Path, data_root: Path, provider: str) -> tuple[str | None, str | None]:
    """Extract (symbol, interval) for candles from file path."""
    rel = path.relative_to(data_root / "candles" / provider)
    parts = rel.parts
    if len(parts) >= 2:
        return path.stem, parts[0]
    return None, None


# ---------------------------------------------------------------------------
# Main migration logic
# ---------------------------------------------------------------------------


def migrate(
    table_name: str,
    data_root: Path,
    iceberg_bucket: str,
    glue_database: str,
    aws_region: str,
    aws_profile: str | None,
    provider: str | None = None,
    after: date | None = None,
    before: date | None = None,
    limit: int | None = None,
    dry_run: bool = False,
) -> None:
    if table_name not in TABLE_DEFS:
        logger.error("Unknown table: %s", table_name)
        sys.exit(1)

    converter = _CONVERTERS[table_name]
    table_def = TABLE_DEFS[table_name]

    logger.info("Discovering old parquets for %s...", table_name)
    files = _discover_old_parquets(
        data_root, table_name, provider=provider, after=after, before=before
    )
    logger.info("Found %d files", len(files))

    if limit:
        files = files[:limit]

    if dry_run:
        for path, prov, root, sample_dt in files:
            logger.info(
                "  [dry-run] %s (provider=%s, root=%s, date=%s)", path.name, prov, root, sample_dt
            )
        logger.info("Dry run complete. %d files would be migrated.", len(files))
        return

    # Connect to ledger + Iceberg
    ledger = Ledger(DATABASE_URL)
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
    iceberg_table = catalog.load_table(f"{glue_database}.{table_name}")

    committed = 0
    skipped = 0
    errors = 0
    start = time.monotonic()

    try:
        for path, prov, root, sample_dt in files:
            # Build a relative path key for the ledger
            rel_key = _ledger_key(path, data_root, table_name, prov)

            # Skip if already committed
            existing = ledger.get(rel_key, table_name)
            if existing and existing.status == "committed":
                skipped += 1
                continue

            try:
                arrow_table = pq.read_table(path)
                converted = converter(arrow_table)

                # Derive metadata for normalizers that need it
                kwargs: dict[str, object] = {"sample_date": sample_dt}
                if table_name == "candles":
                    symbol, interval = _candle_metadata(path, data_root, prov)
                    kwargs["symbol"] = symbol
                    kwargs["interval"] = interval

                normalized, diagnostics = normalize_table(converted, table_name, **kwargs)

                # Pre-sort
                normalized = _sort_table(normalized, table_def.sort_keys)

                # Append to Iceberg
                iceberg_table.append(normalized)

                # Record in ledger
                sha = _sha256_file(path)
                size = path.stat().st_size
                ledger.record_discovered(
                    path=rel_key,
                    table_name=table_name,
                    sha256=sha,
                    size_bytes=size,
                    provider=prov,
                    root=root,
                    sample_date=sample_dt.isoformat(),
                    file_format="parquet",
                )
                ledger.mark_committed([rel_key], table_name)
                committed += 1

                if committed % 100 == 0:
                    elapsed = time.monotonic() - start
                    logger.info(
                        "Progress: %d committed, %d skipped, %d errors (%.1fs)",
                        committed,
                        skipped,
                        errors,
                        elapsed,
                    )

            except Exception:
                logger.exception("Failed to migrate %s", path)
                errors += 1

    finally:
        ledger.close()

    elapsed = time.monotonic() - start
    logger.info(
        "Migration complete: %d committed, %d skipped, %d errors (%.1fs)",
        committed,
        skipped,
        errors,
        elapsed,
    )


def _ledger_key(path: Path, data_root: Path, table_name: str, provider: str) -> str:
    """Build a ledger-compatible relative path key."""
    dir_name = _DATA_DIRS.get(table_name, table_name)
    base = data_root / dir_name
    return str(path.relative_to(base))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Migrate old tickrake parquets to Iceberg lakehouse"
    )
    parser.add_argument("--table", required=True, choices=sorted(_CONVERTERS.keys()))
    parser.add_argument("--provider", default=None, help="Filter to specific provider")
    parser.add_argument("--after", default=None, help="sample_date >= YYYY-MM-DD")
    parser.add_argument("--before", default=None, help="sample_date < YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=None, help="Max files to process")
    parser.add_argument("--dry-run", action="store_true", help="List files without migrating")
    parser.add_argument("--data-root", default=str(Path.home() / ".tickrake" / "data"))
    parser.add_argument("--iceberg-bucket", default=None, help="S3 bucket (or set ICEBERG_BUCKET)")
    parser.add_argument("--glue-database", default="tractatus")
    parser.add_argument("--aws-region", default="us-east-1")
    parser.add_argument("--aws-profile", default=None)

    args = parser.parse_args()

    iceberg_bucket = args.iceberg_bucket or os.environ.get("ICEBERG_BUCKET")
    aws_profile = args.aws_profile or os.environ.get("AWS_PROFILE")
    if not iceberg_bucket and not args.dry_run:
        parser.error("--iceberg-bucket or ICEBERG_BUCKET env var required")

    parsed_after = date.fromisoformat(args.after) if args.after else None
    parsed_before = date.fromisoformat(args.before) if args.before else None

    migrate(
        table_name=args.table,
        data_root=Path(args.data_root),
        iceberg_bucket=iceberg_bucket or "",
        glue_database=args.glue_database,
        aws_region=args.aws_region,
        aws_profile=aws_profile,
        provider=args.provider,
        after=parsed_after,
        before=parsed_before,
        limit=args.limit,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
