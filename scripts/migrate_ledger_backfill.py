#!/usr/bin/env python3
"""Backfill the postgres ledger for files already published to the lakehouse.

Scans the prepared migration directory and records each file as committed
in the ledger. This is a one-time fix for migration files that were published
without ledger tracking.

Usage:
    python scripts/migrate_ledger_backfill.py --input /tmp/migrate
    python scripts/migrate_ledger_backfill.py --input /tmp/migrate --table options --dry-run
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import time
from pathlib import Path

from tractatus.catalog.ledger import Ledger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("migrate_ledger_backfill")

DATABASE_URL = "postgresql://tractatus_user:tractatus@127.0.0.1:5432/tractatus"

# All table names we migrated
ALL_TABLES = [
    "candles",
    "fundamentals",
    "level_one_equities",
    "level_one_futures",
    "options",
    "order_book",
]


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def _parse_prepared_path(
    parquet_path: Path, table_dir: Path, table_name: str
) -> tuple[str, str, str] | None:
    """Extract (provider, root, sample_date) from a prepared parquet path."""
    rel = parquet_path.relative_to(table_dir)
    parts = rel.parts

    if table_name == "options":
        # provider/YYYY/MM/DD/ROOT_samples_YYYY-MM-DD.parquet
        if len(parts) >= 5:
            provider = parts[0]
            root = parquet_path.stem.replace(f"_samples_{parts[1]}-{parts[2]}-{parts[3]}", "")
            # Extract root more carefully
            name = parquet_path.stem
            if "_samples_" in name:
                root = name.split("_samples_")[0]
            sample_date = f"{parts[1]}-{parts[2]}-{parts[3]}"
            return provider, root, sample_date
        return None

    if table_name in ("level_one_equities", "level_one_futures", "order_book"):
        # provider/YYYY/MM/DD/SYMBOL.parquet
        if len(parts) >= 5:
            provider = parts[0]
            root = parquet_path.stem
            sample_date = f"{parts[1]}-{parts[2]}-{parts[3]}"
            return provider, root, sample_date
        return None

    if table_name == "candles":
        # provider/frequency/SYMBOL.parquet
        if len(parts) >= 3:
            provider = parts[0]
            root = parquet_path.stem
            return provider, root, "1970-01-01"
        return None

    if table_name == "fundamentals":
        # provider/YYYY/MM/DD.parquet
        if len(parts) >= 4:
            provider = parts[0]
            sample_date = f"{parts[1]}-{parts[2]}-{parquet_path.stem}"
            return provider, "all", sample_date
        return None

    return None


def backfill(
    input_dir: Path,
    tables: list[str],
    dry_run: bool = False,
) -> None:
    ledger = Ledger(DATABASE_URL)
    total_recorded = 0
    total_skipped = 0

    try:
        for table_name in tables:
            table_dir = input_dir / table_name
            if not table_dir.is_dir():
                logger.info("No directory for %s, skipping", table_name)
                continue

            parquet_files = sorted(table_dir.rglob("*.parquet"))
            logger.info("Found %d prepared files for %s", len(parquet_files), table_name)

            recorded = 0
            skipped = 0
            start = time.monotonic()

            batch_paths: list[str] = []

            for p in parquet_files:
                parsed = _parse_prepared_path(p, table_dir, table_name)
                if parsed is None:
                    logger.warning("Could not parse: %s", p)
                    continue

                provider, root, sample_date = parsed
                # Use relative path from table_dir as the ledger key
                rel_key = str(p.relative_to(table_dir))

                if dry_run:
                    logger.info(
                        "  [dry-run] %s (provider=%s, root=%s, date=%s)",
                        rel_key,
                        provider,
                        root,
                        sample_date,
                    )
                    recorded += 1
                    continue

                # Check if already recorded
                existing = ledger.get(rel_key, table_name)
                if existing and existing.status == "committed":
                    skipped += 1
                    continue

                sha = _sha256_file(p)
                size = p.stat().st_size

                ledger.record_discovered(
                    path=rel_key,
                    table_name=table_name,
                    sha256=sha,
                    size_bytes=size,
                    provider=provider,
                    root=root,
                    sample_date=sample_date,
                    file_format="parquet",
                )
                batch_paths.append(rel_key)
                recorded += 1

                # Commit in batches
                if len(batch_paths) >= 500:
                    ledger.mark_committed(batch_paths, table_name)
                    batch_paths = []

                if recorded % 1000 == 0:
                    elapsed = time.monotonic() - start
                    logger.info(
                        "  %s: %d recorded, %d skipped (%.1fs)",
                        table_name,
                        recorded,
                        skipped,
                        elapsed,
                    )

            # Final batch
            if batch_paths:
                ledger.mark_committed(batch_paths, table_name)

            elapsed = time.monotonic() - start
            logger.info(
                "%s: %d recorded, %d skipped (%.1fs)",
                table_name,
                recorded,
                skipped,
                elapsed,
            )
            total_recorded += recorded
            total_skipped += skipped

    finally:
        ledger.close()

    logger.info("Backfill complete: %d total recorded, %d skipped", total_recorded, total_skipped)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill ledger from prepared migration files")
    parser.add_argument(
        "-i", "--input", required=True, help="Root migration directory (e.g. /tmp/migrate)"
    )
    parser.add_argument("--table", default=None, help="Backfill a specific table (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="List files without recording")

    args = parser.parse_args()

    tables = [args.table] if args.table else ALL_TABLES

    backfill(
        input_dir=Path(args.input),
        tables=tables,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
