"""Convert old-format tickrake order book parquet files to NDJSON.

Old format: multiple time-stamped parquet files per symbol per day
  ~/.tickrake/data/order_book/schwab/2026/MM/DD/{SYMBOL}_{HHMMSSz}.parquet
  Schema: received_at_ms (int64 ms), symbol, service, book_time_ms (int64 ms),
          bids_json, asks_json

New format: one NDJSON file per symbol per day
  ~/.tickrake/data/order_book/schwab/2026/MM/DD/{SYMBOL}.ndjson
  Each line: {"job_type": "order_book", "provider": "schwab", "symbol": ...,
              "service": ..., "received_at": "YYYY-MM-DDTHH:MM:SSZ",
              "book_time": "YYYY-MM-DDTHH:MM:SSZ", "bids_json": ..., "asks_json": ...}

Usage:
    uv run python scripts/convert_order_book_parquet_to_ndjson.py
    uv run python scripts/convert_order_book_parquet_to_ndjson.py --dry-run
    uv run python scripts/convert_order_book_parquet_to_ndjson.py --date 2026-09-18

Parquet files are not deleted — remove them manually after verifying the output.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq


def ms_to_iso(ms: int) -> str:
    """Convert millisecond epoch to ISO8601 UTC string (seconds precision)."""
    return datetime.fromtimestamp(ms / 1000.0, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def convert_day(day_dir: Path, dry_run: bool) -> tuple[int, int]:
    """Convert all parquet files in a day directory to per-symbol NDJSON.

    Returns (symbols_written, rows_written).
    """
    parquet_files = sorted(day_dir.glob("*.parquet"))
    if not parquet_files:
        return 0, 0

    # Group parquet files by symbol (prefix before first underscore+timestamp)
    by_symbol: dict[str, list[Path]] = defaultdict(list)
    for f in parquet_files:
        # Filename: {SYMBOL}_{HHMMSSz}.parquet — symbol may contain underscores
        # but the timestamp suffix is always 6 digits followed by Z
        match = re.match(r"^(.+)_\d{6}Z\.parquet$", f.name)
        if not match:
            print(f"  WARNING: skipping unexpected filename: {f.name}", file=sys.stderr)
            continue
        symbol = match.group(1)
        by_symbol[symbol].append(f)

    symbols_written = 0
    rows_written = 0

    for symbol, files in sorted(by_symbol.items()):
        ndjson_path = day_dir / f"{symbol}.ndjson"
        if ndjson_path.exists():
            print(f"  SKIP {ndjson_path} (already exists)")
            continue

        rows: list[str] = []
        for parquet_file in files:
            table = pq.read_table(parquet_file)
            d = table.to_pydict()
            for i in range(len(d["symbol"])):
                record = {
                    "job_type": "order_book",
                    "provider": "schwab",
                    "symbol": d["symbol"][i],
                    "service": d["service"][i],
                    "received_at": ms_to_iso(d["received_at_ms"][i]),
                    "book_time": ms_to_iso(d["book_time_ms"][i]),
                    "bids_json": d["bids_json"][i],
                    "asks_json": d["asks_json"][i],
                }
                rows.append(json.dumps(record, separators=(",", ":")))

        if not rows:
            continue

        if dry_run:
            print(f"  DRY RUN: would write {len(rows)} rows to {ndjson_path}")
        else:
            ndjson_path.write_text("\n".join(rows) + "\n")
            print(f"  Wrote {len(rows)} rows to {ndjson_path}")

        symbols_written += 1
        rows_written += len(rows)

    return symbols_written, rows_written


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print what would be written without writing."
    )
    parser.add_argument(
        "--date", help="Process only this date (YYYY-MM-DD). Defaults to all dates."
    )
    parser.add_argument(
        "--data-root",
        default=str(Path.home() / ".tickrake" / "data" / "order_book" / "schwab" / "2026"),
        help="Root directory to scan (default: ~/.tickrake/data/order_book/schwab/2026).",
    )
    args = parser.parse_args()

    root = Path(args.data_root)
    if not root.exists():
        print(f"error: data root not found: {root}", file=sys.stderr)
        sys.exit(1)

    if args.date:
        parts = args.date.split("-")
        day_dirs = [root / parts[1] / parts[2]]
    else:
        day_dirs = sorted(root.glob("*/*"))

    total_symbols = 0
    total_rows = 0

    for day_dir in day_dirs:
        if not day_dir.is_dir():
            continue
        # Reconstruct date from path for display
        date_str = f"2026-{day_dir.parent.name}-{day_dir.name}"
        print(f"{date_str}:")
        symbols, rows = convert_day(day_dir, dry_run=args.dry_run)
        if symbols == 0:
            print("  (no parquet files or all already converted)")
        total_symbols += symbols
        total_rows += rows

    print(f"\nDone: {total_symbols} symbol files, {total_rows} rows total.")
    if args.dry_run:
        print("(dry run — nothing written)")


if __name__ == "__main__":
    main()
