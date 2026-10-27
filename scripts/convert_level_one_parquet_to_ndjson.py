"""Convert old-format tickrake level_one parquet files to NDJSON.

Old format: multiple time-stamped parquet files per symbol per day
  ~/.tickrake/data/level_one/schwab/2026/MM/DD/{SYMBOL}_{HHMMSSz}.parquet
  Schema: received_at_ms (int64 ms), symbol, service, quote_time_ms (int64 ms),
          trade_time_ms (int64 ms), bid, ask, last, bid_size, ask_size,
          volume, mark, extra_json

New format: one NDJSON file per symbol per day
  ~/.tickrake/data/level_one/schwab/2026/MM/DD/{SYMBOL}.ndjson
  Each line: {"job_type": "level_one", "provider": "schwab", "symbol": ...,
              "service": ..., "received_at": "YYYY-MM-DDTHH:MM:SSZ",
              "quote_time": "...", "trade_time": "...", "bid": ..., ...}

Oct 19+ NDJSON files are already in the correct format — only convert parquet files
(Sep 18 – Oct 16).

Usage:
    uv run python scripts/convert_level_one_parquet_to_ndjson.py
    uv run python scripts/convert_level_one_parquet_to_ndjson.py --dry-run
    uv run python scripts/convert_level_one_parquet_to_ndjson.py --date 2026-09-18

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


def ms_to_iso(ms: int | None) -> str | None:
    """Convert millisecond epoch to ISO8601 UTC string (seconds precision)."""
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def convert_day(day_dir: Path, dry_run: bool) -> tuple[int, int]:
    """Convert all parquet files in a day directory to per-symbol NDJSON.

    Returns (symbols_written, rows_written).
    """
    parquet_files = sorted(day_dir.glob("*.parquet"))
    if not parquet_files:
        return 0, 0

    # Group parquet files by symbol — timestamp suffix is 6 digits followed by Z
    by_symbol: dict[str, list[Path]] = defaultdict(list)
    for f in parquet_files:
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
                    "job_type": "level_one",
                    "provider": "schwab",
                    "symbol": d["symbol"][i],
                    "service": d["service"][i],
                    "received_at": ms_to_iso(d["received_at_ms"][i]),
                    "quote_time": ms_to_iso(d["quote_time_ms"][i]),
                    "trade_time": ms_to_iso(d["trade_time_ms"][i]),
                    "bid": d["bid"][i],
                    "ask": d["ask"][i],
                    "last": d["last"][i],
                    "mark": d["mark"][i],
                    "bid_size": d["bid_size"][i],
                    "ask_size": d["ask_size"][i],
                    "volume": d["volume"][i],
                    "extra_json": d["extra_json"][i],
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
        default=str(Path.home() / ".tickrake" / "data" / "level_one" / "schwab" / "2026"),
        help="Root directory to scan (default: ~/.tickrake/data/level_one/schwab/2026).",
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
